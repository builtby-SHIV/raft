package raft

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"sync"
	"time"

	googleproto "google.golang.org/protobuf/proto"
	"raft/proto"
)

type Transport interface {
	RequestVote(
		ctx context.Context, 
		id string, 
		a *proto.RequestVoteArgs,
	) (proto.RequestVoteReply, error)
	AppendEntries(
		ctx context.Context, 
		id string, 
		args *proto.AppendEntriesArgs,
	) (proto.AppendEntriesReply, error)
	BroadcastAppendEntries(args *proto.AppendEntriesArgs)
	ListenLoop(addr string)
	// Consume() <-chan rpcEvent
	Close() error
}

// NOTE: remember to make tcp port as key fo Peers
type TCPTransport struct {
	HTTPAddr	string
	TCPAddr		string
	Peers		map[string]*peerConn
	ctx			context.Context
	cancel		context.CancelFunc
	mu			sync.RWMutex
}

type peerConn struct {
	conn	net.Conn
	mu		sync.RWMutex
}

func NewTCPTransport(HTTPAddr, TCPAddr string, ctx context.Context) *TCPTransport {
	newCtx, cancel := context.WithCancel(ctx)
	return &TCPTransport{
		HTTPAddr: HTTPAddr,
		TCPAddr:  TCPAddr,
		Peers:    make(map[string]*peerConn),
		ctx:      newCtx,
		cancel:   cancel,
	}
}

func (t *TCPTransport) Add(addr string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.Peers == nil {
		t.Peers = make(map[string]*peerConn)
	}
	t.Peers[addr] = &peerConn{}
}

func (t *TCPTransport) ListenLoop(addr string, rpCh chan *RPCEvent) {
	var(
		listener net.Listener
		lc net.ListenConfig
		conn net.Conn
		err error
	)
	
	if listener, err = lc.Listen(t.ctx, "tcp", addr); err != nil {
		log.Fatalf("unable to start tcp connection on port %v, %v", addr, err)
	}
	go func(ln net.Listener) { 
		<-t.ctx.Done(); 
		ln.Close() 
	}(listener)

	backoff := 50 * time.Millisecond
	for {
		conn, err = listener.Accept()
		if err != nil {
			if errors.Is(err, net.ErrClosed) { 
				return 
			}
			select {
			case <-time.After(backoff):
				fmt.Printf("Error in listening to incoming message %v\n", err)
				if backoff < 500 * time.Millisecond {
					backoff *= 2
				}
				continue
			case <-t.ctx.Done():
				return
			}
		} 
		go handleIncoming(conn, t.ctx, rpCh)
	}

}

func handleIncoming(conn net.Conn, ctx context.Context, rpCh chan *RPCEvent) {
	defer conn.Close()
	for {
		msgType, msg, err := DecodeAndReadMessage(conn)
		if err != nil {
			if !errors.Is(err, io.EOF) && !errors.Is(err, net.ErrClosed) {
				fmt.Printf("Error in reading the incoming message %v\n", err)
			}
			return
		}

		sendBack := make(chan *SendBack)

		select {
		case rpCh <- &RPCEvent{msgType: msgType, msgRecv: msg, sendBack: sendBack}:
		case <-ctx.Done():
			return
		}

		select {
		case data := <-sendBack:
			if data != nil && data.msgSent != nil {
				if err := EncodeAndWriteMessage(conn, data.msgType, data.msgSent); err != nil {
					fmt.Printf("Error in encoding the message %v\n", err)
					return
				}
			}
		case <-ctx.Done():
			return
		}
	}
}

func (t *TCPTransport) sendToPeers(
	addr string, 
	msgType byte,
	args any,
	replyCh chan *RPCEvent,
) {
	done := make(chan struct{})
	defer close(done)
	go func(ctx context.Context) {
		select {
		case <-t.ctx.Done():
		case <-done:
		}
	}(t.ctx)

	var (
		reply googleproto.Message
		err   error
	)

	switch msgType {
	case TypeAppendEntries:
		if a, ok := args.(*proto.AppendEntriesArgs); ok {
			reply, err = t.AppendEntries(addr, a)
		} else {
			err = fmt.Errorf("expected *proto.AppendEntriesArgs, got %T", args)
		}
	case TypeInstallSnapShot:
		if a, ok := args.(*proto.InstallSnapShotArgs); ok {
			reply, err = t.sendInstallSnapshots(addr, a)
		} else {
			err = fmt.Errorf("expected *proto.InstallSnapShotArgs, got %T", args)
		}
	default:
		err = fmt.Errorf("unknown msgType: %d", msgType)
	}

	if err != nil {
		fmt.Printf("Error in sending or receiving message %v\n", err)
	}

	sendBack := make(chan *SendBack)

	var msgSent googleproto.Message
	if m, ok := args.(googleproto.Message); ok {
		msgSent = m
	}

	select {
	case replyCh <- &RPCEvent{msgType: msgType, msgRecv: reply, msgSent: msgSent, peer: addr, sendBack: sendBack}:
	case <-t.ctx.Done():
		return
	}

	go func() {
		for {
			select {
			case data, ok := <-sendBack:
				if !ok || data == nil {
					return
				}
				switch data.msgType {
				case TypeAppendEntries, TypeInstallSnapShot:
					go t.sendToPeers(addr, data.msgType, data.msgSent, replyCh)
				}
			case <-t.ctx.Done():
				return
			case <-done:
				return
			}
		}
	}()
}

func (t *TCPTransport) AppendEntries(
	addr string, 
	args *proto.AppendEntriesArgs,
) (*proto.AppendEntriesReply, error) {
	t.mu.RLock()
	peer, exists := t.Peers[addr]
	t.mu.RUnlock() 
	if !exists {
		return &proto.AppendEntriesReply{}, fmt.Errorf("peer %s not found", addr)
	}

	done := make(chan struct{})
	defer close(done)
	go func(ctx context.Context) {
		select {
		case <-t.ctx.Done():
			if peer.conn != nil {
				peer.conn.Close()
				peer.conn = nil
			}
		case <-done:
		}
	}(t.ctx)

	var(
		conn net.Conn
		err error
	)

	if peer.conn == nil {
		backoff := 50 * time.Millisecond
		for {
			dialer := &net.Dialer{}
			conn, err = dialer.DialContext(t.ctx, "tcp", addr)
			if err == nil {
				break
			}

			select {
			case <-time.After(backoff):
				if backoff < 500 * time.Millisecond {
					backoff *= 2
				}
			case <-t.ctx.Done():
				return nil, nil
			}
		}
		peer.mu.Lock()
		peer.conn = conn
		peer.mu.Unlock()
	}

	if err := EncodeAndWriteMessage(peer.conn, TypeAppendEntries, args); err != nil {
		return &proto.AppendEntriesReply{}, err
	}

	_, decodedMsg, err := DecodeAndReadMessage(peer.conn)
	if err != nil {
		return &proto.AppendEntriesReply{}, fmt.Errorf("Error in decoding the message %w", err)
	}
	
	return decodedMsg.(*proto.AppendEntriesReply), nil
}

func (t *TCPTransport) SendInstallSnapshots(
	addr string, 
	args *proto.InstallSnapShotArgs,
) (*proto.AppendEntriesReply, error) {
	return t.sendInstallSnapshots(addr, args)
}

func (t *TCPTransport) sendInstallSnapshots(
	addr string, 
	args *proto.InstallSnapShotArgs,
) (*proto.AppendEntriesReply, error) {
	t.mu.RLock()
	peer, exists := t.Peers[addr]
	t.mu.RUnlock() 
	if !exists {
		return &proto.AppendEntriesReply{}, fmt.Errorf("peer %s not found", addr)
	}

	done := make(chan struct{})
	defer close(done)
	go func(ctx context.Context) {
		select {
		case <-t.ctx.Done():
			if peer.conn != nil {
				peer.conn.Close()
				peer.conn = nil
			}
		case <-done:
		}
	}(t.ctx)

	var(
		conn net.Conn
		err error
	)

	if peer.conn == nil {
		backoff := 50 * time.Millisecond
		for {
			dialer := &net.Dialer{}
			conn, err = dialer.DialContext(t.ctx, "tcp", addr)
			if err == nil {
				break
			}

			select {
			case <-time.After(backoff):
				if backoff < 500 * time.Millisecond {
					backoff *= 2
				}
			case <-t.ctx.Done():
				return nil, nil
			}
		}
		peer.mu.Lock()
		peer.conn = conn
		peer.mu.Unlock()
	}

	if err := EncodeAndWriteMessage(peer.conn, TypeInstallSnapShot, args); err != nil {
		return &proto.AppendEntriesReply{}, err
	}

	_, decodedMsg, err := DecodeAndReadMessage(peer.conn)
	if err != nil {
		return &proto.AppendEntriesReply{}, fmt.Errorf("Error in decoding the message %w", err)
	}
	
	return decodedMsg.(*proto.AppendEntriesReply), nil
}

func (t *TCPTransport) sendToRequestVote(
	addr string, 
	msgType byte,
	args *proto.RequestVoteArgs,
	replyCh chan *RPCEvent,
) {
	done := make(chan struct{})
	defer close(done)
	go func(ctx context.Context) {
		select {
		case <-t.ctx.Done():
		case <-done:
		}
	}(t.ctx)

	reply, err := t.RequestVote(addr, args)
	if err != nil {
		fmt.Printf("Error in sending or receiving message %v\n", err)
	}

	sendBack := make(chan *SendBack)

	select {
	case replyCh <- &RPCEvent{msgType: msgType, msgRecv: reply, msgSent: args, peer: addr, sendBack: sendBack}:
	case <-t.ctx.Done():
	}
}

func (t *TCPTransport) RequestVote(
	addr string, 
	args *proto.RequestVoteArgs,
) (*proto.RequestVoteReply, error) {
	t.mu.RLock()
	peer, exists := t.Peers[addr]
	t.mu.RUnlock() 
	if !exists {
		return &proto.RequestVoteReply{}, fmt.Errorf("peer %s not found", addr)
	}

	done := make(chan struct{})
	defer close(done)
	go func(ctx context.Context) {
		select {
		case <-t.ctx.Done():
			if peer.conn != nil {
				peer.conn.Close()
				peer.conn = nil
			}
		case <-done:
		}
	}(t.ctx)

	var(
		conn net.Conn
		err error
	)

	if peer.conn == nil {
		backoff := 50 * time.Millisecond
		for {
			dialer := &net.Dialer{}
			conn, err = dialer.DialContext(t.ctx, "tcp", addr)
			if err == nil {
				break
			}

			select {
			case <-time.After(backoff):
				if backoff < 500 * time.Millisecond {
					backoff *= 2
				}
			case <-t.ctx.Done():
				return nil, nil
			}
		}
		peer.mu.Lock()
		peer.conn = conn
		peer.mu.Unlock()
	}

	if err := EncodeAndWriteMessage(peer.conn, TypeRequestVote, args); err != nil {
		return &proto.RequestVoteReply{}, err
	}

	_, decodedMsg, err := DecodeAndReadMessage(peer.conn)
	if err != nil {
		return &proto.RequestVoteReply{}, err
	}
	
	return decodedMsg.(*proto.RequestVoteReply), nil
}