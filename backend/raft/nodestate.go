package raft

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"raft/proto"
	"raft/storage"

	googleproto "google.golang.org/protobuf/proto"
)

type RPCEvent struct {
	peer     string
	msgType  byte
	sendBack chan *SendBack
	msgRecv  googleproto.Message
	msgSent  googleproto.Message
}

type SendBack struct {
	msgType byte
	msgSent googleproto.Message
}

type ClientCmd struct {
	Command    proto.Command
	Key, Value string
	Committed  bool
	Reply      chan error
}

type LeaderState struct {
	nextIndex  map[string]uint64
	matchIndex map[string]uint64
	inflight   map[string]bool
	pending    map[uint64]chan error // client waiting on log index N
	snapshots  map[string]*snapshotSendState
	mu         sync.RWMutex
}

type snapshotSendState struct {
	lastIncludedIndex uint64
	lastIncludedTerm  uint64
	offset            uint64
	totalSize         uint64
	sentDone          bool
}

type snapShotMetaData struct {
	lastIncludedTerm, lastIncludedIndex uint64
}

type appendEntry struct {
	data  *proto.KVPair
	index uint64
	term  uint64
}

type NodeState struct {
	ID                 uint64
	Role               string
	CommitIndex        uint64
	statePath          string
	tp                 *TCPTransport
	ls                 *LeaderState
	replyCh            chan *RPCEvent  // incoming requests from the forwarders
	ClientCh           chan *ClientCmd // client commands
	applyCh            chan []*proto.RaftLog
	stopCh             chan struct{}
	cfgDir             string
	entries            []*appendEntry
	kv                 map[string]string // committed key/value state
	appliedIndex       uint64
	RaftState          *proto.RaftState
	Log                *storage.WAL
	ssMetaData         *snapShotMetaData
	snapshotTempPath   string
	snapshotTempIndex  uint64
	snapshotTempTerm   uint64
	snapshotRecvOffset uint64
	ctx                context.Context
	cancel             context.CancelFunc
	mu                 sync.RWMutex

	instanceID       string
	events           *eventBus
	term             atomic.Uint64
	leaderID         uint64
	lastHeartbeat    time.Time
	electionDeadline time.Time
	electionTimeout  time.Duration
	startedAt        time.Time
}

const (
	RoleLeader         string = "leader"
	RoleFollower       string = "follower"
	RoleCandidate      string = "candidate"
	RoleUnknown        string = "unknown"
	nodeStateFile             = "raft_state.bin"
	WALDirectorySuffix        = "_wal"
	MAX_Entries        uint64 = 50
)

func NewNodeState(
	id uint64,
	httpAddr,
	tcpAddr,
	dir,
	role string,
	ctx context.Context,
	raftState *proto.RaftState) *NodeState {
	wal, err := storage.OpenWAL(dir, true, 128000, 1000)
	if err != nil {
		// cancel()
		return nil
	}

	if raftState == nil {
		raftState = &proto.RaftState{
			Term:     0,
			VotedFor: "",
		}
	}

	newCtx, cancel := context.WithCancel(ctx)
	tp := NewTCPTransport(httpAddr, tcpAddr, newCtx)
	for _, peer := range DefaultPeers {
		if !isSelfAddress(tp.TCPAddr, peer) {
			tp.Add(peer)
		}
	}
	ns := &NodeState{
		ID:            id,
		Role:          role,
		RaftState:     raftState,
		Log:           wal,
		tp:            tp,
		replyCh:       make(chan *RPCEvent, 100),
		ClientCh:      make(chan *ClientCmd, 100),
		stopCh:        make(chan struct{}),
		cfgDir:        dir,
		ctx:           newCtx,
		cancel:        cancel,
		kv:            make(map[string]string),
		ssMetaData:    loadSnapshotMetaData(wal),
		instanceID:    newInstanceID(),
		events:        newEventBus(512),
		startedAt:     time.Now(),
		lastHeartbeat: time.Now(),
	}
	ns.term.Store(raftState.Term)

	_ = ns.HydrateEntries()
	ns.rebuildKV()
	ns.emit("node.started", "", fmt.Sprintf("N%d started as %s", id, role), map[string]any{
		"role": role,
		"term": raftState.Term,
	})

	if role == RoleLeader {
		ns.InitLeaderState()
	}

	return ns
}

func loadSnapshotMetaData(wal *storage.WAL) *snapShotMetaData {
	meta := &snapShotMetaData{}
	if wal == nil {
		return meta
	}
	index, term, err := wal.LatestSnapshotMetadata()
	if err != nil {
		return meta
	}
	meta.lastIncludedIndex = index
	meta.lastIncludedTerm = term
	return meta
}

func (ns *NodeState) refreshSnapshotMetaData() {
	if ns.Log == nil {
		return
	}
	index, term, err := ns.Log.LatestSnapshotMetadata()
	if err != nil {
		return
	}
	if ns.ssMetaData == nil {
		ns.ssMetaData = &snapShotMetaData{}
	}
	if index >= ns.ssMetaData.lastIncludedIndex {
		ns.ssMetaData.lastIncludedIndex = index
		ns.ssMetaData.lastIncludedTerm = term
	}
}

func (ns *NodeState) HydrateEntries() error {
	ns.mu.Lock()
	defer ns.mu.Unlock()

	if ns.Log == nil {
		return nil
	}

	logs, err := ns.Log.ReadAll()
	if err != nil {
		return err
	}

	ns.entries = make([]*appendEntry, 0, len(logs))
	for _, entry := range logs {
		var valPtr *string
		if entry.Value != nil {
			v := string(entry.Value)
			valPtr = &v
		}
		ns.entries = append(ns.entries, &appendEntry{
			data: &proto.KVPair{
				Key:     entry.Key,
				Value:   valPtr,
				Command: entry.Command,
			},
			index: entry.Index,
			term:  entry.Term,
		})
	}
	return nil
}

var (
	ErrNotLeader    = errors.New("node is not the leader")
	ErrShuttingDown = errors.New("node is shutting down")
)

func (ns *NodeState) Apply(ctx context.Context, cmd *ClientCmd) error {
	if cmd == nil {
		return errors.New("nil client command")
	}
	if ctx == nil {
		ctx = context.Background()
	}

	ns.mu.RLock()
	role := ns.Role
	ns.mu.RUnlock()
	if role != RoleLeader {
		return ErrNotLeader
	}

	if cmd.Reply == nil {
		cmd.Reply = make(chan error, 1)
	}

	select {
	case ns.ClientCh <- cmd:
	case <-ctx.Done():
		return ctx.Err()
	case <-ns.ctx.Done():
		return ErrShuttingDown
	}

	select {
	case err := <-cmd.Reply:
		return err
	case <-ctx.Done():
		return ctx.Err()
	case <-ns.ctx.Done():
		return ErrShuttingDown
	}
}

func (ns *NodeState) Get(key string) (string, bool) {
	ns.mu.RLock()
	defer ns.mu.RUnlock()
	if ns.kv == nil {
		return "", false
	}
	v, ok := ns.kv[key]
	return v, ok
}

func (ns *NodeState) Start() {
	go ns.tp.ListenLoop(ns.tp.TCPAddr, ns.replyCh)
	for {
		select {
		case <-ns.ctx.Done():
			ns.failPending(ErrShuttingDown)
			return
		default:
		}

		switch ns.Role {
		case RoleLeader:
			ns.Role = ns.runLeader()
		case RoleFollower:
			ns.Role = ns.runFollower()
		case RoleCandidate:
			ns.Role = ns.runCandidate()
		default:
			ns.Role = ns.runFollower()
		}
	}
}

func (ns *NodeState) Close() error {
	ns.failPending(ErrShuttingDown)
	if ns.cancel != nil {
		ns.cancel()
	}
	if ns.Log != nil {
		return ns.Log.Close()
	}
	return nil
}

func (ns *NodeState) rebuildKV() {
	ns.mu.Lock()
	defer ns.mu.Unlock()
	if ns.kv == nil {
		ns.kv = make(map[string]string)
	}
	for _, e := range ns.entries {
		ns.applyEntryLocked(e)
		if e.index > ns.appliedIndex {
			ns.appliedIndex = e.index
		}
	}
}

func (ns *NodeState) applyEntryLocked(e *appendEntry) {
	if e == nil || e.data == nil {
		return
	}
	switch e.data.Command {
	case proto.Command_PUT:
		value := ""
		if e.data.Value != nil {
			value = *e.data.Value
		}
		ns.kv[e.data.Key] = value
	case proto.Command_DELETE:
		delete(ns.kv, e.data.Key)
	}
}

func (ns *NodeState) applyCommittedLocked() {
	if ns.kv == nil {
		ns.kv = make(map[string]string)
	}
	for _, e := range ns.entries {
		if e.index <= ns.appliedIndex || e.index > ns.CommitIndex {
			continue
		}
		ns.applyEntryLocked(e)
		ns.appliedIndex = e.index
	}
	if ns.ls == nil {
		return
	}
	for index, reply := range ns.ls.pending {
		if index > ns.CommitIndex {
			continue
		}
		select {
		case reply <- nil:
		default:
		}
		delete(ns.ls.pending, index)
	}
}

// failPending answers every in-flight client command with err.
func (ns *NodeState) failPending(err error) {
	ns.mu.Lock()
	defer ns.mu.Unlock()
	if ns.ls == nil {
		return
	}
	for index, reply := range ns.ls.pending {
		select {
		case reply <- err:
		default:
		}
		delete(ns.ls.pending, index)
	}
}

func (ns *NodeState) lastIndex() uint64 {
	ns.mu.RLock()
	defer ns.mu.RUnlock()
	if len(ns.entries) > 0 {
		return ns.entries[len(ns.entries)-1].index
	}
	if ns.ssMetaData != nil {
		return ns.ssMetaData.lastIncludedIndex
	}
	return 0
}

func (ns *NodeState) termAt(index uint64) (uint64, bool) {
	if index == 0 {
		return 0, true
	}
	ns.mu.RLock()
	defer ns.mu.RUnlock()
	if ns.ssMetaData != nil {
		if index == ns.ssMetaData.lastIncludedIndex {
			return ns.ssMetaData.lastIncludedTerm, true
		}
		if index < ns.ssMetaData.lastIncludedIndex {
			return 0, false
		}
	}
	for _, e := range ns.entries {
		if e.index == index {
			return e.term, true
		}
	}
	return 0, false
}

func (ns *NodeState) advanceCommitIndex() {
	lastIndex := ns.lastIndex()
	if lastIndex <= ns.CommitIndex {
		return
	}
	lastTerm, ok := ns.termAt(lastIndex)
	if !ok || lastTerm != ns.RaftState.Term {
		return
	}

	ns.tp.mu.RLock()
	total := len(ns.tp.Peers) + 1 // peer maps never contain self
	ns.tp.mu.RUnlock()
	quorum := total/2 + 1

	for index := lastIndex; index > ns.CommitIndex; index-- {
		count := 1 
		for peer, matched := range ns.ls.matchIndex {
			if isSelfAddress(ns.tp.TCPAddr, peer) {
				continue
			}
			if matched >= index {
				count++
			}
		}
		if count < quorum {
			continue
		}

		ns.mu.Lock()
		if index > ns.CommitIndex {
			ns.CommitIndex = index
			ns.applyCommittedLocked()
		}
		ns.mu.Unlock()
		return
	}
}

func (ns *NodeState) InitLeaderState() {
	ns.mu.Lock()
	defer ns.mu.Unlock()

	if ns.ls == nil {
		ns.ls = NewLeaderState()
	}

	var lastLogIndex uint64
	if ns.Log != nil {
		_, lastIdx, err := ns.Log.GetLastTermAndIndex()
		if err == nil {
			lastLogIndex = lastIdx
		}
	}
	if lastLogIndex == 0 && len(ns.entries) > 0 {
		lastLogIndex = ns.entries[len(ns.entries)-1].index
	}

	var defaultNextIndex uint64
	if lastLogIndex == 0 {
		defaultNextIndex = 1
	} else {
		defaultNextIndex = lastLogIndex + 1
	}

	ns.tp.mu.RLock()
	defer ns.tp.mu.RUnlock()

	for peerAddr := range ns.tp.Peers {
		if isSelfAddress(ns.tp.TCPAddr, peerAddr) {
			ns.ls.nextIndex[peerAddr] = defaultNextIndex
			ns.ls.matchIndex[peerAddr] = lastLogIndex
		} else {
			ns.ls.nextIndex[peerAddr] = defaultNextIndex
			ns.ls.matchIndex[peerAddr] = 0
		}
	}
	if ns.tp.TCPAddr != "" {
		if _, ok := ns.ls.nextIndex[ns.tp.TCPAddr]; !ok {
			ns.ls.nextIndex[ns.tp.TCPAddr] = defaultNextIndex
			ns.ls.matchIndex[ns.tp.TCPAddr] = lastLogIndex
		}
	}
}

func (ns *NodeState) AddPeer(addr string) {
	ns.tp.Add(addr)
	if ns.ls != nil {
		var lastLogIndex uint64
		if ns.Log != nil {
			_, lastIdx, err := ns.Log.GetLastTermAndIndex()
			if err == nil {
				lastLogIndex = lastIdx
			}
		}
		if lastLogIndex == 0 && len(ns.entries) > 0 {
			lastLogIndex = ns.entries[len(ns.entries)-1].index
		}

		var defaultNextIndex uint64
		if lastLogIndex == 0 {
			defaultNextIndex = 1
		} else {
			defaultNextIndex = lastLogIndex + 1
		}

		ns.ls.mu.Lock()
		ns.ls.nextIndex[addr] = defaultNextIndex
		ns.ls.matchIndex[addr] = 0
		ns.ls.mu.Unlock()
	}
}

func LoadOrInitNodeState(dataDir string) (*NodeState, error) {
	// _, cancel := context.WithCancel(context.Background())

	wal, err := storage.OpenWAL(dataDir, true, 128000, 1000)
	if err != nil {
		// cancel()
		return nil, err
	}

	statePath := filepath.Join(dataDir, nodeStateFile)

	data, err := os.ReadFile(statePath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			ns := &NodeState{
				statePath: statePath,
				RaftState: &proto.RaftState{
					Term:     0,
					VotedFor: "",
				},
				Log:           wal,
				replyCh:       make(chan *RPCEvent, 100),
				ClientCh:      make(chan *ClientCmd, 100),
				cfgDir:        dataDir,
				ssMetaData:    loadSnapshotMetaData(wal),
				instanceID:    newInstanceID(),
				events:        newEventBus(512),
				startedAt:     time.Now(),
				lastHeartbeat: time.Now(),
			}
			ns.term.Store(0)
			if err := ns.Save(); err != nil {
				return nil, fmt.Errorf("creating %s: %w", statePath, err)
			}
			_ = ns.HydrateEntries()
			return ns, nil
		}
		return nil, fmt.Errorf("reading %s: %w", statePath, err)
	}

	var rs proto.RaftState
	proto.MustUnmarshal(data, &rs)

	ns := &NodeState{
		statePath:     statePath,
		RaftState:     &rs,
		Log:           wal,
		replyCh:       make(chan *RPCEvent, 100),
		ClientCh:      make(chan *ClientCmd, 100),
		cfgDir:        dataDir,
		ssMetaData:    loadSnapshotMetaData(wal),
		instanceID:    newInstanceID(),
		events:        newEventBus(512),
		startedAt:     time.Now(),
		lastHeartbeat: time.Now(),
	}
	ns.term.Store(rs.Term)
	_ = ns.HydrateEntries()
	return ns, nil
}

func (ns *NodeState) Save() error {
	ns.mu.RLock()
	defer ns.mu.RUnlock()

	if ns.statePath == "" {
		return nil
	}

	if ns.RaftState == nil {
		ns.RaftState = &proto.RaftState{
			Term:     0,
			VotedFor: "",
		}
	}

	payload := &proto.RaftState{
		Term:     ns.RaftState.Term,
		VotedFor: ns.RaftState.VotedFor,
	}

	data := proto.MustMarshal(payload)

	dir := filepath.Dir(ns.statePath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("creating directory %s: %w", dir, err)
	}

	tmp := ns.statePath + ".tmp"
	if err := os.WriteFile(tmp, data, 0644); err != nil {
		return fmt.Errorf("writing node state: %w", err)
	}
	if err := os.Rename(tmp, ns.statePath); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("committing node state: %w", err)
	}
	return nil
}

func (ns *NodeState) Modify(role string, newRaftState *proto.RaftState) {
	ns.mu.Lock()
	if role != "" {
		ns.Role = role
	}
	ns.RaftState = newRaftState
	ns.mu.Unlock()

	_ = ns.Save()
}

func (ns *NodeState) UpdateRole(role string) error {
	ns.mu.Lock()
	defer ns.mu.Unlock()
	ns.Role = role
	return nil
}

func (ns *NodeState) run() {
	ns.Start()
}

func (ns *NodeState) runCandidate() string {
	select {
	case <-time.After(time.Second):
	case <-ns.ctx.Done():
		return ""
	}
	return RoleFollower
}

func (ns *NodeState) runLeader() string {
	ns.InitLeaderState()

	interval := (time.Duration(float64(ns.ID) * 2.5)) * time.Second
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			ns.formatAndSend(nil)
		case rp := <-ns.replyCh:
			if rp == nil {
				continue
			}
			ns.ls.inflight[rp.peer] = false
			switch rp.msgType {
			case TypeAppendEntriesReply:
				recv, ok := rp.msgRecv.(*proto.AppendEntriesReply)
				if !ok {
					break
				}
				sent, ok := rp.msgSent.(*proto.AppendEntriesArgs)
				if !ok {
					break
				}
				if recv.Term == ns.RaftState.Term {
					if recv.Success {
						matched := sent.PrevLogIndex + uint64(len(sent.Entries))
						if matched > ns.ls.matchIndex[rp.peer] {
							ns.ls.matchIndex[rp.peer] = matched
						}
						ns.ls.nextIndex[rp.peer] = ns.ls.matchIndex[rp.peer] + 1
						ns.advanceCommitIndex()
						if ns.ls.matchIndex[rp.peer] < ns.lastIndex() {
							ns.sendAppendEntries(rp.peer)
						}
					} else if recv.ConflictIndex == 0 {
						// A failed RPC (for example the follower process is
						// down) yields an empty reply. Keep nextIndex as it is
						// so the next heartbeat retries this peer instead of
						// tearing down replication to every peer.
					} else {
						// conflictIndex is the last index in follower's log + 1
						ns.ls.nextIndex[rp.peer] = recv.ConflictIndex
						if err := ns.handleLaggingFollower(recv, rp); err != nil {
							fmt.Printf("Resetting nextIndex for %s: %v\n", rp.peer, err)
							ns.ls.nextIndex[rp.peer] = 1
						}
					}
				} else if recv.Term > ns.RaftState.Term {
					cfg, err := storage.GetProcessConfig(ns.cfgDir)
					if err != nil {
						fmt.Printf("Error in accessing data in config file %v\n", err)
						ns.tp.cancel()
					}
					cfg.Role = RoleFollower
					cfg.CurrentTerm = recv.Term
					if err := storage.OverwriteProcessConfig(ns.cfgDir, cfg); err != nil {
						fmt.Printf("Error in overwriting data in config file %v\n", err)
						ns.tp.cancel()
					}

					ns.failPending(ErrNotLeader)
					ns.tp.cancel()
					ns.Role = RoleFollower
					ns.RaftState.Term = recv.Term
					ns.RaftState.VotedFor = ""
					return RoleFollower
				}
			case TypeInstallSnapShot:
				if recv, ok := rp.msgRecv.(*proto.InstallSnapShotReply); ok && recv.Term > ns.RaftState.Term {
					cfg, err := storage.GetProcessConfig(ns.cfgDir)
					if err != nil {
						fmt.Printf("Error in accessing data in config file %v\n", err)
						ns.tp.cancel()
					}
					cfg.Role = RoleFollower
					cfg.CurrentTerm = recv.Term
					if err := storage.OverwriteProcessConfig(ns.cfgDir, cfg); err != nil {
						fmt.Printf("Error in overwriting data in config file %v\n", err)
						ns.tp.cancel()
					}

					delete(ns.ls.snapshots, rp.peer)
					ns.failPending(ErrNotLeader)
					ns.tp.cancel()
					ns.Role = RoleFollower
					ns.RaftState.Term = recv.Term
					ns.RaftState.VotedFor = ""
					return RoleFollower
				}
				ns.handleInstallSnapshotReply(rp)
			}
		case cmd := <-ns.ClientCh:
			reply := cmd.Reply
			if reply == nil {
				reply = make(chan error, 1)
			}
			var valPtr *string
			var valBytes []byte
			if cmd.Value != "" {
				v := cmd.Value
				valPtr = &v
				valBytes = []byte(cmd.Value)
			}
			entry := &proto.KVPair{
				Term:    ns.RaftState.Term,
				Key:     cmd.Key,
				Value:   valPtr,
				Command: cmd.Command,
			}
			entry.Index = ns.ls.matchIndex[ns.tp.TCPAddr] + 1

			if err := ns.Log.WriteEntry(entry.Index, entry.Command, entry.Key, valBytes, entry.Term); err != nil {
				fmt.Printf("Error writing client command to WAL %v\n", err)
				select {
				case reply <- err:
				default:
				}
				continue
			}

			ns.ls.nextIndex[ns.tp.TCPAddr] = entry.Index + 1
			ns.ls.matchIndex[ns.tp.TCPAddr] = entry.Index
			ns.entries = append(ns.entries, &appendEntry{
				data:  entry,
				term:  entry.Term,
				index: entry.Index,
			})

			if ns.ls.pending == nil {
				ns.ls.pending = make(map[uint64]chan error)
			}
			ns.ls.pending[entry.Index] = reply

			ns.formatAndSend(entry)
		case <-ns.ctx.Done():
			ns.cancel()
			return ""
		}
	}
}

func electionInterval(addr string) time.Duration {
	multiplier := 1.0
	if addr != "" {
		if digit, err := strconv.ParseFloat(addr[len(addr)-1:], 32); err == nil && digit > 0 {
			multiplier = digit
		}
	}
	return time.Duration(multiplier * 2.5 * float64(time.Second))
}

func (ns *NodeState) runFollower() string {
	interval := electionInterval(ns.tp.TCPAddr)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			// start election by becoming candidate
			return RoleCandidate
		case rp := <-ns.replyCh:
			if rp == nil {
				continue
			}
			switch rp.msgType {
			case TypeAppendEntries:
				recv, ok := rp.msgRecv.(*proto.AppendEntriesArgs)
				if !ok || recv == nil {
					if rp.sendBack != nil {
						rp.sendBack <- &SendBack{
							msgType: TypeAppendEntriesReply,
							msgSent: &proto.AppendEntriesReply{Term: ns.RaftState.Term},
						}
					}
					continue
				}

				if recv.Term < ns.RaftState.Term {
					reply := &proto.AppendEntriesReply{
						Term:          ns.RaftState.Term,
						ConflictIndex: 0,
						ConflictTerm:  0,
						Success:       false,
					}
					rp.sendBack <- &SendBack{msgType: TypeAppendEntriesReply, msgSent: reply}
				} else if recv.Term > ns.RaftState.Term {
					cfg, err := storage.GetProcessConfig(ns.cfgDir)
					if err != nil {
						fmt.Printf("Error in accessing data in config file %v\n", err)
						ns.tp.cancel()
					}
					cfg.CurrentTerm = recv.Term
					if err := storage.OverwriteProcessConfig(ns.cfgDir, cfg); err != nil {
						fmt.Printf("Error in overwriting data in config file %v\n", err)
						ns.tp.cancel()
					}

					ticker.Reset(interval)
					ns.RaftState.Term = recv.Term
					reply, err := ns.handleAppendEntries(recv)
					if err != nil {
						fmt.Printf("Error in handling appendentriesargs %v\n", err)
						ns.tp.cancel()
						break
					}
					rp.sendBack <- &SendBack{msgType: TypeAppendEntriesReply, msgSent: reply}
				} else {
					ticker.Reset(interval)
					reply, err := ns.handleAppendEntries(recv)
					if err != nil {
						fmt.Printf("Error in handling appendentriesargs %v\n", err)
						ns.tp.cancel()
						break
					}
					rp.sendBack <- &SendBack{msgType: TypeAppendEntriesReply, msgSent: reply}
				}
			case TypeInstallSnapShot:
				recv, ok := rp.msgRecv.(*proto.InstallSnapShotArgs)
				if !ok || recv == nil {
					if rp.sendBack != nil {
						rp.sendBack <- &SendBack{
							msgType: TypeInstallSnapShotReply,
							msgSent: &proto.InstallSnapShotReply{Term: ns.RaftState.Term},
						}
					}
					continue
				}
				if recv.Term > ns.RaftState.Term {
					cfg, err := storage.GetProcessConfig(ns.cfgDir)
					if err != nil {
						fmt.Printf("Error in accessing data in config file %v\n", err)
						ns.tp.cancel()
					}
					cfg.CurrentTerm = recv.Term
					if err := storage.OverwriteProcessConfig(ns.cfgDir, cfg); err != nil {
						fmt.Printf("Error in overwriting data in config file %v\n", err)
						ns.tp.cancel()
					}
				}
				ticker.Reset(interval)
				reply, err := ns.handleInstallSnapshot(recv)
				if err != nil {
					fmt.Printf("Error in handling install snapshot %v\n", err)
				}
				if reply == nil {
					reply = &proto.InstallSnapShotReply{Term: ns.RaftState.Term}
				}
				rp.sendBack <- &SendBack{
					msgType: TypeInstallSnapShotReply,
					msgSent: reply,
				}
			}
		case <-ns.ctx.Done():
			ns.cancel()
			return ""
		}
	}
}

const snapshotChunkSize = 64 * 1024

func (ns *NodeState) beginSnapshotToPeer(addr string) (*proto.InstallSnapShotArgs, error) {
	index, term, err := ns.Log.LatestSnapshotMetadata()
	if err != nil {
		return nil, err
	}
	if index == 0 {
		return nil, errors.New("no snapshot available")
	}

	if ns.ls.snapshots == nil {
		ns.ls.snapshots = make(map[string]*snapshotSendState)
	}
	ns.ls.snapshots[addr] = &snapshotSendState{
		lastIncludedIndex: index,
		lastIncludedTerm:  term,
	}

	return ns.nextSnapshotChunk(addr)
}

func (ns *NodeState) nextSnapshotChunk(addr string) (*proto.InstallSnapShotArgs, error) {
	state := ns.ls.snapshots[addr]
	if state == nil {
		return nil, errors.New("no snapshot transfer in progress")
	}

	chunk, err := ns.Log.ReadSnapshotChunk(state.offset, snapshotChunkSize)
	if err != nil {
		return nil, err
	}

	if state.lastIncludedIndex != 0 && chunk.LastIncludedIndex != state.lastIncludedIndex {
		state.offset = 0
		chunk, err = ns.Log.ReadSnapshotChunk(0, snapshotChunkSize)
		if err != nil {
			return nil, err
		}
	}

	state.lastIncludedIndex = chunk.LastIncludedIndex
	state.lastIncludedTerm = chunk.LastIncludedTerm
	state.totalSize = chunk.TotalSize
	state.sentDone = chunk.NextOffset >= chunk.TotalSize

	return &proto.InstallSnapShotArgs{
		Term:              ns.RaftState.Term,
		LeaderId:          ns.ID,
		LastIncludedIndex: chunk.LastIncludedIndex,
		LastIncludedTerm:  chunk.LastIncludedTerm,
		Offset:            chunk.Offset,
		Data:              chunk.Data,
		Done:              state.sentDone,
	}, nil
}

func (ns *NodeState) sendSnapshotChunk(addr string) {
	args, err := ns.nextSnapshotChunk(addr)
	if err != nil {
		fmt.Printf("Error building snapshot chunk for %s: %v\n", addr, err)
		delete(ns.ls.snapshots, addr)
		return
	}

	ns.ls.inflight[addr] = true
	go ns.tp.sendToPeers(addr, TypeInstallSnapShot, args, ns.replyCh)
}

func (ns *NodeState) handleInstallSnapshotReply(rp *RPCEvent) {
	state := ns.ls.snapshots[rp.peer]
	if state == nil {
		return
	}

	if rp.msgRecv == nil {
		ns.sendSnapshotChunk(rp.peer)
		return
	}

	reply, ok := rp.msgRecv.(*proto.InstallSnapShotReply)
	if !ok {
		delete(ns.ls.snapshots, rp.peer)
		return
	}

	if reply.Success {
		state.offset = reply.Offset
		if reply.Done || state.sentDone {
			ns.ls.matchIndex[rp.peer] = state.lastIncludedIndex
			ns.ls.nextIndex[rp.peer] = state.lastIncludedIndex + 1
			delete(ns.ls.snapshots, rp.peer)
			return
		}
	} else {
		if reply.Offset <= state.totalSize {
			state.offset = reply.Offset
		} else {
			state.offset = 0
		}
	}

	ns.sendSnapshotChunk(rp.peer)
}

func (ns *NodeState) formatAndSend(clientEntry *proto.KVPair) {
	_ = clientEntry

	ns.tp.mu.RLock()
	peers := make([]string, 0, len(ns.tp.Peers))
	for addr := range ns.tp.Peers {
		if !isSelfAddress(ns.tp.TCPAddr, addr) {
			peers = append(peers, addr)
		}
	}
	ns.tp.mu.RUnlock()

	for _, addr := range peers {
		ns.sendAppendEntries(addr)
	}
}

func (ns *NodeState) sendAppendEntries(addr string) {
	if ns.ls == nil || ns.ls.inflight[addr] {
		return
	}

	nextIndex := ns.ls.nextIndex[addr]
	if nextIndex == 0 {
		nextIndex = 1
		ns.ls.nextIndex[addr] = nextIndex
	}
	prevLogIndex := nextIndex - 1

	prevLogTerm, ok := ns.termAt(prevLogIndex)
	if !ok {
		args, err := ns.beginSnapshotToPeer(addr)
		if err != nil {
			fmt.Printf("Error starting snapshot transfer for %s: %v\n", addr, err)
			return
		}
		ns.ls.inflight[addr] = true
		go ns.tp.sendToPeers(addr, TypeInstallSnapShot, args, ns.replyCh)
		return
	}

	var entriesToSend []*proto.KVPair
	for _, entry := range ns.entries {
		if entry.index >= nextIndex {
			entriesToSend = append(entriesToSend, entry.data)
		}
	}

	args := &proto.AppendEntriesArgs{
		Term:         ns.RaftState.Term,
		LeaderID:     ns.ID,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		LeaderCommit: ns.CommitIndex,
		Entries:      entriesToSend,
	}

	ns.ls.inflight[addr] = true
	go ns.tp.sendToPeers(addr, TypeAppendEntries, args, ns.replyCh)
}

func (ns *NodeState) handleInstallSnapshot(recv *proto.InstallSnapShotArgs) (*proto.InstallSnapShotReply, error) {
	ns.mu.Lock()
	defer ns.mu.Unlock()

	reply := &proto.InstallSnapShotReply{Term: ns.RaftState.Term}

	if recv.Term < ns.RaftState.Term {
		return reply, nil
	}
	if recv.Term > ns.RaftState.Term {
		ns.RaftState.Term = recv.Term
		ns.RaftState.VotedFor = ""
		reply.Term = recv.Term
	}

	if ns.ssMetaData == nil {
		ns.ssMetaData = &snapShotMetaData{}
	}
	if recv.LastIncludedIndex <= ns.ssMetaData.lastIncludedIndex {
		reply.Success = true
		reply.Done = true
		return reply, nil
	}

	if recv.Offset == 0 {
		if ns.snapshotTempPath != "" {
			_ = os.Remove(ns.snapshotTempPath)
		}

		tempPath := filepath.Join(
			ns.Log.SnapshotDirectory(),
			fmt.Sprintf("snapshot-%d.tmp", recv.LastIncludedIndex),
		)
		file, err := os.OpenFile(tempPath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0644)
		if err != nil {
			ns.resetIncomingSnapshotLocked()
			return reply, err
		}

		if err := binary.Write(file, binary.LittleEndian, recv.LastIncludedIndex); err != nil {
			_ = file.Close()
			_ = os.Remove(tempPath)
			ns.resetIncomingSnapshotLocked()
			return reply, err
		}
		if err := binary.Write(file, binary.LittleEndian, recv.LastIncludedTerm); err != nil {
			_ = file.Close()
			_ = os.Remove(tempPath)
			ns.resetIncomingSnapshotLocked()
			return reply, err
		}
		if err := file.Sync(); err != nil {
			_ = file.Close()
			_ = os.Remove(tempPath)
			ns.resetIncomingSnapshotLocked()
			return reply, err
		}
		if err := file.Close(); err != nil {
			_ = os.Remove(tempPath)
			ns.resetIncomingSnapshotLocked()
			return reply, err
		}

		ns.snapshotTempPath = tempPath
		ns.snapshotTempIndex = recv.LastIncludedIndex
		ns.snapshotTempTerm = recv.LastIncludedTerm
		ns.snapshotRecvOffset = 0
	} else {
		if ns.snapshotTempPath == "" ||
			ns.snapshotTempIndex != recv.LastIncludedIndex ||
			ns.snapshotTempTerm != recv.LastIncludedTerm {
			reply.Offset = ns.snapshotRecvOffset
			return reply, nil
		}
		if recv.Offset != ns.snapshotRecvOffset {
			reply.Offset = ns.snapshotRecvOffset
			return reply, nil
		}
	}

	if len(recv.Data) > 0 {
		file, err := os.OpenFile(ns.snapshotTempPath, os.O_APPEND|os.O_WRONLY, 0644)
		if err != nil {
			ns.discardIncomingSnapshotLocked()
			return reply, err
		}
		if _, err := file.Write(recv.Data); err != nil {
			_ = file.Close()
			ns.discardIncomingSnapshotLocked()
			return reply, err
		}
		if err := file.Sync(); err != nil {
			_ = file.Close()
			ns.discardIncomingSnapshotLocked()
			return reply, err
		}
		if err := file.Close(); err != nil {
			ns.discardIncomingSnapshotLocked()
			return reply, err
		}
		ns.snapshotRecvOffset += uint64(len(recv.Data))
	}

	reply.Success = true
	reply.Offset = ns.snapshotRecvOffset
	reply.Done = recv.Done

	if recv.Done {
		if err := ns.Log.InstallSnapshot(ns.snapshotTempPath, recv.LastIncludedIndex, recv.LastIncludedTerm); err != nil {
			ns.discardIncomingSnapshotLocked()
			return reply, err
		}

		ns.ssMetaData.lastIncludedIndex = recv.LastIncludedIndex
		ns.ssMetaData.lastIncludedTerm = recv.LastIncludedTerm
		ns.entries = slices.DeleteFunc(ns.entries, func(e *appendEntry) bool {
			return e.index <= recv.LastIncludedIndex
		})
		if ns.CommitIndex < recv.LastIncludedIndex {
			ns.CommitIndex = recv.LastIncludedIndex
		}

		ns.resetIncomingSnapshotLocked()
		reply.Offset = 0
	}

	return reply, nil
}

func (ns *NodeState) resetIncomingSnapshotLocked() {
	ns.snapshotTempPath = ""
	ns.snapshotTempIndex = 0
	ns.snapshotTempTerm = 0
	ns.snapshotRecvOffset = 0
}

func (ns *NodeState) discardIncomingSnapshotLocked() {
	if ns.snapshotTempPath != "" {
		_ = os.Remove(ns.snapshotTempPath)
	}
	ns.resetIncomingSnapshotLocked()
}

func (ns *NodeState) handleAppendEntries(recv *proto.AppendEntriesArgs) (*proto.AppendEntriesReply, error) {
	ns.mu.Lock()
	defer ns.mu.Unlock()

	ns.refreshSnapshotMetaData()

	var reply *proto.AppendEntriesReply
	idx := ns.entries[len(ns.entries) - 1].index

	if idx == recv.PrevLogIndex {
		for _, entry := range recv.Entries {
			var val []byte
			if entry.Value != nil {
				val = []byte(*entry.Value)
			}
			if err := ns.Log.WriteEntry(entry.Index, entry.Command, entry.Key, val, entry.Term); err != nil {
				return nil, err
			}
			ns.entries = append(ns.entries, &appendEntry{
				data:  entry,
				index: entry.Index,
				term:  entry.Term,
			})
		}

		reply = &proto.AppendEntriesReply{
			Term:          ns.RaftState.Term,
			ConflictIndex: 0,
			ConflictTerm:  0,
			Success:       true,
		}
	} else if idx < recv.PrevLogIndex {
		reply = &proto.AppendEntriesReply{
			Term:          ns.RaftState.Term,
			ConflictIndex: idx + 1,
			ConflictTerm:  0,
			Success:       false,
		}
	} else {
		reply = &proto.AppendEntriesReply{
			Term:    ns.RaftState.Term,
			Success: true,
		}

		appendEntries := func(entries []*proto.KVPair) error {
			for _, e := range entries {
				var val []byte
				if e.Value != nil {
					val = []byte(*e.Value)
				}
				if err := ns.Log.WriteEntry(e.Index, e.Command, e.Key, val, e.Term); err != nil {
					return err
				}
				ns.entries = append(ns.entries, &appendEntry{
					data:  e,
					index: e.Index,
					term:  e.Term,
				})
			}
			return nil
		}

		if ns.ssMetaData.lastIncludedIndex < recv.PrevLogIndex {
			for i, entry := range recv.Entries {
				if entry.Index > ns.ssMetaData.lastIncludedIndex {
					_, lastIdx, err := ns.Log.GetLastTermAndIndex()
					if err != nil {
						return nil, err
					}
					if lastIdx < entry.Index {
						// our log ends before this entry, so append it and everything after it
						if err := appendEntries(recv.Entries[i:]); err != nil {
							return nil, err
						}
						break
					} else {
						walEntries, _, _, err := ns.Log.GetEntriesFrom(entry.Index)
						if err != nil {
							return nil, err
						}
						p := 0
						for j := i; j < len(recv.Entries); j++ {
							incoming := recv.Entries[j]
							for p < len(walEntries) && walEntries[p].Index < incoming.Index {
								p++
							}
							if p >= len(walEntries) || walEntries[p].Index != incoming.Index {
								// the WAL ends before this entry, so append it and the rest
								if err := appendEntries(recv.Entries[j:]); err != nil {
									return nil, err
								}
								break
							}
							if walEntries[p].Term == incoming.Term {
								p++
								continue
							}
							// terms differ truncate log from this entry onwards
							truncateAt := walEntries[p].Index
							if _, err := ns.Log.Repair(truncateAt); err != nil {
								return nil, err
							}
							ns.entries = slices.DeleteFunc(ns.entries, func(le *appendEntry) bool {
								return le.index >= truncateAt
							})
							if err := appendEntries(recv.Entries[j:]); err != nil {
								return nil, err
							}
							break
						}
						break
					}
				} else {
					continue
				}
			}
		} else if ns.ssMetaData.lastIncludedIndex == recv.PrevLogIndex {
			// the snapshot ends at PrevLogIndex merge against the WAL from the start
			walEntries, err := ns.Log.ReadAll()
			if err != nil {
				return nil, err
			}
			p := 0
			for j := 0; j < len(recv.Entries); j++ {
				incoming := recv.Entries[j]
				for p < len(walEntries) && walEntries[p].Index < incoming.Index {
					p++
				}
				if p >= len(walEntries) || walEntries[p].Index != incoming.Index {
					// the WAL ends before this entry append it and the rest
					if err := appendEntries(recv.Entries[j:]); err != nil {
						return nil, err
					}
					break
				}
				if walEntries[p].Term == incoming.Term {
					p++
					continue
				}
				// terms differ truncate log from this entry onwards
				truncateAt := walEntries[p].Index
				if _, err := ns.Log.Repair(truncateAt); err != nil {
					return nil, err
				}
				ns.entries = slices.DeleteFunc(ns.entries, func(le *appendEntry) bool {
					return le.index >= truncateAt
				})
				if err := appendEntries(recv.Entries[j:]); err != nil {
					return nil, err
				}
				break
			}
		}
	}

	_, idx, err := ns.Log.GetLastTermAndIndex()
	if err != nil {
		return nil, err
	}
	ns.CommitIndex = max(ns.CommitIndex, min(idx, recv.LeaderCommit))
	ns.applyCommittedLocked()
	return reply, nil
}

func sendBackNow(rp *RPCEvent, msgType byte, args googleproto.Message) {
	if rp == nil || rp.sendBack == nil {
		return
	}
	select {
	case rp.sendBack <- &SendBack{msgType: msgType, msgSent: args}:
	default:
	}
}

func (ns *NodeState) handleLaggingFollower(recv *proto.AppendEntriesReply, rp *RPCEvent) error {
	if len(ns.entries) == 0 {
		return nil
	}
	if ns.entries[0].index <= recv.ConflictIndex {
		jumptoIdx := recv.ConflictIndex - ns.entries[0].index
		var entries []*proto.KVPair
		for _, entry := range ns.entries[jumptoIdx:] {
			new_entry := &proto.KVPair{
				Term:    entry.term,
				Index:   entry.index,
				Key:     entry.data.Key,
				Value:   entry.data.Value,
				Command: entry.data.Command,
			}
			entries = append(entries, new_entry)
		}

		if jumptoIdx == 0 {
			lastTerm, lastIdx, err := ns.Log.GetLastTermAndIndex()
			if err != nil {
				return err
			}
			args := &proto.AppendEntriesArgs{
				Term:         ns.RaftState.Term,
				LeaderID:     ns.ID,
				PrevLogIndex: lastIdx,
				PrevLogTerm:  lastTerm,
				LeaderCommit: ns.CommitIndex,
				Entries:      entries,
			}

			sendBackNow(rp, TypeAppendEntries, args)
		} else {
			args := &proto.AppendEntriesArgs{
				Term:         ns.RaftState.Term,
				LeaderID:     ns.ID,
				PrevLogIndex: ns.entries[jumptoIdx-1].index,
				PrevLogTerm:  ns.entries[jumptoIdx-1].term,
				LeaderCommit: ns.CommitIndex,
				Entries:      entries,
			}
			sendBackNow(rp, TypeAppendEntries, args)
		}
	} else {
		_, firstIdx, err := ns.Log.GetFirstTermAndIndex()
		if err != nil {
			return err
		}
		if firstIdx > 0 && recv.ConflictIndex >= firstIdx {
			entries, idx, term, err := ns.Log.GetEntriesFrom(recv.ConflictIndex)
			if err != nil {
				return err
			}

			for _, entry := range ns.entries {
				new_entry := &proto.KVPair{
					Term:    entry.term,
					Index:   entry.index,
					Key:     entry.data.Key,
					Value:   entry.data.Value,
					Command: entry.data.Command,
				}
				entries = append(entries, new_entry)
			}

			args := &proto.AppendEntriesArgs{
				Term:         ns.RaftState.Term,
				LeaderID:     ns.ID,
				PrevLogIndex: idx,
				PrevLogTerm:  term,
				LeaderCommit: ns.CommitIndex,
				Entries:      entries,
			}

			sendBackNow(rp, TypeAppendEntries, args)
		} else {
			args, err := ns.beginSnapshotToPeer(rp.peer)
			if err != nil {
				return err
			}
			ns.ls.inflight[rp.peer] = true
			sendBackNow(rp, TypeInstallSnapShot, args)
		}
	}
	return nil
}