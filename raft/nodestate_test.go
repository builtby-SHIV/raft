package raft

import (
	"context"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"raft/proto"
	"raft/storage"
)

func TestHydrateEntriesFromLogOnStartup(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "raft_test_hydrate_*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	walDir := filepath.Join(tempDir, "wal")
	wal, err := storage.OpenWAL(walDir, false, 1024*1024, 10)
	if err != nil {
		t.Fatalf("failed to open wal: %v", err)
	}

	// Write entries into WAL
	_ = wal.WriteEntry(proto.Command_PUT, "k1", []byte("v1"), 1)
	_ = wal.WriteEntry(proto.Command_PUT, "k2", []byte("v2"), 1)
	_ = wal.WriteEntry(proto.Command_DELETE, "k1", nil, 2)
	_ = wal.Close()

	// Startup NodeState
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ns := NewNodeState(1, ":8001", ":9001", tempDir, RoleFollower, ctx, nil)
	if ns == nil {
		t.Fatalf("failed to initialize NodeState")
	}
	defer ns.Log.Close()

	if len(ns.entries) != 3 {
		t.Fatalf("expected 3 hydrated entries, got %d", len(ns.entries))
	}

	if ns.entries[0].index != 1 || ns.entries[0].term != 1 || ns.entries[0].data.Key != "k1" || ns.entries[0].data.GetValue() != "v1" {
		t.Fatalf("unexpected entry 0: %+v", ns.entries[0])
	}
	if ns.entries[1].index != 2 || ns.entries[1].term != 1 || ns.entries[1].data.Key != "k2" || ns.entries[1].data.GetValue() != "v2" {
		t.Fatalf("unexpected entry 1: %+v", ns.entries[1])
	}
	if ns.entries[2].index != 3 || ns.entries[2].term != 2 || ns.entries[2].data.Key != "k1" || ns.entries[2].data.Command != proto.Command_DELETE {
		t.Fatalf("unexpected entry 2: %+v", ns.entries[2])
	}
}

func TestLeaderInitNextAndMatchIndex(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "raft_test_leader_init_*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 1. Empty Log -> nextIndex should be 1, matchIndex should be 0
	nsEmpty := NewNodeState(1, ":8001", ":9001", tempDir, RoleLeader, ctx, nil)
	if nsEmpty == nil {
		t.Fatalf("failed to create empty NodeState")
	}
	defer nsEmpty.Log.Close()

	nsEmpty.AddPeer(":9002")
	nsEmpty.AddPeer(":9003")

	if nsEmpty.ls.nextIndex[":9002"] != 1 || nsEmpty.ls.matchIndex[":9002"] != 0 {
		t.Fatalf("expected nextIndex=1, matchIndex=0 for empty log; got next=%d, match=%d",
			nsEmpty.ls.nextIndex[":9002"], nsEmpty.ls.matchIndex[":9002"])
	}
	if nsEmpty.ls.nextIndex[":9003"] != 1 || nsEmpty.ls.matchIndex[":9003"] != 0 {
		t.Fatalf("expected nextIndex=1, matchIndex=0 for empty log; got next=%d, match=%d",
			nsEmpty.ls.nextIndex[":9003"], nsEmpty.ls.matchIndex[":9003"])
	}

	// 2. Non-empty log -> nextIndex should be lastLogIndex + 1, matchIndex should be 0
	tempDir2, err := os.MkdirTemp("", "raft_test_leader_init2_*")
	if err != nil {
		t.Fatalf("failed to create temp dir 2: %v", err)
	}
	defer os.RemoveAll(tempDir2)

	walDir2 := filepath.Join(tempDir2, "wal")
	wal2, err := storage.OpenWAL(walDir2, false, 1024*1024, 10)
	if err != nil {
		t.Fatalf("failed to open wal: %v", err)
	}
	for i := 1; i <= 5; i++ {
		_ = wal2.WriteEntry(proto.Command_PUT, "key", []byte("val"), 2)
	}
	_ = wal2.Close()

	nsPopulated := NewNodeState(1, ":8001", ":9001", tempDir2, RoleLeader, ctx, nil)
	if nsPopulated == nil {
		t.Fatalf("failed to create populated NodeState")
	}
	defer nsPopulated.Log.Close()

	nsPopulated.AddPeer(":9002")
	nsPopulated.AddPeer(":9003")

	// Last log index is 5 -> nextIndex should be 5 + 1 = 6, matchIndex = 0
	if nsPopulated.ls.nextIndex[":9002"] != 6 || nsPopulated.ls.matchIndex[":9002"] != 0 {
		t.Fatalf("expected nextIndex=6, matchIndex=0; got next=%d, match=%d",
			nsPopulated.ls.nextIndex[":9002"], nsPopulated.ls.matchIndex[":9002"])
	}
	if nsPopulated.ls.nextIndex[":9003"] != 6 || nsPopulated.ls.matchIndex[":9003"] != 0 {
		t.Fatalf("expected nextIndex=6, matchIndex=0; got next=%d, match=%d",
			nsPopulated.ls.nextIndex[":9003"], nsPopulated.ls.matchIndex[":9003"])
	}
}

func TestTransportSendBackCreation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	tp := NewTCPTransport(":8001", "127.0.0.1:0", ctx)
	rpCh := make(chan *RPCEvent, 10)

	// Listen on dynamic port
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}
	defer ln.Close()
	serverAddr := ln.Addr().String()

	// Client sends a message to server
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		handleIncoming(conn, ctx, rpCh)
	}()

	clientConn, err := net.Dial("tcp", serverAddr)
	if err != nil {
		t.Fatalf("failed to dial: %v", err)
	}
	defer clientConn.Close()

	// Encode and send RequestVote
	req := &proto.RequestVoteArgs{
		Term:         1,
		CandidateID:  2,
		LastLogIndex: 0,
		LastLogTerm:  0,
	}
	if err := EncodeAndWriteMessage(clientConn, TypeRequestVote, req); err != nil {
		t.Fatalf("failed to write message: %v", err)
	}

	// Verify server received RPCEvent with non-nil sendBack channel
	select {
	case event := <-rpCh:
		if event.sendBack == nil {
			t.Fatalf("expected non-nil sendBack channel in RPCEvent")
		}
		// Send reply back through sendBack
		reply := &proto.RequestVoteReply{
			Term:        1,
			VoteGranted: true,
		}
		event.sendBack <- &SendBack{
			msgType: TypeRequestVoteReply,
			msgSent: reply,
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for RPCEvent")
	}

	// Verify client receives decoded reply
	msgType, msg, err := DecodeAndReadMessage(clientConn)
	if err != nil {
		t.Fatalf("failed to read response: %v", err)
	}
	if msgType != TypeRequestVoteReply {
		t.Fatalf("expected TypeRequestVoteReply, got %d", msgType)
	}
	reply, ok := msg.(*proto.RequestVoteReply)
	if !ok || !reply.VoteGranted {
		t.Fatalf("unexpected reply: %+v", msg)
	}

	_ = tp
}

func TestSendToPeersSwitch(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Server listening
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}
	defer ln.Close()
	serverAddr := ln.Addr().String()

	serverRPCh := make(chan *RPCEvent, 10)
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go handleIncoming(conn, ctx, serverRPCh)
		}
	}()

	senderTP := NewTCPTransport(":8002", "127.0.0.1:0", ctx)
	senderTP.Add(serverAddr)
	senderReplyCh := make(chan *RPCEvent, 10)

	// 1. Send AppendEntriesArgs through sendToPeers
	valStr := "v1"
	appendArgs := &proto.AppendEntriesArgs{
		Term:         1,
		LeaderID:     1,
		PrevLogIndex: 0,
		PrevLogTerm:  0,
		LeaderCommit: 0,
		Entries: []*proto.KVPair{
			{
				Key:     "k1",
				Value:   &valStr,
				Command: proto.Command_PUT,
			},
		},
	}

	go senderTP.sendToPeers(serverAddr, TypeAppendEntries, appendArgs, senderReplyCh)

	select {
	case ev := <-serverRPCh:
		if ev.msgType != TypeAppendEntries {
			t.Fatalf("expected TypeAppendEntries, got %d", ev.msgType)
		}
		ev.sendBack <- &SendBack{
			msgType: TypeAppendEntriesReply,
			msgSent: &proto.AppendEntriesReply{
				Term:    1,
				Success: true,
			},
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for AppendEntries")
	}

	select {
	case replyEv := <-senderReplyCh:
		reply, ok := replyEv.msgRecv.(*proto.AppendEntriesReply)
		if !ok || !reply.Success {
			t.Fatalf("unexpected reply: %+v", replyEv)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for reply")
	}

	// 2. Send InstallSnapShotArgs through sendToPeers
	snapArgs := &proto.InstallSnapShotArgs{
		Term:              1,
		LeaderId:          1,
		LastIncludedIndex: 5,
		LastIncludedTerm:  1,
		Done:              true,
	}

	go senderTP.sendToPeers(serverAddr, TypeInstallSnapShot, snapArgs, senderReplyCh)

	select {
	case ev := <-serverRPCh:
		if ev.msgType != TypeInstallSnapShot {
			t.Fatalf("expected TypeInstallSnapShot, got %d", ev.msgType)
		}
		ev.sendBack <- &SendBack{
			msgType: TypeAppendEntriesReply,
			msgSent: &proto.AppendEntriesReply{
				Term:    1,
				Success: true,
			},
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for InstallSnapShot")
	}

	select {
	case replyEv := <-senderReplyCh:
		reply, ok := replyEv.msgRecv.(*proto.AppendEntriesReply)
		if !ok || !reply.Success {
			t.Fatalf("unexpected reply: %+v", replyEv)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for reply")
	}
}

func TestHandleLaggingFollowerFirstIndex(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "raft_test_lagging_*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	walDir := filepath.Join(tempDir, "wal")
	wal, err := storage.OpenWAL(walDir, false, 1024*1024, 10)
	if err != nil {
		t.Fatalf("failed to open wal: %v", err)
	}

	for i := 1; i <= 6; i++ {
		_ = wal.WriteEntry(proto.Command_PUT, "k", []byte("v"), 1)
	}
	// Create snapshot up to index 6
	_, err = wal.CreateSnapshot()
	if err != nil {
		t.Fatalf("failed to create snapshot: %v", err)
	}
	// Repair/compact away entries 1..6
	_, err = wal.Repair(1)
	if err != nil {
		t.Fatalf("failed to repair: %v", err)
	}
	// Write entry 7 into WAL
	_ = wal.WriteEntry(proto.Command_PUT, "k7", []byte("v7"), 2)
	_ = wal.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ns := NewNodeState(1, ":8001", ":9001", tempDir, RoleLeader, ctx, nil)
	if ns == nil {
		t.Fatalf("failed to init NodeState")
	}
	defer ns.Log.Close()

	// 1. Follower conflicts at index 5 (which is compacted, firstIdx is 7).
	// Because 5 < firstIdx, it should send InstallSnapShot.
	sendBackCh := make(chan *SendBack, 10)
	rp := &RPCEvent{
		sendBack: sendBackCh,
	}
	reply := &proto.AppendEntriesReply{
		Term:          1,
		Success:       false,
		ConflictIndex: 5,
	}

	err = ns.handleLaggingFollower(reply, rp)
	if err != nil {
		t.Fatalf("unexpected error in handleLaggingFollower: %v", err)
	}

	select {
	case sb := <-sendBackCh:
		if sb.msgType != TypeInstallSnapShot {
			t.Fatalf("expected TypeInstallSnapShot for compacted index 5, got %d", sb.msgType)
		}
	case <-time.After(time.Second):
		t.Fatalf("timed out waiting for snapshot sendBack")
	}

	// 2. Follower conflicts at index 7 (which is in the WAL, firstIdx is 7).
	// Because 7 >= firstIdx, it should send AppendEntries.
	reply2 := &proto.AppendEntriesReply{
		Term:          1,
		Success:       false,
		ConflictIndex: 7,
	}

	err = ns.handleLaggingFollower(reply2, rp)
	if err != nil {
		t.Fatalf("unexpected error in handleLaggingFollower: %v", err)
	}

	select {
	case sb := <-sendBackCh:
		if sb.msgType != TypeAppendEntries {
			t.Fatalf("expected TypeAppendEntries for index 7, got %d", sb.msgType)
		}
	case <-time.After(time.Second):
		t.Fatalf("timed out waiting for append entries sendBack")
	}
}

