package raft

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"net"
	"os"
	"testing"
	"time"

	"raft/proto"
	"raft/storage"
)

func TestIsSelfAddress(t *testing.T) {
	cases := []struct {
		self  string
		peer  string
		match bool
	}{
		{":7001", ":7001", true},
		{"127.0.0.1:7001", ":7001", true},
		{"0.0.0.0:7001", "localhost:7001", true},
		{"[::1]:7001", ":7001", true},
		{":7001", ":7002", false},
		{"10.0.0.5:7001", ":7001", false},
		{"10.0.0.5:7001", "10.0.0.6:7001", false},
		{"", ":7001", false},
	}
	for _, tc := range cases {
		if got := isSelfAddress(tc.self, tc.peer); got != tc.match {
			t.Fatalf("isSelfAddress(%q, %q) = %v, want %v", tc.self, tc.peer, got, tc.match)
		}
	}
}

func TestDefaultPeersExcludeSelf(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	for _, self := range []string{":7001", "127.0.0.1:7003", "localhost:7005"} {
		ns := NewNodeState(1, ":8001", self, t.TempDir(), RoleFollower, ctx, nil)
		if ns == nil {
			t.Fatalf("failed to init NodeState for %s", self)
		}

		if len(ns.tp.Peers) != len(DefaultPeers)-1 {
			ns.Log.Close()
			t.Fatalf("expected %d peers for self %s, got %d", len(DefaultPeers)-1, self, len(ns.tp.Peers))
		}

		_, selfPort, err := net.SplitHostPort(self)
		if err != nil {
			ns.Log.Close()
			t.Fatalf("invalid self address %s: %v", self, err)
		}
		for _, peer := range DefaultPeers {
			_, peerPort, _ := net.SplitHostPort(peer)
			_, exists := ns.tp.Peers[peer]
			if peerPort == selfPort {
				if exists {
					ns.Log.Close()
					t.Fatalf("peer %s must not be registered for self %s", peer, self)
				}
				continue
			}
			if !exists {
				ns.Log.Close()
				t.Fatalf("expected default peer %s for self %s", peer, self)
			}
		}
		ns.Log.Close()
	}
}

func TestHydrateEntriesFromLogOnStartup(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "raft_test_hydrate_*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	wal, err := storage.OpenWAL(tempDir, false, 1024*1024, 10)
	if err != nil {
		t.Fatalf("failed to open wal: %v", err)
	}

	// Write entries into WAL
	_ = wal.WriteEntry(1, proto.Command_PUT, "k1", []byte("v1"), 1)
	_ = wal.WriteEntry(2, proto.Command_PUT, "k2", []byte("v2"), 1)
	_ = wal.WriteEntry(3, proto.Command_DELETE, "k1", nil, 2)
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

	wal2, err := storage.OpenWAL(tempDir2, false, 1024*1024, 10)
	if err != nil {
		t.Fatalf("failed to open wal: %v", err)
	}
	for i := 1; i <= 5; i++ {
		_ = wal2.WriteEntry(uint64(i), proto.Command_PUT, "key", []byte("val"), 2)
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
			msgType: TypeInstallSnapShotReply,
			msgSent: &proto.InstallSnapShotReply{
				Term:    1,
				Offset:  0,
				Success: true,
				Done:    true,
			},
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for InstallSnapShot")
	}

	select {
	case replyEv := <-senderReplyCh:
		reply, ok := replyEv.msgRecv.(*proto.InstallSnapShotReply)
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

	wal, err := storage.OpenWAL(tempDir, false, 1024*1024, 10)
	if err != nil {
		t.Fatalf("failed to open wal: %v", err)
	}

	for i := 1; i <= 6; i++ {
		_ = wal.WriteEntry(uint64(i), proto.Command_PUT, "k", []byte("v"), 1)
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
	_ = wal.WriteEntry(7, proto.Command_PUT, "k7", []byte("v7"), 2)
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

// encodeSnapshotBody serialises KVPairs the same way the WAL stores snapshot
// bodies: an int64 size prefix followed by the marshalled entry.
func encodeSnapshotBody(t *testing.T, entries []*proto.KVPair) []byte {
	t.Helper()

	var buf bytes.Buffer
	for _, entry := range entries {
		data := proto.MustMarshal(entry)
		if err := binary.Write(&buf, binary.LittleEndian, int64(len(data))); err != nil {
			t.Fatalf("failed to encode snapshot entry: %v", err)
		}
		if _, err := buf.Write(data); err != nil {
			t.Fatalf("failed to encode snapshot entry: %v", err)
		}
	}
	return buf.Bytes()
}

func TestHandleInstallSnapshotChunkedTransfer(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "raft_test_snapshot_chunks_*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ns := NewNodeState(1, ":8011", ":9011", tempDir, RoleFollower, ctx, nil)
	if ns == nil {
		t.Fatalf("failed to init NodeState")
	}
	defer ns.Log.Close()

	value := func(s string) *string { return &s }
	body := encodeSnapshotBody(t, []*proto.KVPair{
		{Key: "a", Value: value("one"), Command: proto.Command_PUT},
		{Key: "b", Value: value("two"), Command: proto.Command_PUT},
		{Key: "c", Value: value("three"), Command: proto.Command_PUT},
	})

	// Split the body in the middle of a serialised entry on purpose.
	split := len(body) / 2
	if split == 0 {
		split = 1
	}
	chunks := [][]byte{body[:split], body[split:]}

	const lastIncludedIndex, lastIncludedTerm = uint64(12), uint64(3)

	reply, err := ns.handleInstallSnapshot(&proto.InstallSnapShotArgs{
		Term:              lastIncludedTerm,
		LeaderId:          2,
		LastIncludedIndex: lastIncludedIndex,
		LastIncludedTerm:  lastIncludedTerm,
		Offset:            0,
		Data:              chunks[0],
	})
	if err != nil {
		t.Fatalf("unexpected error on first chunk: %v", err)
	}
	if !reply.Success || reply.Offset != uint64(len(chunks[0])) {
		t.Fatalf("unexpected first reply: %+v", reply)
	}
	if ns.snapshotTempPath == "" {
		t.Fatalf("expected a temporary snapshot file")
	}

	// A chunk that does not match the expected offset must be rejected and
	// must report the current offset so the leader can resume.
	reply, err = ns.handleInstallSnapshot(&proto.InstallSnapShotArgs{
		Term:              lastIncludedTerm,
		LeaderId:          2,
		LastIncludedIndex: lastIncludedIndex,
		LastIncludedTerm:  lastIncludedTerm,
		Offset:            1,
		Data:              []byte("garbage"),
	})
	if err != nil {
		t.Fatalf("unexpected error on mismatched chunk: %v", err)
	}
	if reply.Success || reply.Offset != uint64(len(chunks[0])) {
		t.Fatalf("unexpected mismatched reply: %+v", reply)
	}

	reply, err = ns.handleInstallSnapshot(&proto.InstallSnapShotArgs{
		Term:              lastIncludedTerm,
		LeaderId:          2,
		LastIncludedIndex: lastIncludedIndex,
		LastIncludedTerm:  lastIncludedTerm,
		Offset:            uint64(len(chunks[0])),
		Data:              chunks[1],
		Done:              true,
	})
	if err != nil {
		t.Fatalf("unexpected error on final chunk: %v", err)
	}
	if !reply.Success || !reply.Done {
		t.Fatalf("unexpected final reply: %+v", reply)
	}
	if ns.snapshotTempPath != "" {
		t.Fatalf("expected temporary snapshot state to be reset")
	}

	index, term, err := ns.Log.LatestSnapshotMetadata()
	if err != nil {
		t.Fatalf("failed to read snapshot metadata: %v", err)
	}
	if index != lastIncludedIndex || term != lastIncludedTerm {
		t.Fatalf("unexpected snapshot metadata: index=%d term=%d", index, term)
	}
	if ns.ssMetaData.lastIncludedIndex != lastIncludedIndex {
		t.Fatalf("ssMetaData was not updated: %+v", ns.ssMetaData)
	}
	if ns.CommitIndex < lastIncludedIndex {
		t.Fatalf("commit index not advanced: %d", ns.CommitIndex)
	}

	chunk, err := ns.Log.ReadSnapshotChunk(0, len(body))
	if err != nil {
		t.Fatalf("failed to read committed snapshot: %v", err)
	}
	if !bytes.Equal(chunk.Data, body) {
		t.Fatalf("committed snapshot body does not match")
	}
}

func TestLeaderSnapshotChunksUsePerFollowerOffset(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "raft_test_snapshot_leader_*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	wal, err := storage.OpenWAL(tempDir, false, 4*1024*1024, 100)
	if err != nil {
		t.Fatalf("failed to open wal: %v", err)
	}

	large := string(bytes.Repeat([]byte("x"), 30*1024))
	for i := 1; i <= 4; i++ {
		if err := wal.WriteEntry(uint64(i), proto.Command_PUT, fmt.Sprintf("k%d", i), []byte(large), 1); err != nil {
			t.Fatalf("failed to write entry: %v", err)
		}
	}
	if _, err := wal.CreateSnapshot(); err != nil {
		t.Fatalf("failed to create snapshot: %v", err)
	}
	if err := wal.Close(); err != nil {
		t.Fatalf("failed to close wal: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ns := NewNodeState(2, ":8021", ":9021", tempDir, RoleLeader, ctx, nil)
	if ns == nil {
		t.Fatalf("failed to init NodeState")
	}
	defer ns.Log.Close()

	addr := ":9022"
	first, err := ns.beginSnapshotToPeer(addr)
	if err != nil {
		t.Fatalf("failed to begin snapshot: %v", err)
	}
	if first.Offset != 0 || first.Done {
		t.Fatalf("unexpected first chunk: %+v", first)
	}
	if first.LastIncludedIndex != 4 {
		t.Fatalf("unexpected snapshot index: %d", first.LastIncludedIndex)
	}

	state := ns.ls.snapshots[addr]
	if state == nil {
		t.Fatalf("expected per-follower snapshot state")
	}

	// Simulate the follower acknowledging exactly the bytes it received.
	state.offset = uint64(len(first.Data))
	second, err := ns.nextSnapshotChunk(addr)
	if err != nil {
		t.Fatalf("failed to build next chunk: %v", err)
	}
	if second.Offset != uint64(len(first.Data)) {
		t.Fatalf("expected offset %d, got %d", len(first.Data), second.Offset)
	}
	if len(second.Data) == 0 {
		t.Fatalf("expected the second chunk to contain data")
	}

	// Advance through the remaining chunks exactly as a follower would.
	var received []byte
	received = append(received, first.Data...)
	current := second
	for {
		received = append(received, current.Data...)
		state.offset = current.Offset + uint64(len(current.Data))
		if current.Done {
			break
		}
		current, err = ns.nextSnapshotChunk(addr)
		if err != nil {
			t.Fatalf("failed to build next chunk: %v", err)
		}
	}

	chunk, err := ns.Log.ReadSnapshotChunk(0, len(received))
	if err != nil {
		t.Fatalf("failed to read snapshot: %v", err)
	}
	if !bytes.Equal(received, chunk.Data) {
		t.Fatalf("chunked transfer did not reproduce the snapshot body")
	}

	// A second follower must start from its own offset, not inherit the first.
	other := ":9023"
	otherFirst, err := ns.beginSnapshotToPeer(other)
	if err != nil {
		t.Fatalf("failed to begin snapshot for second follower: %v", err)
	}
	if otherFirst.Offset != 0 {
		t.Fatalf("expected second follower to start at offset 0, got %d", otherFirst.Offset)
	}
}

func freeLoopbackAddr(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to reserve port: %v", err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()
	return addr
}

// TestLeaderApplyCommitsToFollower exercises the HTTP-facing path: a command
// submitted to the leader must be written to the WAL, replicated, committed,
// applied to the local state, and reported back to exactly the caller.
func TestLeaderApplyCommitsToFollower(t *testing.T) {
	peers := []string{
		freeLoopbackAddr(t),
		freeLoopbackAddr(t),
		freeLoopbackAddr(t),
	}

	oldPeers := DefaultPeers
	DefaultPeers = peers
	defer func() { DefaultPeers = oldPeers }()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	nodes := make([]*NodeState, 0, len(peers))
	for i, addr := range peers {
		role := RoleFollower
		if i == 0 {
			role = RoleLeader
		}
		ns := NewNodeState(uint64(i+1), ":0", addr, t.TempDir(), role, ctx, nil)
		if ns == nil {
			t.Fatalf("failed to initialize node %d", i+1)
		}
		go ns.Start()
		nodes = append(nodes, ns)
	}
	defer func() {
		for _, ns := range nodes {
			_ = ns.Close()
		}
	}()

	applyCtx, applyCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer applyCancel()

	err := nodes[0].Apply(applyCtx, &ClientCmd{
		Command: proto.Command_PUT,
		Key:     "alpha",
		Value:   "beta",
	})
	if err != nil {
		t.Fatalf("Apply on leader: %v", err)
	}

	if got, ok := nodes[0].Get("alpha"); !ok || got != "beta" {
		t.Fatalf("leader did not apply committed value: %q, %v", got, ok)
	}
	if nodes[0].CommitIndex == 0 {
		t.Fatalf("expected leader CommitIndex to advance")
	}

	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if got, ok := nodes[1].Get("alpha"); ok && got == "beta" {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("follower never applied the committed entry")
}

func TestApplyRejectedOnFollower(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ns := NewNodeState(2, ":0", freeLoopbackAddr(t), t.TempDir(), RoleFollower, ctx, nil)
	if ns == nil {
		t.Fatalf("failed to initialize follower")
	}
	defer ns.Close()

	applyCtx, applyCancel := context.WithTimeout(context.Background(), time.Second)
	defer applyCancel()

	err := ns.Apply(applyCtx, &ClientCmd{Command: proto.Command_PUT, Key: "k", Value: "v"})
	if !errors.Is(err, ErrNotLeader) {
		t.Fatalf("expected ErrNotLeader, got %v", err)
	}
}
