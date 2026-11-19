// Package tests holds end-to-end replication tests for the Raft backend.
//
// The tests start real NodeState instances over loopback TCP (one leader plus
// followers, the same fixed-role topology main.go exposes) and drive them
// through the exported API, so they exercise the transport, the leader role
// loop, WAL persistence and the state machine together.
package tests

import (
	"context"
	"fmt"
	"net"
	"testing"
	"time"

	"raft/proto"
	"raft/raft"
)

const (
	startTimeout = 10 * time.Second
	opTimeout    = 10 * time.Second
	waitTimeout  = 20 * time.Second
	pollInterval = 25 * time.Millisecond
)

// testNode is one process in the cluster. dir is kept so a node can be
// restarted with the same WAL.
type testNode struct {
	id   uint64
	role string
	addr string
	dir  string
	ns   *raft.NodeState
}

// testCluster owns the five loopback addresses and the running NodeStates.
// Test instances must not run in parallel because raft.DefaultPeers is a
// package-level global.
type testCluster struct {
	t      *testing.T
	ctx    context.Context
	cancel context.CancelFunc
	nodes  []*testNode
}

func reserveAddr(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve loopback port: %v", err)
	}
	addr := ln.Addr().String()
	if err := ln.Close(); err != nil {
		t.Fatalf("close reserved listener: %v", err)
	}
	return addr
}

func dialable(addr string) bool {
	conn, err := net.DialTimeout("tcp", addr, 150*time.Millisecond)
	if err != nil {
		return false
	}
	_ = conn.Close()
	return true
}

func waitFor(t *testing.T, timeout time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(pollInterval)
	}
	t.Fatalf("timed out after %s waiting for %s", timeout, what)
}

// newTestCluster reserves size loopback ports, points raft.DefaultPeers at
// them, and starts node 1 as leader with the rest as followers.
func newTestCluster(t *testing.T, size int) *testCluster {
	t.Helper()

	peers := make([]string, size)
	for i := range peers {
		peers[i] = reserveAddr(t)
	}

	oldPeers := raft.DefaultPeers
	raft.DefaultPeers = peers
	t.Cleanup(func() { raft.DefaultPeers = oldPeers })

	ctx, cancel := context.WithCancel(context.Background())
	c := &testCluster{t: t, ctx: ctx, cancel: cancel}

	for i := 0; i < size; i++ {
		role := raft.RoleFollower
		if i == 0 {
			role = raft.RoleLeader
		}
		c.nodes = append(c.nodes, &testNode{
			id:   uint64(i + 1),
			role: role,
			addr: peers[i],
			dir:  t.TempDir(),
		})
	}

	for i := range c.nodes {
		c.start(i)
	}

	t.Cleanup(func() {
		for _, n := range c.nodes {
			if n.ns != nil {
				_ = n.ns.Close()
				n.ns = nil
			}
		}
		cancel()
	})
	return c
}

func (c *testCluster) start(i int) {
	c.t.Helper()
	n := c.nodes[i]
	if n.ns != nil {
		c.t.Fatalf("node %d is already running", n.id)
	}
	ns := raft.NewNodeState(n.id, "127.0.0.1:0", n.addr, n.dir, n.role, c.ctx, nil)
	if ns == nil {
		c.t.Fatalf("node %d failed to initialize (storage %s)", n.id, n.dir)
	}
	n.ns = ns
	go ns.Start()
	waitFor(c.t, startTimeout, fmt.Sprintf("N%d listening on %s", n.id, n.addr), func() bool {
		return dialable(n.addr)
	})
}

func (c *testCluster) stop(i int) {
	c.t.Helper()
	n := c.nodes[i]
	if n.ns == nil {
		return
	}
	if err := n.ns.Close(); err != nil {
		c.t.Fatalf("close N%d: %v", n.id, err)
	}
	n.ns = nil
	waitFor(c.t, startTimeout, fmt.Sprintf("N%d port %s to close", n.id, n.addr), func() bool {
		return !dialable(n.addr)
	})
}

func (c *testCluster) stopAll() {
	for i := range c.nodes {
		c.stop(i)
	}
}

func (c *testCluster) restartAll() {
	for i := range c.nodes {
		c.start(i)
	}
}

func (c *testCluster) liveNodes() []int {
	live := make([]int, 0, len(c.nodes))
	for i, n := range c.nodes {
		if n.ns != nil {
			live = append(live, i)
		}
	}
	return live
}

func (c *testCluster) snapshot(i int) raft.NodeStateResponse {
	c.t.Helper()
	n := c.nodes[i]
	if n.ns == nil {
		c.t.Fatalf("cannot snapshot stopped node N%d", n.id)
	}
	return n.ns.SnapshotState(0)
}

func (c *testCluster) apply(i int, cmd proto.Command, key, value string) error {
	c.t.Helper()
	n := c.nodes[i]
	if n.ns == nil {
		c.t.Fatalf("cannot apply to stopped node N%d", n.id)
	}
	ctx, cancel := context.WithTimeout(context.Background(), opTimeout)
	defer cancel()
	return n.ns.Apply(ctx, &raft.ClientCmd{Command: cmd, Key: key, Value: value})
}

func (c *testCluster) mustApply(i int, cmd proto.Command, key, value string) {
	c.t.Helper()
	if err := c.apply(i, cmd, key, value); err != nil {
		c.t.Fatalf("apply %s %q on N%d: %v", cmd, key, c.nodes[i].id, err)
	}
}

func (c *testCluster) get(i int, key string) (string, bool) {
	c.t.Helper()
	n := c.nodes[i]
	if n.ns == nil {
		c.t.Fatalf("cannot read from stopped node N%d", n.id)
	}
	return n.ns.Get(key)
}

// waitValue waits until every running node has key=value in its state machine.
func (c *testCluster) waitValue(key, value string) {
	c.t.Helper()
	waitFor(c.t, waitTimeout, fmt.Sprintf("%q=%q on every running node", key, value), func() bool {
		for _, i := range c.liveNodes() {
			if got, ok := c.get(i, key); !ok || got != value {
				return false
			}
		}
		return true
	})
}

// waitAbsent waits until no running node has key in its state machine.
func (c *testCluster) waitAbsent(key string) {
	c.t.Helper()
	waitFor(c.t, waitTimeout, fmt.Sprintf("%q removed from every running node", key), func() bool {
		for _, i := range c.liveNodes() {
			if _, ok := c.get(i, key); ok {
				return false
			}
		}
		return true
	})
}

// waitCommitCaughtUp waits until every running node reports the leader's
// commit index, which means the followers have learned about the commit.
func (c *testCluster) waitCommitCaughtUp() {
	c.t.Helper()
	waitFor(c.t, waitTimeout, "all running nodes to reach the leader commit index", func() bool {
		leader := c.snapshot(0).Node
		for _, i := range c.liveNodes() {
			info := c.snapshot(i).Node
			if info.CommitIndex < leader.CommitIndex || info.LastIndex < leader.LastIndex {
				return false
			}
		}
		return true
	})
}

func identicalLogs(t *testing.T, want, got raft.NodeStateResponse, label string) {
	t.Helper()
	if len(got.Log) != len(want.Log) {
		t.Fatalf("%s log length = %d, leader = %d", label, len(got.Log), len(want.Log))
	}
	for j := range want.Log {
		a, b := want.Log[j], got.Log[j]
		if a.Index != b.Index || a.Term != b.Term || a.Command != b.Command || a.Key != b.Key {
			t.Fatalf("%s log[%d] = %+v, leader = %+v", label, j, b, a)
		}
		if (a.Value == nil) != (b.Value == nil) {
			t.Fatalf("%s log[%d] value presence differs: %+v vs %+v", label, j, b, a)
		}
		if a.Value != nil && *a.Value != *b.Value {
			t.Fatalf("%s log[%d] value = %q, leader = %q", label, j, *b.Value, *a.Value)
		}
	}
}
