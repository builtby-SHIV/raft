package raft

import (
	"fmt"
	"os"
	"sort"
	"sync"
	"time"

	"raft/proto"
)

func newInstanceID() string {
	return fmt.Sprintf("%d-%d", os.Getpid(), time.Now().UnixNano())
}

func (ns *NodeState) currentTerm() uint64 {
	return ns.term.Load()
}

// Event is a single, human-readable thing that happened inside a node. The
// visualizer merges events from every node into one timeline so a reader can
// follow elections and log replication step by step.
type Event struct {
	Seq        uint64         `json:"seq"`
	InstanceID string         `json:"instanceId"`
	NodeID     uint64         `json:"nodeId"`
	Type       string         `json:"type"`
	Term       uint64         `json:"term"`
	Peer       string         `json:"peer,omitempty"`
	Message    string         `json:"message"`
	Detail     map[string]any `json:"detail,omitempty"`
	Time       int64          `json:"time"` // Unix milliseconds
}

type eventBus struct {
	mu     sync.Mutex
	next   uint64
	size   int
	buffer []Event
	subs   map[chan Event]struct{}
}

func newEventBus(size int) *eventBus {
	if size <= 0 {
		size = 256
	}
	return &eventBus{
		size:   size,
		buffer: make([]Event, 0, size),
		subs:   make(map[chan Event]struct{}),
	}
}

func (b *eventBus) emit(ev Event) Event {
	b.mu.Lock()
	b.next++
	ev.Seq = b.next
	b.buffer = append(b.buffer, ev)
	if len(b.buffer) > b.size {
		// Copy instead of re-slicing so the retained backing array does not
		// keep growing as events stream in.
		trimmed := make([]Event, b.size)
		copy(trimmed, b.buffer[len(b.buffer)-b.size:])
		b.buffer = trimmed
	}
	subs := make([]chan Event, 0, len(b.subs))
	for ch := range b.subs {
		subs = append(subs, ch)
	}
	b.mu.Unlock()

	for _, ch := range subs {
		select {
		case ch <- ev:
		default:
			// A slow reader must never block the consensus loop.
		}
	}
	return ev
}

func (b *eventBus) recent(since uint64, limit int) []Event {
	b.mu.Lock()
	defer b.mu.Unlock()
	if limit <= 0 || limit > b.size {
		limit = b.size
	}
	out := make([]Event, 0, minInt(limit, len(b.buffer)))
	for _, ev := range b.buffer {
		if ev.Seq <= since {
			continue
		}
		out = append(out, ev)
	}
	if len(out) > limit {
		out = out[len(out)-limit:]
	}
	return out
}

func (b *eventBus) subscribe() (<-chan Event, func()) {
	ch := make(chan Event, 32)
	b.mu.Lock()
	b.subs[ch] = struct{}{}
	b.mu.Unlock()
	cancel := func() {
		b.mu.Lock()
		if _, ok := b.subs[ch]; ok {
			delete(b.subs, ch)
			close(ch)
		}
		b.mu.Unlock()
	}
	return ch, cancel
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// emit records an event using the node's current term. It is safe to call
// while state locks are held: the event bus owns an independent mutex.
func (ns *NodeState) emit(eventType, peer, message string, detail map[string]any) {
	if ns.events == nil {
		return
	}
	ns.events.emit(Event{
		InstanceID: ns.instanceID,
		NodeID:     ns.ID,
		Type:       eventType,
		Term:       ns.currentTerm(),
		Peer:       peer,
		Message:    message,
		Detail:     detail,
		Time:       time.Now().UnixMilli(),
	})
}

// RecentEvents returns events newer than since, oldest first.
func (ns *NodeState) RecentEvents(since uint64, limit int) []Event {
	if ns.events == nil {
		return nil
	}
	return ns.events.recent(since, limit)
}

// SubscribeEvents streams newly emitted events. Stop must be called when the
// reader goes away.
func (ns *NodeState) SubscribeEvents() (<-chan Event, func()) {
	if ns.events == nil {
		ch := make(chan Event)
		close(ch)
		return ch, func() {}
	}
	return ns.events.subscribe()
}

// peerList returns the registered peer addresses in a stable order.
func (ns *NodeState) peerList() []string {
	ns.tp.mu.RLock()
	defer ns.tp.mu.RUnlock()
	peers := make([]string, 0, len(ns.tp.Peers))
	for addr := range ns.tp.Peers {
		if isSelfAddress(ns.tp.TCPAddr, addr) {
			continue
		}
		peers = append(peers, addr)
	}
	sort.Strings(peers)
	return peers
}

func (ns *NodeState) electionDeadlineSnapshot() (deadline time.Time, timeout time.Duration) {
	ns.mu.RLock()
	defer ns.mu.RUnlock()
	return ns.electionDeadline, ns.electionTimeout
}

func summarizeEntries(entries []*appendEntry) []map[string]any {
	out := make([]map[string]any, 0, len(entries))
	for _, entry := range entries {
		if entry == nil {
			continue
		}
		item := map[string]any{
			"index": entry.index,
			"term":  entry.term,
		}
		if entry.data != nil {
			item["key"] = entry.data.Key
			item["command"] = commandName(entry.data.Command)
			if entry.data.Value != nil {
				item["value"] = *entry.data.Value
			}
		}
		out = append(out, item)
	}
	return out
}

func commandName(cmd proto.Command) string {
	switch cmd {
	case proto.Command_PUT:
		return "PUT"
	case proto.Command_DELETE:
		return "DELETE"
	case proto.Command_WRITE_SST:
		return "WRITE_SST"
	default:
		return fmt.Sprintf("CMD_%d", int32(cmd))
	}
}
