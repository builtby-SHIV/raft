package raft

import (
	"sort"
	"time"
)

type LogEntryStatus struct {
	Index     uint64  `json:"index"`
	Term      uint64  `json:"term"`
	Command   string  `json:"command"`
	Key       string  `json:"key"`
	Value     *string `json:"value,omitempty"`
	Committed bool    `json:"committed"`
}

type LeaderStatus struct {
	NextIndex      map[string]uint64 `json:"nextIndex"`
	MatchIndex     map[string]uint64 `json:"matchIndex"`
	Inflight       map[string]bool   `json:"inflight"`
	PendingIndexes []uint64          `json:"pendingIndexes"`
}

type SnapshotStatus struct {
	LastIncludedIndex uint64 `json:"lastIncludedIndex"`
	LastIncludedTerm  uint64 `json:"lastIncludedTerm"`
}

type NodeInfo struct {
	ID                 uint64   `json:"id"`
	InstanceID         string   `json:"instanceId"`
	Role               string   `json:"role"`
	Term               uint64   `json:"term"`
	VotedFor           string   `json:"votedFor"`
	LeaderID           uint64   `json:"leaderId"`
	CommitIndex        uint64   `json:"commitIndex"`
	AppliedIndex       uint64   `json:"appliedIndex"`
	LastIndex          uint64   `json:"lastIndex"`
	LastTerm           uint64   `json:"lastTerm"`
	HTTPAddr           string   `json:"httpAddr"`
	TCPAddr            string   `json:"tcpAddr"`
	Peers              []string `json:"peers"`
	UptimeMs           int64    `json:"uptimeMs"`
	LastHeartbeatMs    int64    `json:"lastHeartbeatMs"`
	ElectionDeadlineMs int64    `json:"electionDeadlineMs"`
	ElectionTimeoutMs  int64    `json:"electionTimeoutMs"`
	KVCount            int      `json:"kvCount"`
}

type NodeStateResponse struct {
	Node     NodeInfo          `json:"node"`
	Leader   *LeaderStatus     `json:"leader,omitempty"`
	Log      []LogEntryStatus  `json:"log"`
	KV       map[string]string `json:"kv"`
	Snapshot SnapshotStatus    `json:"snapshot"`
	Events   []Event           `json:"events"`
}

func toLogEntryStatus(entry *appendEntry, commitIndex uint64) LogEntryStatus {
	status := LogEntryStatus{
		Index:     entry.index,
		Term:      entry.term,
		Committed: entry.index <= commitIndex,
	}
	if entry.data != nil {
		status.Command = commandName(entry.data.Command)
		status.Key = entry.data.Key
		if entry.data.Value != nil {
			value := *entry.data.Value
			status.Value = &value
		}
	}
	return status
}

func (ns *NodeState) SnapshotState(limit int) NodeStateResponse {
	resp := NodeStateResponse{
		Log: []LogEntryStatus{},
		KV:  map[string]string{},
	}

	ns.mu.RLock()
	role := ns.Role
	term := ns.RaftState.Term
	votedFor := ns.RaftState.VotedFor
	leaderID := ns.leaderID
	commitIndex := ns.CommitIndex
	appliedIndex := ns.appliedIndex
	lastHeartbeat := ns.lastHeartbeat
	electionDeadline := ns.electionDeadline
	electionTimeout := ns.electionTimeout
	instanceID := ns.instanceID
	kvCount := len(ns.kv)
	for key, value := range ns.kv {
		resp.KV[key] = value
	}
	var snapshotIndex, snapshotTerm uint64
	if ns.ssMetaData != nil {
		snapshotIndex = ns.ssMetaData.lastIncludedIndex
		snapshotTerm = ns.ssMetaData.lastIncludedTerm
	}

	start := 0
	if limit > 0 && len(ns.entries) > limit {
		start = len(ns.entries) - limit
	}
	logEntries := make([]*appendEntry, 0, len(ns.entries)-start)
	for _, entry := range ns.entries[start:] {
		if entry != nil {
			logEntries = append(logEntries, entry)
		}
	}
	lastIndex := ns.lastIndexLocked()
	lastTerm := ns.lastTermLocked()
	ns.mu.RUnlock()

	for _, entry := range logEntries {
		resp.Log = append(resp.Log, toLogEntryStatus(entry, commitIndex))
	}

	now := time.Now()
	info := NodeInfo{
		ID:                 ns.ID,
		InstanceID:         instanceID,
		Role:               role,
		Term:               term,
		VotedFor:           votedFor,
		LeaderID:           leaderID,
		CommitIndex:        commitIndex,
		AppliedIndex:       appliedIndex,
		LastIndex:          lastIndex,
		LastTerm:           lastTerm,
		HTTPAddr:           ns.tp.HTTPAddr,
		TCPAddr:            ns.tp.TCPAddr,
		Peers:              ns.peerList(),
		UptimeMs:           now.Sub(ns.startedAt).Milliseconds(),
		KVCount:            kvCount,
		ElectionTimeoutMs:  electionTimeout.Milliseconds(),
		LastHeartbeatMs:    0,
		ElectionDeadlineMs: 0,
	}
	if !lastHeartbeat.IsZero() {
		info.LastHeartbeatMs = lastHeartbeat.UnixMilli()
	}
	if !electionDeadline.IsZero() {
		info.ElectionDeadlineMs = electionDeadline.UnixMilli()
	}
	resp.Node = info
	resp.Snapshot = SnapshotStatus{
		LastIncludedIndex: snapshotIndex,
		LastIncludedTerm:  snapshotTerm,
	}

	ns.mu.RLock()
	ls := ns.ls
	ns.mu.RUnlock()
	if ls != nil {
		ls.mu.RLock()
		leader := &LeaderStatus{
			NextIndex:      make(map[string]uint64, len(ls.nextIndex)),
			MatchIndex:     make(map[string]uint64, len(ls.matchIndex)),
			Inflight:       make(map[string]bool, len(ls.inflight)),
			PendingIndexes: make([]uint64, 0, len(ls.pending)),
		}
		for addr, index := range ls.nextIndex {
			leader.NextIndex[addr] = index
		}
		for addr, index := range ls.matchIndex {
			leader.MatchIndex[addr] = index
		}
		for addr, inflight := range ls.inflight {
			leader.Inflight[addr] = inflight
		}
		for index := range ls.pending {
			leader.PendingIndexes = append(leader.PendingIndexes, index)
		}
		ls.mu.RUnlock()
		sort.Slice(leader.PendingIndexes, func(i, j int) bool {
			return leader.PendingIndexes[i] < leader.PendingIndexes[j]
		})
		resp.Leader = leader
	}

	resp.Events = ns.RecentEvents(0, 100)
	return resp
}

func (ns *NodeState) lastIndexLocked() uint64 {
	if len(ns.entries) > 0 {
		return ns.entries[len(ns.entries)-1].index
	}
	if ns.ssMetaData != nil {
		return ns.ssMetaData.lastIncludedIndex
	}
	return 0
}

func (ns *NodeState) lastTermLocked() uint64 {
	if len(ns.entries) > 0 {
		return ns.entries[len(ns.entries)-1].term
	}
	if ns.ssMetaData != nil {
		return ns.ssMetaData.lastIncludedTerm
	}
	return 0
}

func (ns *NodeState) lastLogInfo() (index uint64, term uint64) {
	ns.mu.RLock()
	defer ns.mu.RUnlock()
	return ns.lastIndexLocked(), ns.lastTermLocked()
}
