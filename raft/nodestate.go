package raft

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"sync"
	"time"

	"raft/proto"
	"raft/storage"

	googleproto "google.golang.org/protobuf/proto"
)

type RPCEvent struct {
	peer		string
	msgType 	byte
	sendBack	chan *SendBack
	msgRecv		googleproto.Message
	msgSent		googleproto.Message
}

type SendBack struct {
	msgType 	byte
	msgSent		googleproto.Message
}

type ClientCmd struct {
	Command    proto.Command
	Key, Value string
	Committed 	bool
}

type result struct {
	err error
}

type LeaderState struct {
	nextIndex	map[string]uint64
	matchIndex	map[string]uint64
	inflight	map[string]bool
	pending		map[uint64]chan result // client waiting on log index N
	mu			sync.RWMutex
}

type snapShotMetaData struct {
	lastIncludedTerm, lastIncludedIndex uint64
}

type appendEntry struct {
	data	*proto.KVPair
	index	uint64
	term	uint64
}

type NodeState struct {
	ID          uint64
	Role        string
	CommitIndex uint64
	statePath   string
	tp          *TCPTransport
	ls			*LeaderState
	replyCh     chan *RPCEvent // incoming requests from the forwarders
	ClientCh    chan *ClientCmd // client commands
	applyCh     chan []*proto.RaftLog
	stopCh      chan struct{}
	cfgDir		string
	entries		[]*appendEntry
	RaftState   *proto.RaftState
	Log         *storage.WAL
	ssMetaData	*snapShotMetaData
	ctx         context.Context
	cancel      context.CancelFunc
	mu          sync.RWMutex
}

const (
	RoleLeader 			  string = "leader"
	RoleFollower          string = "follower"
	RoleCandidate      	  string = "candidate"
	RoleUnknown        	  string = "unknown"
	nodeStateFile      		   = "raft_state.bin"
	WALDirectorySuffix 		   = "_wal"
	MAX_Entries			uint64 = 50
)

func NewNodeState(
	id uint64,
	httpAddr,
	tcpAddr,
	dir,
	role string,
	ctx context.Context,
	raftState *proto.RaftState) *NodeState {
	walDir := filepath.Join(dir, "wal")
	wal, err := storage.OpenWAL(walDir, true, 128000, 1000)
	if err != nil {
		// cancel()
		return nil
	}

	if raftState == nil {
		raftState = &proto.RaftState{
			Term:    0,
			VotedFor: "",
		}
	}

	newCtx, cancel := context.WithCancel(ctx)
	tp := NewTCPTransport(httpAddr, tcpAddr, newCtx)
	ns := &NodeState{
		ID:        id,
		Role:      role,
		RaftState: raftState,
		Log:       wal,
		tp:        tp,
		replyCh:   make(chan *RPCEvent, 100),
		ClientCh:  make(chan *ClientCmd, 100),
		stopCh:    make(chan struct{}),
		ctx:       newCtx,
		cancel:    cancel,
	}

	_ = ns.HydrateEntries()

	if role == RoleLeader {
		ns.InitLeaderState()
	}

	return ns
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
		if peerAddr == ns.tp.TCPAddr {
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

	walDir := filepath.Join(dataDir, "wal")
	wal, err := storage.OpenWAL(walDir, true, 128000, 1000)
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
					Term:    0,
					VotedFor: "",
				},
				Log:      wal,
				replyCh:  make(chan *RPCEvent, 100),
				ClientCh: make(chan *ClientCmd, 100),
			}
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
		statePath: statePath,
		RaftState: &rs,
		Log:       wal,
		replyCh:   make(chan *RPCEvent, 100),
		ClientCh:  make(chan *ClientCmd, 100),
	}
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
			Term:    0,
			VotedFor: "",
		}
	}

	payload := &proto.RaftState{
		Term:    ns.RaftState.Term,
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
	go ns.tp.ListenLoop(ns.tp.TCPAddr, ns.replyCh)
	for {
		switch ns.Role {
		case RoleLeader:
			ns.Role = ns.runLeader()
		case RoleFollower:
			ns.Role = ns.runFollower()
		case RoleCandidate:
			ns.Role = ns.runCandidate()
		}
	}
}

func (ns *NodeState) runCandidate() string {
	return RoleCandidate
}

func (ns *NodeState) runLeader() string {
	ns.InitLeaderState()

	interval := (time.Duration(float64(ns.ID) * 2.5)) * time.Second
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			ns.tp.mu.RLock()
			ns.formatAndSend(&proto.KVPair{})
			ns.tp.mu.RUnlock()
		case rp := <-ns.replyCh:
			ns.ls.inflight[rp.peer] = false
			switch rp.msgType {
				case TypeAppendEntriesReply:
				recv := (rp.msgRecv).(*proto.AppendEntriesReply)
				sent := (rp.msgSent).(*proto.AppendEntriesArgs)
				if recv.Term == ns.RaftState.Term {
					if recv.Success {
						ns.ls.matchIndex[rp.peer] = max(
							ns.ls.matchIndex[rp.peer], sent.PrevLogIndex + uint64(len(sent.Entries)))
						ns.ls.nextIndex[rp.peer] = ns.ls.matchIndex[rp.peer] + 1
						if ns.CommitIndex < ns.ls.matchIndex[ns.tp.TCPAddr] {
							var higherIdx, count uint64
							for _, idx := range ns.ls.matchIndex {
								if idx > higherIdx {
									higherIdx = idx
									count = 1
								} else {
									count += 1
								}
							}
							if count > uint64(len(ns.ls.matchIndex) / 2) {
								ns.CommitIndex += 1
								ns.ClientCh<- &ClientCmd{ Committed: true }
							}
						}
					} else {
						// conflictIndex is the last index in follower's log + 1
						ns.ls.nextIndex[rp.peer] = recv.ConflictIndex
						if err := ns.handleLaggingFollower(recv, rp); err != nil {
							fmt.Printf("Error in handling a lagging follower %v\n", err)
							ns.tp.cancel()
							break
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
					
					ns.tp.cancel()
					ns.Role = RoleFollower
					ns.RaftState.Term = recv.Term
					ns.RaftState.VotedFor = ""
					return RoleFollower
				}
			}
		case cmd := <-ns.ClientCh:
			var valPtr *string
			if cmd.Value != "" {
				v := cmd.Value
				valPtr = &v
			}
			entry := &proto.KVPair{
				Key:     cmd.Key,
				Value:   valPtr,
				Command: cmd.Command,
			}
			
			ns.ls.nextIndex[ns.tp.TCPAddr] += 1
			ns.ls.matchIndex[ns.tp.TCPAddr] += 1
			ns.entries = append(ns.entries, &appendEntry{ 
				data: entry, 
				term: ns.RaftState.Term, 
				index: ns.ls.matchIndex[ns.tp.TCPAddr],
			})
			
			ns.formatAndSend(entry)
		case <-ns.ctx.Done():
			ns.cancel()
			return ""
		}
	}
}

func (ns *NodeState) runFollower() string {
	lastDigit, err := strconv.ParseFloat(ns.tp.TCPAddr[len(ns.tp.TCPAddr) - 1:], 32)
	if err != nil {
		fmt.Printf("Error in parsing last digit %v\n", err)
	}
	interval := (time.Duration(lastDigit * 2.5)) * time.Second
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			// start election by becoming candidate
			return RoleCandidate
		case rp := <-ns.replyCh:
			// handle incoming messages
			switch rp.msgType {
				case TypeAppendEntries:
				recv := (rp.msgRecv).(*proto.AppendEntriesArgs)
				
				if recv.Term < ns.RaftState.Term {
					reply := &proto.AppendEntriesReply{
						Term: ns.RaftState.Term,
						ConflictIndex: 0,
						ConflictTerm: 0,
						Success: false,
					}
					rp.sendBack<- &SendBack{ msgType: TypeAppendEntriesReply, msgSent: reply }
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
						break
					}
					rp.sendBack<- &SendBack{ msgType: TypeAppendEntriesReply, msgSent: reply }
				} else {
					ticker.Reset(interval)
					reply, err := ns.handleAppendEntries(recv)
					if err != nil {
						fmt.Printf("Error in handling appendentriesargs %v\n", err)
						break
					}
					rp.sendBack<- &SendBack{ msgType: TypeAppendEntriesReply, msgSent: reply }
				}

			}
		case <-ns.ctx.Done():
			ns.cancel()
			return ""
		}
	}
}

// func (ns *NodeState) runCandidate() Role {
// }

func (ns *NodeState) formatAndSend(entry *proto.KVPair) {
	for addr, _ := range ns.tp.Peers {
		if addr != ns.tp.TCPAddr && !ns.ls.inflight[addr] {
			ns.ls.inflight[addr] = true

			prevLogIndex := ns.ls.nextIndex[addr] - 1
			var prevLogTerm uint64
			if len(ns.entries) > 0 && prevLogIndex >= ns.entries[0].index {
				for _, e := range ns.entries {
					if e.index == prevLogIndex {
						prevLogTerm = e.term
						break
					}
				}
				var entriesToSend []*proto.KVPair
				if entry != nil && (entry.Key != "" || entry.Command != proto.Command_PUT || entry.Value != nil) {
					entriesToSend = []*proto.KVPair{entry}
				}
				args := &proto.AppendEntriesArgs{
					Term:         ns.RaftState.Term,
					LeaderID:     ns.ID,
					PrevLogIndex: prevLogIndex,
					PrevLogTerm:  prevLogTerm,
					LeaderCommit: ns.CommitIndex,
					Entries:      entriesToSend,
				}
				go ns.tp.sendToPeers(addr, TypeAppendEntries, args, ns.replyCh)
			} else if prevLogIndex == 0 {
				var entriesToSend []*proto.KVPair
				if entry != nil && (entry.Key != "" || entry.Command != proto.Command_PUT || entry.Value != nil) {
					entriesToSend = []*proto.KVPair{entry}
				}
				args := &proto.AppendEntriesArgs{
					Term:         ns.RaftState.Term,
					LeaderID:     ns.ID,
					PrevLogIndex: 0,
					PrevLogTerm:  0,
					LeaderCommit: ns.CommitIndex,
					Entries:      entriesToSend,
				}
				go ns.tp.sendToPeers(addr, TypeAppendEntries, args, ns.replyCh)
			} else {
				// get prevLogIndex and prevLogTerm from raft log otherwise send snapshots
				_, firstIdx, err := ns.Log.GetFirstTermAndIndex()
				if err != nil {
					fmt.Printf("Error in retrieving first term and index from log %v\n", err)
					break
				}
				if firstIdx > 0 && prevLogIndex+1 >= firstIdx {
					entries, idx, term, err := ns.Log.GetEntriesFrom(prevLogIndex + 1)
					if err != nil {
						fmt.Printf("Error in retrieving last entries from log %v\n", err)
						break
					}
					
					for _, entry := range ns.entries {
						new_entry := &proto.KVPair{
							Key:     entry.data.Key,
							Value:   entry.data.Value,
							Command: entry.data.Command,
						}
						entries = append(entries, new_entry)
					}
		
					args := &proto.AppendEntriesArgs{
						Term: ns.RaftState.Term,
						LeaderID:     ns.ID,
						PrevLogIndex: idx,
						PrevLogTerm:  term,
						LeaderCommit: ns.CommitIndex,
						Entries:      entries,
					}
					
					go ns.tp.sendToPeers(addr, TypeAppendEntries, args, ns.replyCh)
				} else {
					term, idx, ch, err := ns.Log.StreamSnapshot()
					if err != nil {
						fmt.Printf("Error in streaming the snapshot %v\n", err)
						break
					}
					var entries []*proto.KVPair
					for {
						entry, ok := <-ch
						if ok {
							if len(entries) == 100 {
								args := &proto.InstallSnapShotArgs{
									Term:              ns.RaftState.Term,
									LeaderId:          ns.ID,
									LastIncludedIndex: idx,
									LastIncludedTerm:  term,
									Data:              entries,
									Done:              false,
								}
								go ns.tp.sendToPeers(addr, TypeInstallSnapShot, args, ns.replyCh)
								
								entry = nil
							}
							entries = append(entries, entry)
							continue
						}
						args := &proto.InstallSnapShotArgs{
							Term:              ns.RaftState.Term,
							LeaderId:          ns.ID,
							LastIncludedIndex: idx,
							LastIncludedTerm:  term,
							Data:              entries,
							Done:              true,
						}
						
						go ns.tp.sendToPeers(addr, TypeInstallSnapShot, args, ns.replyCh)
		
						break
					}
				}
			}
		}
	}
}

func (ns *NodeState) handleAppendEntries(recv *proto.AppendEntriesArgs) (*proto.AppendEntriesReply, error) {
	var reply *proto.AppendEntriesReply
	if ns.ssMetaData.lastIncludedIndex == recv.PrevLogIndex {
		// add to raft Log
		for idx, entry := range recv.Entries {
			ns.entries = append(ns.entries, &appendEntry{ 
				data: entry, 
				index: recv.PrevLogIndex + uint64(idx) + 1, 
				term: recv.Term, 
			})
		}

		reply = &proto.AppendEntriesReply{
			Term: ns.RaftState.Term,
			ConflictIndex: 0,
			ConflictTerm: 0,
			Success: true,
		}
	} else if ns.ssMetaData.lastIncludedIndex < recv.PrevLogIndex {
		// the leader's next index for this node is too far back so it should've sent a snapshot instead
		reply = &proto.AppendEntriesReply{
			Term: ns.RaftState.Term,
			ConflictIndex: ns.ssMetaData.lastIncludedIndex + 1,
			ConflictTerm: 0,
			Success: false,
		}
	} else {
		sliceIdx := recv.PrevLogIndex - ns.ssMetaData.lastIncludedIndex - 1
		if ns.entries[sliceIdx].index == recv.PrevLogIndex {
			// add to raft Log
			for idx, entry := range recv.Entries {
				ns.entries = append(ns.entries, &appendEntry{ 
					data: entry, 
					index: recv.PrevLogIndex + uint64(idx) + 1, 
					term: recv.Term,
				})
			}
	
			reply = &proto.AppendEntriesReply{
				Term: ns.RaftState.Term,
				ConflictIndex: 0,
				ConflictTerm: 0,
				Success: true,
			}
		} else if ns.entries[sliceIdx].index < recv.PrevLogIndex {
			reply = &proto.AppendEntriesReply{
				Term: ns.RaftState.Term,
				ConflictIndex: ns.entries[sliceIdx].index + 1,
				ConflictTerm: 0,
				Success: false,
			}
		} else {
			if ns.entries[sliceIdx].term != recv.PrevLogTerm {
				ns.Log.Repair(sliceIdx)
				ns.entries = slices.Delete(ns.entries, int(sliceIdx), len(ns.entries))
				// add to raft Log
				for idx, entry := range recv.Entries {
					ns.entries = append(ns.entries, &appendEntry{ 
						data: entry, 
						index: recv.PrevLogIndex + uint64(idx) + 1, 
						term: recv.Term,
					})
				}
			} else {
				for idx, entry := range recv.Entries {
			        targetIndex := recv.PrevLogIndex + uint64(idx) + 1
			        localSliceIdx := targetIndex - ns.ssMetaData.lastIncludedIndex - 1
			        
			        if localSliceIdx < uint64(len(ns.entries)) {
			            if ns.entries[localSliceIdx].term != recv.Term {
			                ns.Log.Repair(localSliceIdx)
			                ns.entries = slices.Delete(ns.entries, int(localSliceIdx), len(ns.entries))
			                
			                ns.entries = append(ns.entries, &appendEntry{
			                    data:  entry,
			                    index: targetIndex,
			                    term:  recv.Term,
			                })
			            } else {
			                continue
			            }
			        } else {
			            ns.entries = append(ns.entries, &appendEntry{
			                data:  entry,
			                index: targetIndex,
			                term:  recv.Term,
			            })
			        }
			    }
			}
		}
	}
	
	ns.CommitIndex = min(ns.CommitIndex, recv.LeaderCommit)
	return reply, nil
}

func (ns *NodeState) handleLaggingFollower(recv *proto.AppendEntriesReply, rp *RPCEvent) error {
	if ns.entries[0].index <= recv.ConflictIndex {
		jumptoIdx := recv.ConflictIndex - ns.entries[0].index
		var entries []*proto.KVPair
		for _, entry := range ns.entries[jumptoIdx:] {
			new_entry := &proto.KVPair{
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
				Term: ns.RaftState.Term,
				LeaderID:     ns.ID,
				PrevLogIndex: lastIdx,
				PrevLogTerm:  lastTerm,
				LeaderCommit: ns.CommitIndex,
				Entries:      entries,
			}

			rp.sendBack<- &SendBack{ msgType: TypeAppendEntries, msgSent: args }
		} else {
			args := &proto.AppendEntriesArgs{
				Term: ns.RaftState.Term,
				LeaderID:     ns.ID,
				PrevLogIndex: ns.entries[jumptoIdx - 1].index,
				PrevLogTerm:  ns.entries[jumptoIdx - 1].term,
				LeaderCommit: ns.CommitIndex,
				Entries:      entries,
			}
			rp.sendBack<- &SendBack{ msgType: TypeAppendEntries, msgSent: args }
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
					Key:     entry.data.Key,
					Value:   entry.data.Value,
					Command: entry.data.Command,
				}
				entries = append(entries, new_entry)
			}

			args := &proto.AppendEntriesArgs{
				Term: ns.RaftState.Term,
				LeaderID:     ns.ID,
				PrevLogIndex: idx,
				PrevLogTerm:  term,
				LeaderCommit: ns.CommitIndex,
				Entries:      entries,
			}

			rp.sendBack<- &SendBack{ msgType: TypeAppendEntries, msgSent: args }
		} else {
			// send snapshot
			term, idx, ch, err := ns.Log.StreamSnapshot()
			if err != nil {
				return err
			}
			var entries []*proto.KVPair
			for {
				entry, ok := <-ch
				if ok {
					if len(entries) == 100 {
						args := &proto.InstallSnapShotArgs{
							Term:              ns.RaftState.Term,
							LeaderId:          ns.ID,
							LastIncludedIndex: idx,
							LastIncludedTerm:  term,
							Data:              entries,
							Done:              false,
						}
						rp.sendBack<- &SendBack{
							msgType: TypeInstallSnapShot,
							msgSent: args,
						}
						
						entry = nil
					}
					entries = append(entries, entry)
					continue
				}
				args := &proto.InstallSnapShotArgs{
					Term:              ns.RaftState.Term,
					LeaderId:          ns.ID,
					LastIncludedIndex: idx,
					LastIncludedTerm:  term,
					Data:              entries,
					Done:              true,
				}
				
				rp.sendBack<- &SendBack{
					msgType: TypeInstallSnapShot,
					msgSent: args,
				}

				break
			}
		}
	}
	return nil
}