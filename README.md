# Raft Distributed Key-Value Store & Visualizer

A distributed, fault-tolerant key-value store built in Go implementing core components of the Raft consensus algorithm, paired with an interactive real-time cluster visualizer and process orchestrator built in Next.js and TypeScript.

---

## Table of Contents

- [Overview](#overview)
- [Architecture & Design](#architecture--design)
  - [Core Consensus Engine (`raft/`)](#core-consensus-engine-raft)
  - [Storage Engine & WAL (`storage/`)](#storage-engine--wal-storage)
  - [RPC Protocol (`proto/`)](#rpc-protocol-proto)
  - [HTTP API (`main.go`)](#http-api-maingo)
  - [Web Dashboard & Process Manager (`web/`)](#web-dashboard--process-manager-web)
- [Features](#features)
- [Current Implementation Status](#current-implementation-status)
- [Cluster Configuration & Topology](#cluster-configuration--topology)
- [Getting Started](#getting-started)
  - [Prerequisites](#prerequisites)
  - [Running the Web Visualizer (Recommended)](#running-the-web-visualizer-recommended)
  - [Running Nodes Manually from CLI](#running-nodes-manually-from-cli)
- [HTTP API Reference](#http-api-reference)
- [Directory Structure](#directory-structure)
- [Testing](#testing)

---

## Overview

This project implements a distributed consensus key-value store based on the Raft consensus protocol. It demonstrates how a multi-node cluster reliably replicates state across nodes, persists logs to disk via a Write-Ahead Log (WAL), safely commits state machine transitions through quorum consensus, and compacts history using log snapshots.

The project consists of two major sub-systems:
1. **Consensus & Storage Node Engine (Go)**: A high-performance Go application managing peer TCP connections, Protobuf RPC serialization, append-only disk logging with CRC validation, snapshots, and a RESTful client interface.
2. **Cluster Visualizer & Manager (Next.js & React)**: A web dashboard that allows operators to launch, stop, monitor, and query cluster nodes in real time, inspect synchronized logs, verify key-value state matrices, and observe cluster consensus events.

---

## Architecture & Design

```
+-------------------------------------------------------------+
|               Next.js Web Visualizer (Port 3000)            |
|   - Real-time Node Status       - Unified Log Matrix        |
|   - Process Manager / Controls  - Key-Value State Matrix    |
|   - Command Dispatcher          - Live Event Stream         |
+-------------------------------------------------------------+
                               |
               HTTP Polling & Command Execution
                               v
+-------------------------------------------------------------+
|                 Cluster Nodes (N1 - N5)                     |
|                                                             |
|   +-------------------+              +------------------+   |
|   |   Node 1 (Leader) |   TCP RPC    | Node 2 (Follower)|   |
|   |  HTTP: :8001      |<------------>| HTTP: :8002      |   |
|   |  TCP:  :7001      | AppendEntries| TCP:  :7002      |   |
|   +-------------------+ InstallSnap- +------------------+   |
|      |        |         shot            |         |         |
|      v        v                         v         v         |
|  [  WAL  ]  [ KV ]                  [  WAL  ]  [ KV ]       |
|  (wal.log)  (State)                 (wal.log)  (State)      |
+-------------------------------------------------------------+
```

### Core Consensus Engine (`raft/`)

- **NodeState (`raft/nodestate.go`)**: Central state container holding node identity, role (`leader`, `follower`, `candidate`), term, commit index, applied index, in-memory log entries, and the applied KV state machine.
- **Leader Operations (`runLeader`)**:
  - Periodically broadcasts heartbeat `AppendEntries` RPCs to all peers.
  - Accepts client mutations via `ClientCh`, assigns monotonically increasing log indices and terms, appends to the local WAL, and initiates parallel replication.
  - Maintains `nextIndex` and `matchIndex` per peer.
  - When an entry is replicated on a quorum (majority) of nodes, advances `CommitIndex`, applies the mutation to the local KV map, and responds to the waiting client request.
  - Detects lagging followers and streams snapshot chunks if the follower requires entries already truncated by log compaction.
- **Follower Operations (`runFollower`)**:
  - Listens for incoming `AppendEntries` and `InstallSnapShot` RPCs.
  - Validates term consistency and verifies log continuity matching `PrevLogIndex` and `PrevLogTerm`.
  - Appends valid entries to the local WAL, updates `CommitIndex` according to the leader's commit level, and applies entries to the local KV map.
  - Returns match/conflict indices to assist the leader in fast backtracking during log synchronization.
- **Transport (`raft/transport.go`, `raft/helpers.go`)**:
  - Asynchronous TCP transport using length-prefixed binary framing (1-byte message type + 8-byte uint64 length + Protobuf payload).
  - Background listener loop and connection reuse per peer.

### Storage Engine & WAL (`storage/`)

- **Storage Layout (`storage/layout.go`)**:
  - Keeps isolated directory structures per process: `data/node-<id>-<port>/`.
  - Subdirectories include `wal/` (active log), `snapshots/` (compacted states), `sstable/`, and `config/` (`config.json`).
- **Write-Ahead Log (`storage/wal.go`)**:
  - Structured binary format with CRC32 checksums for every entry.
  - Immediate `fsync` support or buffered asynchronous writes.
  - Automatic crash recovery: reads and verifies log entries on startup, discarding any corrupted suffix if a crash interrupted a write.
  - Hydrates in-memory log state upon node launch.
- **Snapshot Compaction**:
  - Automatically compacts log history when log length exceeds threshold (`SnapshotThreshold` = 10,000 entries).
  - Encodes point-in-time KV state and snapshot metadata (`LastIncludedIndex`, `LastIncludedTerm`).
  - Implements chunked streaming (`SnapshotChunk`) to send snapshots over TCP without memory exhaustion.

### RPC Protocol (`proto/`)

The wire format is defined via Protocol Buffers in `proto/serialization.proto`:
- **`AppendEntriesArgs` / `AppendEntriesReply`**: Used for leader heartbeats, log entry replication, and log consistency checks.
- **`InstallSnapShotArgs` / `InstallSnapShotReply`**: Used by the leader to stream snapshot chunks to lagging followers.
- **`RequestVoteArgs` / `RequestVoteReply`**: Protocol buffer definitions reserved for Raft leader elections.
- **`KVPair` / `RaftLog`**: Internal structures representing log mutations (`PUT`, `DELETE`).

### HTTP API (`main.go`)

Each node exposes an HTTP REST server for external client interactions and health inspections:
- Supports key-value manipulation (`/put`, `/get`, `/del`).
- Write requests (`/put`, `/del`) must be directed to the leader; requests sent to followers return `503 Service Unavailable` with `node is not the leader`.
- Exposes introspection endpoints (`/state`, `/events`) enabling visualizers to poll live status without mutating consensus state.

### Web Dashboard & Process Manager (`web/`)

- **Dashboard View (`components/dashboard.tsx`)**:
  - **Cluster Header**: Summary metrics showing cluster health, active term, quorum status, and active leader.
  - **Process Controls**: Controls to start, stop, or restart individual nodes, start all, stop all, and rebuild the Go binary.
  - **Command Panel**: Form to issue `PUT`, `GET`, and `DELETE` requests directly to any node in the cluster and inspect HTTP response payloads and execution latencies.
  - **Node Cards**: Displays volatile state for each node (term, role, commit index, applied index, log entry count, KV size, uptime).
  - **Log Matrix**: Side-by-side log comparison across all 5 nodes, highlighting uncommitted vs. committed entries.
  - **KV Matrix**: Cross-cluster comparison of the replicated state machine to ensure linearizable consistency.
  - **Event Feed**: Live event log showing cluster activities (`node.started`, `log.appended`, `log.committed`, etc.).
- **Process Orchestrator (`web/lib/process-manager.ts`)**:
  - Built-in Node.js process supervisor that builds the Go binary (`.raft-demo/raftnode.exe`) and manages child processes, recording stdout/stderr output into a circular buffer for log inspection.

---

## Features

- **Distributed Consensus Log Replication**: Leader accepts client commands, assigns indices, and replicates entries to followers via TCP.
- **Quorum-based Commits**: Entries are marked committed only after successful replication to a majority ($N/2 + 1$) of nodes.
- **Durable Write-Ahead Logging (WAL)**: All entries are written to disk with CRC32 integrity verification before replication ack.
- **Crash Recovery & Repair**: On boot, nodes scan the WAL, verify checksums, discard corrupted entries, and restore in-memory state.
- **Log Compaction & Snapshot Transfer**: Periodic snapshot creation truncates WAL log files. Leaders stream snapshots in chunks to lagging followers.
- **Role & State Machine Isolation**: Clean separation between Raft consensus layer, storage engine, and in-memory key-value state machine.
- **Observability Endpoints**: In-memory ring buffer tracking consensus events and read-only JSON snapshots of node internals.
- **Comprehensive Web Visualizer**: Interactive Next.js UI for real-time monitoring and cluster management.

---

## Current Implementation Status

> [!IMPORTANT]
> **Leader Election is currently NOT implemented.**
>
> - **Current Behavior**: Cluster node roles (`leader`, `follower`) are statically assigned at startup (e.g., Node 1 is initialized with `--role=leader`, and Nodes 2 through 5 are initialized with `--role=follower`).
> - **Candidate State**: `runCandidate` currently serves as a fallback that sleeps and reverts to `follower`, allowing nodes to continue processing incoming `AppendEntries` RPCs without entering election loops or deadlocking.
> - **Protocol Buffers**: While `RequestVoteArgs` and `RequestVoteReply` messages are declared in `proto/serialization.proto`, dynamic election voting and leader election triggers are left for future development.

---

## Cluster Configuration & Topology

The default cluster configuration consists of 5 nodes:

| Node ID | Role     | HTTP Port | TCP Port | Storage Directory            |
|---------|----------|-----------|----------|------------------------------|
| **1**   | Leader   | `:8001`   | `:7001`  | `data/node-1-7001`           |
| **2**   | Follower | `:8002`   | `:7002`  | `data/node-2-7002`           |
| **3**   | Follower | `:8003`   | `:7003`  | `data/node-3-7003`           |
| **4**   | Follower | `:8004`   | `:7004`  | `data/node-4-7004`           |
| **5**   | Follower | `:8005`   | `:7005`  | `data/node-5-7005`           |

---

## Getting Started

### Prerequisites

- **Go**: `1.22+` (configured with `go 1.26+` module settings)
- **Node.js**: `v18.17+` or `v20+`
- **npm** or compatible package manager

---

### Running the Web Visualizer (Recommended)

The web dashboard includes a built-in process manager that will automatically compile the Go binary and start the 5-node cluster.

1. Navigate to the `web` directory:
   ```bash
   cd web
   ```

2. Install frontend dependencies:
   ```bash
   npm install
   ```

3. Launch the development server:
   ```bash
   npm run dev
   ```

4. Open your browser and navigate to:
   ```
   http://localhost:3000
   ```

5. From the dashboard:
   - Click **Start All** to launch all five cluster nodes.
   - Use the **Command Panel** to submit `PUT`, `GET`, and `DELETE` operations.
   - Observe real-time log replication in the **Log Matrix** and state convergence in the **KV Matrix**.

---

### Running Nodes Manually from CLI

You can also run individual nodes directly from the command line:

```bash
# Terminal 1: Launch Node 1 as Leader
go run main.go --id=1 --http=:8001 --tcp=:7001 --role=leader

# Terminal 2: Launch Node 2 as Follower
go run main.go --id=2 --http=:8002 --tcp=:7002 --role=follower

# Terminal 3: Launch Node 3 as Follower
go run main.go --id=3 --http=:8003 --tcp=:7003 --role=follower

# Terminal 4: Launch Node 4 as Follower
go run main.go --id=4 --http=:8004 --tcp=:7004 --role=follower

# Terminal 5: Launch Node 5 as Follower
go run main.go --id=5 --http=:8005 --tcp=:7005 --role=follower
```

---

## HTTP API Reference

All requests accept and return JSON.

### 1. Write / Update Key (`POST /put`)
Appends a `PUT` operation to the leader's log and waits for quorum commit before responding.
*Must be sent to the current leader.*

```bash
curl -X POST http://localhost:8001/put \
  -H "Content-Type: application/json" \
  -d '{"Key": "greeting", "Val": "hello world"}'
```

**Response (`201 Created`):**
```json
{
  "Message": "Key-Val pair created"
}
```

### 2. Read Key (`GET /get`)
Reads a key from the node's committed key-value state machine.

```bash
curl "http://localhost:8001/get?key=greeting"
```

**Response (`200 OK`):**
```json
{
  "key": "greeting",
  "value": "hello world"
}
```

### 3. Delete Key (`DELETE /del`)
Appends a `DELETE` operation to the leader's log and waits for quorum commit before responding.
*Must be sent to the current leader.*

```bash
curl -X DELETE "http://localhost:8001/del?key=greeting"
```

**Response (`200 OK`):**
```json
{
  "msg": "Key successfully deleted"
}
```

### 4. Node State Snapshot (`GET /state`)
Returns a comprehensive read-only snapshot of the node's internal state.

```bash
curl "http://localhost:8001/state?limit=50"
```

**Response (`200 OK`):**
```json
{
  "node": {
    "id": 1,
    "role": "leader",
    "term": 0,
    "commitIndex": 3,
    "appliedIndex": 3,
    "lastIndex": 3,
    "lastTerm": 0,
    "httpAddr": ":8001",
    "tcpAddr": ":7001",
    "kvCount": 1
  },
  "leader": {
    "nextIndex": { ":7002": 4, ":7003": 4, ":7004": 4, ":7005": 4 },
    "matchIndex": { ":7002": 3, ":7003": 3, ":7004": 3, ":7005": 3 }
  },
  "log": [
    { "index": 1, "term": 0, "command": "PUT", "key": "greeting", "value": "hello world", "committed": true }
  ],
  "kv": {
    "greeting": "hello world"
  }
}
```

### 5. Event Stream (`GET /events`)
Returns recent consensus events recorded by the node's event bus.

```bash
curl "http://localhost:8001/events?since=0&limit=10"
```

---

## Directory Structure

```
raft/
|-- data/                         # Persistent storage directories for cluster nodes
|   |-- node-1-7001/
|   |   |-- config/config.json    # Persisted node metadata (term, role, ports)
|   |   |-- snapshots/            # Compacted log snapshots
|   |   `-- wal/wal.log           # Write-ahead log file
|   `-- ...
|-- proto/                        # Protobuf definitions & generated Go code
|   |-- helpers.go
|   |-- serialization.pb.go
|   `-- serialization.proto
|-- raft/                         # Core Raft consensus engine
|   |-- events.go                 # Consensus event bus & audit tracking
|   |-- helpers.go                # Framing, message types, network serialization
|   |-- nodestate.go              # Main consensus state machine & event loops
|   |-- nodestate_test.go         # Consensus unit and integration tests
|   |-- status.go                 # JSON status response models for monitoring
|   `-- transport.go              # TCP socket transport & connection management
|-- storage/                      # Persistence engine
|   |-- config.go                 # Configuration persistence & serialization
|   |-- layout.go                 # Directory layout management
|   |-- layout_test.go            # Layout tests
|   |-- wal.go                    # Write-ahead log with CRC32 & snapshotting
|   `-- wal_test.go               # WAL tests
|-- web/                          # Next.js interactive web visualizer
|   |-- app/                      # Next.js App Router (pages and API routes)
|   |-- components/               # React UI components (Dashboard, Matrices, Controls)
|   |-- lib/                      # Node.js process manager, types, and cluster client
|   |-- package.json
|   `-- tsconfig.json
|-- go.mod
|-- go.sum
|-- main.go                       # Server bootstrap and HTTP REST handler
|-- main_test.go                  # End-to-end HTTP and consensus tests
`-- README.md
```

---

## Testing

Run the full Go test suite covering consensus loops, snapshot streaming, log hydration, and WAL storage:

```bash
go test -v ./...
```
