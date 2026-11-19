# Raft Live Lab

A Next.js dashboard for the Go Raft node in this repository. It runs a fixed
five-process cluster and visualizes replication, quorum commits, crashes and
catch-up while you PUT, GET and DELETE keys.

## Run it

```bash
cd web
npm install
npm run dev        # http://localhost:3000
```

The first page load builds the Go binary into `.raft-demo/raftnode.exe`
(repository root) and starts five processes:

| node | role     | HTTP  | RPC   |
| ---- | -------- | ----- | ----- |
| N1   | leader   | :8001 | :7001 |
| N2   | follower | :8002 | :7002 |
| N3   | follower | :8003 | :7003 |
| N4   | follower | :8004 | :7004 |
| N5   | follower | :8005 | :7005 |

WAL and snapshot data lives in `<repo>/data/node-<id>-<rpcPort>`.

The binary is rebuilt automatically when a Go source file is newer than the
binary **and** no node process is running, so use **restart all** after changing
the Go code. The binary cannot be replaced while Windows has it open.

## What the UI shows

- **Node cards** — role, term, commit/last index, applied keys, a per-term log
  strip, leader replication cursors (`matchIndex`/`nextIndex`) and kill /
  restart / logs buttons.
- **Replicated log** — one row per log index, one column per process. A cell
  appears when that node persists the entry; it turns solid once the leader's
  commit index covers the row.
- **State machine** — the key/value pairs each node has applied, with stale or
  missing values flagged.
- **Consensus timeline** — `node.started` events from the backend plus events
  derived from state diffs (log appends, commit advances, process lifecycle).
- **Client console** — PUT/DELETE are routed to the leader (or to the node you
  pick, to see the leader-only rejection); GET fans out to all five nodes.

## Cluster behavior

Roles are fixed at startup (`-role leader|follower`), so N1 stays leader until
its process stops and **elections are intentionally not implemented**. Killing
N1 stops all writes; restarting it resumes them. Killing a follower keeps writes
working as long as a quorum (3 of 5) is alive, and the restarted follower
catches up on the next AppendEntries round.

**Wipe cluster data** deletes the WAL directories after a confirmation prompt.

## APIs used by the UI

The Go node exposes two read-only endpoints in addition to `/put`, `/get`,
`/del`:

- `GET /state?limit=250` — node info, log entries, applied KV, snapshot
  metadata, leader cursors.
- `GET /events?since=<seq>` — recent consensus events.

The Next.js server proxies them through `/api/cluster`, owns the child
processes through `/api/processes`, and routes commands through `/api/command`.
