import type {
  ClusterView,
  NodeView,
  RaftEvent,
  TimelineCategory,
  TimelineEvent,
} from "./types";

const MAX_TIMELINE = 180;

export function categorizeType(type: string): TimelineCategory {
  if (
    type.includes("vote") ||
    type.includes("election") ||
    type.includes("term") ||
    type.includes("role")
  ) {
    return "election";
  }
  if (
    type.includes("append") ||
    type.includes("commit") ||
    type.includes("log") ||
    type.includes("snapshot") ||
    type.includes("apply")
  ) {
    return "replication";
  }
  if (type.includes("client") || type.includes("command")) {
    return "client";
  }
  return "lifecycle";
}

function summarizeDetail(detail?: Record<string, unknown>): string | undefined {
  if (!detail) {
    return undefined;
  }
  if (Array.isArray(detail.entries)) {
    const entries = detail.entries as Array<Record<string, unknown>>;
    const first = entries[0];
    const last = entries[entries.length - 1];
    if (first && last) {
      return `entries ${first.index}–${last.index} (${entries.length})`;
    }
  }
  const parts = Object.entries(detail)
    .filter(([, value]) => value !== undefined && value !== null)
    .slice(0, 5)
    .map(([key, value]) => {
      if (typeof value === "object") {
        return undefined;
      }
      return `${key}=${String(value)}`;
    })
    .filter(Boolean);
  return parts.length ? parts.join(" · ") : undefined;
}

export function raftEventToTimeline(event: RaftEvent): TimelineEvent {
  return {
    id: `raft:${event.instanceId}:${event.seq}`,
    time: event.time,
    nodeId: event.nodeId,
    category: categorizeType(event.type),
    type: event.type,
    title: event.message,
    detail: summarizeDetail(event.detail),
    term: event.term,
  };
}

export function collectBackendEvents(cluster: ClusterView): TimelineEvent[] {
  const events: TimelineEvent[] = [];
  for (const view of cluster.nodes) {
    for (const event of view.state?.events ?? []) {
      events.push(raftEventToTimeline(event));
    }
  }
  return events;
}

function makeId(parts: Array<string | number | null | undefined>): string {
  return parts.filter((part) => part !== null && part !== undefined).join(":");
}

function findPrevious(prev: ClusterView, nodeId: number): NodeView | undefined {
  return prev.nodes.find((node) => node.config.id === nodeId);
}

/**
 * Derive a human timeline from differences between two state snapshots. This
 * complements the node's own event stream and makes replication visible even
 * when the backend only exposes state.
 */
export function diffCluster(
  prev: ClusterView | null,
  next: ClusterView,
): TimelineEvent[] {
  if (!prev) {
    return [];
  }
  const events: TimelineEvent[] = [];
  const now = Date.now();

  for (const view of next.nodes) {
    const previous = findPrevious(prev, view.config.id);
    if (!previous) {
      continue;
    }
    const node = view.state;
    const before = previous.state;

    // Process lifecycle transitions.
    if (previous.process.phase !== view.process.phase) {
      const phase = view.process.phase;
      if (phase === "stopped" || phase === "crashed") {
        events.push({
          id: makeId([
            "proc",
            view.config.id,
            phase,
            view.process.exitedAt,
            view.process.startedAt,
          ]),
          time: view.process.exitedAt ?? now,
          nodeId: view.config.id,
          category: "lifecycle",
          type: phase === "crashed" ? "process.crashed" : "process.stopped",
          title:
            phase === "crashed"
              ? `${view.config.name} crashed${view.process.exitCode !== undefined && view.process.exitCode !== null ? ` (exit ${view.process.exitCode})` : ""}`
              : `${view.config.name} process killed`,
          detail: view.process.lastError,
        });
      } else if (phase === "running" && previous.process.phase !== "running") {
        events.push({
          id: makeId([
            "proc",
            view.config.id,
            "running",
            view.process.startedAt,
          ]),
          time: view.process.startedAt ?? now,
          nodeId: view.config.id,
          category: "lifecycle",
          type: "process.started",
          title: `${view.config.name} process running`,
          detail: view.process.pid ? `pid ${view.process.pid}` : undefined,
        });
      }
    }

    if (!node || !before) {
      continue;
    }

    // Identity changes mean a brand-new process reused the same node id.
    if (before.node.instanceId !== node.node.instanceId) {
      events.push({
        id: makeId([
          "instance",
          view.config.id,
          node.node.instanceId,
        ]),
        time: now,
        nodeId: view.config.id,
        category: "lifecycle",
        type: "node.restarted",
        title: `${view.config.name} recovered from disk`,
        detail: `term ${node.node.term} · log ${node.node.lastIndex} entries`,
        term: node.node.term,
      });
    }

    // Role or term changes are the visible part of a Raft election.
    if (
      before.node.role !== node.node.role ||
      before.node.term !== node.node.term
    ) {
      events.push({
        id: makeId([
          "role",
          view.config.id,
          node.node.role,
          node.node.term,
          node.node.instanceId,
        ]),
        time: now,
        nodeId: view.config.id,
        category: "election",
        type: "role.change",
        title: `${view.config.name} is now ${node.node.role} in term ${node.node.term}`,
        detail:
          node.node.votedFor && node.node.votedFor !== "0"
            ? `votedFor=${node.node.votedFor}`
            : undefined,
        term: node.node.term,
      });
    }

    // New entries appended to a local WAL.
    if (node.node.lastIndex > before.node.lastIndex) {
      const delta = node.node.lastIndex - before.node.lastIndex;
      events.push({
        id: makeId([
          "append",
          view.config.id,
          node.node.instanceId,
          node.node.lastIndex,
        ]),
        time: now,
        nodeId: view.config.id,
        category: "replication",
        type: "log.appended",
        title: `${view.config.name} appended ${delta} ${delta === 1 ? "entry" : "entries"} (last index ${node.node.lastIndex})`,
        term: node.node.term,
      });
    }

    // Commit index advances.
    if (node.node.commitIndex > before.node.commitIndex) {
      const delta = node.node.commitIndex - before.node.commitIndex;
      events.push({
        id: makeId([
          "commit",
          view.config.id,
          node.node.instanceId,
          node.node.commitIndex,
        ]),
        time: now,
        nodeId: view.config.id,
        category: "replication",
        type: "commit.advance",
        title: `${view.config.name} committed up to index ${node.node.commitIndex}`,
        detail: `+${delta} ${delta === 1 ? "entry" : "entries"}`,
        term: node.node.term,
      });
    }
  }

  return events;
}

export function mergeTimeline(
  current: TimelineEvent[],
  incoming: TimelineEvent[],
): TimelineEvent[] {
  if (incoming.length === 0) {
    return current;
  }
  const byId = new Map<string, TimelineEvent>();
  for (const event of current) {
    byId.set(event.id, event);
  }
  let changed = false;
  for (const event of incoming) {
    if (!byId.has(event.id)) {
      byId.set(event.id, event);
      changed = true;
    }
  }
  if (!changed) {
    return current;
  }
  return [...byId.values()]
    .sort((a, b) => b.time - a.time)
    .slice(0, MAX_TIMELINE);
}

export function makeClientEvent(
  nodeId: number,
  title: string,
  detail?: string,
  term?: number,
): TimelineEvent {
  return {
    id: makeId(["client", nodeId, Date.now(), Math.random()]),
    time: Date.now(),
    nodeId,
    category: "client",
    type: "client.command",
    title,
    detail,
    term,
  };
}
