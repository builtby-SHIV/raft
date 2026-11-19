import { NODES, QUORUM, nodeBaseUrl, type NodeConfig } from "./nodes";
import { processStatuses } from "./process-manager";
import type {
  ClusterView,
  NodeSnapshot,
  NodeView,
  ProcessStatus,
} from "./types";

const STATE_TIMEOUT_MS = 1200;

export interface FetchedNodeState {
  state: NodeSnapshot | null;
  error?: string;
}

export async function fetchNodeSnapshot(
  node: NodeConfig,
  limit = 250,
): Promise<FetchedNodeState> {
  try {
    const response = await fetch(
      `${nodeBaseUrl(node)}/state?limit=${Math.max(0, limit)}`,
      {
        cache: "no-store",
        headers: { accept: "application/json" },
        signal: AbortSignal.timeout(STATE_TIMEOUT_MS),
      },
    );
    if (!response.ok) {
      return { state: null, error: `HTTP ${response.status}` };
    }
    return { state: (await response.json()) as NodeSnapshot };
  } catch {
    return { state: null, error: "unreachable" };
  }
}

function stoppedStatus(nodeId: number): ProcessStatus {
  return {
    nodeId,
    phase: "stopped",
    logs: [],
  };
}

export async function getClusterView(limit = 250): Promise<ClusterView> {
  const statuses = new Map(
    processStatuses().map((status) => [status.nodeId, status]),
  );

  const fetched = await Promise.all(
    NODES.map((node) => fetchNodeSnapshot(node, limit)),
  );

  const nodes: NodeView[] = NODES.map((config, index) => ({
    config,
    process: statuses.get(config.id) ?? stoppedStatus(config.id),
    state: fetched[index].state,
    error: fetched[index].error,
  }));

  const leaderView = nodes.find(
    (view) => view.state?.node.role === "leader",
  );
  const leaderId = leaderView?.config.id ?? null;

  const terms = nodes.map((view) => view.state?.node.term ?? 0);
  const term = Math.max(...terms, 0);

  return {
    nodes,
    leaderId,
    term,
    quorum: QUORUM,
    generatedAt: Date.now(),
  };
}

export interface LeaderLookup {
  config: NodeConfig;
  state: NodeSnapshot;
}

export async function findLeader(): Promise<LeaderLookup | null> {
  const fetched = await Promise.all(
    NODES.map((node) => fetchNodeSnapshot(node, 1)),
  );
  for (let index = 0; index < NODES.length; index += 1) {
    const state = fetched[index].state;
    if (state?.node.role === "leader") {
      return { config: NODES[index], state };
    }
  }
  return null;
}

export function sleep(ms: number) {
  return new Promise<void>((resolve) => setTimeout(resolve, ms));
}
