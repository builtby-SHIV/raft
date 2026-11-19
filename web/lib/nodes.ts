export type NodeRole = "leader" | "follower";

export interface NodeConfig {
  id: number;
  name: string;
  httpPort: number;
  tcpPort: number;
  role: NodeRole;
}

// The demo cluster is deliberately static: one leader and four followers.
// Every process also learns the full peer list from raft.DefaultPeers.
export const NODES: NodeConfig[] = [1, 2, 3, 4, 5].map((id) => ({
  id,
  name: `N${id}`,
  httpPort: 8000 + id,
  tcpPort: 7000 + id,
  role: id === 1 ? "leader" : "follower",
}));

export const LEADER_ID = 1;
export const QUORUM = Math.floor(NODES.length / 2) + 1;

export function nodeById(id: number): NodeConfig | undefined {
  return NODES.find((node) => node.id === id);
}

export function nodeByTcpAddr(addr: string): NodeConfig | undefined {
  const port = Number(addr.split(":").pop());
  return NODES.find((node) => node.tcpPort === port);
}

export function nodeBaseUrl(node: NodeConfig): string {
  return `http://127.0.0.1:${node.httpPort}`;
}
