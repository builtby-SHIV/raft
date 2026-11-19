export type LogCommand = "PUT" | "DELETE" | "WRITE_SST" | string;

export interface RaftEvent {
  seq: number;
  instanceId: string;
  nodeId: number;
  type: string;
  term: number;
  peer?: string;
  message: string;
  detail?: Record<string, unknown>;
  time: number;
}

export type ProcessPhase =
  | "starting"
  | "running"
  | "stopping"
  | "stopped"
  | "crashed";

export interface ProcessLogLine {
  t: number;
  stream: "stdout" | "stderr";
  text: string;
}

export interface ProcessStatus {
  nodeId: number;
  phase: ProcessPhase;
  pid?: number;
  startedAt?: number;
  exitedAt?: number;
  exitCode?: number | null;
  lastError?: string;
  logs: ProcessLogLine[];
}

export interface NodeConfig {
  id: number;
  name: string;
  httpPort: number;
  tcpPort: number;
  role: "leader" | "follower";
}

export interface LogEntry {
  index: number;
  term: number;
  command: LogCommand;
  key: string;
  value?: string | null;
  committed: boolean;
}

export interface LeaderCursors {
  nextIndex: Record<string, number>;
  matchIndex: Record<string, number>;
  inflight: Record<string, boolean>;
  pendingIndexes: number[];
}

export interface NodeInfo {
  id: number;
  instanceId: string;
  role: "leader" | "follower" | "candidate" | "unknown";
  term: number;
  votedFor: string;
  leaderId: number;
  commitIndex: number;
  appliedIndex: number;
  lastIndex: number;
  lastTerm: number;
  httpAddr: string;
  tcpAddr: string;
  peers: string[];
  uptimeMs: number;
  lastHeartbeatMs: number;
  electionDeadlineMs: number;
  electionTimeoutMs: number;
  kvCount: number;
}

export interface SnapshotMeta {
  lastIncludedIndex: number;
  lastIncludedTerm: number;
}

export interface NodeSnapshot {
  node: NodeInfo;
  leader?: LeaderCursors;
  log: LogEntry[];
  kv: Record<string, string>;
  snapshot: SnapshotMeta;
  events: RaftEvent[];
}

export interface NodeView {
  config: NodeConfig;
  process: ProcessStatus;
  state: NodeSnapshot | null;
  error?: string;
}

export interface ClusterView {
  nodes: NodeView[];
  leaderId: number | null;
  term: number;
  quorum: number;
  generatedAt: number;
}

export type TimelineCategory =
  | "election"
  | "replication"
  | "client"
  | "lifecycle";

export interface TimelineEvent {
  id: string;
  time: number;
  nodeId: number;
  category: TimelineCategory;
  type: string;
  title: string;
  detail?: string;
  term?: number;
}

export interface CommandResult {
  ok: boolean;
  op: string;
  targetNodeId?: number;
  leaderId?: number | null;
  status?: number;
  message: string;
  results?: Array<{
    nodeId: number;
    status: number;
    value?: string;
    error?: string;
  }>;
}
