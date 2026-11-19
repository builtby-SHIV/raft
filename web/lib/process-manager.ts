import { spawn, execFile, type ChildProcess } from "node:child_process";
import { promisify } from "node:util";
import fs from "node:fs";
import fsp from "node:fs/promises";
import path from "node:path";

import { NODES } from "./nodes";
import { findRepoRoot } from "./repo-root";
import type {
  ProcessLogLine,
  ProcessPhase,
  ProcessStatus,
} from "./types";

const execFileAsync = promisify(execFile);
const MAX_LOG_LINES = 300;

interface RuntimeEntry {
  child: ChildProcess | null;
  phase: ProcessPhase;
  pid?: number;
  startedAt?: number;
  exitedAt?: number;
  exitCode?: number | null;
  lastError?: string;
  intentionalStop: boolean;
  logs: ProcessLogLine[];
}

interface ManagerState {
  initialized: boolean;
  entries: Map<number, RuntimeEntry>;
  buildPromise: Promise<string> | null;
  root: string;
  demoDir: string;
  binaryPath: string;
  pidsFile: string;
}

const globalStore = globalThis as unknown as {
  __raftDemoManager?: ManagerState;
};

const SOURCE_DIRS = ["raft", "proto", "storage"];
const SOURCE_FILES = ["main.go", "go.mod", "go.sum"];

function sleep(ms: number) {
  return new Promise<void>((resolve) => setTimeout(resolve, ms));
}

function emptyEntry(): RuntimeEntry {
  return {
    child: null,
    phase: "stopped",
    intentionalStop: false,
    logs: [],
  };
}

function manager(): ManagerState {
  if (globalStore.__raftDemoManager) {
    return globalStore.__raftDemoManager;
  }

  const root = findRepoRoot();
  const demoDir = path.join(root, ".raft-demo");
  const state: ManagerState = {
    initialized: false,
    entries: new Map(NODES.map((node) => [node.id, emptyEntry()])),
    buildPromise: null,
    root,
    demoDir,
    binaryPath: path.join(
      demoDir,
      process.platform === "win32" ? "raftnode.exe" : "raftnode",
    ),
    pidsFile: path.join(demoDir, "pids.json"),
  };
  globalStore.__raftDemoManager = state;
  return state;
}

function entryFor(nodeId: number): RuntimeEntry {
  const state = manager();
  let entry = state.entries.get(nodeId);
  if (!entry) {
    entry = emptyEntry();
    state.entries.set(nodeId, entry);
  }
  return entry;
}

function pushLog(
  entry: RuntimeEntry,
  stream: "stdout" | "stderr",
  text: string,
) {
  const trimmed = text.replace(/\s+$/, "");
  if (!trimmed) {
    return;
  }
  entry.logs.push({ t: Date.now(), stream, text: trimmed });
  if (entry.logs.length > MAX_LOG_LINES) {
    entry.logs.splice(0, entry.logs.length - MAX_LOG_LINES);
  }
}

function pipeLines(
  child: ChildProcess,
  stream: "stdout" | "stderr",
  entry: RuntimeEntry,
) {
  const source = stream === "stdout" ? child.stdout : child.stderr;
  if (!source) {
    return;
  }
  let buffer = "";
  source.setEncoding("utf8");
  source.on("data", (chunk: string) => {
    buffer += chunk;
    const lines = buffer.split(/\r?\n/);
    buffer = lines.pop() ?? "";
    for (const line of lines) {
      pushLog(entry, stream, line);
    }
  });
  source.on("end", () => {
    if (buffer.trim()) {
      pushLog(entry, stream, buffer);
      buffer = "";
    }
  });
}

function writePids() {
  const state = manager();
  const pids: Record<string, number> = {};
  for (const [nodeId, entry] of state.entries) {
    if (entry.pid && entry.phase !== "stopped" && entry.phase !== "crashed") {
      pids[String(nodeId)] = entry.pid;
    }
  }
  try {
    fs.mkdirSync(state.demoDir, { recursive: true });
    fs.writeFileSync(state.pidsFile, JSON.stringify(pids, null, 2));
  } catch {
    // Best effort: PID cleanup is a convenience, not a requirement.
  }
}

function killStaleProcesses() {
  const state = manager();
  if (!fs.existsSync(state.pidsFile)) {
    return;
  }
  try {
    const raw = fs.readFileSync(state.pidsFile, "utf8");
    const pids = JSON.parse(raw) as Record<string, number>;
    for (const pid of Object.values(pids)) {
      if (!Number.isInteger(pid) || pid <= 0) {
        continue;
      }
      try {
        process.kill(pid, 0);
        process.kill(pid);
        console.warn(`[raft-demo] stopped orphaned node process ${pid}`);
      } catch {
        // Already gone.
      }
    }
  } catch {
    // Ignore malformed pid files.
  }
  try {
    fs.unlinkSync(state.pidsFile);
  } catch {
    // Ignore.
  }
}

function initialize() {
  const state = manager();
  if (state.initialized) {
    return;
  }
  state.initialized = true;
  killStaleProcesses();
}

async function collectSourceMtime(dir: string, latest: { value: number }) {
  const full = path.isAbsolute(dir) ? dir : path.join(manager().root, dir);
  if (!fs.existsSync(full)) {
    return;
  }
  const stat = await fsp.stat(full);
  if (stat.isFile()) {
    latest.value = Math.max(latest.value, stat.mtimeMs);
    return;
  }
  const entries = await fsp.readdir(full, { withFileTypes: true });
  for (const entry of entries) {
    if (entry.name === "node_modules" || entry.name === ".git") {
      continue;
    }
    const child = path.join(full, entry.name);
    const childStat = await fsp.stat(child);
    if (childStat.isDirectory()) {
      await collectSourceMtime(child, latest);
    } else if (entry.name.endsWith(".go")) {
      latest.value = Math.max(latest.value, childStat.mtimeMs);
    }
  }
}

async function newestSourceMtime(): Promise<number> {
  const latest = { value: 0 };
  for (const dir of SOURCE_DIRS) {
    await collectSourceMtime(dir, latest);
  }
  for (const file of SOURCE_FILES) {
    await collectSourceMtime(file, latest);
  }
  return latest.value;
}

function anyProcessActive(): boolean {
  return [...manager().entries.values()].some(
    (entry) =>
      entry.phase === "running" ||
      entry.phase === "starting" ||
      entry.phase === "stopping",
  );
}

async function ensureBinary(force = false): Promise<string> {
  const state = manager();
  initialize();

  if (state.buildPromise) {
    return state.buildPromise;
  }

  state.buildPromise = (async () => {
    await fsp.mkdir(state.demoDir, { recursive: true });

    const binaryExists = fs.existsSync(state.binaryPath);
    if (!force && binaryExists) {
      // Cannot overwrite a running Windows executable, so only rebuild when
      // the cluster is idle and a source file is newer than the binary.
      if (anyProcessActive()) {
        return state.binaryPath;
      }
      const sourceMtime = await newestSourceMtime();
      const binaryMtime = (await fsp.stat(state.binaryPath)).mtimeMs;
      if (binaryMtime >= sourceMtime) {
        return state.binaryPath;
      }
    }

    try {
      await execFileAsync("go", ["build", "-o", state.binaryPath, "."], {
        cwd: state.root,
        timeout: 180_000,
        windowsHide: true,
        maxBuffer: 20 * 1024 * 1024,
      });
    } catch (error) {
      const err = error as { stderr?: string; stdout?: string; message?: string };
      const output = [err.stderr, err.stdout, err.message]
        .filter(Boolean)
        .join("\n")
        .trim();
      throw new Error(
        `go build failed in ${state.root}${output ? `:\n${output}` : ""}`,
      );
    }
    return state.binaryPath;
  })();

  try {
    return await state.buildPromise;
  } finally {
    state.buildPromise = null;
  }
}

export interface StartNodeOptions {
  forceBuild?: boolean;
}

export async function startNode(
  nodeId: number,
  options: StartNodeOptions = {},
): Promise<ProcessStatus> {
  initialize();
  const node = NODES.find((candidate) => candidate.id === nodeId);
  if (!node) {
    throw new Error(`Unknown node ${nodeId}`);
  }
  const entry = entryFor(nodeId);
  if (entry.child && (entry.phase === "running" || entry.phase === "starting")) {
    return statusFor(nodeId);
  }

  const binary = await ensureBinary(options.forceBuild ?? false);

  entry.phase = "starting";
  entry.intentionalStop = false;
  entry.exitCode = undefined;
  entry.exitedAt = undefined;
  entry.lastError = undefined;
  entry.logs = [
    {
      t: Date.now(),
      stream: "stdout",
      text: `$ raftnode -id ${node.id} -http :${node.httpPort} -tcp :${node.tcpPort} -role ${node.role}`,
    },
  ];

  const child = spawn(
    binary,
    [
      "-id",
      String(node.id),
      "-http",
      `:${node.httpPort}`,
      "-tcp",
      `:${node.tcpPort}`,
      "-role",
      node.role,
    ],
    {
      cwd: manager().root,
      windowsHide: true,
      stdio: ["ignore", "pipe", "pipe"],
      env: { ...process.env },
    },
  );

  entry.child = child;
  entry.pid = child.pid;
  pipeLines(child, "stdout", entry);
  pipeLines(child, "stderr", entry);

  child.on("spawn", () => {
    if (entry.child !== child) {
      return;
    }
    entry.phase = "running";
    entry.startedAt = Date.now();
    writePids();
  });

  child.on("error", (error) => {
    if (entry.child !== child) {
      return;
    }
    entry.phase = "crashed";
    entry.lastError = error.message;
    pushLog(entry, "stderr", `spawn error: ${error.message}`);
    writePids();
  });

  child.on("exit", (code, signal) => {
    if (entry.child !== child) {
      return;
    }
    entry.child = null;
    entry.pid = undefined;
    entry.exitCode = code;
    entry.exitedAt = Date.now();
    entry.phase = entry.intentionalStop ? "stopped" : "crashed";
    pushLog(
      entry,
      "stderr",
      entry.intentionalStop
        ? `process stopped${signal ? ` (${signal})` : ""}`
        : `process exited with code ${code ?? "null"}${signal ? ` (${signal})` : ""}`,
    );
    writePids();
  });

  writePids();
  await sleep(40);
  return statusFor(nodeId);
}

export async function killNode(nodeId: number): Promise<ProcessStatus> {
  initialize();
  const entry = entryFor(nodeId);
  const child = entry.child;
  if (!child) {
    entry.phase = entry.phase === "crashed" ? "crashed" : "stopped";
    entry.pid = undefined;
    writePids();
    return statusFor(nodeId);
  }

  entry.intentionalStop = true;
  entry.phase = "stopping";

  const exited = new Promise<void>((resolve) => {
    child.once("exit", () => resolve());
  });

  try {
    child.kill("SIGKILL");
  } catch {
    // Fall through to taskkill on Windows.
  }

  const timedOut = await Promise.race([
    exited.then(() => false),
    sleep(1500).then(() => true),
  ]);

  if (timedOut && process.platform === "win32" && entry.pid) {
    try {
      await execFileAsync("taskkill", [
        "/PID",
        String(entry.pid),
        "/T",
        "/F",
      ]);
    } catch {
      // Ignore: the process may have exited in the meantime.
    }
  }

  if (entry.child === child) {
    entry.child = null;
    entry.pid = undefined;
    entry.exitedAt = Date.now();
    entry.phase = "stopped";
    pushLog(entry, "stderr", "process killed");
  }
  writePids();
  return statusFor(nodeId);
}

export async function restartNode(nodeId: number): Promise<ProcessStatus> {
  await killNode(nodeId);
  await sleep(250);
  return startNode(nodeId);
}

export async function startAll(): Promise<void> {
  initialize();
  await ensureBinary();
  // Start the followers first, then the leader, so the first AppendEntries
  // call already has live listeners to connect to.
  const ordered = [...NODES].sort((a, b) => {
    if (a.role === b.role) {
      return a.id - b.id;
    }
    return a.role === "leader" ? 1 : -1;
  });
  for (const node of ordered) {
    await startNode(node.id);
    await sleep(80);
  }
}

export async function stopAll(): Promise<void> {
  initialize();
  await Promise.all(NODES.map((node) => killNode(node.id)));
}

export async function restartAll(): Promise<void> {
  await stopAll();
  await startAll();
}

export async function wipeData(): Promise<void> {
  await stopAll();
  const root = manager().root;
  for (const node of NODES) {
    for (const dir of [
      path.join(root, "data", `node-${node.id}-${node.tcpPort}`),
      path.join(root, "data", `node-${node.id}`),
    ]) {
      await fsp.rm(dir, { recursive: true, force: true });
    }
  }
  try {
    await fsp.unlink(manager().pidsFile);
  } catch {
    // Ignore.
  }
}

function statusFor(nodeId: number): ProcessStatus {
  const entry = entryFor(nodeId);
  return {
    nodeId,
    phase: entry.phase,
    pid: entry.pid,
    startedAt: entry.startedAt,
    exitedAt: entry.exitedAt,
    exitCode: entry.exitCode,
    lastError: entry.lastError,
    logs: entry.logs.slice(-60),
  };
}

export function processStatuses(): ProcessStatus[] {
  initialize();
  return [...manager().entries.keys()]
    .sort((a, b) => a - b)
    .map((nodeId) => statusFor(nodeId));
}

export async function processStatusesWithFullLogs(): Promise<ProcessStatus[]> {
  const statuses = processStatuses();
  return statuses.map((status) => {
    const entry = entryFor(status.nodeId);
    return { ...status, logs: entry.logs.slice(-MAX_LOG_LINES) };
  });
}
