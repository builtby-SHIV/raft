"use client";

import { useCallback, useEffect, useRef, useState } from "react";

import type {
  ClusterView,
  CommandResult,
  ProcessLogLine,
  TimelineEvent,
} from "@/lib/types";
import {
  collectBackendEvents,
  diffCluster,
  makeClientEvent,
  mergeTimeline,
} from "@/lib/timeline";
import { ClusterHeader } from "./cluster-header";
import { CommandPanel } from "./command-panel";
import { EventFeed } from "./event-feed";
import { KeyValueMatrix } from "./kv-matrix";
import { LogMatrix } from "./log-matrix";
import { LogModal } from "./log-modal";
import { NodeCard } from "./node-card";

type HeaderAction =
  | "startAll"
  | "stopAll"
  | "restartAll"
  | "killLeader"
  | "wipe";

interface ToastState {
  id: number;
  kind: "ok" | "error" | "info";
  message: string;
}

const SEED_COLORS = ["teal", "amber", "pink", "violet", "sky"];

export function Dashboard() {
  const [cluster, setCluster] = useState<ClusterView | null>(null);
  const clusterRef = useRef<ClusterView | null>(null);
  const [timeline, setTimeline] = useState<TimelineEvent[]>([]);
  const [bootError, setBootError] = useState<string | null>(null);
  const [busy, setBusy] = useState<string | null>(null);
  const [toast, setToast] = useState<ToastState | null>(null);
  const [pendingOp, setPendingOp] = useState<"put" | "delete" | "get" | null>(
    null,
  );
  const [seeding, setSeeding] = useState(false);
  const [commandResult, setCommandResult] = useState<CommandResult | null>(null);
  const [logNodeId, setLogNodeId] = useState<number | null>(null);
  const [logLines, setLogLines] = useState<ProcessLogLine[]>([]);
  const [logsLoading, setLogsLoading] = useState(false);
  const autoStartRef = useRef(false);

  const notify = useCallback(
    (kind: ToastState["kind"], message: string) => {
      setToast({ id: Date.now(), kind, message });
    },
    [],
  );

  const applyCluster = useCallback((next: ClusterView) => {
    const previous = clusterRef.current;
    clusterRef.current = next;
    setCluster(next);
    const backend = collectBackendEvents(next);
    const derived = diffCluster(previous, next);
    setTimeline((current) =>
      mergeTimeline(current, [...backend, ...derived]),
    );
  }, []);

  const refresh = useCallback(async () => {
    try {
      const response = await fetch("/api/cluster", { cache: "no-store" });
      if (!response.ok) {
        let message = `Cluster API returned ${response.status}`;
        try {
          const payload = (await response.json()) as { error?: string };
          if (payload.error) {
            message = payload.error;
          }
        } catch {
          // Keep the HTTP message.
        }
        throw new Error(message);
      }
      const data = (await response.json()) as ClusterView;
      applyCluster(data);
      setBootError(null);
    } catch (error) {
      setBootError(
        error instanceof Error ? error.message : "Could not reach the cluster API",
      );
    }
  }, [applyCluster]);

  const runAction = useCallback(
    async (
      action: string,
      nodeId?: number,
      options: { quiet?: boolean } = {},
    ) => {
      const busyKey = nodeId ? `${action}:${nodeId}` : action;
      setBusy(busyKey);
      try {
        const response = await fetch("/api/processes", {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: JSON.stringify({ action, nodeId }),
        });
        const payload = (await response.json()) as {
          error?: string;
          cluster?: ClusterView;
        };
        if (!response.ok) {
          throw new Error(payload.error ?? `Action failed (${response.status})`);
        }
        if (payload.cluster) {
          applyCluster(payload.cluster);
        }
        if (action === "wipe") {
          setTimeline([]);
          setCommandResult(null);
          setLogNodeId(null);
        }
        if (!options.quiet) {
          const labels: Record<string, string> = {
            startAll: "Starting five node processes…",
            stopAll: "All node processes stopped",
            restartAll: "Restarting the cluster…",
            killLeader: "Leader process killed",
            wipe: "Data wiped — cluster is stopped",
            kill: `N${nodeId} killed`,
            restart: `N${nodeId} restarted`,
            start: `N${nodeId} started`,
          };
          notify("info", labels[action] ?? `${action} complete`);
        }
      } catch (error) {
        const message =
          error instanceof Error ? error.message : "Process action failed";
        setBootError(message);
        notify("error", message);
      } finally {
        setBusy(null);
        void refresh();
      }
    },
    [applyCluster, notify, refresh],
  );

  // Poll every 750ms; a single-flight loop avoids overlapping requests.
  useEffect(() => {
    let cancelled = false;
    let timer: ReturnType<typeof setTimeout> | null = null;
    const tick = async () => {
      await refresh();
      if (!cancelled) {
        timer = setTimeout(tick, 750);
      }
    };
    void tick();
    return () => {
      cancelled = true;
      if (timer) {
        clearTimeout(timer);
      }
    };
  }, [refresh]);

  // Start the demo cluster on the first visit. The manager is idempotent, and
  // sessionStorage keeps an explicit stop from being undone by a refresh.
  useEffect(() => {
    if (!cluster || autoStartRef.current) {
      return;
    }
    autoStartRef.current = true;
    const anyActive = cluster.nodes.some(
      (node) =>
        node.process.phase !== "stopped" && node.process.phase !== "crashed",
    );
    if (anyActive) {
      return;
    }
    if (window.sessionStorage.getItem("raft-demo-autostart") === "done") {
      return;
    }
    window.sessionStorage.setItem("raft-demo-autostart", "done");
    void runAction("startAll", undefined, { quiet: true });
  }, [cluster, runAction]);

  useEffect(() => {
    if (!toast) {
      return;
    }
    const timer = setTimeout(() => setToast(null), 4500);
    return () => clearTimeout(timer);
  }, [toast]);

  const loadLogs = useCallback(async (nodeId: number) => {
    setLogsLoading(true);
    try {
      const response = await fetch("/api/processes", { cache: "no-store" });
      const payload = (await response.json()) as {
        processes?: Array<{ nodeId: number; logs: ProcessLogLine[] }>;
        error?: string;
      };
      const process = payload.processes?.find((item) => item.nodeId === nodeId);
      setLogLines(process?.logs ?? []);
    } catch {
      setLogLines([
        {
          t: Date.now(),
          stream: "stderr",
          text: "Could not load process logs from the Next.js server.",
        },
      ]);
    } finally {
      setLogsLoading(false);
    }
  }, []);

  useEffect(() => {
    if (logNodeId === null) {
      return;
    }
    void loadLogs(logNodeId);
    const timer = setInterval(() => {
      void loadLogs(logNodeId);
    }, 2000);
    return () => clearInterval(timer);
  }, [logNodeId, loadLogs]);

  const openLogs = useCallback((nodeId: number) => {
    setLogNodeId(nodeId);
  }, []);

  const handleHeaderAction = useCallback(
    (action: HeaderAction) => {
      if (action === "wipe") {
        const confirmed = window.confirm(
          "Stop all five nodes and delete their WAL, snapshots and config from data/? This cannot be undone.",
        );
        if (!confirmed) {
          return;
        }
      }
      void runAction(action);
    },
    [runAction],
  );

  const handleRestart = useCallback(
    (nodeId: number) => {
      const current = clusterRef.current?.nodes.find(
        (node) => node.config.id === nodeId,
      );
      const alive =
        current !== undefined &&
        (current.process.phase === "running" ||
          current.process.phase === "starting");
      void runAction(alive ? "restart" : "start", nodeId);
    },
    [runAction],
  );

  const handleKill = useCallback(
    (nodeId: number) => {
      void runAction("kill", nodeId);
    },
    [runAction],
  );

  const sendCommand = useCallback(
    async (
      op: "put" | "delete" | "get",
      key: string,
      value: string,
      nodeId: number | null,
    ) => {
      setPendingOp(op);
      try {
        const response = await fetch("/api/command", {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: JSON.stringify({ op, key, value, nodeId }),
        });
        let payload: CommandResult & { error?: string };
        try {
          payload = (await response.json()) as CommandResult & {
            error?: string;
          };
        } catch {
          throw new Error(`Command endpoint returned ${response.status}`);
        }
        if (payload.error) {
          throw new Error(payload.error);
        }
        if (!response.ok && typeof payload.op !== "string") {
          throw new Error(`Command failed (${response.status})`);
        }
        setCommandResult(payload);

        const eventNode =
          payload.targetNodeId ??
          nodeId ??
          payload.results?.find((item) => item.status === 200)?.nodeId ??
          clusterRef.current?.leaderId ??
          1;
        const label =
          op === "put"
            ? `PUT ${key}${value ? ` = ${value}` : ""}`
            : op === "delete"
              ? `DELETE ${key}`
              : `GET ${key}`;
        setTimeline((current) =>
          mergeTimeline(current, [
            makeClientEvent(
              eventNode,
              payload.ok
                ? `${label} accepted by N${eventNode}`
                : `${label} rejected by N${eventNode}`,
              payload.message,
              clusterRef.current?.term,
            ),
          ]),
        );
        notify(
          payload.ok ? "ok" : "error",
          payload.ok ? payload.message : `N${eventNode}: ${payload.message}`,
        );
        setTimeout(() => {
          void refresh();
        }, 150);
      } catch (error) {
        const message =
          error instanceof Error ? error.message : "Command failed";
        setCommandResult({
          ok: false,
          op,
          targetNodeId: nodeId ?? undefined,
          message,
        });
        notify("error", message);
      } finally {
        setPendingOp(null);
      }
    },
    [notify, refresh],
  );

  const seedEntries = useCallback(async () => {
    if (seeding) {
      return;
    }
    setSeeding(true);
    try {
      for (let index = 0; index < SEED_COLORS.length; index += 1) {
        await sendCommand(
          "put",
          `seed-${index + 1}`,
          SEED_COLORS[index],
          null,
        );
      }
    } finally {
      setSeeding(false);
    }
  }, [seeding, sendCommand]);

  const nodes = cluster?.nodes ?? [];
  const leaderId = cluster?.leaderId ?? null;
  const leaderView = nodes.find((node) => node.config.id === leaderId);
  const leaderLastIndex = leaderView?.state?.node.lastIndex ?? 0;
  const aliveCount = nodes.filter(
    (node) =>
      node.process.phase === "running" || node.process.phase === "starting",
  ).length;
  const syncedCount = leaderId
    ? nodes.filter(
        (node) =>
          node.config.id !== leaderId &&
          node.state !== null &&
          node.process.phase === "running" &&
          node.state.node.lastIndex === leaderLastIndex,
      ).length
    : 0;
  const now = cluster?.generatedAt ?? Date.now();
  const logView =
    cluster?.nodes.find((node) => node.config.id === logNodeId) ?? null;

  return (
    <main className="app-shell">
      <ClusterHeader
        cluster={cluster}
        aliveCount={aliveCount}
        syncedCount={syncedCount}
        busy={busy}
        onAction={handleHeaderAction}
      />

      {bootError && (
        <div className="error-banner" role="alert">
          <strong>cluster manager:</strong> {bootError}
        </div>
      )}

      <div className="hint-bar">
        <div className="hint">
          <span className="hint-num">01</span>
          <p>
            <b>Seed entries</b> and watch each PUT land in the log matrix, then
            turn solid once a quorum commits it.
          </p>
        </div>
        <div className="hint">
          <span className="hint-num">02</span>
          <p>
            <b>Kill a follower</b>, keep writing, then restart it — its column
            catches up from the leader&apos;s next AppendEntries.
          </p>
        </div>
        <div className="hint">
          <span className="hint-num">03</span>
          <p>
            <b>Route a PUT to a follower</b> to see the leader-only rejection,
            or kill N1 and watch writes stop.
          </p>
        </div>
      </div>

      {!cluster && !bootError ? (
        <div className="boot-state">
          <span className="boot-spinner" aria-hidden />
          <p>Connecting to the process manager…</p>
        </div>
      ) : (
        <>
          <section className="nodes-grid" aria-label="Cluster processes">
            {nodes.map((view) => (
              <NodeCard
                key={view.config.id}
                view={view}
                leaderId={leaderId}
                leaderLastIndex={leaderLastIndex}
                busy={busy}
                now={now}
                onKill={handleKill}
                onRestart={handleRestart}
                onLogs={openLogs}
              />
            ))}
          </section>

          <div className="workspace">
            <div className="workspace-main">
              <CommandPanel
                nodes={nodes.map((node) => node.config)}
                leaderId={leaderId}
                aliveCount={aliveCount}
                pendingOp={pendingOp}
                seeding={seeding}
                result={commandResult}
                onSend={sendCommand}
                onSeed={seedEntries}
              />
              <LogMatrix
                nodes={nodes}
                leaderId={leaderId}
                quorum={cluster?.quorum ?? 3}
              />
              <KeyValueMatrix nodes={nodes} leaderId={leaderId} />
            </div>
            <EventFeed
              events={timeline}
              onClear={() => setTimeline([])}
            />
          </div>
        </>
      )}

      <footer className="app-footer">
        <span>5 processes · HTTP 8001–8005 · TCP 7001–7005</span>
        <span>on-disk WAL in ./data · UI polls /state every 750 ms</span>
        <span>derived timeline · no hidden state</span>
      </footer>

      <LogModal
        view={logView}
        lines={logLines}
        loading={logsLoading}
        onClose={() => setLogNodeId(null)}
        onRefresh={() => {
          if (logNodeId !== null) {
            void loadLogs(logNodeId);
          }
        }}
      />

      {toast && (
        <div className={`toast toast-${toast.kind}`} role="status" key={toast.id}>
          <span className="toast-dot" />
          {toast.message}
        </div>
      )}
    </main>
  );
}
