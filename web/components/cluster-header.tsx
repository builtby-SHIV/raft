"use client";

import type { ClusterView } from "@/lib/types";
import { formatClock } from "@/lib/format";
import {
  LayersIcon,
  PlayIcon,
  RestartIcon,
  SkullIcon,
  StopIcon,
  TrashIcon,
} from "./icons";

interface ClusterHeaderProps {
  cluster: ClusterView | null;
  aliveCount: number;
  syncedCount: number;
  busy: string | null;
  onAction: (
    action: "startAll" | "stopAll" | "restartAll" | "killLeader" | "wipe",
  ) => void;
}

export function ClusterHeader({
  cluster,
  aliveCount,
  syncedCount,
  busy,
  onAction,
}: ClusterHeaderProps) {
  const leaderId = cluster?.leaderId ?? null;
  const term = cluster?.term ?? 0;
  const total = cluster?.nodes.length ?? 5;
  const quorum = cluster?.quorum ?? 3;
  const quorumOk = aliveCount >= quorum;

  return (
    <header className="app-header">
      <div className="brand">
        <div className="brand-mark" aria-hidden>
          <LayersIcon />
        </div>
        <div>
          <h1 className="brand-title">
            Raft <span>Live Lab</span>
          </h1>
          <p className="brand-sub">
            entry replication, elections &amp; crashes on a real 5-process cluster
          </p>
        </div>
      </div>

      <div className="header-stats">
        <div className="header-stat">
          <span className="header-stat-label">term</span>
          <span className="header-stat-value">{term}</span>
        </div>
        <div className={`header-stat ${leaderId ? "is-live" : "is-warn"}`}>
          <span className="header-stat-label">leader</span>
          <span className="header-stat-value">
            {leaderId ? `N${leaderId}` : "none"}
          </span>
        </div>
        <div className={`header-stat ${quorumOk ? "is-live" : "is-warn"}`}>
          <span className="header-stat-label">quorum</span>
          <span className="header-stat-value">
            {aliveCount}/{total}
            <em> need {quorum}</em>
          </span>
        </div>
        <div className="header-stat">
          <span className="header-stat-label">in sync</span>
          <span className="header-stat-value">
            {leaderId ? syncedCount : "—"}
            <em> followers</em>
          </span>
        </div>
      </div>

      <div className="header-actions">
        <button
          type="button"
          className="btn btn-primary btn-sm"
          disabled={busy !== null || aliveCount === total}
          onClick={() => onAction("startAll")}
          title="Build and start all five node processes"
        >
          <PlayIcon className="icon-sm" />
          {busy === "startAll" ? "starting…" : "start cluster"}
        </button>
        <button
          type="button"
          className="btn btn-ghost btn-sm"
          disabled={busy !== null || aliveCount === total}
          onClick={() => onAction("restartAll")}
          title="Stop every process and start them again"
        >
          <RestartIcon className={`icon-sm ${busy === "restartAll" ? "spin" : ""}`} />
          restart all
        </button>
        <button
          type="button"
          className="btn btn-ghost btn-sm"
          disabled={busy !== null || aliveCount === 0}
          onClick={() => onAction("stopAll")}
          title="Stop every node process"
        >
          <StopIcon className="icon-sm" />
          stop
        </button>
        <button
          type="button"
          className="btn btn-danger-outline btn-sm"
          disabled={busy !== null || leaderId === null}
          onClick={() => onAction("killLeader")}
          title="Kill the process that currently believes it is the leader"
        >
          <SkullIcon className="icon-sm" />
          kill leader
        </button>
        <button
          type="button"
          className="btn-icon btn-icon-quiet"
          disabled={busy !== null}
          onClick={() => onAction("wipe")}
          title="Stop all nodes and delete their WAL / config data"
          aria-label="Wipe cluster data"
        >
          <TrashIcon />
        </button>
      </div>

      <div className="header-foot">
        <span className={`live-dot ${aliveCount > 0 ? "on" : "off"}`} />
        <span>{aliveCount > 0 ? "live" : "offline"}</span>
        <span className="dot-sep">·</span>
        <span>static membership · 1 leader + 4 followers</span>
        {cluster && (
          <>
            <span className="dot-sep">·</span>
            <span>polled {formatClock(cluster.generatedAt)}</span>
          </>
        )}
      </div>
    </header>
  );
}
