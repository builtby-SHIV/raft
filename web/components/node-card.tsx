"use client";

import type { NodeView } from "@/lib/types";
import { nodeByTcpAddr } from "@/lib/nodes";
import {
  formatAgo,
  formatDuration,
  termColor,
  termSoftColor,
  truncate,
} from "@/lib/format";
import {
  RestartIcon,
  SkullIcon,
  PlayIcon,
  TerminalIcon,
} from "./icons";

interface NodeCardProps {
  view: NodeView;
  leaderId: number | null;
  leaderLastIndex: number;
  busy: string | null;
  now: number;
  onKill: (nodeId: number) => void;
  onRestart: (nodeId: number) => void;
  onLogs: (nodeId: number) => void;
}

function phaseLabel(phase: NodeView["process"]["phase"]): string {
  switch (phase) {
    case "running":
      return "live";
    case "starting":
      return "starting";
    case "stopping":
      return "stopping";
    case "crashed":
      return "crashed";
    default:
      return "stopped";
  }
}

export function NodeCard({
  view,
  leaderId,
  leaderLastIndex,
  busy,
  now,
  onKill,
  onRestart,
  onLogs,
}: NodeCardProps) {
  const node = view.state?.node;
  const phase = view.process.phase;
  const alive = phase === "running" || phase === "starting";
  const role = node?.role ?? view.config.role;
  const isLeader = role === "leader";
  const lastIndex = node?.lastIndex ?? 0;
  const commitIndex = node?.commitIndex ?? 0;
  const behind = Math.max(0, leaderLastIndex - lastIndex);
  const uncommitted = Math.max(0, lastIndex - commitIndex);
  const entries = view.state?.log ?? [];
  const visibleEntries = entries.slice(-26);
  const killBusy = busy === `kill:${view.config.id}`;
  const restartBusy = busy === `restart:${view.config.id}`;
  const busyHere = killBusy || restartBusy;

  const cursors = view.state?.leader;
  const peers = node?.peers ?? [];

  const cardClass = [
    "node-card",
    isLeader ? "is-leader" : "",
    !alive ? "is-dead" : "",
    phase === "crashed" ? "is-crashed" : "",
    phase === "starting" ? "is-starting" : "",
  ]
    .filter(Boolean)
    .join(" ");

  return (
    <article className={cardClass}>
      <header className="node-card-head">
        <div className="node-identity">
          <span className="node-name">{view.config.name}</span>
          {isLeader && <span className="leader-star" title="Leader">★</span>}
          <span className={`phase-dot phase-${phase}`} aria-hidden />
        </div>
        <div className="node-badges">
          <span className={`role-badge role-${role}`}>{role}</span>
          <span className={`phase-badge phase-badge-${phase}`}>
            {phaseLabel(phase)}
          </span>
        </div>
      </header>

      <div className="node-stats">
        <div className="stat">
          <span className="stat-label">term</span>
          <span className="stat-value">{node?.term ?? "—"}</span>
        </div>
        <div className="stat">
          <span className="stat-label">commit</span>
          <span className="stat-value">{node?.commitIndex ?? "—"}</span>
        </div>
        <div className="stat">
          <span className="stat-label">last</span>
          <span className="stat-value">{node?.lastIndex ?? "—"}</span>
        </div>
        <div className="stat">
          <span className="stat-label">keys</span>
          <span className="stat-value">{node?.kvCount ?? "—"}</span>
        </div>
      </div>

      <div className="node-sync">
        {isLeader ? (
          <span className="sync-line">
            <span className="sync-dot leader" /> leading term {node?.term ?? 0}
          </span>
        ) : leaderId && !alive ? (
          <span className="sync-line muted">
            <span className="sync-dot off" /> leader N{leaderId} · not running
          </span>
        ) : leaderId ? (
          <span className="sync-line">
            <span className="sync-dot" /> leader N{leaderId}
            <span className={behind === 0 ? "sync-ok" : "sync-lag"}>
              {behind === 0 ? "in sync" : `behind ${behind}`}
            </span>
          </span>
        ) : (
          <span className="sync-line muted">
            <span className="sync-dot off" /> no leader responding
          </span>
        )}
        {uncommitted > 0 && (
          <span className="uncommitted-badge" title="Entries replicated locally but not committed by a quorum">
            {uncommitted} uncommitted
          </span>
        )}
      </div>

      <div className="log-strip" aria-label="Log entries colored by term">
        {visibleEntries.length === 0 ? (
          <span className="log-strip-empty">empty log</span>
        ) : (
          visibleEntries.map((entry) => (
            <span
              key={`${entry.index}-${entry.term}`}
              className={`log-chip ${entry.committed ? "is-committed" : "is-pending"}`}
              style={{
                background: entry.committed
                  ? termColor(entry.term, 0.85)
                  : termSoftColor(entry.term, 0.45),
                borderColor: termColor(entry.term, 0.9),
              }}
              title={`#${entry.index} · term ${entry.term} · ${entry.command} ${entry.key}${
                entry.value ? ` = ${truncate(entry.value, 40)}` : ""
              }${entry.committed ? " · committed" : " · uncommitted"}`}
            />
          ))
        )}
        {entries.length > visibleEntries.length && (
          <span className="log-strip-more" title={`${entries.length} entries total`}>
            +{entries.length - visibleEntries.length}
          </span>
        )}
      </div>

      {isLeader && peers.length > 0 && (
        <div className="replication-list">
          <div className="replication-title">
            replication cursors
            <span className="muted">match / next</span>
          </div>
          {peers.map((peer) => {
            const target = nodeByTcpAddr(peer);
            const match = cursors?.matchIndex[peer] ?? 0;
            const next = cursors?.nextIndex[peer] ?? 0;
            const inflight = cursors?.inflight[peer] ?? false;
            const pct =
              lastIndex > 0
                ? Math.max(0, Math.min(100, (match / lastIndex) * 100))
                : 100;
            return (
              <div className="replication-row" key={peer}>
                <span className="replication-peer">
                  {target ? target.name : peer}
                  {inflight && <span className="inflight-dot" title="RPC in flight" />}
                </span>
                <span className="replication-bar">
                  <span
                    className="replication-fill"
                    style={{ width: `${pct}%` }}
                  />
                </span>
                <span className="replication-index">
                  {match}/{next}
                </span>
              </div>
            );
          })}
        </div>
      )}

      {!alive && (
        <div className="death-note">
          <div className="death-title">
            process {phaseLabel(phase)}
            {view.process.exitCode !== undefined &&
              view.process.exitCode !== null && (
                <span className="death-code">exit {view.process.exitCode}</span>
              )}
          </div>
          {view.process.lastError && (
            <div className="death-detail">{view.process.lastError}</div>
          )}
          {view.process.logs.length > 0 && (
            <div className="death-log">
              {view.process.logs
                .slice(-2)
                .map((line, index) => (
                  <span key={`${line.t}-${index}`}>{truncate(line.text, 70)}</span>
                ))}
            </div>
          )}
          <div className="death-detail">
            last seen{" "}
            {formatAgo(
              view.process.exitedAt ?? view.process.startedAt ?? 0,
              now,
            )}
          </div>
        </div>
      )}

      {alive && view.error && (
        <div className="node-api-error">
          state API unavailable: {view.error}
        </div>
      )}

      <footer className="node-card-foot">
        <div className="node-meta">
          {alive ? (
            <>
              <span>pid {view.process.pid ?? "—"}</span>
              <span>{formatDuration(node?.uptimeMs ?? 0)}</span>
              <span>{view.config.tcpPort}</span>
              {node?.votedFor && node.votedFor !== "0" && (
                <span>vote {node.votedFor}</span>
              )}
            </>
          ) : (
            <span>{view.state ? `last commit ${commitIndex}` : "no state"}</span>
          )}
        </div>
        <div className="node-actions">
          {alive ? (
            <button
              type="button"
              className="btn-icon btn-icon-danger"
              title={`Kill ${view.config.name}`}
              aria-label={`Kill ${view.config.name}`}
              disabled={busyHere}
              onClick={() => onKill(view.config.id)}
            >
              <SkullIcon />
            </button>
          ) : (
            <button
              type="button"
              className="btn-icon btn-icon-play"
              title={`Start ${view.config.name}`}
              aria-label={`Start ${view.config.name}`}
              disabled={busyHere}
              onClick={() => onRestart(view.config.id)}
            >
              <PlayIcon />
            </button>
          )}
          <button
            type="button"
            className="btn-icon"
            title={`Restart ${view.config.name}`}
            aria-label={`Restart ${view.config.name}`}
            disabled={busyHere || phase === "starting" || phase === "stopping"}
            onClick={() => onRestart(view.config.id)}
          >
            <RestartIcon className={restartBusy ? "spin" : undefined} />
          </button>
          <button
            type="button"
            className="btn-icon"
            title={`${view.config.name} logs`}
            aria-label={`${view.config.name} logs`}
            onClick={() => onLogs(view.config.id)}
          >
            <TerminalIcon />
          </button>
        </div>
      </footer>
    </article>
  );
}
