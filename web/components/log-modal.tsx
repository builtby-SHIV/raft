"use client";

import { useEffect, useRef } from "react";

import type { NodeView, ProcessLogLine } from "@/lib/types";
import { formatClock } from "@/lib/format";
import { RefreshIcon, TerminalIcon, XIcon } from "./icons";

interface LogModalProps {
  view: NodeView | null;
  lines: ProcessLogLine[];
  loading: boolean;
  onClose: () => void;
  onRefresh: () => void;
}

export function LogModal({
  view,
  lines,
  loading,
  onClose,
  onRefresh,
}: LogModalProps) {
  const scrollRef = useRef<HTMLDivElement | null>(null);

  useEffect(() => {
    function onKey(event: KeyboardEvent) {
      if (event.key === "Escape") {
        onClose();
      }
    }
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, [onClose]);

  useEffect(() => {
    if (scrollRef.current) {
      scrollRef.current.scrollTop = scrollRef.current.scrollHeight;
    }
  }, [lines]);

  if (!view) {
    return null;
  }

  return (
    <div className="modal-backdrop" role="presentation" onClick={onClose}>
      <div
        className="modal"
        role="dialog"
        aria-modal="true"
        aria-label={`${view.config.name} process logs`}
        onClick={(event) => event.stopPropagation()}
      >
        <header className="modal-head">
          <div className="modal-title">
            <TerminalIcon className="icon-md" />
            <div>
              <h2>{view.config.name} process</h2>
              <p>
                {view.process.phase} · pid {view.process.pid ?? "—"} · tcp :{view.config.tcpPort}
              </p>
            </div>
          </div>
          <div className="modal-actions">
            <button
              type="button"
              className="btn btn-ghost btn-sm"
              onClick={onRefresh}
              disabled={loading}
            >
              <RefreshIcon className={`icon-sm ${loading ? "spin" : ""}`} />
              refresh
            </button>
            <button
              type="button"
              className="btn-icon"
              onClick={onClose}
              aria-label="Close logs"
            >
              <XIcon />
            </button>
          </div>
        </header>

        <div className="modal-meta">
          <span>command: raftnode -id {view.config.id} -http :{view.config.httpPort} -tcp :{view.config.tcpPort} -role {view.config.role}</span>
          {view.state && (
            <span>
              term {view.state.node.term} · commit {view.state.node.commitIndex} · last{" "}
              {view.state.node.lastIndex}
            </span>
          )}
        </div>

        <div className="terminal" ref={scrollRef}>
          {lines.length === 0 ? (
            <div className="terminal-empty">
              {loading ? "loading process output…" : "No output captured yet."}
            </div>
          ) : (
            lines.map((line, index) => (
              <div
                key={`${line.t}-${index}`}
                className={`terminal-line ${line.stream === "stderr" ? "is-stderr" : ""}`}
              >
                <span className="terminal-time">{formatClock(line.t)}</span>
                <span className="terminal-stream">{line.stream === "stderr" ? "err" : "out"}</span>
                <span className="terminal-text">{line.text}</span>
              </div>
            ))
          )}
        </div>
      </div>
    </div>
  );
}
