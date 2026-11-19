"use client";

import { useMemo } from "react";

import type { LogEntry, NodeView } from "@/lib/types";
import {
  termColor,
  termSoftColor,
  truncate,
} from "@/lib/format";

interface LogMatrixProps {
  nodes: NodeView[];
  leaderId: number | null;
  quorum: number;
}

interface CellEntry {
  entry: LogEntry;
  nodeId: number;
}

export function LogMatrix({ nodes, leaderId, quorum }: LogMatrixProps) {
  const orderedNodes = useMemo(
    () => [...nodes].sort((a, b) => a.config.id - b.config.id),
    [nodes],
  );

  const { indexes, entryMap, terms, leaderCommit } = useMemo(() => {
    const map = new Map<number, Map<number, CellEntry>>();
    const indexSet = new Set<number>();
    const termSet = new Set<number>();

    for (const view of orderedNodes) {
      const nodeMap = new Map<number, CellEntry>();
      for (const entry of view.state?.log ?? []) {
        nodeMap.set(entry.index, { entry, nodeId: view.config.id });
        indexSet.add(entry.index);
        termSet.add(entry.term);
      }
      map.set(view.config.id, nodeMap);
    }

    const leaderView = orderedNodes.find(
      (view) => view.config.id === leaderId,
    );
    const commit =
      leaderView?.state?.node.commitIndex ??
      Math.max(
        0,
        ...orderedNodes.map((view) => view.state?.node.commitIndex ?? 0),
      );

    return {
      indexes: [...indexSet].sort((a, b) => a - b).slice(-34),
      entryMap: map,
      terms: [...termSet].sort((a, b) => a - b),
      leaderCommit: commit,
    };
  }, [orderedNodes, leaderId]);

  const termForIndex = (index: number): number | null => {
    for (const view of orderedNodes) {
      const cell = entryMap.get(view.config.id)?.get(index);
      if (cell) {
        return cell.entry.term;
      }
    }
    return null;
  };

  return (
    <section className="panel log-panel" aria-labelledby="log-title">
      <div className="panel-head">
        <div>
          <h2 id="log-title" className="panel-title">
            Replicated log
          </h2>
          <p className="panel-subtitle">
            One row per log index, one column per process. A cell appears as
            soon as that node writes the entry to its WAL.
          </p>
        </div>
        <div className="term-legend" aria-label="Term colors">
          {terms.length === 0 && <span className="muted">no terms yet</span>}
          {terms.map((term) => (
            <span
              key={term}
              className="term-chip"
              style={{
                borderColor: termColor(term, 0.6),
                background: termSoftColor(term, 0.16),
                color: termColor(term, 0.95),
              }}
            >
              term {term}
            </span>
          ))}
        </div>
      </div>

      {indexes.length === 0 ? (
        <div className="empty-state">
          <strong>No entries yet.</strong>
          <span>
            Send a PUT from the client console and watch it fan out across the
            columns.
          </span>
        </div>
      ) : (
        <div className="matrix-scroll">
          <table className="data-table log-matrix">
            <thead>
              <tr>
                <th className="index-col">idx</th>
                <th className="term-col">term</th>
                {orderedNodes.map((view) => {
                  const node = view.state?.node;
                  const isLeader = view.config.id === leaderId;
                  return (
                    <th key={view.config.id} className="node-col">
                      <span className="th-node">
                        {isLeader && <span className="mini-star">★</span>}
                        {view.config.name}
                        <span
                          className={`mini-role ${isLeader ? "leader" : "follower"}`}
                        >
                          {node?.role ?? view.config.role}
                        </span>
                      </span>
                      <span className="th-sub">
                        commit {node?.commitIndex ?? "—"}
                      </span>
                    </th>
                  );
                })}
              </tr>
            </thead>
            <tbody>
              {indexes.map((index) => {
                const committed = index <= leaderCommit;
                const rowTerm = termForIndex(index);
                return (
                  <tr
                    key={index}
                    className={committed ? "row-committed" : "row-pending"}
                  >
                    <td className="index-col">
                      <span className="index-value">#{index}</span>
                      {committed ? (
                        <span className="commit-mark" title={`Committed by ${quorum} nodes`}>
                          ✓
                        </span>
                      ) : (
                        <span className="pending-mark" title="Not committed yet">
                          ○
                        </span>
                      )}
                    </td>
                    <td className="term-col">
                      {rowTerm !== null ? (
                        <span
                          className="term-dot"
                          style={{ background: termColor(rowTerm) }}
                          title={`term ${rowTerm}`}
                        >
                          {rowTerm}
                        </span>
                      ) : (
                        "—"
                      )}
                    </td>
                    {orderedNodes.map((view) => {
                      const cell = entryMap.get(view.config.id)?.get(index);
                      if (!cell) {
                        return (
                          <td key={view.config.id} className="entry-missing">
                            <span title="This node does not have this entry">—</span>
                          </td>
                        );
                      }
                      const entry = cell.entry;
                      const cellCommitted =
                        entry.committed && index <= leaderCommit;
                      return (
                        <td
                          key={view.config.id}
                          className={`entry-cell ${cellCommitted ? "committed" : "pending"}`}
                          style={{
                            background: termSoftColor(
                              entry.term,
                              cellCommitted ? 0.2 : 0.08,
                            ),
                            borderLeftColor: termColor(entry.term, 0.85),
                          }}
                          title={`N${cell.nodeId} · #${entry.index} · term ${entry.term} · ${entry.command} ${entry.key}${
                            entry.value ? ` = ${entry.value}` : ""
                          }`}
                        >
                          <span
                            className={`entry-command ${entry.command.toLowerCase()}`}
                          >
                            {entry.command === "DELETE"
                              ? "DEL"
                              : entry.command === "PUT"
                                ? "PUT"
                                : entry.command}
                          </span>
                          <span className="entry-key">{truncate(entry.key, 14)}</span>
                          {entry.value && (
                            <span className="entry-value">
                              ={truncate(entry.value, 13)}
                            </span>
                          )}
                        </td>
                      );
                    })}
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>
      )}

      <div className="matrix-legend">
        <span><i className="legend-swatch committed" /> committed (index ≤ leader commit {leaderCommit})</span>
        <span><i className="legend-swatch pending" /> written but uncommitted</span>
        <span><i className="legend-swatch missing" /> missing on that node</span>
        <span className="legend-q">quorum = {quorum}/5</span>
      </div>
    </section>
  );
}
