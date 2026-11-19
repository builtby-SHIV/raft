"use client";

import { useMemo } from "react";

import type { NodeView } from "@/lib/types";
import { truncate } from "@/lib/format";

interface KeyValueMatrixProps {
  nodes: NodeView[];
  leaderId: number | null;
}

export function KeyValueMatrix({ nodes, leaderId }: KeyValueMatrixProps) {
  const orderedNodes = useMemo(
    () => [...nodes].sort((a, b) => a.config.id - b.config.id),
    [nodes],
  );

  const { keys, rows } = useMemo(() => {
    const keySet = new Set<string>();
    for (const view of orderedNodes) {
      for (const key of Object.keys(view.state?.kv ?? {})) {
        keySet.add(key);
      }
    }
    const sortedKeys = [...keySet].sort().slice(0, 40);
    const mapped = sortedKeys.map((key) => {
      const values = orderedNodes.map((view) => {
        const kv = view.state?.kv ?? {};
        return Object.prototype.hasOwnProperty.call(kv, key)
          ? kv[key]
          : undefined;
      });
      const present = values.filter((value): value is string => value !== undefined);
      const distinct = new Set(present);
      return {
        key,
        values,
        mismatch: distinct.size > 1 || present.length !== orderedNodes.length,
      };
    });
    return { keys: sortedKeys, rows: mapped };
  }, [orderedNodes]);

  return (
    <section className="panel kv-panel" aria-labelledby="kv-title">
      <div className="panel-head">
        <div>
          <h2 id="kv-title" className="panel-title">
            State machine
          </h2>
          <p className="panel-subtitle">
            Values applied from the committed log. Followers can lag behind the
            leader until the next AppendEntries batch.
          </p>
        </div>
        <span className="counter-badge">{keys.length} keys</span>
      </div>

      {keys.length === 0 ? (
        <div className="empty-state">
          <strong>No committed keys.</strong>
          <span>
            PUT a key and it will appear here after a majority acknowledges it.
          </span>
        </div>
      ) : (
        <div className="matrix-scroll kv-scroll">
          <table className="data-table kv-matrix">
            <thead>
              <tr>
                <th className="kv-key-col">key</th>
                {orderedNodes.map((view) => {
                  const isLeader = view.config.id === leaderId;
                  return (
                    <th key={view.config.id} className="kv-node-col">
                      <span className="th-node">
                        {isLeader && <span className="mini-star">★</span>}
                        {view.config.name}
                      </span>
                      <span className="th-sub">
                        {view.state?.node.kvCount ?? 0} keys
                      </span>
                    </th>
                  );
                })}
              </tr>
            </thead>
            <tbody>
              {rows.map((row) => (
                <tr
                  key={row.key}
                  className={row.mismatch ? "kv-row-mismatch" : undefined}
                >
                  <td className="kv-key-col">
                    <span className="kv-key mono">{row.key}</span>
                    {row.mismatch && (
                      <span className="stale-tag" title="Nodes disagree about this key">
                        diverged
                      </span>
                    )}
                  </td>
                  {orderedNodes.map((view, index) => {
                    const value = row.values[index];
                    const leaderValue =
                      leaderId !== null
                        ? row.values[
                            orderedNodes.findIndex(
                              (candidate) => candidate.config.id === leaderId,
                            )
                          ]
                        : undefined;
                    const stale =
                      value !== undefined &&
                      leaderValue !== undefined &&
                      value !== leaderValue;
                    return (
                      <td
                        key={view.config.id}
                        className={`kv-cell ${
                          value === undefined
                            ? "missing"
                            : stale
                              ? "stale"
                              : "present"
                        }`}
                        title={value ?? "key not present"}
                      >
                        {value === undefined ? (
                          <span className="kv-absent">∅</span>
                        ) : (
                          <span className="kv-value mono">
                            {truncate(value, 18)}
                          </span>
                        )}
                      </td>
                    );
                  })}
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </section>
  );
}
