"use client";

import { useMemo, useState, type FormEvent } from "react";

import type { CommandResult } from "@/lib/types";
import type { NodeConfig } from "@/lib/nodes";
import { truncate } from "@/lib/format";
import { DatabaseIcon, PlusIcon, TrashIcon } from "./icons";

interface CommandPanelProps {
  nodes: NodeConfig[];
  leaderId: number | null;
  aliveCount: number;
  pendingOp: "put" | "delete" | "get" | null;
  seeding: boolean;
  result: CommandResult | null;
  onSend: (
    op: "put" | "delete" | "get",
    key: string,
    value: string,
    nodeId: number | null,
  ) => void;
  onSeed: () => void;
}

export function CommandPanel({
  nodes,
  leaderId,
  aliveCount,
  pendingOp,
  seeding,
  result,
  onSend,
  onSeed,
}: CommandPanelProps) {
  const [key, setKey] = useState("color");
  const [value, setValue] = useState("blue");
  const [target, setTarget] = useState("auto");

  const targetNodeId = useMemo(() => {
    if (target === "auto") {
      return null;
    }
    const parsed = Number(target);
    return Number.isFinite(parsed) ? parsed : null;
  }, [target]);

  const writeBusy = pendingOp === "put" || pendingOp === "delete";
  const getBusy = pendingOp === "get";
  const disabled = aliveCount === 0 || pendingOp !== null;

  function submit(op: "put" | "delete" | "get") {
    const trimmedKey = key.trim();
    if (!trimmedKey) {
      return;
    }
    onSend(op, trimmedKey, value, targetNodeId);
  }

  function handleSubmit(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    submit("put");
  }

  return (
    <section className="panel command-panel" aria-labelledby="command-title">
      <div className="panel-head">
        <div>
          <h2 id="command-title" className="panel-title">
            Client console
          </h2>
          <p className="panel-subtitle">
            Writes go through the leader. Reads are served locally by every node.
          </p>
        </div>
        <button
          type="button"
          className="btn btn-ghost btn-sm"
          onClick={onSeed}
          disabled={disabled || seeding}
          title="Append five demo entries in sequence"
        >
          <PlusIcon className="icon-sm" />
          {seeding ? "seeding…" : "seed 5 entries"}
        </button>
      </div>

      <form className="command-form" onSubmit={handleSubmit}>
        <label className="field">
          <span className="field-label">key</span>
          <input
            className="input mono"
            value={key}
            onChange={(event) => setKey(event.target.value)}
            placeholder="color"
            spellCheck={false}
            autoComplete="off"
          />
        </label>
        <label className="field">
          <span className="field-label">value</span>
          <input
            className="input mono"
            value={value}
            onChange={(event) => setValue(event.target.value)}
            placeholder="not used for delete / get"
            spellCheck={false}
            autoComplete="off"
          />
        </label>
        <label className="field field-target">
          <span className="field-label">route to</span>
          <select
            className="input select"
            value={target}
            onChange={(event) => setTarget(event.target.value)}
          >
            <option value="auto">
              auto → leader{leaderId ? ` (N${leaderId})` : ""}
            </option>
            {nodes.map((node) => (
              <option key={node.id} value={node.id}>
                N{node.id} · {node.id === leaderId ? "leader" : "follower"}
              </option>
            ))}
          </select>
        </label>
        <div className="command-buttons">
          <button
            type="submit"
            className="btn btn-primary"
            disabled={disabled || writeBusy}
          >
            <DatabaseIcon className="icon-sm" />
            {pendingOp === "put" ? "writing…" : "PUT"}
          </button>
          <button
            type="button"
            className="btn btn-danger-outline"
            disabled={disabled || writeBusy}
            onClick={() => submit("delete")}
          >
            <TrashIcon className="icon-sm" />
            {pendingOp === "delete" ? "deleting…" : "DELETE"}
          </button>
          <button
            type="button"
            className="btn btn-ghost"
            disabled={disabled || getBusy}
            onClick={() => submit("get")}
          >
            {pendingOp === "get" ? "reading…" : "GET"}
          </button>
        </div>
      </form>

      <p className="command-hint">
        Try routing a PUT to a follower: it rejects the write with{" "}
        <code>node is not the leader</code>. Auto mode waits for the current
        leader and retries while an election settles.
      </p>

      {result && (
        <div className={`command-result ${result.ok ? "is-ok" : "is-error"}`}>
          <div className="command-result-head">
            <span className="result-op">{result.op.toUpperCase()}</span>
            <span className="result-message">{result.message}</span>
            {result.targetNodeId !== undefined && (
              <span className="result-target">via N{result.targetNodeId}</span>
            )}
            {result.status !== undefined && (
              <span className="result-status">HTTP {result.status}</span>
            )}
          </div>
          {result.results && result.results.length > 0 && (
            <div className="read-matrix">
              {result.results.map((item) => (
                <div
                  key={item.nodeId}
                  className={`read-cell ${item.status === 200 ? "found" : "missing"}`}
                >
                  <span className="read-node">N{item.nodeId}</span>
                  <span className="read-value mono">
                    {item.status === 200
                      ? truncate(item.value ?? "", 26)
                      : item.error ?? "not found"}
                  </span>
                </div>
              ))}
            </div>
          )}
          {!result.ok && result.status === 503 && (
            <p className="command-result-hint">
              Only the leader accepts writes. This rejection is Raft doing its
              job — the client must retry against the leader.
            </p>
          )}
        </div>
      )}
    </section>
  );
}
