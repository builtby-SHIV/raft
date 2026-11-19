import { NextResponse } from "next/server";

import {
  fetchNodeSnapshot,
  findLeader,
  getClusterView,
  sleep,
} from "@/lib/cluster";
import { NODES, nodeBaseUrl, type NodeConfig } from "@/lib/nodes";
import type { CommandResult } from "@/lib/types";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";

interface CommandBody {
  op?: "put" | "delete" | "get";
  key?: string;
  value?: string;
  nodeId?: number | null;
}

function normalizeKey(key: string | undefined): string {
  return (key ?? "").trim();
}

function parseNodeMessage(text: string, fallback: string): string {
  if (!text) {
    return fallback;
  }
  try {
    const parsed = JSON.parse(text) as Record<string, unknown>;
    const candidate =
      parsed.Message ?? parsed.msg ?? parsed.error ?? parsed.message;
    if (typeof candidate === "string" && candidate.trim()) {
      return candidate;
    }
  } catch {
    // Plain-text error body from http.Error.
  }
  return text.trim() || fallback;
}

async function waitForLeader(
  timeoutMs = 8000,
): Promise<{ config: NodeConfig; leaderId: number } | null> {
  const deadline = Date.now() + timeoutMs;
  do {
    const leader = await findLeader();
    if (leader) {
      return { config: leader.config, leaderId: leader.config.id };
    }
    await sleep(350);
  } while (Date.now() < deadline);
  return null;
}

async function getFromEveryNode(key: string): Promise<CommandResult> {
  const results = await Promise.all(
    NODES.map(async (node) => {
      try {
        const response = await fetch(
          `${nodeBaseUrl(node)}/get?key=${encodeURIComponent(key)}`,
          {
            cache: "no-store",
            signal: AbortSignal.timeout(2500),
          },
        );
        const text = await response.text();
        if (response.ok) {
          let value = "";
          try {
            const parsed = JSON.parse(text) as { value?: string };
            value = parsed.value ?? "";
          } catch {
            value = text;
          }
          return {
            nodeId: node.id,
            status: response.status,
            value,
          };
        }
        return {
          nodeId: node.id,
          status: response.status,
          error: parseNodeMessage(text, `HTTP ${response.status}`),
        };
      } catch {
        return {
          nodeId: node.id,
          status: 0,
          error: "node unreachable",
        };
      }
    }),
  );

  const found = results.filter((result) => result.status === 200).length;
  return {
    ok: found > 0,
    op: "get",
    message:
      found > 0
        ? `Read "${key}" from ${found}/${NODES.length} nodes`
        : `Key "${key}" was not found on any reachable node`,
    results,
  };
}

export async function POST(request: Request) {
  let body: CommandBody;
  try {
    body = (await request.json()) as CommandBody;
  } catch {
    return NextResponse.json(
      { error: "Invalid JSON body" },
      { status: 400 },
    );
  }

  const op = body.op;
  const key = normalizeKey(body.key);
  if (!op || !["put", "delete", "get"].includes(op)) {
    return NextResponse.json(
      { error: "op must be put, delete or get" },
      { status: 400 },
    );
  }
  if (!key) {
    return NextResponse.json({ error: "A key is required" }, { status: 400 });
  }

  if (op === "get") {
    const result = await getFromEveryNode(key);
    return NextResponse.json(result);
  }

  let target: NodeConfig | null = null;
  let leaderId: number | null = null;

  if (typeof body.nodeId === "number" && body.nodeId > 0) {
    target = NODES.find((node) => node.id === body.nodeId) ?? null;
    if (!target) {
      return NextResponse.json(
        { error: `Unknown node ${body.nodeId}` },
        { status: 400 },
      );
    }
    const leader = await findLeader();
    leaderId = leader?.config.id ?? null;
  } else {
    const leader = await waitForLeader();
    if (!leader) {
      const view = await getClusterView(1);
      const running = view.nodes.filter(
        (node) =>
          node.process.phase === "running" || node.process.phase === "starting",
      ).length;
      return NextResponse.json(
        {
          ok: false,
          op,
          message:
            running === 0
              ? "No node processes are running. Start the cluster first."
              : "No leader is reachable. The cluster needs a running leader to accept writes.",
        } satisfies CommandResult,
        { status: 503 },
      );
    }
    target = leader.config;
    leaderId = leader.leaderId;
  }

  const base = nodeBaseUrl(target);
  const timeout = AbortSignal.timeout(15_000);

  try {
    let response: Response;
    if (op === "put") {
      response = await fetch(`${base}/put`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ Key: key, Val: body.value ?? "" }),
        signal: timeout,
      });
    } else {
      response = await fetch(
        `${base}/del?key=${encodeURIComponent(key)}`,
        { method: "DELETE", signal: timeout },
      );
    }

    const text = await response.text();
    const message = parseNodeMessage(
      text,
      response.ok ? "Command accepted" : `HTTP ${response.status}`,
    );
    const result: CommandResult = {
      ok: response.ok,
      op,
      targetNodeId: target.id,
      leaderId,
      status: response.status,
      message,
    };
    return NextResponse.json(result, {
      status: response.ok ? 200 : response.status,
    });
  } catch (error) {
    return NextResponse.json(
      {
        ok: false,
        op,
        targetNodeId: target.id,
        leaderId,
        message:
          error instanceof Error
            ? `Could not reach N${target.id}: ${error.message}`
            : `Could not reach N${target.id}`,
      } satisfies CommandResult,
      { status: 503 },
    );
  }
}

export async function GET() {
  // Convenience for quick scripting: report reachability of every process.
  const states = await Promise.all(
    NODES.map(async (node) => ({
      nodeId: node.id,
      ...(await fetchNodeSnapshot(node, 1)),
    })),
  );
  return NextResponse.json({ states });
}
