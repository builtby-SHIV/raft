import { NextResponse } from "next/server";

import { getClusterView, sleep } from "@/lib/cluster";
import { NODES } from "@/lib/nodes";
import {
  killNode,
  processStatusesWithFullLogs,
  restartAll,
  restartNode,
  startAll,
  startNode,
  stopAll,
  wipeData,
} from "@/lib/process-manager";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";

function errorMessage(error: unknown): string {
  return error instanceof Error ? error.message : "Unexpected error";
}

export async function GET() {
  try {
    return NextResponse.json({
      processes: await processStatusesWithFullLogs(),
    });
  } catch (error) {
    return NextResponse.json({ error: errorMessage(error) }, { status: 500 });
  }
}

export async function POST(request: Request) {
  let body: { action?: string; nodeId?: number };
  try {
    body = (await request.json()) as { action?: string; nodeId?: number };
  } catch {
    return NextResponse.json({ error: "Invalid JSON body" }, { status: 400 });
  }

  const action = body.action;
  if (!action) {
    return NextResponse.json({ error: "Missing action" }, { status: 400 });
  }

  try {
    switch (action) {
      case "startAll":
        await startAll();
        break;
      case "stopAll":
        await stopAll();
        break;
      case "restartAll":
        await restartAll();
        break;
      case "wipe":
        await wipeData();
        break;
      case "start":
      case "kill":
      case "restart": {
        const nodeId = Number(body.nodeId);
        if (!NODES.some((node) => node.id === nodeId)) {
          return NextResponse.json(
            { error: `Unknown node ${body.nodeId}` },
            { status: 400 },
          );
        }
        if (action === "start") {
          await startNode(nodeId);
        } else if (action === "kill") {
          await killNode(nodeId);
        } else {
          await restartNode(nodeId);
        }
        break;
      }
      case "killLeader": {
        const view = await getClusterView(1);
        if (view.leaderId === null) {
          return NextResponse.json(
            { error: "No leader is currently responding" },
            { status: 409 },
          );
        }
        await killNode(view.leaderId);
        break;
      }
      default:
        return NextResponse.json(
          { error: `Unknown action ${action}` },
          { status: 400 },
        );
    }

    // Give freshly started processes a moment to bind their ports before the
    // client's next poll so the UI does not flash a false "unreachable".
    if (action === "startAll" || action === "start" || action === "restart") {
      await sleep(120);
    }

    return NextResponse.json({
      ok: true,
      cluster: await getClusterView(),
    });
  } catch (error) {
    return NextResponse.json({ error: errorMessage(error) }, { status: 500 });
  }
}
