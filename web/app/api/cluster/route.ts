import { NextResponse } from "next/server";

import { getClusterView } from "@/lib/cluster";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";

export async function GET() {
  try {
    const view = await getClusterView();
    return NextResponse.json(view, {
      headers: { "Cache-Control": "no-store" },
    });
  } catch (error) {
    return NextResponse.json(
      {
        error:
          error instanceof Error
            ? error.message
            : "Could not read the cluster state",
      },
      { status: 500 },
    );
  }
}
