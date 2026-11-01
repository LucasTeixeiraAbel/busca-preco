import { NextRequest, NextResponse } from "next/server";
import { backendUrl } from "@/lib/backend";

export const runtime = "nodejs";

export async function POST(request: NextRequest) {
  try {
    const response = await fetch(`${backendUrl}/auth/magic-link`, {
      method: "POST", headers: { "Content-Type": "application/json" },
      body: JSON.stringify(await request.json()), cache: "no-store", signal: AbortSignal.timeout(5000),
    });
    return NextResponse.json(await response.json(), { status: response.status });
  } catch {
    return NextResponse.json({ detail: "Serviço temporariamente indisponível" }, { status: 502 });
  }
}
