import { NextResponse } from "next/server";
import { backendUrl } from "@/lib/backend";

export const dynamic = "force-dynamic";

export async function GET() {
  try {
    const response = await fetch(`${backendUrl}/feed`, {
      cache: "no-store",
      signal: AbortSignal.timeout(5_000),
    });
    const body = await response.json();
    return NextResponse.json(body, { status: response.status });
  } catch {
    return NextResponse.json({ error: "Feed temporariamente indisponível" }, { status: 503 });
  }
}
