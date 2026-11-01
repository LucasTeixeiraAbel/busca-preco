import { NextRequest, NextResponse } from "next/server";
import { backendUrl, sessionCookie } from "@/lib/backend";

export const runtime = "nodejs";

export async function POST(request: NextRequest) {
  try {
    const body = await request.json() as { token?: string };
    const token = body.token;
    if (!token) return NextResponse.json({ detail: "Token ausente" }, { status: 400 });
    const response = await fetch(`${backendUrl}/auth/verify`, {
      method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ token }),
      cache: "no-store", signal: AbortSignal.timeout(5000),
    });
    if (!response.ok) return NextResponse.json({ detail: "Link inválido ou expirado" }, { status: response.status });
    const data = await response.json() as { access_token: string };
    const result = NextResponse.json({ ok: true });
    result.cookies.set(sessionCookie, data.access_token, {
      httpOnly: true, secure: process.env.NODE_ENV === "production", sameSite: "lax", path: "/", maxAge: 3600,
    });
    return result;
  } catch {
    return NextResponse.json({ detail: "Serviço temporariamente indisponível" }, { status: 502 });
  }
}
