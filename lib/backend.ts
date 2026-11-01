import { NextRequest, NextResponse } from "next/server";

export const backendUrl = process.env.BACKEND_URL ?? "http://127.0.0.1:8000";
export const sessionCookie = "radar_session";

export function sessionToken(request: NextRequest): string | null {
  return request.cookies.get(sessionCookie)?.value ?? null;
}

export async function backendRequest(
  request: NextRequest,
  path: string,
  method: string,
  body?: unknown,
): Promise<NextResponse> {
  const token = sessionToken(request);
  if (!token) return NextResponse.json({ detail: "Sessão necessária" }, { status: 401 });
  try {
    const response = await fetch(`${backendUrl}${path}`, {
      method,
      headers: { Authorization: `Bearer ${token}`, ...(body ? { "Content-Type": "application/json" } : {}) },
      ...(body ? { body: JSON.stringify(body) } : {}),
      cache: "no-store",
      signal: AbortSignal.timeout(5000),
    });
    if (response.status === 204) return new NextResponse(null, { status: 204 });
    const text = await response.text();
    return new NextResponse(text, { status: response.status, headers: { "Content-Type": response.headers.get("content-type") ?? "application/json" } });
  } catch {
    return NextResponse.json({ detail: "Serviço temporariamente indisponível" }, { status: 502 });
  }
}
