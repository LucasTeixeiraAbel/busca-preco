import { NextRequest } from "next/server";
import { backendRequest } from "@/lib/backend";

export async function PUT(request: NextRequest) {
  let body: unknown;
  try { body = await request.json(); } catch { return Response.json({ detail: "JSON inválido" }, { status: 400 }); }
  return backendRequest(request, "/me/push-subscriptions", "PUT", body);
}
export async function DELETE(request: NextRequest) {
  const endpoint = request.nextUrl.searchParams.get("endpoint");
  if (!endpoint) return Response.json({ detail: "Endpoint necessário" }, { status: 400 });
  return backendRequest(request, `/me/push-subscriptions?endpoint=${encodeURIComponent(endpoint)}`, "DELETE");
}
