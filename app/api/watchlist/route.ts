import { NextRequest } from "next/server";
import { backendRequest } from "@/lib/backend";

export const dynamic = "force-dynamic";
export async function GET(request: NextRequest) {
  return backendRequest(request, "/me/watchlist", "GET");
}
