import { NextRequest } from "next/server";
import { backendRequest } from "@/lib/backend";

export async function POST(request: NextRequest) {
  return backendRequest(request, "/me/telegram/link", "POST", {});
}

export async function DELETE(request: NextRequest) {
  return backendRequest(request, "/me/telegram", "DELETE");
}
