import { NextRequest } from "next/server";
import { backendRequest } from "@/lib/backend";

export async function PUT(request: NextRequest, { params }: { params: { productId: string } }) {
  return backendRequest(request, `/me/watchlist/${encodeURIComponent(params.productId)}`, "PUT");
}

export async function DELETE(request: NextRequest, { params }: { params: { productId: string } }) {
  return backendRequest(request, `/me/watchlist/${encodeURIComponent(params.productId)}`, "DELETE");
}
