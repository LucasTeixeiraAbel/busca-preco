import { NextRequest } from "next/server";
import { backendRequest } from "@/lib/backend";

export async function GET(request: NextRequest, { params }: { params: { productId: string } }) {
  const years = request.nextUrl.searchParams.get("years") ?? "1";
  return backendRequest(request, `/me/products/${encodeURIComponent(params.productId)}/history?years=${encodeURIComponent(years)}`, "GET");
}
