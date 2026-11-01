import { NextRequest } from "next/server";
import { backendRequest } from "@/lib/backend";

export async function PUT(request: NextRequest) { return backendRequest(request, "/me/alerts/email", "PUT"); }
export async function DELETE(request: NextRequest) { return backendRequest(request, "/me/alerts/email", "DELETE"); }
