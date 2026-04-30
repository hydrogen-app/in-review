import { withApiProxy } from "@/lib/proxy";

export const dynamic = "force-dynamic";

export const GET = withApiProxy("/api/next");
export const POST = withApiProxy("/api/next");
export const PUT = withApiProxy("/api/next");
export const PATCH = withApiProxy("/api/next");
export const DELETE = withApiProxy("/api/next");
