import { withApiProxy } from "@/lib/proxy";

export const dynamic = "force-dynamic";

export const GET = withApiProxy("/api/sync");
export const POST = withApiProxy("/api/sync");
export const PUT = withApiProxy("/api/sync");
export const PATCH = withApiProxy("/api/sync");
export const DELETE = withApiProxy("/api/sync");
