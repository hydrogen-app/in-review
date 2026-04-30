import { withApiProxy } from "@/lib/proxy";

export const dynamic = "force-dynamic";

export const GET = withApiProxy("/api/repos");
export const POST = withApiProxy("/api/repos");
export const PUT = withApiProxy("/api/repos");
export const PATCH = withApiProxy("/api/repos");
export const DELETE = withApiProxy("/api/repos");
