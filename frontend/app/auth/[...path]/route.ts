import { withApiProxy } from "@/lib/proxy";

export const dynamic = "force-dynamic";

export const GET = withApiProxy("/auth");
export const POST = withApiProxy("/auth");
