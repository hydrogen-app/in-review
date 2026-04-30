import { NextRequest } from "next/server";
import { getApiBase } from "./api";

const hopByHopHeaders = new Set([
  "connection",
  "content-encoding",
  "content-length",
  "keep-alive",
  "transfer-encoding",
  "upgrade"
]);

type RouteContext = {
  params: Promise<{ path?: string[] }>;
};

export async function proxyToApi(request: NextRequest, context: RouteContext, prefix: string): Promise<Response> {
  const params = await context.params;
  const path = params.path?.map(encodeURIComponent).join("/") || "";
  const target = new URL(`${prefix}${path ? `/${path}` : ""}${request.nextUrl.search}`, getApiBase());
  const headers = new Headers(request.headers);

  headers.delete("host");
  headers.delete("content-length");

  const method = request.method.toUpperCase();
  const upstream = await fetch(target, {
    method,
    headers,
    body: method === "GET" || method === "HEAD" ? undefined : request.body,
    duplex: method === "GET" || method === "HEAD" ? undefined : "half",
    redirect: "manual"
  } as RequestInit);

  const responseHeaders = new Headers(upstream.headers);
  hopByHopHeaders.forEach((header) => responseHeaders.delete(header));

  return new Response(upstream.body, {
    status: upstream.status,
    statusText: upstream.statusText,
    headers: responseHeaders
  });
}

export function withApiProxy(prefix: string) {
  return (request: NextRequest, context: RouteContext) => proxyToApi(request, context, prefix);
}
