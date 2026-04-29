import path from "node:path";
import { fileURLToPath } from "node:url";

const apiBase = process.env.API_BASE_URL || "http://localhost:8080";
const appDir = path.dirname(fileURLToPath(import.meta.url));

/** @type {import('next').NextConfig} */
const nextConfig = {
  turbopack: {
    root: appDir
  },
  async rewrites() {
    return [
      { source: "/api/next/:path*", destination: `${apiBase}/api/next/:path*` },
      { source: "/api/sync/:path*", destination: `${apiBase}/api/sync/:path*` },
      { source: "/api/repos/:path*", destination: `${apiBase}/api/repos/:path*` },
      { source: "/auth/:path*", destination: `${apiBase}/auth/:path*` },
      { source: "/badge/:path*", destination: `${apiBase}/badge/:path*` }
    ];
  }
};

export default nextConfig;
