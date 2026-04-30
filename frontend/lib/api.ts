import { cookies } from "next/headers";

export function getApiBase(): string {
  return process.env["API_BASE_URL"] || "http://localhost:8080";
}

export async function apiGet<T>(path: string, init?: RequestInit): Promise<T> {
  const cookieHeader = (await cookies()).toString();
  const response = await fetch(`${getApiBase()}${path}`, {
    cache: "no-store",
    ...init,
    headers: {
      ...(cookieHeader ? { cookie: cookieHeader } : {}),
      ...(init?.headers || {})
    }
  });

  if (!response.ok) {
    let message = `Request failed: ${response.status}`;
    try {
      const body = (await response.json()) as { message?: string; error?: string };
      message = body.message || body.error || message;
    } catch {
      // Keep the status fallback.
    }
    throw new Error(message);
  }

  return response.json() as Promise<T>;
}

export function qs(params: Record<string, string | number | undefined | null>): string {
  const sp = new URLSearchParams();
  Object.entries(params).forEach(([key, value]) => {
    if (value !== undefined && value !== null && value !== "") {
      sp.set(key, String(value));
    }
  });
  const out = sp.toString();
  return out ? `?${out}` : "";
}
