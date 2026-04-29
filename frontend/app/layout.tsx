import type { Metadata } from "next";
import type { ReactNode } from "react";
import Script from "next/script";
import Link from "next/link";

import "./globals.css";
import { apiGet } from "@/lib/api";
import type { BaseData } from "@/types/api";
import { HiWidget } from "@/components/HiWidget";

export const metadata: Metadata = {
  title: "ngmi - PR Review Leaderboards",
  description:
    "Global leaderboards for GitHub PR review time. Track speed, reviewers, and merge stats across any public repo."
};

async function currentUser(): Promise<string> {
  try {
    const session = await apiGet<BaseData>("/api/next/session");
    return session.CurrentUser || "";
  } catch {
    return "";
  }
}

export default async function RootLayout({ children }: { children: ReactNode }) {
  const user = await currentUser();

  return (
    <html lang="en">
      <body>
        <Script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.4/dist/chart.umd.min.js" strategy="afterInteractive" />
        <nav className="nav">
          <Link href="/" className="nav-logo">
            <span>ngmi</span>
          </Link>
          <div className="nav-links">
            <Link href="/stats" className="nav-link">
              Stats
            </Link>{" "}
            <Link href="/blog" className="nav-link">
              Blog
            </Link>{" "}
            {user ? (
              <>
                <Link href="/dashboard" className="nav-link">
                  Dashboard
                </Link>{" "}
                <form method="POST" action="/auth/logout" style={{ display: "inline", margin: 0 }}>
                  <button
                    type="submit"
                    className="nav-link"
                    style={{ background: "none", border: "none", cursor: "pointer", padding: 0, font: "inherit" }}
                  >
                    Logout
                  </button>
                </form>
              </>
            ) : (
              <Link href="/auth/login" className="nav-link">
                Login
              </Link>
            )}
          </div>
        </nav>

        <main>{children}</main>
        <HiWidget />

        <footer className="footer">
          <p>
            <span className="footer-logo">ngmi</span>
            &nbsp;·&nbsp; If you aren&apos;t reviewing, you&apos;re ngmi. &nbsp;·&nbsp;
            <Link href="/data" className="nav-link">
              Data
            </Link>{" "}
            <a href="https://github.com/hydrogen-app/in-review" target="_blank" rel="noopener" className="nav-link">
              GitHub ↗
            </a>
          </p>
        </footer>
      </body>
    </html>
  );
}
