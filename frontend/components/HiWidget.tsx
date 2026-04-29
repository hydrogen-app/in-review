"use client";

import Link from "next/link";
import { usePathname } from "next/navigation";
import { useEffect, useState } from "react";

import type { HiData } from "@/types/api";

export function HiWidget() {
  const pathname = usePathname();
  const [data, setData] = useState<HiData | null>(null);

  useEffect(() => {
    if (!pathname) return;
    fetch(`/api/next/hi?path=${encodeURIComponent(pathname)}`, { credentials: "include" })
      .then((res) => (res.ok ? res.json() : null))
      .then((body: HiData | null) => setData(body))
      .catch(() => setData(null));
  }, [pathname]);

  async function react(key: string) {
    if (!pathname) return;
    const body = new URLSearchParams({ path: pathname, reaction: key });
    const res = await fetch("/api/next/hi", {
      method: "POST",
      body,
      credentials: "include",
      headers: { "content-type": "application/x-www-form-urlencoded" }
    });
    if (res.ok) setData((await res.json()) as HiData);
  }

  if (!data) return <div id="hi-widget" className="hi-widget" />;

  if (data.didHi) {
    const myEmoji = data.reactions.find((r) => r.key === data.myReaction)?.emoji || "hi";
    return (
      <div id="hi-widget" className="hi-widget hi-done">
        <span className="hi-you">
          <span className="hi-wave-emoji">{myEmoji}</span> hi back!
        </span>
        <span className="hi-divider">·</span>
        <div className="hi-reaction-bar">
          {data.reactions.map((rx) =>
            data.reactionCounts[rx.key] ? (
              <span key={rx.key} className="hi-react-item">
                {rx.emoji} {data.reactionCounts[rx.key]}
              </span>
            ) : null
          )}
        </div>
        {data.todayCount > 0 ? <span className="hi-today">· +{data.todayCount} today</span> : null}
        <span className="hi-total">{data.total} total</span>
        <Link href="/hi-wall" className="hi-wall-link">
          wall →
        </Link>
      </div>
    );
  }

  const dots = Math.min(data.total, 3);
  return (
    <div id="hi-widget" className="hi-widget">
      <div className="hi-reactions-wrap">
        <span className="hi-prompt">say hi:</span>
        <div className="hi-reaction-btns">
          {data.reactions.map((rx) => (
            <button key={rx.key} className="hi-reaction-btn" title={rx.key} onClick={() => react(rx.key)}>
              {rx.emoji}
            </button>
          ))}
        </div>
      </div>
      <div className="hi-meta">
        <div className="hi-avatar-stack">
          {Array.from({ length: dots }).map((_, i) => (
            <div className="hi-dot" key={i} />
          ))}
        </div>
        <span className="hi-count">
          {data.total === 0 ? "be the first" : data.total === 1 ? "1 said hi" : `${data.total} said hi`}
        </span>
      </div>
      <Link href="/hi-wall" className="hi-wall-link">
        wall →
      </Link>
    </div>
  );
}
