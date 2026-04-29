"use client";

import Link from "next/link";
import { useEffect, useState } from "react";

import { formatDuration, formatNumber } from "@/lib/format";
import type { LeaderboardSearchData } from "@/types/api";

export function LeaderboardSearch({ category }: { category: string }) {
  const [query, setQuery] = useState("");
  const [result, setResult] = useState<LeaderboardSearchData | null>(null);

  useEffect(() => {
    const q = query.trim();
    if (!q) {
      setResult(null);
      return;
    }
    const handle = window.setTimeout(() => {
      fetch(`/api/next/leaderboard/${category}/search?q=${encodeURIComponent(q)}`)
        .then((res) => (res.ok ? res.json() : null))
        .then((body: LeaderboardSearchData | null) => setResult(body))
        .catch(() => setResult(null));
    }, 300);
    return () => window.clearTimeout(handle);
  }, [category, query]);

  const userCategory = category === "reviewers" || category === "gatekeepers" || category === "authors";

  return (
    <div className="lb-search-wrap">
      <input
        type="text"
        className="search-input"
        placeholder={userCategory ? "Search by username..." : "Search by repo (owner/repo)..."}
        autoComplete="off"
        value={query}
        onChange={(event) => setQuery(event.target.value)}
      />
      {result && !result.Empty ? (
        <div className="lb-search-result">
          {result.NotTracked ? (
            <>
              <span className="lb-search-stats">Not tracked yet.</span>
              <Link className="link" href={result.TrackURL}>
                Track it →
              </Link>
            </>
          ) : userCategory ? (
            <>
              {result.AvatarURL ? (
                // eslint-disable-next-line @next/next/no-img-element
                <img src={result.AvatarURL} className="board-avatar" alt="" />
              ) : null}
              <Link href={`/user/${result.Login}`} className="lb-search-name mono">
                @{result.Login}
              </Link>
              <span className="lb-search-stats">#{result.Rank}</span>
              {category === "authors" ? (
                <span className="lb-search-stats">{formatNumber(result.MergedPRs)} merged · {formatDuration(result.AvgMergeTimeSecs)} avg</span>
              ) : (
                <span className="lb-search-stats">{formatNumber(result.TotalReviews)} reviews · {formatNumber(result.ChangesRequested)} blocks</span>
              )}
            </>
          ) : (
            <>
              <Link href={`/repo/${result.FullName}`} className="lb-search-name mono">
                {result.FullName}
              </Link>
              <span className="lb-search-stats">speed #{result.SpeedRank || "-"}</span>
              <span className="lb-search-stats">graveyard #{result.GraveyardRank || "-"}</span>
              <span className="lb-search-stats">{formatDuration(result.AvgSecs)} avg</span>
            </>
          )}
        </div>
      ) : null}
    </div>
  );
}
