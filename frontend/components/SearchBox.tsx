"use client";

import Link from "next/link";
import { useEffect, useState } from "react";

import { formatDuration, formatNumber } from "@/lib/format";
import type { SearchData, SearchResult } from "@/types/api";

function resultHref(result: SearchResult): string {
  if (result.Type === "repo") return `/repo/${result.FullName}`;
  if (result.Type === "org") return `/org/${result.FullName}`;
  return `/user/${result.FullName}`;
}

export function SearchBox() {
  const [query, setQuery] = useState("");
  const [data, setData] = useState<SearchData | null>(null);

  useEffect(() => {
    const q = query.trim();
    if (!q) {
      setData(null);
      return;
    }
    const handle = window.setTimeout(() => {
      fetch(`/api/next/search?q=${encodeURIComponent(q)}`, { credentials: "include" })
        .then((res) => (res.ok ? res.json() : null))
        .then((body: SearchData | null) => setData(body))
        .catch(() => setData(null));
    }, 350);
    return () => window.clearTimeout(handle);
  }, [query]);

  return (
    <div className="search-wrap">
      <div className="search-box">
        <input
          id="main-search"
          type="text"
          className="search-input"
          placeholder="golang/go, torvalds, kubernetes..."
          autoComplete="off"
          autoFocus
          value={query}
          onChange={(event) => setQuery(event.target.value)}
        />
      </div>
      {data && query.trim() ? (
        <div id="search-results" className="search-results">
          {data.Results?.length ? (
            data.Results.map((result) => (
              <Link href={resultHref(result)} className="sr-item" key={`${result.Type}:${result.FullName}`}>
                {result.AvatarURL ? (
                  // eslint-disable-next-line @next/next/no-img-element
                  <img src={result.AvatarURL} className="sr-avatar" alt="" />
                ) : (
                  <span className="sr-icon">{result.Type.toUpperCase()}</span>
                )}
                <span className="sr-body">
                  <span className="sr-name-row">
                    <span className="sr-name mono">{result.Type === "repo" ? result.FullName : `@${result.FullName}`}</span>
                    <span className="sr-type-badge">{result.Type}</span>
                    {result.Language ? <span className="sr-lang">{result.Language}</span> : null}
                    {result.Stars ? <span className="sr-stars">{formatNumber(result.Stars)} stars</span> : null}
                  </span>
                  {result.Description ? <span className="sr-desc">{result.Description}</span> : null}
                  {result.Type === "repo" && result.MergedPRs ? (
                    <span className="sr-stats">
                      <span>{formatNumber(result.MergedPRs)} merged PRs</span>
                      <span>{formatDuration(result.AvgMergeTime)} avg</span>
                      {result.SpeedRank ? <span className="sr-stat rank">#{result.SpeedRank} speed</span> : null}
                    </span>
                  ) : null}
                </span>
                <span className="sr-arrow">→</span>
              </Link>
            ))
          ) : (
            <div className="sr-empty">No results.</div>
          )}
        </div>
      ) : null}
    </div>
  );
}
