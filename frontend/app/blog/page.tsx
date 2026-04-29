import Link from "next/link";

import { apiGet } from "@/lib/api";
import { formatDuration, formatNumber, rankBadge } from "@/lib/format";
import type { BlogData } from "@/types/api";

export default async function BlogPage() {
  const data = await apiGet<BlogData>("/api/next/blog");

  return (
    <article className="blog-post">
      <div className="breadcrumb">
        <Link href="/" className="bc-link">ngmi</Link>
        <span className="bc-sep">/</span>
        <span className="bc-current">blog</span>
      </div>

      <header className="blog-header">
        <h1 className="blog-title">Are AI Tools Making PRs Bigger?</h1>
        <p className="blog-byline">
          ngmi.review &nbsp;·&nbsp; February 2026 &nbsp;·&nbsp;
          <span className="blog-live-badge"><span className="blog-live-dot" />live data</span>
        </p>
      </header>

      <div className="blog-live-wrap">
        <div className="blog-live-header">
          <span className="blog-live-label">Live snapshot</span>
          <span className="blog-live-updated muted">cached</span>
        </div>
        <div className="blog-stats-grid">
          <div className="blog-stat">
            <span className="blog-stat-num">{formatNumber(data.TotalPRs)}</span>
            <span className="blog-stat-label">PRs analyzed</span>
          </div>
          <div className="blog-stat">
            <span className="blog-stat-num">{formatNumber(data.TotalRepos)}</span>
            <span className="blog-stat-label">repos tracked</span>
          </div>
          <div className="blog-stat">
            <span className="blog-stat-num">{formatDuration(data.LiveStats.MedianSecs)}</span>
            <span className="blog-stat-label">median review time</span>
          </div>
          <div className="blog-stat">
            <span className="blog-stat-num">{formatNumber(data.TotalReviews)}</span>
            <span className="blog-stat-label">reviews logged</span>
          </div>
        </div>
        <div className="blog-live-cols">
          <div>
            <p className="blog-lb-heading">Top reviewers</p>
            <div className="blog-lb">
              {data.TopReviewers?.map((entry) => (
                <div className="blog-lb-row" key={entry.Name}>
                  <span className="blog-lb-rank">{rankBadge(entry.Rank)}</span>
                  <span className="blog-lb-name"><Link href={`/user/${entry.Name}`} className="link">{entry.Name}</Link></span>
                  <span className="blog-lb-val">{formatNumber(entry.Count)} reviews</span>
                </div>
              ))}
            </div>
          </div>
          <div>
            <p className="blog-lb-heading">Fastest repos</p>
            <div className="blog-lb">
              {data.TopSpeed?.map((entry) => (
                <div className="blog-lb-row" key={entry.Name}>
                  <span className="blog-lb-rank">{rankBadge(entry.Rank)}</span>
                  <span className="blog-lb-name"><Link href={`/repo/${entry.Name}`} className="link">{entry.Name}</Link></span>
                  <span className="blog-lb-val">{formatDuration(entry.Value)} avg</span>
                </div>
              ))}
            </div>
          </div>
        </div>
      </div>

      <section className="blog-section">
        <h2>The Question</h2>
        <p className="blog-prose">As AI coding tools change how quickly software gets written, PR review becomes the bottleneck worth measuring.</p>
      </section>
      <section className="blog-section">
        <h2>What the Data Shows</h2>
        <p className="blog-prose">The live dataset tracks merged public GitHub pull requests, review activity, and merge latency across searched repositories.</p>
      </section>
      <section className="blog-section">
        <h2>The AI Inflection Point</h2>
        <p className="blog-prose">
          <Link href="/stats" className="link">Explore the full interactive charts →</Link>
        </p>
      </section>
      <section className="blog-section">
        <h2>Methodology</h2>
        <p className="blog-prose">
          Data is collected from public GitHub repositories via the GitHub REST API. Only merged pull requests are included in timing calculations, and review time is measured from PR open to PR merge.
        </p>
      </section>
    </article>
  );
}
