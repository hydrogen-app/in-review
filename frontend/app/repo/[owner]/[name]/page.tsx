import Link from "next/link";

import { RelationGraph } from "@/components/RelationGraph";
import { RepoCharts } from "@/components/RepoCharts";
import { SyncButton } from "@/components/SyncButton";
import { apiGet, qs } from "@/lib/api";
import { formatDuration, formatNumber, percent, rankBadge, rankClass, timeAgo, timeChipClass } from "@/lib/format";
import { repoGraphPreview } from "@/lib/graph-preview";
import type { RepoData } from "@/types/api";

type Props = {
  params: Promise<{ owner: string; name: string }>;
  searchParams: Promise<{ trim?: string }>;
};

export default async function RepoPage({ params, searchParams }: Props) {
  const { owner, name } = await params;
  const sp = await searchParams;
  const trim = Number(sp.trim || 0);
  const data = await apiGet<RepoData>(`/api/next/repo/${owner}/${name}${qs({ trim })}`);
  const repo = data.Repo;

  return (
    <div className="page-wrap">
      <div className="breadcrumb">
        <Link href="/" className="bc-link">
          ngmi
        </Link>
        <span className="bc-sep">/</span>
        <Link href={data.OwnerUser?.IsOrg ? `/org/${repo.Owner}` : `/user/${repo.Owner}`} className="bc-link">
          {repo.Owner}
        </Link>
        <span className="bc-sep">/</span>
        <span className="bc-current">{repo.Name}</span>
      </div>

      <div className="repo-header">
        <div className="repo-title-row">
          <h1 className="page-title mono">{repo.FullName}</h1>
          {repo.Language ? <span className="lang-badge">{repo.Language}</span> : null}
          {repo.Stars ? <span className="stars-badge">{formatNumber(repo.Stars)} stars</span> : null}
        </div>
        {repo.Description ? <p className="repo-desc">{repo.Description}</p> : null}
        <div className="sync-row">
          <span className={`sync-badge ${data.IsSyncing ? "syncing" : "done"}`}>{data.IsSyncing ? "Syncing..." : `Synced ${timeAgo(repo.LastSynced)}`}</span>
          <SyncButton owner={repo.Owner} name={repo.Name} />
          {data.ShareURL ? (
            <a href={data.ShareURL} target="_blank" rel="noopener" className="btn btn-sm btn-outline">
              Share on X →
            </a>
          ) : null}
        </div>
        <div className="badge-snippet-wrap">
          <span className="badge-snippet-label">README badge:</span>
          <code className="badge-snippet">[![ngmi](https://ngmi.review/badge/{repo.Owner}/{repo.Name}.svg)](https://ngmi.review/repo/{repo.Owner}/{repo.Name})</code>
        </div>
      </div>

      <div className="stats-grid">
        <Stat label="Merged PRs" value={formatNumber(repo.MergedPRCount)} />
        <Stat label="Avg Merge Time" value={formatDuration(repo.AvgMergeTimeSecs)} />
        <Stat label="Fastest PR" value={formatDuration(repo.MinMergeTimeSecs)} className="speed" />
        <Stat label="Slowest PR" value={formatDuration(repo.MaxMergeTimeSecs)} className="slow" />
        {data.SpeedRank ? <Stat label="Global Speed Rank" value={`#${data.SpeedRank}`} className="highlight" /> : null}
      </div>

      {repo.MergedPRCount ? <RepoCharts owner={repo.Owner} name={repo.Name} trim={trim} /> : null}

      <RelationGraph src={`/api/next/graph/repo/${encodeURIComponent(repo.Owner)}/${encodeURIComponent(repo.Name)}`} initialData={repoGraphPreview(data)} />

      {data.TopReviewers?.length ? (
        <section className="section">
          <h2 className="section-title">Top Reviewers</h2>
          <div className="reviewer-list">
            {data.TopReviewers.map((reviewer, index) => (
              <Link href={`/user/${reviewer.Login}`} className="reviewer-row" key={reviewer.Login}>
                <span className={`reviewer-rank ${rankClass(index + 1)}`}>{rankBadge(index + 1)}</span>
                {reviewer.AvatarURL ? (
                  // eslint-disable-next-line @next/next/no-img-element
                  <img src={reviewer.AvatarURL} className="reviewer-avatar" alt={`@${reviewer.Login}`} />
                ) : null}
                <div className="reviewer-info">
                  <span className="reviewer-name mono">@{reviewer.Login}</span>
                  <div className="reviewer-stats">
                    <span className="badge badge-blue">{formatNumber(reviewer.TotalReviews)} reviews</span>
                    {reviewer.Approvals ? <span className="badge badge-green">✓ {formatNumber(reviewer.Approvals)} approved</span> : null}
                    {reviewer.ChangesRequested ? <span className="badge badge-red">↺ {formatNumber(reviewer.ChangesRequested)} blocked</span> : null}
                  </div>
                </div>
                <div className="reviewer-bars">
                  <div className="mini-bar">
                    <div className="mini-bar-fill green" style={{ width: `${percent(reviewer.Approvals, reviewer.TotalReviews)}%` }} />
                  </div>
                </div>
              </Link>
            ))}
          </div>
        </section>
      ) : null}

      {data.RecentPRs?.length ? (
        <section className="section">
          <h2 className="section-title">Recent Merged PRs</h2>
          <div className="pr-table-wrap">
            <table className="pr-table">
              <thead>
                <tr>
                  <th>#</th>
                  <th>Title</th>
                  <th>Author</th>
                  <th>Time</th>
                  <th>Reviews</th>
                  <th>Blocks</th>
                </tr>
              </thead>
              <tbody>
                {data.RecentPRs.map((pr) => (
                  <tr className="pr-row" key={pr.ID}>
                    <td className="pr-num mono">#{pr.Number}</td>
                    <td className="pr-title">
                      <a href={`https://github.com/${pr.RepoFullName}/pull/${pr.Number}`} target="_blank" rel="noopener" className="pr-link">
                        {pr.Title}
                      </a>
                    </td>
                    <td className="pr-author">
                      <Link href={`/user/${pr.AuthorLogin}`} className="pr-author-link mono">
                        @{pr.AuthorLogin}
                      </Link>
                    </td>
                    <td className="pr-time">
                      {pr.MergeTimeSecs ? <span className={timeChipClass(pr.MergeTimeSecs)}>{formatDuration(pr.MergeTimeSecs)}</span> : <span className="muted">-</span>}
                    </td>
                    <td className="pr-reviews">{pr.ReviewCount}</td>
                    <td className="pr-blocks">{pr.ChangesRequestedCount ? <span className="badge badge-red">{pr.ChangesRequestedCount}x</span> : <span className="badge badge-green">✓</span>}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </section>
      ) : !data.IsSyncing ? (
        <div className="empty-state">
          <p>No data yet. Sync is in progress - check back in a moment.</p>
        </div>
      ) : null}
    </div>
  );
}

function Stat({ label, value, className = "" }: { label: string; value: string; className?: string }) {
  return (
    <div className={`stat-card ${className === "highlight" ? "highlight" : ""}`}>
      <span className={`stat-card-num ${className !== "highlight" ? className : ""}`}>{value}</span>
      <span className="stat-card-label">{label}</span>
    </div>
  );
}
