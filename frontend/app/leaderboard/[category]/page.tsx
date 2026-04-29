import Link from "next/link";

import { LeaderboardSearch } from "@/components/LeaderboardSearch";
import { apiGet, qs } from "@/lib/api";
import { formatDuration, formatNumber, percent, rankBadge, rankClass, timeChipClass } from "@/lib/format";
import type { LeaderboardPageData } from "@/types/api";

type Props = {
  params: Promise<{ category: string }>;
  searchParams: Promise<{ offset?: string }>;
};

export default async function LeaderboardPage({ params, searchParams }: Props) {
  const { category } = await params;
  const sp = await searchParams;
  const offset = Number(sp.offset || 0);
  const data = await apiGet<LeaderboardPageData>(`/api/next/leaderboard/${category}${qs({ offset })}`);

  return (
    <div className="page-wrap">
      <div className="breadcrumb">
        <Link href="/" className="bc-link">
          ngmi
        </Link>
        <span className="bc-sep">/</span>
        <span className="bc-current">{data.Title}</span>
      </div>

      <div className="lb-page-header">
        <div>
          <h1 className="page-title">{data.Title}</h1>
          <p className="repo-desc">{data.Description}</p>
        </div>
        <Link href="/" className="btn btn-outline">
          Back to leaderboards
        </Link>
      </div>

      <LeaderboardSearch category={category} />

      {data.RepoRows?.length ? <RepoTable rows={data.RepoRows} /> : null}
      {data.UserRows?.length && (category === "reviewers" || category === "gatekeepers") ? <ReviewerTable rows={data.UserRows} category={category} /> : null}
      {data.UserRows?.length && category === "authors" ? <AuthorTable rows={data.UserRows} /> : null}
      {data.CleanRows?.length ? <CleanTable rows={data.CleanRows} /> : null}

      {data.HasMore ? (
        <div className="pagination">
          <Link href={`/leaderboard/${category}?offset=${data.NextOffset}`} className="page-btn">
            Next →
          </Link>
        </div>
      ) : null}

      {!data.RepoRows?.length && !data.UserRows?.length && !data.CleanRows?.length ? (
        <div className="empty-state">
          <p>No data yet. Search for repos to populate this leaderboard.</p>
        </div>
      ) : null}
    </div>
  );
}

function RepoTable({ rows }: { rows: LeaderboardPageData["RepoRows"] }) {
  return (
    <div className="pr-table-wrap">
      <table className="pr-table">
        <thead>
          <tr>
            <th>Rank</th>
            <th>Repository</th>
            <th>Avg Time</th>
            <th>Fastest PR</th>
            <th>Slowest PR</th>
            <th>Merged PRs</th>
          </tr>
        </thead>
        <tbody>
          {rows.map((row) => (
            <tr className="pr-row" key={row.FullName}>
              <td><span className={`board-rank ${rankClass(row.Rank)}`}>{rankBadge(row.Rank)}</span></td>
              <td><Link href={`/repo/${row.FullName}`} className="pr-link mono">{row.FullName}</Link></td>
              <td><span className={timeChipClass(row.AvgSecs)}>{formatDuration(row.AvgSecs)}</span></td>
              <td className="speed">{formatDuration(row.MinSecs)}</td>
              <td className="slow">{formatDuration(row.MaxSecs)}</td>
              <td className="muted">{formatNumber(row.PRCount)}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

function ReviewerTable({ rows, category }: { rows: LeaderboardPageData["UserRows"]; category: string }) {
  return (
    <div className="pr-table-wrap">
      <table className="pr-table">
        <thead>
          <tr>
            <th>Rank</th>
            <th>User</th>
            <th>{category === "reviewers" ? "Total Reviews" : "Changes Requested"}</th>
            <th>Approvals</th>
            <th>Changes Requested</th>
            <th>Approval Rate</th>
          </tr>
        </thead>
        <tbody>
          {rows.map((row) => (
            <tr className="pr-row" key={row.Login}>
              <td><span className={`board-rank ${rankClass(row.Rank)}`}>{rankBadge(row.Rank)}</span></td>
              <td>
                <Link href={`/user/${row.Login}`} className="lb-user-cell">
                  {row.AvatarURL ? (
                    // eslint-disable-next-line @next/next/no-img-element
                    <img src={row.AvatarURL} className="reviewer-avatar" alt="" />
                  ) : null}
                  <span className="mono">@{row.Login}</span>
                </Link>
              </td>
              <td className="fw-bold">{formatNumber(row.Total)}</td>
              <td className="green">{formatNumber(row.Approvals)}</td>
              <td className="warn">{formatNumber(row.ChangesRequested)}</td>
              <td className="muted">{row.Total ? `${percent(row.Approvals, row.Total)}%` : "-"}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

function AuthorTable({ rows }: { rows: LeaderboardPageData["UserRows"] }) {
  return (
    <div className="pr-table-wrap">
      <table className="pr-table">
        <thead>
          <tr>
            <th>Rank</th>
            <th>Author</th>
            <th>Merged PRs</th>
            <th>Avg Merge Time</th>
          </tr>
        </thead>
        <tbody>
          {rows.map((row) => (
            <tr className="pr-row" key={row.Login}>
              <td><span className={`board-rank ${rankClass(row.Rank)}`}>{rankBadge(row.Rank)}</span></td>
              <td><Link href={`/user/${row.Login}`} className="lb-user-cell">@{row.Login}</Link></td>
              <td className="fw-bold">{formatNumber(row.MergedPRs)}</td>
              <td>{row.AvgMergeTimeSecs ? <span className={timeChipClass(row.AvgMergeTimeSecs)}>{formatDuration(row.AvgMergeTimeSecs)}</span> : "-"}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

function CleanTable({ rows }: { rows: LeaderboardPageData["CleanRows"] }) {
  return (
    <div className="pr-table-wrap">
      <table className="pr-table">
        <thead>
          <tr>
            <th>Rank</th>
            <th>Repository</th>
            <th>Clean Rate</th>
            <th>Avg Merge Time</th>
            <th>Total PRs</th>
          </tr>
        </thead>
        <tbody>
          {rows.map((row) => (
            <tr className="pr-row" key={row.FullName}>
              <td><span className={`board-rank ${rankClass(row.Rank)}`}>{rankBadge(row.Rank)}</span></td>
              <td><Link href={`/repo/${row.FullName}`} className="pr-link mono">{row.FullName}</Link></td>
              <td><span className="clean-rate-chip">{row.CleanPct}% approved first try</span></td>
              <td>{formatDuration(row.AvgSecs)}</td>
              <td className="muted">{formatNumber(row.Total)}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}
