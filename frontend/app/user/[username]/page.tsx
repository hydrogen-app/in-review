import Link from "next/link";
import { redirect } from "next/navigation";

import { RelationGraph } from "@/components/RelationGraph";
import { UserCharts } from "@/components/UserCharts";
import { apiGet } from "@/lib/api";
import { formatDuration, formatNumber, percent, timeAgo, timeChipClass } from "@/lib/format";
import type { UserData } from "@/types/api";

type Props = {
  params: Promise<{ username: string }>;
};

export default async function UserPage({ params }: Props) {
  const { username } = await params;
  const data = await apiGet<UserData>(`/api/next/user/${username}`);
  if (data.redirect) redirect(data.redirect);
  const user = data.User;
  const approvalRate = percent(data.ReviewerStats?.Approvals, data.ReviewerStats?.TotalReviews);

  return (
    <div className="page-wrap">
      <div className="user-header">
        {user.AvatarURL ? (
          // eslint-disable-next-line @next/next/no-img-element
          <img src={user.AvatarURL} className="user-avatar" alt={`@${user.Login}`} />
        ) : null}
        <div className="user-info">
          <h1 className="page-title">
            {user.Name || `@${user.Login}`}
            {data.IsNGMI ? <span className="ngmi-stamp">ngmi</span> : null}
          </h1>
          <p className="user-login mono">@{user.Login}</p>
          {user.Bio ? <p className="user-bio">{user.Bio}</p> : null}
          <div className="user-meta">
            {user.Company ? <span className="meta-item">{user.Company}</span> : null}
            {user.Location ? <span className="meta-item">{user.Location}</span> : null}
            {user.Followers ? <span className="meta-item">{formatNumber(user.Followers)} followers</span> : null}
            {user.PublicRepos ? <span className="meta-item">{formatNumber(user.PublicRepos)} repos</span> : null}
            {data.ReviewerStats?.LastReviewedAt ? <span className="meta-item muted">last reviewed {timeAgo(data.ReviewerStats.LastReviewedAt)}</span> : null}
          </div>
        </div>
        <a href={`https://github.com/${user.Login}`} target="_blank" rel="noopener" className="btn btn-outline">
          GitHub ↗
        </a>
      </div>

      {!data.ReviewerStats?.TotalReviews ? (
        <div className="verdict-card verdict-shame">
          <p className="verdict-text">No reviews on record. ngmi.</p>
        </div>
      ) : (
        <div className="verdict-card">
          <p className="verdict-text">
            {data.ReviewerRank ? `#${data.ReviewerRank} globally - ` : ""}
            {formatNumber(data.ReviewerStats.TotalReviews)} reviews, {approvalRate}% approval rate.
          </p>
          {data.ShareURL ? (
            <div className="verdict-actions">
              <a href={data.ShareURL} target="_blank" rel="noopener" className="btn btn-sm btn-outline">
                Share on X →
              </a>
            </div>
          ) : null}
        </div>
      )}

      {data.ReviewerRank || data.GatekeeperRank || data.AuthorRank ? (
        <div className="rank-banner">
          {data.ReviewerRank ? (
            <div className="rank-pill">
              <Link href="/leaderboard/reviewers">#{data.ReviewerRank} Reviewer</Link>
              <span className="rank-pill-sub">globally</span>
            </div>
          ) : null}
          {data.GatekeeperRank ? (
            <div className="rank-pill warn">
              <Link href="/leaderboard/gatekeepers">#{data.GatekeeperRank} Gatekeeper</Link>
              <span className="rank-pill-sub">globally</span>
            </div>
          ) : null}
          {data.AuthorRank ? (
            <div className="rank-pill">
              <Link href="/leaderboard/authors">#{data.AuthorRank} Author</Link>
              <span className="rank-pill-sub">globally</span>
            </div>
          ) : null}
        </div>
      ) : null}

      <RelationGraph src={`/api/next/graph/user/${encodeURIComponent(user.Login)}`} />

      <div className="two-col">
        <div className="col-card">
          <h2 className="col-title">As a Reviewer</h2>
          {data.ReviewerStats ? (
            <>
              <div className="col-stats">
                <Metric label="Total Reviews" value={formatNumber(data.ReviewerStats.TotalReviews)} />
                <Metric label="Approvals" value={formatNumber(data.ReviewerStats.Approvals)} className="green" />
                <Metric label="Changes Requested" value={formatNumber(data.ReviewerStats.ChangesRequested)} className="warn" />
                <Metric label="Comments" value={formatNumber(data.ReviewerStats.Comments)} className="muted" />
              </div>
              {data.ReviewerStats.TotalReviews ? (
                <div className="approval-bar-wrap">
                  <div className="approval-bar">
                    <div className="approval-fill" style={{ width: `${approvalRate}%` }} />
                  </div>
                  <span className="approval-label">{approvalRate}% approval rate</span>
                </div>
              ) : null}
            </>
          ) : (
            <p className="col-empty">No review data yet for this user.</p>
          )}
        </div>

        <div className="col-card">
          <h2 className="col-title">As an Author</h2>
          {data.AuthorStats ? (
            <div className="col-stats">
              <Metric label="Merged PRs" value={formatNumber(data.AuthorStats.MergedPRs)} />
              <Metric label="Avg Merge Time" value={formatDuration(data.AuthorStats.AvgMergeTimeSecs)} className="speed" />
              {data.AuthorStats.TotalPRs ? <Metric label="Merge Rate" value={`${percent(data.AuthorStats.MergedPRs, data.AuthorStats.TotalPRs)}%`} /> : null}
              {data.AuthorStats.TotalLinesWritten ? <Metric label="Total Lines Written" value={formatNumber(data.AuthorStats.TotalLinesWritten)} /> : null}
              {data.AuthorStats.AvgPRSize ? <Metric label="Avg PR Size" value={formatNumber(Math.round(data.AuthorStats.AvgPRSize))} /> : null}
              {data.AuthorStats.CleanApprovalRate ? <Metric label="Clean Approval Rate" value={`${Math.round(data.AuthorStats.CleanApprovalRate)}%`} className="green" /> : null}
              {data.AuthorStats.AvgChangesRequested ? <Metric label="Avg Changes Requested" value={`${data.AuthorStats.AvgChangesRequested.toFixed(1)}x`} className="warn" /> : null}
            </div>
          ) : (
            <p className="col-empty">No PR author data yet for this user.</p>
          )}
        </div>
      </div>

      {data.FastestPR || data.SlowestPR ? (
        <section className="section">
          <h2 className="section-title">Personal Records</h2>
          <div className="two-col">
            {data.FastestPR ? <Record title="Fastest Merge" tone="speed" pr={data.FastestPR} /> : null}
            {data.SlowestPR ? <Record title="Slowest Merge" tone="slow" pr={data.SlowestPR} /> : null}
          </div>
        </section>
      ) : null}

      <UserCharts username={user.Login} />

      {data.ReviewersOfMe?.length || data.AuthorsIReview?.length ? (
        <section className="section">
          <h2 className="section-title">Collaborators</h2>
          <div className="two-col">
            <PeopleList title="Reviews my PRs most" people={data.ReviewersOfMe} suffix="reviews" />
            <PeopleList title="I review most" people={data.AuthorsIReview} suffix="reviews" />
          </div>
        </section>
      ) : null}

      {data.ReviewedRepos?.length ? (
        <section className="section">
          <h2 className="section-title">Repos Reviewed Most</h2>
          <table className="pr-table">
            <thead>
              <tr>
                <th>Repository</th>
                <th>Reviews Given</th>
              </tr>
            </thead>
            <tbody>
              {data.ReviewedRepos.map((repo) => (
                <tr className="pr-row" key={repo.FullName}>
                  <td>
                    <Link href={`/repo/${repo.FullName}`} className="pr-link mono">
                      {repo.FullName}
                    </Link>
                  </td>
                  <td>{formatNumber(repo.Count)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </section>
      ) : null}

      {data.ContributedRepos?.length ? (
        <section className="section">
          <h2 className="section-title">Repos Contributed To</h2>
          <table className="pr-table">
            <thead>
              <tr>
                <th>Repository</th>
                <th>Merged PRs</th>
                <th>Avg Merge Time</th>
                <th>Fastest</th>
              </tr>
            </thead>
            <tbody>
              {data.ContributedRepos.map((repo) => (
                <tr className="pr-row" key={repo.FullName}>
                  <td>
                    <Link href={`/repo/${repo.FullName}`} className="pr-link mono">
                      {repo.FullName}
                    </Link>
                  </td>
                  <td>{formatNumber(repo.MergedPRCount)}</td>
                  <td>
                    <span className={timeChipClass(repo.AvgMergeTimeSecs)}>{formatDuration(repo.AvgMergeTimeSecs)}</span>
                  </td>
                  <td className="speed">{formatDuration(repo.MinMergeTimeSecs)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </section>
      ) : null}
    </div>
  );
}

function Metric({ label, value, className = "" }: { label: string; value: string; className?: string }) {
  return (
    <div className="col-stat">
      <span className={`col-stat-num ${className}`}>{value}</span>
      <span className="col-stat-label">{label}</span>
    </div>
  );
}

function Record({ title, tone, pr }: { title: string; tone: "speed" | "slow"; pr: NonNullable<UserData["FastestPR"]> }) {
  return (
    <div className="col-card">
      <h3 className="col-title">{title}</h3>
      <div className="col-stats">
        <Metric label="merge time" value={formatDuration(pr.MergeTimeSecs)} className={tone} />
      </div>
      <a href={`https://github.com/${pr.RepoFullName}/pull/${pr.Number}`} target="_blank" rel="noopener" className="pr-link mono record-pr-title">
        {pr.RepoFullName} #{pr.Number}
      </a>
      <p className="record-pr-desc">{pr.Title}</p>
    </div>
  );
}

function PeopleList({ title, people, suffix }: { title: string; people?: { Login: string; AvatarURL: string; Count: number }[]; suffix: string }) {
  if (!people?.length) return <div className="col-card" />;
  return (
    <div className="col-card">
      <h3 className="col-title">{title}</h3>
      <div className="board-rows">
        {people.map((person, index) => (
          <Link href={`/user/${person.Login}`} className="board-row" key={person.Login}>
            <span className="board-rank">#{index + 1}</span>
            {person.AvatarURL ? (
              // eslint-disable-next-line @next/next/no-img-element
              <img src={person.AvatarURL} className="board-avatar" alt="" />
            ) : null}
            <span className="board-name mono">@{person.Login}</span>
            <span className="board-val">
              {formatNumber(person.Count)} {suffix}
            </span>
          </Link>
        ))}
      </div>
    </div>
  );
}
