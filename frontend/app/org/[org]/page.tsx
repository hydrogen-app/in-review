import Link from "next/link";
import { redirect } from "next/navigation";

import { TimeCharts, type TimePayload } from "@/components/Charts";
import { RelationGraph } from "@/components/RelationGraph";
import { apiGet, qs } from "@/lib/api";
import { formatDuration, formatNumber, jsonPayload, rankBadge, rankClass } from "@/lib/format";
import { orgGraphPreview } from "@/lib/graph-preview";
import type { OrgData } from "@/types/api";

type Props = {
  params: Promise<{ org: string }>;
  searchParams: Promise<{ trim?: string }>;
};

export default async function OrgPage({ params, searchParams }: Props) {
  const { org } = await params;
  const sp = await searchParams;
  const trim = Number(sp.trim || 0);
  const data = await apiGet<OrgData>(`/api/next/org/${org}${qs({ trim })}`);
  if (data.redirect) redirect(data.redirect);
  const time = jsonPayload<TimePayload>(data.TimeChartJSON);

  return (
    <div className="page-wrap">
      <div className="user-header">
        {data.Org.AvatarURL ? (
          // eslint-disable-next-line @next/next/no-img-element
          <img src={data.Org.AvatarURL} className="user-avatar" alt={data.Org.Login} />
        ) : null}
        <div className="user-info">
          <div className="org-type-badge">Organization</div>
          <h1 className="page-title">{data.Org.Name || data.Org.Login}</h1>
          <p className="user-login mono">@{data.Org.Login}</p>
          {data.Org.Bio ? <p className="user-bio">{data.Org.Bio}</p> : null}
          <div className="user-meta">
            {data.TotalMergedPRs ? <span>{formatNumber(data.TotalMergedPRs)} merged PRs tracked</span> : null}
            {data.Org.PublicRepos ? <span>{formatNumber(data.Org.PublicRepos)} public repos</span> : null}
          </div>
        </div>
        <a href={`https://github.com/${data.Org.Login}`} target="_blank" rel="noopener" className="btn btn-outline">
          GitHub ↗
        </a>
      </div>

      <TimeCharts payload={time} />

      <RelationGraph src={`/api/next/graph/org/${encodeURIComponent(data.Org.Login)}`} initialData={orgGraphPreview(data)} />

      <div className="two-col">
        <OrgBoard title="In-Org Review Champions" entries={data.ReviewerBoard} suffix="reviews" />
        <OrgBoard title="In-Org Gatekeepers" entries={data.GatekeeperBoard} suffix="blocks" warn />
      </div>

      <section className="section">
        <h2 className="section-title">Repos</h2>
        {data.Repos?.length ? (
          <div className="org-repos">
            {data.Repos.map((repo) => (
              <Link href={`/repo/${repo.FullName}`} className="org-repo-card" key={repo.FullName}>
                <div className="org-repo-top">
                  <span className="org-repo-name mono">{repo.Name}</span>
                  {repo.Language ? <span className="lang-badge sm">{repo.Language}</span> : null}
                  {repo.Stars ? <span className="stars-badge sm">{formatNumber(repo.Stars)} stars</span> : null}
                  {repo.SyncStatus ? <span className={`sync-badge ${repo.SyncStatus} sm`}>{repo.SyncStatus}</span> : null}
                </div>
                {repo.Description ? <p className="org-repo-desc">{repo.Description}</p> : null}
                {repo.MergedPRCount ? (
                  <div className="org-repo-stats">
                    <span>{formatNumber(repo.MergedPRCount)} merged PRs</span>
                    <span> · </span>
                    <span>avg {formatDuration(repo.AvgMergeTimeSecs)}</span>
                    <span> · </span>
                    <span>fastest {formatDuration(repo.MinMergeTimeSecs)}</span>
                  </div>
                ) : null}
              </Link>
            ))}
          </div>
        ) : (
          <div className="empty-state">
            <p>Syncing top repos for {data.Org.Login}... check back in a moment.</p>
          </div>
        )}
      </section>
    </div>
  );
}

function OrgBoard({ title, entries, suffix, warn = false }: { title: string; entries?: OrgData["ReviewerBoard"]; suffix: string; warn?: boolean }) {
  return (
    <div className="col-card">
      <h2 className="col-title">{title}</h2>
      {entries?.length ? (
        <div className="board-rows">
          {entries.map((entry) => (
            <Link href={`/user/${entry.Name}`} className="board-row" key={entry.Name}>
              <span className={`board-rank ${rankClass(entry.Rank)}`}>{rankBadge(entry.Rank)}</span>
              {entry.Extra ? (
                // eslint-disable-next-line @next/next/no-img-element
                <img src={entry.Extra} className="board-avatar" alt="" />
              ) : null}
              <span className="board-name mono">@{entry.Name}</span>
              <span className={`board-val ${warn ? "warn" : ""}`}>
                {formatNumber(entry.Count)} {suffix}
              </span>
            </Link>
          ))}
        </div>
      ) : (
        <p className="col-empty">No data yet.</p>
      )}
    </div>
  );
}
