import Link from "next/link";

import { apiGet } from "@/lib/api";
import { formatNumber, timeAgo } from "@/lib/format";
import type { DashboardData } from "@/types/api";

export default async function DashboardPage() {
  let data: DashboardData;
  try {
    data = await apiGet<DashboardData>("/api/next/dashboard");
  } catch {
    return (
      <div className="error-page">
        <div className="error-code">401</div>
        <div className="error-title">Login Required</div>
        <p className="error-msg">Sign in with GitHub to view your dashboard.</p>
        <div className="error-actions">
          <a href="/auth/login" className="btn">Login</a>
        </div>
      </div>
    );
  }

  return (
    <div className="page-wrap">
      <div className="user-header">
        {data.AvatarURL ? (
          // eslint-disable-next-line @next/next/no-img-element
          <img src={data.AvatarURL} alt={`@${data.Login}`} className="user-avatar" />
        ) : null}
        <div className="user-info">
          <h1 className="page-title">@{data.Login}</h1>
          <p className="user-login">Your dashboard</p>
        </div>
      </div>

      {!data.HasInstall ? (
        <div className="verdict-card">
          <p className="verdict-text">Connect your repos</p>
          <a href={data.InstallURL} className="btn">Install GitHub App →</a>
        </div>
      ) : (
        <>
          <section className="section">
            <h2 className="section-title">Tracked repos</h2>
            {data.TrackedRepos?.length ? (
              <div className="org-repos">
                {data.TrackedRepos.map((repo) => (
                  <Link href={`/repo/${repo.FullName}`} className="org-repo-card" key={repo.FullName}>
                    <div className="org-repo-top">
                      <span className="org-repo-name mono">{repo.FullName}</span>
                      <span className={`sync-badge ${repo.SyncStatus}`}>{repo.SyncStatus}</span>
                    </div>
                    <div className="org-repo-stats">
                      <span>{formatNumber(repo.MergedPRCount)} merged PRs</span>
                      {repo.LastSynced ? <span> · synced {timeAgo(repo.LastSynced)}</span> : null}
                    </div>
                  </Link>
                ))}
              </div>
            ) : (
              <p className="col-empty">No repos tracked yet.</p>
            )}
          </section>

          {data.AvailableRepos?.length ? (
            <section className="section">
              <h2 className="section-title">Available repos</h2>
              <p className="repo-desc">These repos are accessible via your GitHub App installation but not yet tracked.</p>
              <div className="org-repos">
                {data.AvailableRepos.map((repo) => (
                  <div className="org-repo-card" key={repo}>
                    <div className="org-repo-top">
                      <span className="org-repo-name mono">{repo}</span>
                      <form method="POST" action="/api/repos/add" style={{ marginLeft: "auto" }}>
                        <input type="hidden" name="repo" value={repo} />
                        <button type="submit" className="btn btn-sm">+ Track</button>
                      </form>
                    </div>
                  </div>
                ))}
              </div>
            </section>
          ) : null}

          <div className="verdict-card">
            <p className="verdict-text">Want to track more repos?</p>
            <a href={data.InstallURL} className="btn btn-outline">Update GitHub App installation →</a>
          </div>
        </>
      )}
    </div>
  );
}
