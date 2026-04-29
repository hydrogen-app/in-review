import Link from "next/link";

import { BoardCard } from "@/components/Leaderboards";
import { SearchBox } from "@/components/SearchBox";
import { apiGet } from "@/lib/api";
import { formatNumber } from "@/lib/format";
import type { HomeData } from "@/types/api";

export default async function HomePage() {
  const data = await apiGet<HomeData>("/api/next/home");

  return (
    <>
      <section className="hero">
        <div className="hero-inner">
          <h1 className="hero-title">
            If you aren&apos;t reviewing,
            <br />
            you&apos;re ngmi.
          </h1>
          <p className="hero-sub">
            Global leaderboards for GitHub PR review time.
            <br />
            Search any public repo, user, or org.
          </p>

          <SearchBox />

          {data.PopularVisits?.length || data.RecentVisits?.length ? (
            <div className="quick-pills">
              <span className="pill-label">Try:</span>{" "}
              {data.PopularVisits?.map((visit) => (
                <Link href={visit.Path} className="pill pill-hot" key={visit.Path}>
                  {visit.Label}
                </Link>
              ))}
              {data.RecentVisits?.map((visit) => (
                <Link href={visit.Path} className="pill" key={visit.Path}>
                  {visit.Label}
                </Link>
              ))}
            </div>
          ) : null}
        </div>
      </section>

      <div className="stats-bar">
        <div className="stat-item">
          <span className="stat-num">{formatNumber(data.TotalRepos)}</span>
          <span className="stat-label">Repos Tracked</span>
        </div>
        <div className="stat-item">
          <span className="stat-num">{formatNumber(data.TotalPRs)}</span>
          <span className="stat-label">PRs Analyzed</span>
        </div>
        <div className="stat-item">
          <span className="stat-num">{formatNumber(data.TotalReviews)}</span>
          <span className="stat-label">Reviews Logged</span>
        </div>
      </div>

      <section className="leaderboards">
        <div className="section-header">
          <h2 className="section-title">Global Leaderboards</h2>
          <p className="section-sub">Populated as repos are searched.</p>
        </div>
        <div className="board-grid">
          <BoardCard href="/leaderboard/speed" label="FAST" title="Speed Demons" description="Fastest avg PR-to-merge time" entries={data.SpeedDemons} kind="repo-duration" />
          <BoardCard href="/leaderboard/graveyard" label="SLOW" title="PR Graveyard" description="Slowest avg PR-to-merge time" entries={data.PRGraveyard} kind="repo-duration" />
          <BoardCard href="/leaderboard/reviewers" label="TOP" title="Review Champions" description="Most reviews submitted, globally" entries={data.ReviewChamps} kind="user-reviews" />
          <BoardCard href="/leaderboard/gatekeepers" label="GATE" title="Gatekeepers" description="Most request-changes sent" entries={data.Gatekeepers} kind="user-blocks" />
          <BoardCard href="/leaderboard/authors" label="MERGE" title="Merge Masters" description="Authors with most merged PRs" entries={data.MergeMasters} kind="user-merged" />
          <BoardCard href="/leaderboard/oneshot" label="CLEAN" title="One-Shot Heroes" description="PRs approved first try" entries={data.OneShot} kind="repo-percent" />
        </div>
      </section>
    </>
  );
}
