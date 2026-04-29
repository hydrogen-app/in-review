import Link from "next/link";

import { formatDuration, formatNumber, rankBadge, rankClass } from "@/lib/format";
import type { LeaderboardEntry } from "@/types/api";

type BoardProps = {
  href: string;
  label: string;
  title: string;
  description: string;
  entries: LeaderboardEntry[];
  kind: "repo-duration" | "repo-percent" | "user-reviews" | "user-blocks" | "user-merged";
};

export function BoardCard({ href, label, title, description, entries, kind }: BoardProps) {
  return (
    <div className="board-card">
      <Link href={href} className="board-header board-header-link">
        <span className="board-emoji">{label}</span>
        <div>
          <h3 className="board-title">{title}</h3>
          <p className="board-desc">{description}</p>
        </div>
        <span className="board-header-arrow">→</span>
      </Link>
      <div className="board-rows">
        {entries?.length ? (
          entries.map((entry) => {
            const userKind = kind.startsWith("user");
            return (
              <Link href={userKind ? `/user/${entry.Name}` : `/repo/${entry.Name}`} className="board-row" key={`${title}:${entry.Rank}:${entry.Name}`}>
                <span className={`board-rank ${rankClass(entry.Rank)}`}>{rankBadge(entry.Rank)}</span>
                {entry.Extra ? (
                  // eslint-disable-next-line @next/next/no-img-element
                  <img src={entry.Extra} className="board-avatar" alt="" />
                ) : null}
                <span className="board-name mono">{userKind ? `@${entry.Name}` : entry.Name}</span>
                <span className={`board-val ${kind === "repo-duration" ? (entry.Value < 86400 ? "speed" : entry.Value > 2592000 ? "slow" : "") : ""} ${kind === "user-blocks" ? "warn" : ""} ${kind === "repo-percent" ? "green" : ""}`}>
                  {valueLabel(entry, kind)}
                </span>
              </Link>
            );
          })
        ) : (
          <p className="board-empty">Syncing popular repos...</p>
        )}
      </div>
      <Link href={href} className="board-view-all">
        View full leaderboard →
      </Link>
    </div>
  );
}

function valueLabel(entry: LeaderboardEntry, kind: BoardProps["kind"]): string {
  switch (kind) {
    case "repo-duration":
      return formatDuration(entry.Value);
    case "repo-percent":
      return `${entry.Value}% clean`;
    case "user-blocks":
      return `${formatNumber(entry.Count)} blocks`;
    case "user-merged":
      return `${formatNumber(entry.Count)} merged`;
    case "user-reviews":
    default:
      return `${formatNumber(entry.Count)} reviews`;
  }
}
