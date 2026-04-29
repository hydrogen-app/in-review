export function formatDuration(secs?: number | null): string {
  if (!secs || secs <= 0) return "-";
  const minutes = Math.floor(secs / 60);
  const hours = secs / 3600;
  const days = Math.floor(hours / 24);
  if (secs < 3600) return `${minutes}m`;
  if (secs < 86400) return `${hours.toFixed(1)}h`;
  if (days === 1) return "1 day";
  if (days < 30) return `${days} days`;
  const months = Math.floor(days / 30);
  if (months === 1) return "1 month";
  if (months < 12) return `${months} months`;
  const years = Math.floor(days / 365);
  return years === 1 ? "1 year" : `${years} years`;
}

export function formatNumber(n?: number | null): string {
  const value = n ?? 0;
  if (value < 1000) return `${value}`;
  if (value < 1_000_000) return `${(value / 1000).toFixed(1)}k`;
  return `${(value / 1_000_000).toFixed(1)}M`;
}

export function timeAgo(value?: string | null): string {
  if (!value) return "never";
  const then = new Date(value).getTime();
  if (Number.isNaN(then)) return "never";
  const diff = Date.now() - then;
  if (diff < 60_000) return "just now";
  if (diff < 3_600_000) return `${Math.floor(diff / 60_000)}m ago`;
  if (diff < 86_400_000) return `${Math.floor(diff / 3_600_000)}h ago`;
  const days = Math.floor(diff / 86_400_000);
  if (days === 1) return "1 day ago";
  if (days < 30) return `${days} days ago`;
  return `${Math.floor(days / 30)} months ago`;
}

export function percent(part?: number | null, whole?: number | null): number {
  if (!part || !whole) return 0;
  return Math.floor((part * 100) / whole);
}

export function rankBadge(rank: number): string {
  return `#${rank}`;
}

export function rankClass(rank: number): string {
  if (rank === 1) return "rank-gold";
  if (rank === 2) return "rank-silver";
  if (rank === 3) return "rank-bronze";
  return "rank-other";
}

export function jsonPayload<T>(raw?: string | null): T | null {
  if (!raw) return null;
  try {
    return JSON.parse(raw) as T;
  } catch {
    return null;
  }
}

export function timeChipClass(secs?: number | null): string {
  if (!secs) return "time-chip";
  if (secs < 86400) return "time-chip speed";
  if (secs > 2592000) return "time-chip slow";
  return "time-chip";
}
