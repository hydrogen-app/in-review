import { clsx, type ClassValue } from "clsx";
import { twMerge } from "tailwind-merge";

export function cn(...inputs: ClassValue[]) {
	return twMerge(clsx(inputs));
}

// eslint-disable-next-line @typescript-eslint/no-explicit-any
export type WithoutChild<T> = T extends { child?: any } ? Omit<T, "child"> : T;
// eslint-disable-next-line @typescript-eslint/no-explicit-any
export type WithoutChildren<T> = T extends { children?: any } ? Omit<T, "children"> : T;
export type WithoutChildrenOrChild<T> = WithoutChildren<WithoutChild<T>>;
export type WithElementRef<T, U extends HTMLElement = HTMLElement> = T & { ref?: U | null };

// ── Formatting helpers (mirrors Go handler logic) ──────────────────────────────

export function formatDuration(secs: number): string {
	if (!secs || secs <= 0) return '—';
	if (secs < 3600) return `${Math.floor(secs / 60)}m`;
	if (secs < 86400) return `${(secs / 3600).toFixed(1)}h`;
	const days = Math.floor(secs / 86400);
	if (days === 1) return '1 day';
	if (days < 30) return `${days} days`;
	const months = Math.floor(days / 30);
	if (months === 1) return '1 month';
	if (months < 12) return `${months} months`;
	const years = Math.floor(days / 365);
	if (years === 1) return '1 year';
	return `${years} years`;
}

export function formatDurationShort(secs: number): string {
	if (!secs || secs <= 0) return '—';
	if (secs < 60) return `${secs}s`;
	if (secs < 3600) return `${Math.floor(secs / 60)}m`;
	if (secs < 86400) return `${Math.floor(secs / 3600)}h`;
	return `${Math.floor(secs / 86400)}d`;
}

export function timeAgo(dateStr: string | null): string {
	if (!dateStr) return 'never';
	const d = (Date.now() - new Date(dateStr).getTime()) / 1000;
	if (d < 60) return 'just now';
	if (d < 3600) return `${Math.floor(d / 60)}m ago`;
	if (d < 86400) return `${Math.floor(d / 3600)}h ago`;
	const days = Math.floor(d / 86400);
	if (days === 1) return '1 day ago';
	if (days < 30) return `${days} days ago`;
	const months = Math.floor(days / 30);
	if (months === 1) return '1 month ago';
	return `${months} months ago`;
}

export function formatNumber(n: number): string {
	if (n < 1000) return String(n);
	if (n < 1_000_000) return `${(n / 1000).toFixed(1)}k`;
	return `${(n / 1_000_000).toFixed(1)}M`;
}

export function rankClass(rank: number): string {
	if (rank === 1) return 'text-yellow-400';
	if (rank === 2) return 'text-slate-300';
	if (rank === 3) return 'text-amber-600';
	return 'text-muted-foreground';
}
