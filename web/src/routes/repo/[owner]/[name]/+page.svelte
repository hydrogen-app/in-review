<script lang="ts">
	import { onMount, onDestroy } from 'svelte';
	import { page } from '$app/stores';
	import { Badge } from '$lib/components/ui/badge';
	import { Button } from '$lib/components/ui/button';
	import { Card, CardHeader, CardTitle, CardContent } from '$lib/components/ui/card';
	import { Skeleton } from '$lib/components/ui/skeleton';
	import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from '$lib/components/ui/table';
	import * as Chart from '$lib/components/ui/chart/index.js';
	import { LineChart, BarChart } from 'layerchart';
	import { formatDuration, formatNumber, rankClass, timeAgo } from '$lib/utils';
	import { toRows } from '$lib/types';
	import type { Repo, ReviewerStats, PullRequest } from '$lib/types';

	let { data } = $props();
	const d = $derived(data.repo);
	const repo: Repo = $derived(d.Repo);
	const owner = $derived($page.params.owner);
	const name = $derived($page.params.name);

	let syncStatus = $state<{ status: string; queuePos: number; timeAgo: string } | null>(null);
	let pollInterval: ReturnType<typeof setInterval>;

	async function fetchSyncStatus() {
		try {
			const r = await fetch(`/api/v1/sync-status/${owner}/${name}`);
			syncStatus = await r.json();
			if (syncStatus?.status === 'done') clearInterval(pollInterval);
		} catch {}
	}

	async function triggerSync() {
		await fetch(`/api/sync/${owner}/${name}`, { method: 'POST' });
		syncStatus = { status: 'queued', queuePos: 1, timeAgo: '' };
		startPolling();
	}

	function startPolling() {
		clearInterval(pollInterval);
		pollInterval = setInterval(fetchSyncStatus, 2000);
	}

	onMount(() => {
		fetchSyncStatus();
		if (d?.IsSyncing) startPolling();
	});
	onDestroy(() => clearInterval(pollInterval));

	const timeData = $derived(
		d?.TimeChart?.labels
			? toRows(d.TimeChart.labels, {
					avgHours: d.TimeChart.avgHours,
					medianHours: d.TimeChart.medianHours
				})
			: []
	);

	const sizeData = $derived(
		d?.SizeChart?.labels
			? toRows(d.SizeChart.labels, { avgHours: d.SizeChart.avgHours })
			: []
	);

	const timeChartConfig = {
		avgHours: { label: 'Avg', color: 'var(--chart-1)' },
		medianHours: { label: 'Median', color: 'var(--chart-2)' }
	} satisfies Chart.ChartConfig;

	const sizeChartConfig = {
		avgHours: { label: 'Avg Time', color: 'var(--chart-1)' }
	} satisfies Chart.ChartConfig;

	const badgeSnippet = $derived(
		`[![ngmi](https://ngmi.review/badge/${owner}/${name}.svg)](https://ngmi.review/repo/${owner}/${name})`
	);
</script>

<svelte:head>
	<title>{repo?.FullName ?? `${owner}/${name}`} — ngmi</title>
</svelte:head>

<!-- Breadcrumb -->
<div class="mb-3 flex items-center gap-1 text-xs text-muted-foreground">
	<a href="/" class="hover:text-foreground">ngmi</a>
	<span>/</span>
	<a href="/user/{owner}" class="hover:text-foreground">{owner}</a>
	<span>/</span>
	<span class="text-foreground">{name}</span>
</div>

<!-- Header -->
<div class="mb-6">
	<div class="mb-1 flex flex-wrap items-center gap-2">
		<h1 class="font-mono text-base font-bold">{repo?.FullName}</h1>
		{#if repo?.Language}
			<Badge variant="secondary">{repo.Language}</Badge>
		{/if}
		{#if repo?.Stars}
			<Badge variant="outline">{formatNumber(repo.Stars)} ★</Badge>
		{/if}
	</div>
	{#if repo?.Description}
		<p class="mb-2 text-sm text-muted-foreground">{repo.Description}</p>
	{/if}

	<div class="flex flex-wrap items-center gap-2">
		{#if syncStatus}
			{#if syncStatus.status === 'syncing'}
				<Badge variant="secondary" class="animate-pulse">⟳ Syncing…</Badge>
			{:else if syncStatus.status === 'queued'}
				<Badge variant="secondary" class="animate-pulse">⟳ Queue #{syncStatus.queuePos}</Badge>
			{:else if syncStatus.status === 'done'}
				<Badge variant="outline" class="text-green-400">✓ Synced {syncStatus.timeAgo}</Badge>
			{:else}
				<Badge variant="outline">⏳ Pending</Badge>
			{/if}
		{/if}
		<Button size="sm" variant="outline" onclick={triggerSync}>↻ Sync Now</Button>
		{#if d?.ShareURL}
			<a href={d.ShareURL} target="_blank" rel="noopener">
				<Button size="sm" variant="outline">Share on X →</Button>
			</a>
		{/if}
	</div>

	<div class="mt-2 flex items-center gap-2 text-xs">
		<span class="text-muted-foreground">README badge:</span>
		<code class="rounded bg-muted px-1.5 py-0.5 font-mono text-xs">{badgeSnippet}</code>
		<Button size="sm" variant="ghost" class="h-6 px-2 text-xs"
			onclick={() => navigator.clipboard.writeText(badgeSnippet)}>Copy</Button>
	</div>
</div>

<!-- Stats grid -->
<div class="mb-6 grid grid-cols-2 gap-3 sm:grid-cols-3 lg:grid-cols-5">
	<Card class="text-center">
		<CardContent class="pt-4 pb-3">
			<div class="text-xl font-bold">{formatNumber(repo?.MergedPRCount ?? 0)}</div>
			<div class="text-xs text-muted-foreground">Merged PRs</div>
		</CardContent>
	</Card>
	<Card class="text-center">
		<CardContent class="pt-4 pb-3">
			<div class="text-xl font-bold">{formatDuration(repo?.AvgMergeTimeSecs ?? 0)}</div>
			<div class="text-xs text-muted-foreground">Avg Merge Time</div>
		</CardContent>
	</Card>
	<Card class="text-center">
		<CardContent class="pt-4 pb-3">
			<div class="text-xl font-bold text-green-400">{formatDuration(repo?.MinMergeTimeSecs ?? 0)}</div>
			<div class="text-xs text-muted-foreground">Fastest PR</div>
		</CardContent>
	</Card>
	<Card class="text-center">
		<CardContent class="pt-4 pb-3">
			<div class="text-xl font-bold text-red-400">{formatDuration(repo?.MaxMergeTimeSecs ?? 0)}</div>
			<div class="text-xs text-muted-foreground">Slowest PR</div>
		</CardContent>
	</Card>
	{#if d?.SpeedRank}
		<Card class="text-center">
			<CardContent class="pt-4 pb-3">
				<div class="text-xl font-bold text-yellow-400">#{d.SpeedRank}</div>
				<div class="text-xs text-muted-foreground">Global Speed Rank</div>
			</CardContent>
		</Card>
	{/if}
</div>

<!-- Charts -->
{#if timeData.length > 0}
	<Card class="mb-6">
		<CardHeader>
			<CardTitle class="text-sm">Review Time Over Time (hrs)</CardTitle>
		</CardHeader>
		<CardContent>
			<Chart.Container config={timeChartConfig} class="h-48">
				<LineChart
					data={timeData}
					x="label"
					axis={true}
					series={[
						{ key: 'avgHours', label: 'Avg', color: 'var(--chart-1)' },
						{ key: 'medianHours', label: 'Median', color: 'var(--chart-2)' }
					]}
				>
					{#snippet tooltip()}
						<Chart.Tooltip />
					{/snippet}
				</LineChart>
			</Chart.Container>
		</CardContent>
	</Card>
{/if}

{#if sizeData.length > 0}
	<Card class="mb-6">
		<CardHeader>
			<CardTitle class="text-sm">Avg Review Time by PR Size (hrs)</CardTitle>
		</CardHeader>
		<CardContent>
			<Chart.Container config={sizeChartConfig} class="h-40">
				<BarChart
					data={sizeData}
					x="label"
					axis="x"
					series={[{ key: 'avgHours', label: 'Avg Time', color: 'var(--chart-1)' }]}
				>
					{#snippet tooltip()}
						<Chart.Tooltip />
					{/snippet}
				</BarChart>
			</Chart.Container>
		</CardContent>
	</Card>
{/if}

<!-- Top Reviewers -->
{#if d?.TopReviewers?.length > 0}
	<Card class="mb-6">
		<CardHeader>
			<CardTitle class="text-sm">Top Reviewers</CardTitle>
		</CardHeader>
		<CardContent class="space-y-2">
			{#each d.TopReviewers as reviewer, i}
				<a href="/user/{reviewer.Login}" class="flex items-center gap-3 rounded p-1.5 hover:bg-accent">
					<span class="w-7 shrink-0 {rankClass(i + 1)}">#{i + 1}</span>
					{#if reviewer.AvatarURL}
						<img src={reviewer.AvatarURL} alt="" class="size-6 rounded-full" />
					{/if}
					<span class="flex-1 font-mono text-sm">@{reviewer.Login}</span>
					<div class="flex gap-1.5 text-xs">
						<Badge variant="secondary">{reviewer.TotalReviews} reviews</Badge>
						{#if reviewer.Approvals}
							<Badge variant="outline" class="text-green-400">✓ {reviewer.Approvals}</Badge>
						{/if}
						{#if reviewer.ChangesRequested}
							<Badge variant="outline" class="text-red-400">↺ {reviewer.ChangesRequested}</Badge>
						{/if}
					</div>
				</a>
			{/each}
		</CardContent>
	</Card>
{/if}

<!-- Recent PRs -->
{#if d?.RecentPRs?.length > 0}
	<Card>
		<CardHeader>
			<CardTitle class="text-sm">Recent Merged PRs</CardTitle>
		</CardHeader>
		<CardContent class="overflow-x-auto p-0">
			<Table>
				<TableHeader>
					<TableRow>
						<TableHead class="w-12">#</TableHead>
						<TableHead>Title</TableHead>
						<TableHead>Author</TableHead>
						<TableHead>Time</TableHead>
						<TableHead class="w-16">Reviews</TableHead>
						<TableHead class="w-16">Blocks</TableHead>
					</TableRow>
				</TableHeader>
				<TableBody>
					{#each d.RecentPRs as pr}
						<TableRow>
							<TableCell class="font-mono text-xs text-muted-foreground">#{pr.Number}</TableCell>
							<TableCell class="max-w-xs truncate text-xs">
								<a href="https://github.com/{pr.RepoFullName}/pull/{pr.Number}" target="_blank" rel="noopener" class="hover:underline">
									{pr.Title}
								</a>
							</TableCell>
							<TableCell class="text-xs">
								<a href="/user/{pr.AuthorLogin}" class="font-mono hover:underline">@{pr.AuthorLogin}</a>
							</TableCell>
							<TableCell class="text-xs">
								{#if pr.MergeTimeSecs}
									<Badge
										variant={pr.MergeTimeSecs < 86400 ? 'outline' : 'secondary'}
										class={pr.MergeTimeSecs < 86400 ? 'text-green-400' : pr.MergeTimeSecs > 2592000 ? 'text-red-400' : ''}
									>
										{formatDuration(pr.MergeTimeSecs)}
									</Badge>
								{:else}
									—
								{/if}
							</TableCell>
							<TableCell class="text-center text-xs">{pr.ReviewCount}</TableCell>
							<TableCell class="text-xs">
								{#if pr.ChangesRequestedCount}
									<Badge variant="secondary" class="text-red-400">{pr.ChangesRequestedCount}×</Badge>
								{:else}
									<Badge variant="outline" class="text-green-400">✓</Badge>
								{/if}
							</TableCell>
						</TableRow>
					{/each}
				</TableBody>
			</Table>
		</CardContent>
	</Card>
{/if}
