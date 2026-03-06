<script lang="ts">
	import { onMount } from 'svelte';
	import { page } from '$app/stores';
	import { Badge } from '$lib/components/ui/badge';
	import { Card, CardHeader, CardTitle, CardContent } from '$lib/components/ui/card';
	import { Button } from '$lib/components/ui/button';
	import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from '$lib/components/ui/table';
	import * as Chart from '$lib/components/ui/chart/index.js';
	import { LineChart, BarChart } from 'layerchart';
	import { formatDuration, formatNumber, rankClass } from '$lib/utils';
	import { toRows } from '$lib/types';
	import type { User } from '$lib/types';

	let { data } = $props();
	const d = $derived(data.user);
	const user: User = $derived(d?.User);
	const username = $derived($page.params.username);

	const activityData = $derived(
		d?.ActivityChart?.labels
			? toRows(d.ActivityChart.labels, {
					prCounts: d.ActivityChart.prCounts,
					reviewCounts: d.ActivityChart.reviewCounts
				})
			: []
	);

	const sizeBucketData = $derived(
		d?.SizeBucketChart?.labels
			? toRows(d.SizeBucketChart.labels, { prCounts: d.SizeBucketChart.prCounts })
			: []
	);

	const activityConfig = {
		prCounts: { label: 'PRs', color: 'var(--chart-1)' },
		reviewCounts: { label: 'Reviews', color: 'var(--chart-2)' }
	} satisfies Chart.ChartConfig;

	const sizeConfig = {
		prCounts: { label: 'PRs', color: 'var(--chart-1)' }
	} satisfies Chart.ChartConfig;
</script>

<svelte:head>
	<title>@{username} — ngmi</title>
</svelte:head>

<!-- Breadcrumb -->
<div class="mb-3 flex items-center gap-1 text-xs text-muted-foreground">
	<a href="/" class="hover:text-foreground">ngmi</a>
	<span>/</span>
	<span class="text-foreground">@{username}</span>
</div>

<!-- Header -->
<div class="mb-6 flex items-start gap-4">
	{#if user?.AvatarURL}
		<img src={user.AvatarURL} alt="" class="size-16 rounded-full" />
	{/if}
	<div class="min-w-0 flex-1">
		<div class="mb-1 flex flex-wrap items-center gap-2">
			<h1 class="font-mono text-base font-bold">@{user?.Login ?? username}</h1>
			{#if d?.IsNGMI}
				<Badge variant="destructive">ngmi</Badge>
			{/if}
		</div>
		{#if user?.Name}
			<p class="text-sm">{user.Name}</p>
		{/if}
		{#if user?.Bio}
			<p class="text-xs text-muted-foreground">{user.Bio}</p>
		{/if}
		<div class="mt-1 flex flex-wrap gap-3 text-xs text-muted-foreground">
			{#if user?.Company}<span>{user.Company}</span>{/if}
			{#if user?.Location}<span>{user.Location}</span>{/if}
			{#if user?.Followers}<span>{formatNumber(user.Followers)} followers</span>{/if}
		</div>
		{#if d?.ShareURL}
			<div class="mt-2">
				<a href={d.ShareURL} target="_blank" rel="noopener">
					<Button size="sm" variant="outline">Share on X →</Button>
				</a>
			</div>
		{/if}
	</div>
</div>

<!-- Rank badges -->
{#if d?.ReviewerRank || d?.GatekeeperRank || d?.AuthorRank}
	<div class="mb-6 flex flex-wrap gap-2">
		{#if d.ReviewerRank}<Badge variant="secondary" class="font-mono">#{d.ReviewerRank} Reviewer</Badge>{/if}
		{#if d.GatekeeperRank}<Badge variant="secondary" class="font-mono">#{d.GatekeeperRank} Gatekeeper</Badge>{/if}
		{#if d.AuthorRank}<Badge variant="secondary" class="font-mono">#{d.AuthorRank} Author</Badge>{/if}
	</div>
{/if}

<!-- Stats grid -->
<div class="mb-6 grid grid-cols-2 gap-3 sm:grid-cols-3 lg:grid-cols-4">
	{#if d?.ReviewerStats}
		<Card class="text-center">
			<CardContent class="pt-4 pb-3">
				<div class="text-xl font-bold">{formatNumber(d.ReviewerStats.TotalReviews)}</div>
				<div class="text-xs text-muted-foreground">Reviews</div>
			</CardContent>
		</Card>
		<Card class="text-center">
			<CardContent class="pt-4 pb-3">
				<div class="text-xl font-bold text-green-400">{formatNumber(d.ReviewerStats.Approvals)}</div>
				<div class="text-xs text-muted-foreground">Approvals</div>
			</CardContent>
		</Card>
		<Card class="text-center">
			<CardContent class="pt-4 pb-3">
				<div class="text-xl font-bold text-red-400">{formatNumber(d.ReviewerStats.ChangesRequested)}</div>
				<div class="text-xs text-muted-foreground">Blocks</div>
			</CardContent>
		</Card>
	{/if}
	{#if d?.AuthorStats}
		<Card class="text-center">
			<CardContent class="pt-4 pb-3">
				<div class="text-xl font-bold">{formatNumber(d.AuthorStats.MergedPRs)}</div>
				<div class="text-xs text-muted-foreground">Merged PRs</div>
			</CardContent>
		</Card>
		{#if d.AuthorStats.AvgMergeTimeSecs}
			<Card class="text-center">
				<CardContent class="pt-4 pb-3">
					<div class="text-xl font-bold">{formatDuration(d.AuthorStats.AvgMergeTimeSecs)}</div>
					<div class="text-xs text-muted-foreground">Avg Merge Time</div>
				</CardContent>
			</Card>
		{/if}
		{#if d.AuthorStats.CleanApprovalRate}
			<Card class="text-center">
				<CardContent class="pt-4 pb-3">
					<div class="text-xl font-bold">{d.AuthorStats.CleanApprovalRate.toFixed(0)}%</div>
					<div class="text-xs text-muted-foreground">Clean PRs</div>
				</CardContent>
			</Card>
		{/if}
	{/if}
</div>

<!-- Activity Chart -->
{#if activityData.length > 0}
	<Card class="mb-6">
		<CardHeader><CardTitle class="text-sm">Activity Over Time</CardTitle></CardHeader>
		<CardContent>
			<Chart.Container config={activityConfig} class="h-40">
				<LineChart
					data={activityData}
					x="label"
					axis={true}
					series={[
						{ key: 'prCounts', label: 'PRs', color: 'var(--chart-1)' },
						{ key: 'reviewCounts', label: 'Reviews', color: 'var(--chart-2)' }
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

<!-- PR Size Distribution -->
{#if sizeBucketData.length > 0}
	<Card class="mb-6">
		<CardHeader><CardTitle class="text-sm">PR Size Distribution</CardTitle></CardHeader>
		<CardContent>
			<Chart.Container config={sizeConfig} class="h-32">
				<BarChart
					data={sizeBucketData}
					x="label"
					axis="x"
					series={[{ key: 'prCounts', label: 'PRs', color: 'var(--chart-1)' }]}
				>
					{#snippet tooltip()}
						<Chart.Tooltip />
					{/snippet}
				</BarChart>
			</Chart.Container>
		</CardContent>
	</Card>
{/if}

<!-- Record PRs -->
{#if d?.FastestPR || d?.SlowestPR}
	<div class="mb-6 grid grid-cols-1 gap-3 sm:grid-cols-2">
		{#if d.FastestPR}
			<Card>
				<CardHeader class="pb-2"><CardTitle class="text-xs text-green-400">Fastest PR</CardTitle></CardHeader>
				<CardContent class="text-xs">
					<a href="https://github.com/{d.FastestPR.RepoFullName}/pull/{d.FastestPR.Number}" target="_blank" rel="noopener" class="font-medium hover:underline">{d.FastestPR.Title}</a>
					<div class="mt-1 text-muted-foreground">{d.FastestPR.RepoFullName} · {formatDuration(d.FastestPR.MergeTimeSecs)}</div>
				</CardContent>
			</Card>
		{/if}
		{#if d.SlowestPR}
			<Card>
				<CardHeader class="pb-2"><CardTitle class="text-xs text-red-400">Slowest PR</CardTitle></CardHeader>
				<CardContent class="text-xs">
					<a href="https://github.com/{d.SlowestPR.RepoFullName}/pull/{d.SlowestPR.Number}" target="_blank" rel="noopener" class="font-medium hover:underline">{d.SlowestPR.Title}</a>
					<div class="mt-1 text-muted-foreground">{d.SlowestPR.RepoFullName} · {formatDuration(d.SlowestPR.MergeTimeSecs)}</div>
				</CardContent>
			</Card>
		{/if}
	</div>
{/if}

<!-- Reviewed Repos -->
{#if d?.ReviewedRepos?.length > 0}
	<Card class="mb-6">
		<CardHeader><CardTitle class="text-sm">Top Reviewed Repos</CardTitle></CardHeader>
		<CardContent class="space-y-1">
			{#each d.ReviewedRepos as rr}
				<a href="/repo/{rr.FullName}" class="flex items-center justify-between rounded p-1.5 text-xs hover:bg-accent">
					<span class="font-mono">{rr.FullName}</span>
					<Badge variant="secondary">{rr.Count} reviews</Badge>
				</a>
			{/each}
		</CardContent>
	</Card>
{/if}

<!-- Collaborated With -->
{#if (d?.ReviewersOfMe?.length ?? 0) > 0 || (d?.AuthorsIReview?.length ?? 0) > 0}
	<div class="mb-6 grid grid-cols-1 gap-3 sm:grid-cols-2">
		{#if d?.ReviewersOfMe?.length > 0}
			<Card>
				<CardHeader><CardTitle class="text-sm">Reviewed My PRs</CardTitle></CardHeader>
				<CardContent class="space-y-1">
					{#each d.ReviewersOfMe as c}
						<a href="/user/{c.Login}" class="flex items-center gap-2 rounded p-1 text-xs hover:bg-accent">
							{#if c.AvatarURL}<img src={c.AvatarURL} alt="" class="size-5 rounded-full" />{/if}
							<span class="flex-1 font-mono">@{c.Login}</span>
							<Badge variant="secondary">{c.Count}x</Badge>
						</a>
					{/each}
				</CardContent>
			</Card>
		{/if}
		{#if d?.AuthorsIReview?.length > 0}
			<Card>
				<CardHeader><CardTitle class="text-sm">Authors I Review</CardTitle></CardHeader>
				<CardContent class="space-y-1">
					{#each d.AuthorsIReview as c}
						<a href="/user/{c.Login}" class="flex items-center gap-2 rounded p-1 text-xs hover:bg-accent">
							{#if c.AvatarURL}<img src={c.AvatarURL} alt="" class="size-5 rounded-full" />{/if}
							<span class="flex-1 font-mono">@{c.Login}</span>
							<Badge variant="secondary">{c.Count}x</Badge>
						</a>
					{/each}
				</CardContent>
			</Card>
		{/if}
	</div>
{/if}

<!-- Contributed Repos -->
{#if d?.ContributedRepos?.length > 0}
	<Card>
		<CardHeader><CardTitle class="text-sm">Contributed Repos</CardTitle></CardHeader>
		<CardContent class="overflow-x-auto p-0">
			<Table>
				<TableHeader>
					<TableRow>
						<TableHead>Repo</TableHead>
						<TableHead class="w-24">Merged PRs</TableHead>
						<TableHead class="w-28">Avg Time</TableHead>
					</TableRow>
				</TableHeader>
				<TableBody>
					{#each d.ContributedRepos as repo}
						<TableRow>
							<TableCell class="font-mono text-xs">
								<a href="/repo/{repo.FullName}" class="hover:underline">{repo.FullName}</a>
							</TableCell>
							<TableCell class="text-xs">{formatNumber(repo.MergedPRCount)}</TableCell>
							<TableCell class="text-xs">{repo.AvgMergeTimeSecs ? formatDuration(repo.AvgMergeTimeSecs) : '—'}</TableCell>
						</TableRow>
					{/each}
				</TableBody>
			</Table>
		</CardContent>
	</Card>
{/if}
