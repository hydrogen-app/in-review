<script lang="ts">
	import { onMount } from 'svelte';
	import { page } from '$app/stores';
	import { Badge } from '$lib/components/ui/badge';
	import { Card, CardHeader, CardTitle, CardContent } from '$lib/components/ui/card';
	import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from '$lib/components/ui/table';
	import * as Chart from '$lib/components/ui/chart/index.js';
	import { LineChart } from 'layerchart';
	import { formatDuration, formatNumber, rankClass } from '$lib/utils';
	import { toRows } from '$lib/types';
	import type { User } from '$lib/types';

	let { data } = $props();
	const d = $derived(data.org);
	const org: User = $derived(d?.Org);
	const orgName = $derived($page.params.org);

	const timeData = $derived(
		d?.TimeChart?.labels
			? toRows(d.TimeChart.labels, {
					avgHours: d.TimeChart.avgHours,
					medianHours: d.TimeChart.medianHours
				})
			: []
	);

	const timeChartConfig = {
		avgHours: { label: 'Avg', color: 'var(--chart-1)' },
		medianHours: { label: 'Median', color: 'var(--chart-2)' }
	} satisfies Chart.ChartConfig;
</script>

<svelte:head>
	<title>@{orgName} — ngmi</title>
</svelte:head>

<!-- Breadcrumb -->
<div class="mb-3 flex items-center gap-1 text-xs text-muted-foreground">
	<a href="/" class="hover:text-foreground">ngmi</a>
	<span>/</span>
	<span class="text-foreground">@{orgName}</span>
</div>

<!-- Header -->
<div class="mb-6 flex items-start gap-4">
	{#if org?.AvatarURL}
		<img src={org.AvatarURL} alt="" class="size-16 rounded-full" />
	{/if}
	<div class="min-w-0 flex-1">
		<div class="mb-1 flex flex-wrap items-center gap-2">
			<h1 class="font-mono text-base font-bold">@{org?.Login ?? orgName}</h1>
			<Badge variant="secondary">Org</Badge>
			{#if d?.IsSyncing}
				<Badge variant="secondary" class="animate-pulse">⟳ Syncing…</Badge>
			{/if}
		</div>
		{#if org?.Name}<p class="text-sm">{org.Name}</p>{/if}
		{#if org?.Bio}<p class="text-xs text-muted-foreground">{org.Bio}</p>{/if}
	</div>
</div>

<!-- Stats -->
<div class="mb-6 grid grid-cols-2 gap-3 sm:grid-cols-3">
	<Card class="text-center">
		<CardContent class="pt-4 pb-3">
			<div class="text-xl font-bold">{formatNumber(d?.Repos?.length ?? 0)}</div>
			<div class="text-xs text-muted-foreground">Repos Tracked</div>
		</CardContent>
	</Card>
	<Card class="text-center">
		<CardContent class="pt-4 pb-3">
			<div class="text-xl font-bold">{formatNumber(d?.TotalMergedPRs ?? 0)}</div>
			<div class="text-xs text-muted-foreground">Merged PRs</div>
		</CardContent>
	</Card>
</div>

<!-- Time Chart -->
{#if timeData.length > 0}
	<Card class="mb-6">
		<CardHeader><CardTitle class="text-sm">Review Time Over Time (hrs)</CardTitle></CardHeader>
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

<!-- Reviewer & Gatekeeper boards -->
{#if (d?.ReviewerBoard?.length ?? 0) > 0 || (d?.GatekeeperBoard?.length ?? 0) > 0}
	<div class="mb-6 grid grid-cols-1 gap-4 sm:grid-cols-2">
		{#if d?.ReviewerBoard?.length > 0}
			<Card>
				<CardHeader><CardTitle class="text-sm">Top Reviewers</CardTitle></CardHeader>
				<CardContent class="space-y-1">
					{#each d.ReviewerBoard as entry}
						<a href="/user/{entry.Name}" class="flex items-center gap-2 rounded px-1 py-0.5 text-xs hover:bg-accent">
							<span class="w-7 shrink-0 {rankClass(entry.Rank)}">#{entry.Rank}</span>
							{#if entry.Extra}<img src={entry.Extra} alt="" class="size-4 rounded-full" />{/if}
							<span class="flex-1 font-mono">@{entry.Name}</span>
							<span class="text-muted-foreground">{formatNumber(entry.Count)} reviews</span>
						</a>
					{/each}
				</CardContent>
			</Card>
		{/if}
		{#if d?.GatekeeperBoard?.length > 0}
			<Card>
				<CardHeader><CardTitle class="text-sm">Gatekeepers</CardTitle></CardHeader>
				<CardContent class="space-y-1">
					{#each d.GatekeeperBoard as entry}
						<a href="/user/{entry.Name}" class="flex items-center gap-2 rounded px-1 py-0.5 text-xs hover:bg-accent">
							<span class="w-7 shrink-0 {rankClass(entry.Rank)}">#{entry.Rank}</span>
							{#if entry.Extra}<img src={entry.Extra} alt="" class="size-4 rounded-full" />{/if}
							<span class="flex-1 font-mono">@{entry.Name}</span>
							<span class="text-muted-foreground">{formatNumber(entry.Count)} blocks</span>
						</a>
					{/each}
				</CardContent>
			</Card>
		{/if}
	</div>
{/if}

<!-- Repos -->
{#if d?.Repos?.length > 0}
	<Card>
		<CardHeader><CardTitle class="text-sm">Org Repos</CardTitle></CardHeader>
		<CardContent class="overflow-x-auto p-0">
			<Table>
				<TableHeader>
					<TableRow>
						<TableHead>Repo</TableHead>
						<TableHead class="w-24">Merged PRs</TableHead>
						<TableHead class="w-28">Avg Time</TableHead>
						<TableHead class="w-20">Stars</TableHead>
					</TableRow>
				</TableHeader>
				<TableBody>
					{#each d.Repos as repo}
						<TableRow>
							<TableCell class="font-mono text-xs">
								<a href="/repo/{repo.FullName}" class="hover:underline">{repo.FullName}</a>
							</TableCell>
							<TableCell class="text-xs">{formatNumber(repo.MergedPRCount)}</TableCell>
							<TableCell class="text-xs">{repo.AvgMergeTimeSecs ? formatDuration(repo.AvgMergeTimeSecs) : '—'}</TableCell>
							<TableCell class="text-xs text-muted-foreground">{repo.Stars ? `${formatNumber(repo.Stars)} ★` : ''}</TableCell>
						</TableRow>
					{/each}
				</TableBody>
			</Table>
		</CardContent>
	</Card>
{/if}
