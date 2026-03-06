<script lang="ts">
	import { onMount } from 'svelte';
	import { goto } from '$app/navigation';
	import { page } from '$app/stores';
	import { Card, CardHeader, CardTitle, CardContent } from '$lib/components/ui/card';
	import * as Chart from '$lib/components/ui/chart/index.js';
	import { LineChart, BarChart } from 'layerchart';
	import { formatDuration, formatNumber } from '$lib/utils';
	import { toRows } from '$lib/types';

	let { data } = $props();
	const d = $derived(data.stats);

	let minStars = $state(parseInt($page.url.searchParams.get('min_stars') ?? '0'));
	let minContribs = $state(parseInt($page.url.searchParams.get('min_contribs') ?? '0'));

	function applyFilters() {
		goto(`?min_stars=${minStars}&min_contribs=${minContribs}`, { invalidateAll: true });
	}

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
			? toRows(d.SizeChart.labels, {
					avgHours: d.SizeChart.avgHours,
					medianHours: d.SizeChart.medianHours
				})
			: []
	);

	const timeChartConfig = {
		avgHours: { label: 'Avg', color: 'var(--chart-1)' },
		medianHours: { label: 'Median', color: 'var(--chart-2)' }
	} satisfies Chart.ChartConfig;

	const sizeChartConfig = {
		avgHours: { label: 'Avg', color: 'var(--chart-1)' },
		medianHours: { label: 'Median', color: 'var(--chart-2)' }
	} satisfies Chart.ChartConfig;
</script>

<svelte:head>
	<title>Global Stats — ngmi</title>
</svelte:head>

<!-- Breadcrumb -->
<div class="mb-3 flex items-center gap-1 text-xs text-muted-foreground">
	<a href="/" class="hover:text-foreground">ngmi</a>
	<span>/</span>
	<span class="text-foreground">Stats</span>
</div>

<div class="mb-6">
	<h1 class="mb-1 text-base font-bold">Global PR Stats</h1>
	<p class="text-xs text-muted-foreground">Aggregate metrics across all tracked repos.</p>
</div>

<!-- Filters -->
<div class="mb-6 flex flex-wrap items-end gap-3">
	<div>
		<label for="min-stars" class="mb-1 block text-xs text-muted-foreground">Min Stars</label>
		<input id="min-stars" type="number" min="0" bind:value={minStars}
			class="h-8 w-24 rounded border border-border bg-background px-2 text-xs font-mono" />
	</div>
	<div>
		<label for="min-contribs" class="mb-1 block text-xs text-muted-foreground">Min Contributors</label>
		<input id="min-contribs" type="number" min="0" bind:value={minContribs}
			class="h-8 w-24 rounded border border-border bg-background px-2 text-xs font-mono" />
	</div>
	<button onclick={applyFilters} class="h-8 rounded border border-border bg-background px-3 text-xs hover:bg-accent">
		Apply
	</button>
</div>

<!-- Overall stats -->
{#if d?.Overall}
	<div class="mb-6 grid grid-cols-2 gap-3 sm:grid-cols-4">
		<Card class="text-center">
			<CardContent class="pt-4 pb-3">
				<div class="text-xl font-bold">{formatNumber(d.Overall.TotalPRs)}</div>
				<div class="text-xs text-muted-foreground">Total PRs</div>
			</CardContent>
		</Card>
		<Card class="text-center">
			<CardContent class="pt-4 pb-3">
				<div class="text-xl font-bold">{formatNumber(d.Overall.TotalRepos)}</div>
				<div class="text-xs text-muted-foreground">Repos</div>
			</CardContent>
		</Card>
		<Card class="text-center">
			<CardContent class="pt-4 pb-3">
				<div class="text-xl font-bold">{formatDuration(d.Overall.AvgSecs)}</div>
				<div class="text-xs text-muted-foreground">Avg Merge Time</div>
			</CardContent>
		</Card>
		<Card class="text-center">
			<CardContent class="pt-4 pb-3">
				<div class="text-xl font-bold">{formatDuration(d.Overall.MedianSecs)}</div>
				<div class="text-xs text-muted-foreground">Median Merge Time</div>
			</CardContent>
		</Card>
	</div>
{/if}

<!-- Time Series Chart -->
{#if timeData.length > 0}
	<Card class="mb-6">
		<CardHeader><CardTitle class="text-sm">Merge Time Over Time (hrs)</CardTitle></CardHeader>
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

<!-- Size Chart -->
{#if sizeData.length > 0}
	<Card class="mb-6">
		<CardHeader><CardTitle class="text-sm">Avg Merge Time by PR Size (hrs)</CardTitle></CardHeader>
		<CardContent>
			<Chart.Container config={sizeChartConfig} class="h-40">
				<BarChart
					data={sizeData}
					x="label"
					axis="x"
					series={[
						{ key: 'avgHours', label: 'Avg', color: 'var(--chart-1)' },
						{ key: 'medianHours', label: 'Median', color: 'var(--chart-2)' }
					]}
				>
					{#snippet tooltip()}
						<Chart.Tooltip />
					{/snippet}
				</BarChart>
			</Chart.Container>
		</CardContent>
	</Card>
{/if}
