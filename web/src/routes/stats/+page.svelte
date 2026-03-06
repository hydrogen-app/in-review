<script lang="ts">
	import { goto } from '$app/navigation';
	import { page } from '$app/stores';
	import * as Chart from '$lib/components/ui/chart/index.js';
	import { LineChart, BarChart } from 'layerchart';
	import { formatDuration, formatNumber } from '$lib/utils';
	import { toRows } from '$lib/types';

	let { data } = $props();
	const d = $derived(data.stats);

	// ── Controls state ───────────────────────────────────────────────────────────
	let range      = $state('All');
	let showAvg    = $state(true);
	let showMedian = $state(true);
	let trim       = $state(parseInt($page.url.searchParams.get('trim') ?? '0'));
	let minStars   = $state($page.url.searchParams.get('min_stars') ?? '0');
	let minContribs = $state($page.url.searchParams.get('min_contribs') ?? '0');

	const RANGES = ['All', '3M', '6M', '1Y', '2Y', '5Y', '10Y'];
	const RANGE_N: Record<string, number> = {
		All: Infinity, '3M': 3, '6M': 6, '1Y': 12, '2Y': 24, '5Y': 60, '10Y': 120
	};
	const STARS_OPTS = [
		{ label: 'Any', val: '0' }, { label: '100+', val: '100' },
		{ label: '1k+', val: '1000' }, { label: '10k+', val: '10000' }
	];
	const CONTRIBS_OPTS = [
		{ label: 'Any', val: '0' }, { label: '5+', val: '5' },
		{ label: '20+', val: '20' }, { label: '100+', val: '100' }
	];

	function applyServerFilters() {
		goto(`?trim=${trim}&min_stars=${minStars}&min_contribs=${minContribs}`, { invalidateAll: true });
	}
	function setMinStars(val: string) { minStars = val; applyServerFilters(); }
	function setMinContribs(val: string) { minContribs = val; applyServerFilters(); }

	// ── Time chart slicing ───────────────────────────────────────────────────────
	const allLabels = $derived(d?.TimeChart?.labels ?? []);
	const sliceN    = $derived(RANGE_N[range] ?? Infinity);
	const startIdx  = $derived(sliceN === Infinity ? 0 : Math.max(0, allLabels.length - sliceN));
	const labels    = $derived(allLabels.slice(startIdx));

	function tc(arr: number[] | undefined) {
		return (arr ?? []).slice(startIdx);
	}

	// ── Chart data ───────────────────────────────────────────────────────────────
	const prSizeData     = $derived(toRows(labels, { avg: tc(d?.TimeChart?.avgSize),               median: tc(d?.TimeChart?.medianSize) }));
	const reviewTimeData = $derived(toRows(labels, { avg: tc(d?.TimeChart?.avgHours),              median: tc(d?.TimeChart?.medianHours) }));
	const crRateData     = $derived(toRows(labels, { rate: tc(d?.TimeChart?.changesRequestedRate)  }));
	const mergedPRsData  = $derived(toRows(labels, { count: tc(d?.TimeChart?.prCounts)             }));
	const openedPRsData  = $derived(toRows(labels, { count: tc(d?.TimeChart?.openedCounts)         }));
	const mergeRateData  = $derived(toRows(labels, { rate: tc(d?.TimeChart?.mergeVsOpenRate)       }));
	const firstRevData   = $derived(toRows(labels, { avg: tc(d?.TimeChart?.avgFirstReviewHours),   median: tc(d?.TimeChart?.medFirstReviewHours) }));
	const unreviewedData = $derived(toRows(labels, { rate: tc(d?.TimeChart?.unreviewedMergeRate)   }));
	const linesData      = $derived(toRows(labels, { lines: tc(d?.TimeChart?.linesPerContrib)      }));

	// Size bucket data (categorical x-axis, no range slicing)
	const sizeLabels   = $derived(d?.SizeChart?.labels ?? []);
	const sizeAvgTime  = $derived(toRows(sizeLabels, { val: d?.SizeChart?.avgHours              ?? [] }));
	const sizeMedianTime = $derived(toRows(sizeLabels, { val: d?.SizeChart?.medianHours         ?? [] }));
	const sizePRCount  = $derived(toRows(sizeLabels, { val: d?.SizeChart?.prCounts              ?? [] }));
	const sizeCRRate   = $derived(toRows(sizeLabels, { val: d?.SizeChart?.changesRequestedRate  ?? [] }));
	const sizeAvgCR    = $derived(toRows(sizeLabels, { val: d?.SizeChart?.avgChangesRequested   ?? [] }));
	const sizeApproval = $derived(toRows(sizeLabels, { val: d?.SizeChart?.approvalRate          ?? [] }));

	// ── Stat sidebar helpers ─────────────────────────────────────────────────────
	function calcTrend(arr: number[] | undefined) {
		const sliced = (arr ?? []).slice(startIdx);
		const first  = sliced[0] ?? 0;
		const latest = sliced[sliced.length - 1] ?? 0;
		const pct    = first === 0 ? 0 : ((latest - first) / Math.abs(first)) * 100;
		const dir    = Math.abs(pct) < 0.5 ? '→' : pct > 0 ? '▲' : '▼';
		return { latest, first, dir, pct: Math.abs(pct) };
	}

	function fmtHrs(v: number) { return v > 0 ? `${v.toFixed(1)}h`  : '—'; }
	function fmtPct(v: number) { return v > 0 ? `${v.toFixed(1)}%`  : '—'; }
	function fmtNum(v: number) { return v > 0 ? formatNumber(Math.round(v)) : '—'; }
	function fmtDec(v: number) { return v > 0 ? v.toFixed(2)         : '—'; }

	// Build series array for dual-series (avg + median) charts
	function dualSeries(avgKey: string, avgColor: string, medKey: string, medColor: string) {
		const s: { key: string; label: string; color: string }[] = [];
		if (showAvg)    s.push({ key: avgKey, label: 'Avg',    color: avgColor });
		if (showMedian) s.push({ key: medKey, label: 'Median', color: medColor });
		// always keep at least one to avoid empty chart
		if (s.length === 0) s.push({ key: avgKey, label: 'Avg', color: avgColor });
		return s;
	}

	// Trend color helper
	function trendColor(dir: string, higherIsBad = false) {
		if (dir === '→') return 'text-muted-foreground';
		const up = dir === '▲';
		return (up === higherIsBad) ? 'text-[#f85149]' : 'text-[#3fb950]';
	}
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

<div class="mb-5">
	<h1 class="mb-1 text-base font-bold">Global PR Stats</h1>
	<p class="text-xs text-muted-foreground">Aggregate metrics across all tracked repos.</p>
</div>

<!-- Controls bar -->
<div class="mb-6 border border-border p-3 flex flex-wrap items-center gap-4">
	<!-- Range buttons -->
	<div class="flex items-center gap-1">
		{#each RANGES as r}
			<button
				onclick={() => range = r}
				class="px-2 py-0.5 text-xs border cursor-pointer font-mono {range === r ? 'border-foreground text-foreground bg-muted' : 'border-border text-muted-foreground hover:border-foreground hover:text-foreground'}"
			>{r}</button>
		{/each}
	</div>

	<!-- Trim slider -->
	<label class="flex items-center gap-2 text-xs text-muted-foreground">
		trim:
		<input
			type="range" min="0" max="20" step="1"
			bind:value={trim}
			onchange={applyServerFilters}
			class="w-24 accent-[#58a6ff]"
		/>
		<span class="text-foreground min-w-[2rem]">{trim}%</span>
	</label>

	<!-- Stars filter -->
	<div class="flex items-center gap-1 text-xs text-muted-foreground">
		stars:
		{#each STARS_OPTS as opt}
			<button
				onclick={() => setMinStars(opt.val)}
				class="px-2 py-0.5 border cursor-pointer font-mono {minStars === opt.val ? 'border-foreground text-foreground bg-muted' : 'border-border text-muted-foreground hover:text-foreground'}"
			>{opt.label}</button>
		{/each}
	</div>

	<!-- Contribs filter -->
	<div class="flex items-center gap-1 text-xs text-muted-foreground">
		contribs:
		{#each CONTRIBS_OPTS as opt}
			<button
				onclick={() => setMinContribs(opt.val)}
				class="px-2 py-0.5 border cursor-pointer font-mono {minContribs === opt.val ? 'border-foreground text-foreground bg-muted' : 'border-border text-muted-foreground hover:text-foreground'}"
			>{opt.label}</button>
		{/each}
	</div>
</div>

<!-- Overall stats -->
{#if d?.Overall}
	{@const ov = d.Overall}
	<div class="mb-8 grid border border-border" style="grid-template-columns: repeat(4,1fr)">
		<div class="border-r border-border p-3 text-center">
			<div class="text-xl font-bold">{formatNumber(ov.TotalPRs)}</div>
			<div class="text-xs text-muted-foreground">Total PRs</div>
		</div>
		<div class="border-r border-border p-3 text-center">
			<div class="text-xl font-bold">{formatNumber(ov.TotalRepos)}</div>
			<div class="text-xs text-muted-foreground">Repos</div>
		</div>
		<div class="border-r border-border p-3 text-center">
			<div class="text-xl font-bold">{formatDuration(ov.AvgSecs)}</div>
			<div class="text-xs text-muted-foreground">Avg Merge Time</div>
		</div>
		<div class="p-3 text-center">
			<div class="text-xl font-bold">{formatDuration(ov.MedianSecs)}</div>
			<div class="text-xs text-muted-foreground">Median Merge Time</div>
		</div>
	</div>
{/if}

<!-- ═══════════════════════ TIME-SERIES SECTION ═══════════════════════ -->
{#if labels.length > 0}

<h2 class="mb-4 border-b border-border pb-1 text-sm font-bold">Trends Over Time</h2>

<!-- Reusable legend snippet for dual charts -->
{#snippet dualLegend(avgColor: string, medColor: string)}
	<div class="flex items-center gap-3">
		<button
			onclick={() => showAvg = !showAvg}
			class="flex items-center gap-1.5 text-xs cursor-pointer {showAvg ? 'text-foreground' : 'text-muted-foreground line-through'}"
		>
			<span class="inline-block w-5" style="height:2px; background:{avgColor}"></span>avg
		</button>
		<button
			onclick={() => showMedian = !showMedian}
			class="flex items-center gap-1.5 text-xs cursor-pointer {showMedian ? 'text-foreground' : 'text-muted-foreground line-through'}"
		>
			<span class="inline-block w-5" style="height:2px; background:{medColor}"></span>median
		</button>
	</div>
{/snippet}

<!-- 1. PR size over time -->
{@const prSizeStat    = calcTrend(d?.TimeChart?.avgSize)}
{@const prSizeStatMed = calcTrend(d?.TimeChart?.medianSize)}
<div class="mb-5 grid gap-3" style="grid-template-columns: 1fr 210px; align-items: start">
	<div class="border border-border p-3">
		<div class="mb-3 flex items-center justify-between">
			<span class="text-xs text-muted-foreground">PR size over time (lines)</span>
			{@render dualLegend('#d29922', '#f0883e')}
		</div>
		<Chart.Container config={{ avg: { label: 'Avg', color: '#d29922' }, median: { label: 'Median', color: '#f0883e' } }} class="h-[280px]">
			<LineChart data={prSizeData} x="label" axis={true} series={dualSeries('avg', '#d29922', 'median', '#f0883e')}>
				{#snippet tooltip()}<Chart.Tooltip />{/snippet}
			</LineChart>
		</Chart.Container>
	</div>
	<div class="flex flex-col gap-2">
		<div class="border border-border p-3">
			<span class="block text-lg font-bold">{fmtNum(prSizeStat.latest)}</span>
			<span class="block text-xs text-muted-foreground mt-2 pt-2 border-t border-border">
				latest avg · <span class="{trendColor(prSizeStat.dir, true)}">{prSizeStat.dir} {prSizeStat.pct.toFixed(0)}%</span>
			</span>
		</div>
		<div class="border border-border p-3">
			<span class="block text-lg font-bold">{fmtNum(prSizeStatMed.latest)}</span>
			<span class="block text-xs text-muted-foreground mt-2 pt-2 border-t border-border">
				latest median · <span class="{trendColor(prSizeStatMed.dir, true)}">{prSizeStatMed.dir} {prSizeStatMed.pct.toFixed(0)}%</span>
			</span>
		</div>
	</div>
</div>

<!-- 2. Review time over time -->
{@const rtStat    = calcTrend(d?.TimeChart?.avgHours)}
{@const rtStatMed = calcTrend(d?.TimeChart?.medianHours)}
<div class="mb-5 grid gap-3" style="grid-template-columns: 1fr 210px; align-items: start">
	<div class="border border-border p-3">
		<div class="mb-3 flex items-center justify-between">
			<span class="text-xs text-muted-foreground">Review time over time (hrs)</span>
			{@render dualLegend('#d29922', '#f0883e')}
		</div>
		<Chart.Container config={{ avg: { label: 'Avg', color: '#d29922' }, median: { label: 'Median', color: '#f0883e' } }} class="h-[280px]">
			<LineChart data={reviewTimeData} x="label" axis={true} series={dualSeries('avg', '#d29922', 'median', '#f0883e')}>
				{#snippet tooltip()}<Chart.Tooltip />{/snippet}
			</LineChart>
		</Chart.Container>
	</div>
	<div class="flex flex-col gap-2">
		<div class="border border-border p-3">
			<span class="block text-lg font-bold">{fmtHrs(rtStat.latest)}</span>
			<span class="block text-xs text-muted-foreground mt-2 pt-2 border-t border-border">
				latest avg · <span class="{trendColor(rtStat.dir, true)}">{rtStat.dir} {rtStat.pct.toFixed(0)}%</span>
			</span>
		</div>
		<div class="border border-border p-3">
			<span class="block text-lg font-bold">{fmtHrs(rtStatMed.latest)}</span>
			<span class="block text-xs text-muted-foreground mt-2 pt-2 border-t border-border">
				latest median · <span class="{trendColor(rtStatMed.dir, true)}">{rtStatMed.dir} {rtStatMed.pct.toFixed(0)}%</span>
			</span>
		</div>
	</div>
</div>

<!-- 3. Changes requested rate -->
{@const crStat = calcTrend(d?.TimeChart?.changesRequestedRate)}
<div class="mb-5 grid gap-3" style="grid-template-columns: 1fr 210px; align-items: start">
	<div class="border border-border p-3">
		<div class="mb-3 text-xs text-muted-foreground">Changes requested rate (%)</div>
		<Chart.Container config={{ rate: { label: 'CR Rate', color: '#f85149' } }} class="h-[280px]">
			<LineChart data={crRateData} x="label" axis={true} series={[{ key: 'rate', label: 'CR Rate', color: '#f85149' }]}>
				{#snippet tooltip()}<Chart.Tooltip />{/snippet}
			</LineChart>
		</Chart.Container>
	</div>
	<div class="flex flex-col gap-2">
		<div class="border border-border p-3">
			<span class="block text-lg font-bold" style="color:#f85149">{fmtPct(crStat.latest)}</span>
			<span class="block text-xs text-muted-foreground mt-2 pt-2 border-t border-border">latest</span>
		</div>
		<div class="border border-border p-3">
			<span class="block text-lg font-bold">{crStat.dir} {crStat.pct.toFixed(0)}%</span>
			<span class="block text-xs text-muted-foreground mt-2 pt-2 border-t border-border">since first period (first: {fmtPct(crStat.first)})</span>
		</div>
	</div>
</div>

<!-- 4. Merged PRs per month -->
{@const mpStat = calcTrend(d?.TimeChart?.prCounts)}
<div class="mb-5 grid gap-3" style="grid-template-columns: 1fr 210px; align-items: start">
	<div class="border border-border p-3">
		<div class="mb-3 text-xs text-muted-foreground">Merged PRs per month</div>
		<Chart.Container config={{ count: { label: 'Merged', color: '#58a6ff' } }} class="h-[280px]">
			<LineChart data={mergedPRsData} x="label" axis={true} series={[{ key: 'count', label: 'Merged', color: '#58a6ff' }]}>
				{#snippet tooltip()}<Chart.Tooltip />{/snippet}
			</LineChart>
		</Chart.Container>
	</div>
	<div class="flex flex-col gap-2">
		<div class="border border-border p-3">
			<span class="block text-lg font-bold" style="color:#58a6ff">{fmtNum(mpStat.latest)}</span>
			<span class="block text-xs text-muted-foreground mt-2 pt-2 border-t border-border">latest</span>
		</div>
		<div class="border border-border p-3">
			<span class="block text-lg font-bold">{mpStat.dir} {mpStat.pct.toFixed(0)}%</span>
			<span class="block text-xs text-muted-foreground mt-2 pt-2 border-t border-border">since first period (first: {fmtNum(mpStat.first)})</span>
		</div>
	</div>
</div>

<!-- 5. PRs opened per month -->
{@const opStat = calcTrend(d?.TimeChart?.openedCounts)}
<div class="mb-5 grid gap-3" style="grid-template-columns: 1fr 210px; align-items: start">
	<div class="border border-border p-3">
		<div class="mb-3 text-xs text-muted-foreground">PRs opened per month</div>
		<Chart.Container config={{ count: { label: 'Opened', color: '#3fb950' } }} class="h-[280px]">
			<LineChart data={openedPRsData} x="label" axis={true} series={[{ key: 'count', label: 'Opened', color: '#3fb950' }]}>
				{#snippet tooltip()}<Chart.Tooltip />{/snippet}
			</LineChart>
		</Chart.Container>
	</div>
	<div class="flex flex-col gap-2">
		<div class="border border-border p-3">
			<span class="block text-lg font-bold" style="color:#3fb950">{fmtNum(opStat.latest)}</span>
			<span class="block text-xs text-muted-foreground mt-2 pt-2 border-t border-border">latest</span>
		</div>
		<div class="border border-border p-3">
			<span class="block text-lg font-bold">{opStat.dir} {opStat.pct.toFixed(0)}%</span>
			<span class="block text-xs text-muted-foreground mt-2 pt-2 border-t border-border">since first period (first: {fmtNum(opStat.first)})</span>
		</div>
	</div>
</div>

<!-- 6. Merge rate -->
{@const mrStat = calcTrend(d?.TimeChart?.mergeVsOpenRate)}
<div class="mb-5 grid gap-3" style="grid-template-columns: 1fr 210px; align-items: start">
	<div class="border border-border p-3">
		<div class="mb-3 text-xs text-muted-foreground">Merge rate — merged/opened (%)</div>
		<Chart.Container config={{ rate: { label: 'Merge Rate', color: '#e3b341' } }} class="h-[280px]">
			<LineChart data={mergeRateData} x="label" axis={true} series={[{ key: 'rate', label: 'Merge Rate', color: '#e3b341' }]}>
				{#snippet tooltip()}<Chart.Tooltip />{/snippet}
			</LineChart>
		</Chart.Container>
	</div>
	<div class="flex flex-col gap-2">
		<div class="border border-border p-3">
			<span class="block text-lg font-bold" style="color:#e3b341">{fmtPct(mrStat.latest)}</span>
			<span class="block text-xs text-muted-foreground mt-2 pt-2 border-t border-border">latest</span>
		</div>
		<div class="border border-border p-3">
			<span class="block text-lg font-bold">{mrStat.dir} {mrStat.pct.toFixed(0)}%</span>
			<span class="block text-xs text-muted-foreground mt-2 pt-2 border-t border-border">since first period (first: {fmtPct(mrStat.first)})</span>
		</div>
	</div>
</div>

<!-- 7. Time to first review -->
{@const frStat    = calcTrend(d?.TimeChart?.avgFirstReviewHours)}
{@const frStatMed = calcTrend(d?.TimeChart?.medFirstReviewHours)}
<div class="mb-5 grid gap-3" style="grid-template-columns: 1fr 210px; align-items: start">
	<div class="border border-border p-3">
		<div class="mb-3 flex items-center justify-between">
			<span class="text-xs text-muted-foreground">Time to first review (hrs)</span>
			{@render dualLegend('#bc8cff', '#a371f7')}
		</div>
		<Chart.Container config={{ avg: { label: 'Avg', color: '#bc8cff' }, median: { label: 'Median', color: '#a371f7' } }} class="h-[280px]">
			<LineChart data={firstRevData} x="label" axis={true} series={dualSeries('avg', '#bc8cff', 'median', '#a371f7')}>
				{#snippet tooltip()}<Chart.Tooltip />{/snippet}
			</LineChart>
		</Chart.Container>
	</div>
	<div class="flex flex-col gap-2">
		<div class="border border-border p-3">
			<span class="block text-lg font-bold" style="color:#bc8cff">{fmtHrs(frStat.latest)}</span>
			<span class="block text-xs text-muted-foreground mt-2 pt-2 border-t border-border">
				latest avg · <span class="{trendColor(frStat.dir, true)}">{frStat.dir} {frStat.pct.toFixed(0)}%</span>
			</span>
		</div>
		<div class="border border-border p-3">
			<span class="block text-lg font-bold" style="color:#a371f7">{fmtHrs(frStatMed.latest)}</span>
			<span class="block text-xs text-muted-foreground mt-2 pt-2 border-t border-border">
				latest median · <span class="{trendColor(frStatMed.dir, true)}">{frStatMed.dir} {frStatMed.pct.toFixed(0)}%</span>
			</span>
		</div>
	</div>
</div>

<!-- 8. Unreviewed merge rate -->
{@const urStat = calcTrend(d?.TimeChart?.unreviewedMergeRate)}
<div class="mb-5 grid gap-3" style="grid-template-columns: 1fr 210px; align-items: start">
	<div class="border border-border p-3">
		<div class="mb-3 text-xs text-muted-foreground">Unreviewed merge rate (%)</div>
		<Chart.Container config={{ rate: { label: 'Unreviewed', color: '#ffa657' } }} class="h-[280px]">
			<LineChart data={unreviewedData} x="label" axis={true} series={[{ key: 'rate', label: 'Unreviewed', color: '#ffa657' }]}>
				{#snippet tooltip()}<Chart.Tooltip />{/snippet}
			</LineChart>
		</Chart.Container>
	</div>
	<div class="flex flex-col gap-2">
		<div class="border border-border p-3">
			<span class="block text-lg font-bold" style="color:#ffa657">{fmtPct(urStat.latest)}</span>
			<span class="block text-xs text-muted-foreground mt-2 pt-2 border-t border-border">latest</span>
		</div>
		<div class="border border-border p-3">
			<span class="block text-lg font-bold">{urStat.dir} {urStat.pct.toFixed(0)}%</span>
			<span class="block text-xs text-muted-foreground mt-2 pt-2 border-t border-border">since first period (first: {fmtPct(urStat.first)})</span>
		</div>
	</div>
</div>

<!-- 9. Lines per contributor -->
{@const lcStat = calcTrend(d?.TimeChart?.linesPerContrib)}
<div class="mb-10 grid gap-3" style="grid-template-columns: 1fr 210px; align-items: start">
	<div class="border border-border p-3">
		<div class="mb-3 text-xs text-muted-foreground">Lines per contributor (monthly)</div>
		<Chart.Container config={{ lines: { label: 'Lines/Contrib', color: '#39c5cf' } }} class="h-[280px]">
			<LineChart data={linesData} x="label" axis={true} series={[{ key: 'lines', label: 'Lines/Contrib', color: '#39c5cf' }]}>
				{#snippet tooltip()}<Chart.Tooltip />{/snippet}
			</LineChart>
		</Chart.Container>
	</div>
	<div class="flex flex-col gap-2">
		<div class="border border-border p-3">
			<span class="block text-lg font-bold" style="color:#39c5cf">{fmtNum(lcStat.latest)}</span>
			<span class="block text-xs text-muted-foreground mt-2 pt-2 border-t border-border">latest</span>
		</div>
		<div class="border border-border p-3">
			<span class="block text-lg font-bold">{lcStat.dir} {lcStat.pct.toFixed(0)}%</span>
			<span class="block text-xs text-muted-foreground mt-2 pt-2 border-t border-border">since first period (first: {fmtNum(lcStat.first)})</span>
		</div>
	</div>
</div>

{/if}

<!-- ═══════════════════════ SIZE BUCKET SECTION ═══════════════════════ -->
{#if sizeLabels.length > 0}

<h2 class="mb-4 border-b border-border pb-1 text-sm font-bold">By PR Size Bucket</h2>

<!-- 1. Avg review time by size -->
<div class="mb-5 grid gap-3" style="grid-template-columns: 1fr 210px; align-items: start">
	<div class="border border-border p-3">
		<div class="mb-3 text-xs text-muted-foreground">Avg review time by size (hrs)</div>
		<Chart.Container config={{ val: { label: 'Avg Hrs', color: '#d29922' } }} class="h-[240px]">
			<BarChart data={sizeAvgTime} x="label" axis="x" series={[{ key: 'val', label: 'Avg Hrs', color: '#d29922' }]}>
				{#snippet tooltip()}<Chart.Tooltip />{/snippet}
			</BarChart>
		</Chart.Container>
	</div>
	<div class="border border-border p-3">
		<table class="w-full border-collapse text-xs">
			{#each sizeLabels as lbl, i}
				<tr class="border-b border-border last:border-0">
					<td class="py-1.5 pr-2 text-muted-foreground">{lbl}</td>
					<td class="py-1.5 text-right font-semibold">{fmtHrs(d?.SizeChart?.avgHours?.[i] ?? 0)}</td>
				</tr>
			{/each}
		</table>
	</div>
</div>

<!-- 2. Median review time by size -->
<div class="mb-5 grid gap-3" style="grid-template-columns: 1fr 210px; align-items: start">
	<div class="border border-border p-3">
		<div class="mb-3 text-xs text-muted-foreground">Median review time by size (hrs)</div>
		<Chart.Container config={{ val: { label: 'Median Hrs', color: '#f0883e' } }} class="h-[240px]">
			<BarChart data={sizeMedianTime} x="label" axis="x" series={[{ key: 'val', label: 'Median Hrs', color: '#f0883e' }]}>
				{#snippet tooltip()}<Chart.Tooltip />{/snippet}
			</BarChart>
		</Chart.Container>
	</div>
	<div class="border border-border p-3">
		<table class="w-full border-collapse text-xs">
			{#each sizeLabels as lbl, i}
				<tr class="border-b border-border last:border-0">
					<td class="py-1.5 pr-2 text-muted-foreground">{lbl}</td>
					<td class="py-1.5 text-right font-semibold">{fmtHrs(d?.SizeChart?.medianHours?.[i] ?? 0)}</td>
				</tr>
			{/each}
		</table>
	</div>
</div>

<!-- 3. PRs by size bucket -->
<div class="mb-5 grid gap-3" style="grid-template-columns: 1fr 210px; align-items: start">
	<div class="border border-border p-3">
		<div class="mb-3 text-xs text-muted-foreground">PRs by size bucket</div>
		<Chart.Container config={{ val: { label: 'PRs', color: '#58a6ff' } }} class="h-[240px]">
			<BarChart data={sizePRCount} x="label" axis="x" series={[{ key: 'val', label: 'PRs', color: '#58a6ff' }]}>
				{#snippet tooltip()}<Chart.Tooltip />{/snippet}
			</BarChart>
		</Chart.Container>
	</div>
	<div class="border border-border p-3">
		<table class="w-full border-collapse text-xs">
			{#each sizeLabels as lbl, i}
				<tr class="border-b border-border last:border-0">
					<td class="py-1.5 pr-2 text-muted-foreground">{lbl}</td>
					<td class="py-1.5 text-right font-semibold">{fmtNum(d?.SizeChart?.prCounts?.[i] ?? 0)}</td>
				</tr>
			{/each}
		</table>
	</div>
</div>

<!-- 4. Changes requested rate by size -->
<div class="mb-5 grid gap-3" style="grid-template-columns: 1fr 210px; align-items: start">
	<div class="border border-border p-3">
		<div class="mb-3 text-xs text-muted-foreground">Changes requested rate by size (%)</div>
		<Chart.Container config={{ val: { label: 'CR Rate', color: '#f85149' } }} class="h-[240px]">
			<BarChart data={sizeCRRate} x="label" axis="x" series={[{ key: 'val', label: 'CR Rate', color: '#f85149' }]}>
				{#snippet tooltip()}<Chart.Tooltip />{/snippet}
			</BarChart>
		</Chart.Container>
	</div>
	<div class="border border-border p-3">
		<table class="w-full border-collapse text-xs">
			{#each sizeLabels as lbl, i}
				<tr class="border-b border-border last:border-0">
					<td class="py-1.5 pr-2 text-muted-foreground">{lbl}</td>
					<td class="py-1.5 text-right font-semibold">{fmtPct(d?.SizeChart?.changesRequestedRate?.[i] ?? 0)}</td>
				</tr>
			{/each}
		</table>
	</div>
</div>

<!-- 5. Avg changes requested per PR -->
<div class="mb-5 grid gap-3" style="grid-template-columns: 1fr 210px; align-items: start">
	<div class="border border-border p-3">
		<div class="mb-3 text-xs text-muted-foreground">Avg changes requested per PR</div>
		<Chart.Container config={{ val: { label: 'Avg CRs', color: '#da3633' } }} class="h-[240px]">
			<BarChart data={sizeAvgCR} x="label" axis="x" series={[{ key: 'val', label: 'Avg CRs', color: '#da3633' }]}>
				{#snippet tooltip()}<Chart.Tooltip />{/snippet}
			</BarChart>
		</Chart.Container>
	</div>
	<div class="border border-border p-3">
		<table class="w-full border-collapse text-xs">
			{#each sizeLabels as lbl, i}
				<tr class="border-b border-border last:border-0">
					<td class="py-1.5 pr-2 text-muted-foreground">{lbl}</td>
					<td class="py-1.5 text-right font-semibold">{fmtDec(d?.SizeChart?.avgChangesRequested?.[i] ?? 0)}</td>
				</tr>
			{/each}
		</table>
	</div>
</div>

<!-- 6. Clean approval rate by size -->
<div class="mb-10 grid gap-3" style="grid-template-columns: 1fr 210px; align-items: start">
	<div class="border border-border p-3">
		<div class="mb-3 text-xs text-muted-foreground">Clean approval rate by size (%)</div>
		<Chart.Container config={{ val: { label: 'Approval Rate', color: '#3fb950' } }} class="h-[240px]">
			<BarChart data={sizeApproval} x="label" axis="x" series={[{ key: 'val', label: 'Approval Rate', color: '#3fb950' }]}>
				{#snippet tooltip()}<Chart.Tooltip />{/snippet}
			</BarChart>
		</Chart.Container>
	</div>
	<div class="border border-border p-3">
		<table class="w-full border-collapse text-xs">
			{#each sizeLabels as lbl, i}
				<tr class="border-b border-border last:border-0">
					<td class="py-1.5 pr-2 text-muted-foreground">{lbl}</td>
					<td class="py-1.5 text-right font-semibold" style="color:#3fb950">{fmtPct(d?.SizeChart?.approvalRate?.[i] ?? 0)}</td>
				</tr>
			{/each}
		</table>
	</div>
</div>

{/if}

{#if !d}
	<p class="text-sm text-muted-foreground">No stats available yet. Repos are still syncing.</p>
{/if}
