<script lang="ts">
	import { onMount, onDestroy } from 'svelte';
	import { Badge } from '$lib/components/ui/badge';
	import { formatDuration, formatNumber } from '$lib/utils';

	let { data } = $props();
	let liveData = $state<typeof data.blog | null>(null);
	const d = $derived(liveData ?? data.blog);
	let lastUpdated = $state<string | null>(null);
	let interval: ReturnType<typeof setInterval>;

	async function refreshStats() {
		try {
			const r = await fetch('/api/v1/blog');
			if (r.ok) {
				liveData = await r.json();
				lastUpdated = new Date().toLocaleTimeString();
			}
		} catch {}
	}

	onMount(() => {
		interval = setInterval(refreshStats, 30_000);
	});
	onDestroy(() => clearInterval(interval));
</script>

<svelte:head>
	<title>Are AI Tools Making PRs Bigger? — ngmi</title>
</svelte:head>

<!-- Breadcrumb -->
<div class="mb-3 flex items-center gap-1 text-xs text-muted-foreground">
	<a href="/" class="hover:text-foreground">ngmi</a>
	<span>/</span>
	<span class="text-foreground">blog</span>
</div>

<article class="mx-auto max-w-2xl">
	<header class="mb-8">
		<h1 class="mb-2 text-xl font-bold">Are AI Tools Making PRs Bigger?</h1>
		<p class="flex items-center gap-2 text-xs text-muted-foreground">
			<span>ngmi.review</span>
			<span>·</span>
			<span>February 2026</span>
			<span>·</span>
			<span class="flex items-center gap-1">
				<span class="size-1.5 rounded-full bg-green-400 animate-pulse"></span>
				live data
			</span>
		</p>
	</header>

	<!-- Live stats widget -->
	{#if d}
		<div class="mb-8 rounded-md border border-border bg-muted/30 p-4">
			<div class="mb-3 flex items-center justify-between">
				<span class="text-xs font-medium">Live snapshot</span>
				{#if lastUpdated}
					<span class="text-xs text-muted-foreground">updated {lastUpdated}</span>
				{/if}
			</div>
			<div class="grid grid-cols-2 gap-3 sm:grid-cols-4">
				<div class="text-center">
					<div class="text-lg font-bold">{formatNumber(d.TotalRepos)}</div>
					<div class="text-xs text-muted-foreground">Repos</div>
				</div>
				<div class="text-center">
					<div class="text-lg font-bold">{formatNumber(d.TotalPRs)}</div>
					<div class="text-xs text-muted-foreground">PRs</div>
				</div>
				<div class="text-center">
					<div class="text-lg font-bold">{formatNumber(d.TotalReviews)}</div>
					<div class="text-xs text-muted-foreground">Reviews</div>
				</div>
				{#if d.LiveStats?.AvgSecs}
					<div class="text-center">
						<div class="text-lg font-bold">{formatDuration(d.LiveStats.AvgSecs)}</div>
						<div class="text-xs text-muted-foreground">Avg Merge Time</div>
					</div>
				{/if}
			</div>
			{#if d.TopReviewers?.length > 0}
				<div class="mt-3 border-t border-border pt-3">
					<div class="mb-1 text-xs font-medium">Top Reviewers</div>
					<div class="flex flex-wrap gap-1">
						{#each d.TopReviewers as r}
							<a href="/user/{r.Name}">
								<Badge variant="secondary" class="font-mono text-xs">@{r.Name}</Badge>
							</a>
						{/each}
					</div>
				</div>
			{/if}
		</div>
	{/if}

	<section class="mb-6">
		<h2 class="mb-2 text-sm font-bold">The Question</h2>
		<p class="text-sm leading-relaxed text-muted-foreground"></p>
	</section>

	<section class="mb-6">
		<h2 class="mb-2 text-sm font-bold">What the Data Shows</h2>
		<p class="text-sm leading-relaxed text-muted-foreground"></p>
	</section>

	<section class="mb-6">
		<h2 class="mb-2 text-sm font-bold">The AI Inflection Point</h2>
		<p class="text-sm leading-relaxed text-muted-foreground"></p>
		<p class="mt-2 text-sm">
			<a href="/stats" class="text-primary hover:underline">Explore the full interactive charts →</a>
		</p>
	</section>

	<section class="mb-6">
		<h2 class="mb-2 text-sm font-bold">What This Means for Review Time</h2>
		<p class="text-sm leading-relaxed text-muted-foreground"></p>
	</section>

	<section class="mb-6">
		<h2 class="mb-2 text-sm font-bold">Methodology</h2>
		<p class="text-sm leading-relaxed text-muted-foreground">
			Data is collected from public GitHub repositories via the GitHub REST API. Only merged pull
			requests are included in the timing calculations. Review time is measured from PR open to PR
			merge. Repos with fewer than 3 merged PRs are excluded from leaderboards. All raw data is
			browsable on the <a href="/data" class="text-primary hover:underline">data explorer</a>.
		</p>
	</section>
</article>
