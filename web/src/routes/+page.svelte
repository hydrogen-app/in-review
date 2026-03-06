<script lang="ts">
	import { Badge } from '$lib/components/ui/badge';
	import { Card, CardHeader, CardTitle, CardContent } from '$lib/components/ui/card';
	import { Input } from '$lib/components/ui/input';
	import { formatDuration, formatNumber, rankClass } from '$lib/utils';
	import type { LeaderboardEntry, PageVisit, SearchResult } from '$lib/types';
	import { Star } from '@lucide/svelte';

	let { data } = $props();
	const home = $derived(data.home);

	let query = $state('');
	let searchResults = $state<SearchResult[]>([]);
	let searching = $state(false);
	let searchTimer: ReturnType<typeof setTimeout>;

	function onInput(e: Event) {
		const val = (e.target as HTMLInputElement).value;
		query = val;
		clearTimeout(searchTimer);
		if (!val.trim()) {
			searchResults = [];
			return;
		}
		searchTimer = setTimeout(async () => {
			searching = true;
			try {
				const r = await fetch(`/api/v1/search?q=${encodeURIComponent(val)}`);
				const d = await r.json();
				searchResults = d.Results ?? [];
			} catch {
				searchResults = [];
			} finally {
				searching = false;
			}
		}, 400);
	}

	const boards = $derived([
		{ key: 'speed', label: 'Speed Demons', desc: 'Fastest avg PR-to-merge time', tag: 'FAST', entries: home?.SpeedDemons ?? [], href: '/leaderboard/speed', isRepo: true, valFn: (e: LeaderboardEntry) => formatDuration(e.Value) },
		{ key: 'graveyard', label: 'PR Graveyard', desc: 'Slowest avg PR-to-merge time', tag: 'SLOW', entries: home?.PRGraveyard ?? [], href: '/leaderboard/graveyard', isRepo: true, valFn: (e: LeaderboardEntry) => formatDuration(e.Value) },
		{ key: 'reviewers', label: 'Review Champions', desc: 'Most reviews submitted', tag: 'TOP', entries: home?.ReviewChamps ?? [], href: '/leaderboard/reviewers', isRepo: false, valFn: (e: LeaderboardEntry) => `${formatNumber(e.Count)} reviews` },
		{ key: 'gatekeepers', label: 'Gatekeepers', desc: 'Most "Request Changes" sent', tag: 'GATE', entries: home?.Gatekeepers ?? [], href: '/leaderboard/gatekeepers', isRepo: false, valFn: (e: LeaderboardEntry) => `${formatNumber(e.Count)} blocks` },
		{ key: 'authors', label: 'Merge Masters', desc: 'Authors with most merged PRs', tag: 'MERGE', entries: home?.MergeMasters ?? [], href: '/leaderboard/authors', isRepo: false, valFn: (e: LeaderboardEntry) => `${formatNumber(e.Count)} merged` },
		{ key: 'oneshot', label: 'One-Shot Heroes', desc: 'PRs approved first try', tag: 'CLEAN', entries: home?.OneShot ?? [], href: '/leaderboard/oneshot', isRepo: true, valFn: (e: LeaderboardEntry) => `${e.Value}% clean` }
	]);
</script>

<svelte:head>
	<title>ngmi — PR Review Leaderboards</title>
	<meta name="description" content="Global leaderboards for GitHub PR review time." />
</svelte:head>

<!-- Hero -->
<section class="mb-8">
	<h1 class="mb-1 text-lg font-bold">If you aren't reviewing,<br />you're ngmi.</h1>
	<p class="mb-4 text-muted-foreground">
		Global leaderboards for GitHub PR review time.<br />
		Search any public repo, user, or org.
	</p>

	<!-- Search -->
	<div class="relative max-w-lg">
		<Input
			type="text"
			placeholder="golang/go, torvalds, kubernetes…"
			autofocus
			autocomplete="off"
			class="font-mono"
			oninput={onInput}
		/>
		{#if searchResults.length > 0}
			<div class="absolute z-50 mt-1 w-full rounded-md border border-border bg-card shadow-lg">
				{#each searchResults as result}
					<a
						href={result.Type === 'repo' ? `/repo/${result.FullName}` : result.Type === 'org' ? `/org/${result.FullName}` : `/user/${result.FullName}`}
						class="flex items-center gap-3 px-3 py-2 text-sm hover:bg-accent"
					>
						{#if result.AvatarURL}
							<img src={result.AvatarURL} alt="" class="size-5 rounded-full" />
						{/if}
						<div class="min-w-0 flex-1">
							<div class="font-mono font-medium">
								{result.Type !== 'repo' ? '@' : ''}{result.FullName}
							</div>
							{#if result.Description}
								<div class="truncate text-xs text-muted-foreground">{result.Description}</div>
							{/if}
						</div>
						<div class="shrink-0 text-xs text-muted-foreground">
							{#if result.MergedPRs > 0}
								{formatNumber(result.MergedPRs)} PRs · {formatDuration(result.AvgMergeTime)}
							{:else if result.Stars > 0}
								<Star class="inline size-3" />{formatNumber(result.Stars)}
							{/if}
						</div>
					</a>
				{/each}
			</div>
		{/if}
	</div>

	<!-- Quick pills -->
	{#if (home?.PopularVisits?.length ?? 0) > 0 || (home?.RecentVisits?.length ?? 0) > 0}
		<div class="mt-3 flex flex-wrap items-center gap-2">
			<span class="text-xs text-muted-foreground">Try:</span>
			{#each home?.PopularVisits ?? [] as v}
				<a href={v.Path}>
					<Badge variant="secondary" class="cursor-pointer font-mono">{v.Label}</Badge>
				</a>
			{/each}
			{#each home?.RecentVisits ?? [] as v}
				<a href={v.Path}>
					<Badge variant="outline" class="cursor-pointer font-mono">{v.Label}</Badge>
				</a>
			{/each}
		</div>
	{/if}
</section>

<!-- Stats bar -->
{#if home}
	<div class="mb-8 flex items-center gap-6 border-b border-t border-border py-3">
		<div class="text-center">
			<div class="text-xl font-bold">{formatNumber(home.TotalRepos)}</div>
			<div class="text-xs text-muted-foreground">Repos Tracked</div>
		</div>
		<div class="h-8 w-px bg-border"></div>
		<div class="text-center">
			<div class="text-xl font-bold">{formatNumber(home.TotalPRs)}</div>
			<div class="text-xs text-muted-foreground">PRs Analyzed</div>
		</div>
		<div class="h-8 w-px bg-border"></div>
		<div class="text-center">
			<div class="text-xl font-bold">{formatNumber(home.TotalReviews)}</div>
			<div class="text-xs text-muted-foreground">Reviews Logged</div>
		</div>
	</div>
{/if}

<!-- Leaderboards -->
<section>
	<div class="mb-4">
		<h2 class="text-sm font-bold">Global Leaderboards</h2>
		<p class="text-xs text-muted-foreground">Populated as repos are searched.</p>
	</div>

	<div class="grid grid-cols-1 gap-4 sm:grid-cols-2 lg:grid-cols-3">
		{#each boards as board}
			<Card>
				<CardHeader class="pb-2">
					<a href={board.href} class="flex items-start gap-3 no-underline hover:opacity-80">
						<Badge variant="secondary" class="shrink-0 font-mono">{board.tag}</Badge>
						<div>
							<CardTitle class="text-sm">{board.label}</CardTitle>
							<p class="text-xs text-muted-foreground">{board.desc}</p>
						</div>
						<span class="ml-auto text-muted-foreground">→</span>
					</a>
				</CardHeader>
				<CardContent class="pb-2">
					{#if board.entries.length > 0}
						<div class="space-y-1">
							{#each board.entries as entry}
								<a
									href={board.isRepo ? `/repo/${entry.Name}` : `/user/${entry.Name}`}
									class="flex items-center gap-2 rounded px-1 py-0.5 text-xs hover:bg-accent"
								>
									<span class="w-7 shrink-0 {rankClass(entry.Rank)}">{entry.Rank}</span>
									{#if entry.Extra && !board.isRepo}
										<img src={entry.Extra} alt="" class="size-4 rounded-full" />
									{/if}
									<span class="min-w-0 flex-1 truncate font-mono">
										{board.isRepo ? entry.Name : `@${entry.Name}`}
									</span>
									<span class="shrink-0 text-muted-foreground">{board.valFn(entry)}</span>
								</a>
							{/each}
						</div>
					{:else}
						<p class="text-xs text-muted-foreground">Syncing popular repos…</p>
					{/if}
					<a href={board.href} class="mt-2 block text-xs text-primary hover:underline">
						View full leaderboard →
					</a>
				</CardContent>
			</Card>
		{/each}
	</div>
</section>
