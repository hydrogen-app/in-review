<script lang="ts">
	import { page } from '$app/stores';
	import { Badge } from '$lib/components/ui/badge';
	import { Button } from '$lib/components/ui/button';
	import { Input } from '$lib/components/ui/input';
	import { Card, CardHeader, CardTitle, CardContent } from '$lib/components/ui/card';
	import {
		Table,
		TableBody,
		TableCell,
		TableHead,
		TableHeader,
		TableRow
	} from '$lib/components/ui/table';
	import { formatDuration, formatNumber, rankClass } from '$lib/utils';
	import type { RepoLeaderboardRow, UserLeaderboardRow, CleanLeaderboardRow } from '$lib/types';

	let { data } = $props();
	const d = $derived(data.leaderboard);
	const category = $derived($page.params.category);

	const isUserCategory = $derived(
		category === 'reviewers' || category === 'gatekeepers' || category === 'authors'
	);
	const isCleanCategory = $derived(category === 'oneshot');

	let searchQuery = $state('');
	let searchResults = $state<{ RepoRows: RepoLeaderboardRow[]; UserRows: UserLeaderboardRow[] } | null>(null);
	let searching = $state(false);
	let searchTimer: ReturnType<typeof setTimeout>;

	function onSearch(e: Event) {
		const val = (e.target as HTMLInputElement).value;
		searchQuery = val;
		clearTimeout(searchTimer);
		if (!val.trim()) {
			searchResults = null;
			return;
		}
		searchTimer = setTimeout(async () => {
			searching = true;
			try {
				const r = await fetch(
					`/api/v1/leaderboard/${category}/search?q=${encodeURIComponent(val)}`
				);
				searchResults = await r.json();
			} catch {
				searchResults = null;
			} finally {
				searching = false;
			}
		}, 300);
	}

	const displayRepoRows = $derived(searchResults?.RepoRows ?? d?.RepoRows ?? []);
	const displayUserRows = $derived(searchResults?.UserRows ?? d?.UserRows ?? []);
	const displayCleanRows = $derived(d?.CleanRows ?? []);
</script>

<svelte:head>
	<title>{d?.Title ?? category} — ngmi</title>
</svelte:head>

<!-- Breadcrumb -->
<div class="mb-3 flex items-center gap-1 text-xs text-muted-foreground">
	<a href="/" class="hover:text-foreground">ngmi</a>
	<span>/</span>
	<span class="text-foreground">Leaderboard</span>
	<span>/</span>
	<span class="text-foreground">{d?.Title ?? category}</span>
</div>

<!-- Header -->
<div class="mb-6">
	<h1 class="mb-1 text-base font-bold">{d?.Title}</h1>
	{#if d?.Description}
		<p class="text-xs text-muted-foreground">{d.Description}</p>
	{/if}
</div>

<!-- Search -->
<div class="mb-4 max-w-sm">
	<Input
		type="text"
		placeholder={isUserCategory ? 'Search by username…' : 'Search by repo…'}
		class="font-mono text-xs"
		oninput={onSearch}
	/>
</div>

{#if isCleanCategory}
	<!-- Clean leaderboard -->
	<Card>
		<CardContent class="overflow-x-auto p-0">
			<Table>
				<TableHeader>
					<TableRow>
						<TableHead class="w-12">Rank</TableHead>
						<TableHead>Repo</TableHead>
						<TableHead class="w-20">Clean %</TableHead>
						<TableHead class="w-20">Total PRs</TableHead>
						<TableHead class="w-28">Avg Time</TableHead>
					</TableRow>
				</TableHeader>
				<TableBody>
					{#each displayCleanRows as row}
						<TableRow>
							<TableCell class="font-mono text-xs {rankClass(row.Rank)}">#{row.Rank}</TableCell>
							<TableCell class="font-mono text-xs">
								<a href="/repo/{row.FullName}" class="hover:underline">{row.FullName}</a>
							</TableCell>
							<TableCell class="text-xs text-green-400">{row.CleanPct.toFixed(0)}%</TableCell>
							<TableCell class="text-xs">{formatNumber(row.Total)}</TableCell>
							<TableCell class="text-xs">{formatDuration(row.AvgSecs)}</TableCell>
						</TableRow>
					{/each}
				</TableBody>
			</Table>
		</CardContent>
	</Card>
{:else if isUserCategory}
	<!-- User leaderboard -->
	<Card>
		<CardContent class="overflow-x-auto p-0">
			<Table>
				<TableHeader>
					<TableRow>
						<TableHead class="w-12">Rank</TableHead>
						<TableHead>User</TableHead>
						{#if category === 'reviewers'}
							<TableHead class="w-24">Reviews</TableHead>
							<TableHead class="w-20">Approvals</TableHead>
							<TableHead class="w-20">Blocks</TableHead>
						{:else if category === 'gatekeepers'}
							<TableHead class="w-24">Blocks</TableHead>
						{:else}
							<TableHead class="w-24">Merged PRs</TableHead>
							<TableHead class="w-28">Avg Time</TableHead>
						{/if}
					</TableRow>
				</TableHeader>
				<TableBody>
					{#each displayUserRows as row}
						<TableRow>
							<TableCell class="font-mono text-xs {rankClass(row.Rank)}">#{row.Rank}</TableCell>
							<TableCell class="text-xs">
								<a href="/user/{row.Login}" class="flex items-center gap-2 hover:underline">
									{#if row.AvatarURL}
										<img src={row.AvatarURL} alt="" class="size-5 rounded-full" />
									{/if}
									<span class="font-mono">@{row.Login}</span>
								</a>
							</TableCell>
							{#if category === 'reviewers'}
								<TableCell class="text-xs">{formatNumber(row.Total)}</TableCell>
								<TableCell class="text-xs text-green-400">{formatNumber(row.Approvals)}</TableCell>
								<TableCell class="text-xs text-red-400">{formatNumber(row.ChangesRequested)}</TableCell>
							{:else if category === 'gatekeepers'}
								<TableCell class="text-xs text-red-400">{formatNumber(row.ChangesRequested)}</TableCell>
							{:else}
								<TableCell class="text-xs">{formatNumber(row.MergedPRs)}</TableCell>
								<TableCell class="text-xs">
									{#if row.AvgMergeTimeSecs}
										{formatDuration(row.AvgMergeTimeSecs)}
									{:else}
										—
									{/if}
								</TableCell>
							{/if}
						</TableRow>
					{/each}
				</TableBody>
			</Table>
		</CardContent>
	</Card>
{:else}
	<!-- Repo leaderboard -->
	<Card>
		<CardContent class="overflow-x-auto p-0">
			<Table>
				<TableHeader>
					<TableRow>
						<TableHead class="w-12">Rank</TableHead>
						<TableHead>Repo</TableHead>
						<TableHead class="w-28">Avg Time</TableHead>
						<TableHead class="w-24">PRs</TableHead>
					</TableRow>
				</TableHeader>
				<TableBody>
					{#each displayRepoRows as row}
						<TableRow>
							<TableCell class="font-mono text-xs {rankClass(row.Rank)}">#{row.Rank}</TableCell>
							<TableCell class="font-mono text-xs">
								<a href="/repo/{row.FullName}" class="hover:underline">{row.FullName}</a>
							</TableCell>
							<TableCell class="text-xs">
								<Badge
									variant={row.AvgSecs < 86400 ? 'outline' : 'secondary'}
									class={row.AvgSecs < 86400 ? 'text-green-400' : row.AvgSecs > 2592000 ? 'text-red-400' : ''}
								>
									{formatDuration(row.AvgSecs)}
								</Badge>
							</TableCell>
							<TableCell class="text-xs">{formatNumber(row.PRCount)}</TableCell>
						</TableRow>
					{/each}
				</TableBody>
			</Table>
		</CardContent>
	</Card>
{/if}

<!-- Pagination -->
{#if !searchQuery && (d?.HasMore || d?.NextOffset > 0)}
	<div class="mt-4 flex justify-center gap-2">
		{#if d.NextOffset > 50}
			<Button variant="outline" size="sm" onclick={() => history.back()}>← Prev</Button>
		{/if}
		{#if d.HasMore}
			<a href="?offset={d.NextOffset}">
				<Button variant="outline" size="sm">Next →</Button>
			</a>
		{/if}
	</div>
{/if}
