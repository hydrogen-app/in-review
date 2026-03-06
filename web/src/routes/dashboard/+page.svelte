<script lang="ts">
	import { Badge } from '$lib/components/ui/badge';
	import { Button } from '$lib/components/ui/button';
	import { Card, CardHeader, CardTitle, CardContent } from '$lib/components/ui/card';
	import { formatDuration, formatNumber } from '$lib/utils';
	import type { Repo } from '$lib/types';

	let { data } = $props();
	const d = $derived(data.dashboard);
</script>

<svelte:head>
	<title>Dashboard — ngmi</title>
</svelte:head>

<div class="mb-6">
	<div class="mb-1 flex items-center gap-3">
		{#if d?.AvatarURL}
			<img src={d.AvatarURL} alt="" class="size-10 rounded-full" />
		{/if}
		<div>
			<h1 class="font-mono text-base font-bold">@{d?.Login}</h1>
			<p class="text-xs text-muted-foreground">Your dashboard</p>
		</div>
	</div>
</div>

<!-- GitHub App Install -->
{#if !d?.HasInstall}
	<Card class="mb-6">
		<CardContent class="py-4">
			<div class="flex items-center justify-between gap-4">
				<div>
					<p class="text-sm font-medium">Connect your repos</p>
					<p class="text-xs text-muted-foreground">
						Install the GitHub App to track your private repos and get auto-sync.
					</p>
				</div>
				<a href={d?.InstallURL} target="_blank" rel="noopener">
					<Button size="sm">Install GitHub App →</Button>
				</a>
			</div>
		</CardContent>
	</Card>
{/if}

<!-- Tracked Repos -->
<Card>
	<CardHeader>
		<CardTitle class="text-sm">Tracked Repos</CardTitle>
	</CardHeader>
	<CardContent>
		{#if d?.TrackedRepos?.length > 0}
			<div class="space-y-2">
				{#each d.TrackedRepos as repo}
					<div class="flex items-center justify-between rounded p-2 text-xs hover:bg-accent">
						<a href="/repo/{repo.FullName}" class="font-mono hover:underline">{repo.FullName}</a>
						<div class="flex items-center gap-2">
							{#if repo.MergedPRCount}
								<span class="text-muted-foreground">{formatNumber(repo.MergedPRCount)} PRs</span>
							{/if}
							{#if repo.AvgMergeTimeSecs}
								<Badge variant="outline">{formatDuration(repo.AvgMergeTimeSecs)}</Badge>
							{/if}
							<Badge
								variant={repo.SyncStatus === 'done' ? 'outline' : 'secondary'}
								class={repo.SyncStatus === 'done' ? 'text-green-400' : ''}
							>
								{repo.SyncStatus}
							</Badge>
						</div>
					</div>
				{/each}
			</div>
		{:else}
			<p class="text-xs text-muted-foreground">
				No repos tracked yet.
				{#if d?.HasInstall}
					Search for a repo on the <a href="/" class="text-primary hover:underline">home page</a> to start tracking.
				{:else}
					Install the GitHub App to get started.
				{/if}
			</p>
		{/if}
	</CardContent>
</Card>

<!-- Profile link -->
<div class="mt-4 text-xs text-muted-foreground">
	<a href="/user/{d?.Login}" class="text-primary hover:underline">View your public profile →</a>
</div>
