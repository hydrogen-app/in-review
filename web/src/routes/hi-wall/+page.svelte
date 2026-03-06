<script lang="ts">
	import { Badge } from '$lib/components/ui/badge';
	import { formatNumber } from '$lib/utils';

	let { data } = $props();
	const pages = $derived(data.wall?.pages ?? []);
</script>

<svelte:head>
	<title>Hi Wall — ngmi</title>
</svelte:head>

<!-- Breadcrumb -->
<div class="mb-3 flex items-center gap-1 text-xs text-muted-foreground">
	<a href="/" class="hover:text-foreground">ngmi</a>
	<span>/</span>
	<span class="text-foreground">Hi Wall</span>
</div>

<div class="mb-6">
	<h1 class="mb-1 text-base font-bold">Hi Wall</h1>
	<p class="text-xs text-muted-foreground">Pages with the most visitor reactions.</p>
</div>

{#if pages.length > 0}
	<div class="space-y-1">
		{#each pages as p}
			<a
				href={p.path}
				class="flex items-center justify-between rounded p-2 text-xs hover:bg-accent"
			>
				<span class="font-mono text-muted-foreground">{p.path}</span>
				<div class="flex items-center gap-2">
					{#if p.reactions}
						{#each Object.entries(p.reactions) as [emoji, count]}
							<span>{emoji} {count}</span>
						{/each}
					{/if}
					<Badge variant="secondary">{formatNumber(p.total)} total</Badge>
				</div>
			</a>
		{/each}
	</div>
{:else}
	<p class="text-xs text-muted-foreground">No reactions yet.</p>
{/if}
