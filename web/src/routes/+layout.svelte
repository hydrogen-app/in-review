<script lang="ts">
	import './layout.css';
	import favicon from '$lib/assets/favicon.svg';
	import { page } from '$app/stores';
	import { Button } from '$lib/components/ui/button';
	import { Separator } from '$lib/components/ui/separator';

	let { children, data } = $props();
	const currentUser = $derived(data.currentUser ?? '');
</script>

<svelte:head>
	<link rel="icon" href={favicon} />
</svelte:head>

<div class="min-h-screen bg-background font-mono text-sm">
	<!-- Nav -->
	<nav class="mx-auto flex max-w-4xl items-baseline gap-4 border-b border-border px-5 pb-3 pt-4">
		<a href="/" class="font-bold text-foreground no-underline">ngmi</a>
		<div class="ml-auto flex items-center gap-3">
			<a href="/stats" class="text-xs text-muted-foreground hover:text-foreground hover:underline">Stats</a>
			<a href="/blog" class="text-xs text-muted-foreground hover:text-foreground hover:underline">Blog</a>
			{#if currentUser}
				<a href="/dashboard" class="text-xs text-muted-foreground hover:text-foreground hover:underline">Dashboard</a>
				<form method="POST" action="/auth/logout" style="display:inline;margin:0">
					<button type="submit" class="cursor-pointer border-none bg-transparent p-0 font-mono text-xs text-muted-foreground hover:text-foreground hover:underline">
						Logout
					</button>
				</form>
			{:else}
				<a href="/auth/login" class="text-xs text-muted-foreground hover:text-foreground hover:underline">Login</a>
			{/if}
		</div>
	</nav>

	<!-- Main -->
	<main class="mx-auto max-w-4xl px-5 py-6">
		{@render children()}
	</main>

	<!-- Hi widget placeholder (client-side loaded) -->
	<div id="hi-widget-mount" class="mx-auto max-w-4xl px-5"></div>

	<!-- Footer -->
	<footer class="mx-auto max-w-4xl border-t border-border px-5 py-4 text-xs text-muted-foreground">
		<div class="flex flex-wrap items-center gap-2">
			<span class="font-bold text-foreground">ngmi</span>
			<span>·</span>
			<span>If you aren't reviewing, you're ngmi.</span>
			<span>·</span>
			<a href="/data" class="text-muted-foreground hover:text-foreground">Data</a>
			<a
				href="https://github.com/hydrogen-app/in-review"
				target="_blank"
				rel="noopener"
				class="text-muted-foreground hover:text-foreground"
			>
				GitHub ↗
			</a>
		</div>
	</footer>
</div>
