import type { PageServerLoad } from './$types';

export const load: PageServerLoad = async ({ fetch, url, setHeaders }) => {
	const trim = url.searchParams.get('trim') ?? '0';
	const minStars = url.searchParams.get('min_stars') ?? '0';
	const minContribs = url.searchParams.get('min_contribs') ?? '0';

	const resp = await fetch(
		`/api/v1/stats?trim=${trim}&min_stars=${minStars}&min_contribs=${minContribs}`
	);
	if (!resp.ok) return { stats: null };
	const stats = await resp.json();
	setHeaders({ 'cache-control': 'public, s-maxage=120, stale-while-revalidate=600' });
	return { stats };
};
