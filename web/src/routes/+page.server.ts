import type { PageServerLoad } from './$types';

export const load: PageServerLoad = async ({ fetch, cookies, setHeaders }) => {
	const sessionId = cookies.get('session_id');
	const headers: Record<string, string> = {};
	if (sessionId) headers['Cookie'] = `session_id=${sessionId}`;

	const resp = await fetch('/api/v1/home', { headers });
	if (!resp.ok) return { home: null };
	const home = await resp.json();
	if (!sessionId) setHeaders({ 'cache-control': 'public, s-maxage=30, stale-while-revalidate=120' });
	return { home };
};
