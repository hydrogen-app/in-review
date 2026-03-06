import type { PageServerLoad } from './$types';
import { error } from '@sveltejs/kit';

export const load: PageServerLoad = async ({ fetch, cookies, params, url }) => {
	const { category } = params;
	const sessionId = cookies.get('session_id');
	const headers: Record<string, string> = {};
	if (sessionId) headers['Cookie'] = `session_id=${sessionId}`;

	const offset = url.searchParams.get('offset') ?? '0';
	const resp = await fetch(`/api/v1/leaderboard/${category}?offset=${offset}`, { headers });
	if (resp.status === 404) error(404, 'Invalid leaderboard category');
	if (!resp.ok) error(502, 'Could not load leaderboard');
	const data = await resp.json();
	return { leaderboard: data };
};
