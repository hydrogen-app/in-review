import type { PageServerLoad } from './$types';
import { redirect, error } from '@sveltejs/kit';

export const load: PageServerLoad = async ({ fetch, cookies, params }) => {
	const { owner, name } = params;
	const sessionId = cookies.get('session_id');
	const headers: Record<string, string> = {};
	if (sessionId) headers['Cookie'] = `session_id=${sessionId}`;

	const trim = 0; // default, can be added as query param
	const resp = await fetch(`/api/v1/repo/${owner}/${name}?trim=${trim}`, { headers });
	if (resp.status === 404) error(404, 'Repo not found');
	if (!resp.ok) error(502, 'Could not load repo');
	const repo = await resp.json();
	return { repo };
};
