import type { PageServerLoad } from './$types';
import { redirect, error } from '@sveltejs/kit';

export const load: PageServerLoad = async ({ fetch, cookies, params }) => {
	const { username } = params;
	const sessionId = cookies.get('session_id');
	const headers: Record<string, string> = {};
	if (sessionId) headers['Cookie'] = `session_id=${sessionId}`;

	const resp = await fetch(`/api/v1/user/${username}`, { headers });
	if (resp.status === 404) error(404, 'User not found');
	if (!resp.ok) error(502, 'Could not load user');
	const data = await resp.json();
	if (data.redirect) redirect(302, data.redirect);
	return { user: data };
};
