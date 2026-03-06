import type { PageServerLoad } from './$types';
import { redirect, error } from '@sveltejs/kit';

export const load: PageServerLoad = async ({ fetch, cookies }) => {
	const sessionId = cookies.get('session_id');
	if (!sessionId) redirect(302, '/auth/login');

	const headers: Record<string, string> = { Cookie: `session_id=${sessionId}` };
	const resp = await fetch('/api/v1/dashboard', { headers });
	if (resp.status === 401) redirect(302, '/auth/login');
	if (!resp.ok) error(502, 'Could not load dashboard');
	const dashboard = await resp.json();
	return { dashboard };
};
