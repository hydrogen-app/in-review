import type { PageServerLoad } from './$types';

export const load: PageServerLoad = async ({ fetch, cookies }) => {
	const sessionId = cookies.get('session_id');
	const headers: Record<string, string> = {};
	if (sessionId) headers['Cookie'] = `session_id=${sessionId}`;

	const resp = await fetch('/api/v1/home', { headers });
	if (!resp.ok) return { home: null };
	const home = await resp.json();
	return { home };
};
