import type { LayoutServerLoad } from './$types';

export const load: LayoutServerLoad = async ({ fetch, cookies }) => {
	const sessionId = cookies.get('session_id');
	const headers: Record<string, string> = {};
	if (sessionId) {
		headers['Cookie'] = `session_id=${sessionId}`;
	}
	try {
		const resp = await fetch('/api/v1/me', { headers });
		if (resp.ok) {
			const data = await resp.json();
			return { currentUser: data.login || '' };
		}
	} catch {
		// backend not available
	}
	return { currentUser: '' };
};
