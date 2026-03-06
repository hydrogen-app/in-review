import type { LayoutServerLoad } from './$types';

export const load: LayoutServerLoad = async ({ fetch, cookies }) => {
	const sessionId = cookies.get('session_id');
	// Skip the API call entirely for unauthenticated users — saves a round-trip on every page
	if (!sessionId) return { currentUser: '' };

	const headers: Record<string, string> = { Cookie: `session_id=${sessionId}` };
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
