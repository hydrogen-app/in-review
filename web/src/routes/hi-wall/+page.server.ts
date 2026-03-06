import type { PageServerLoad } from './$types';

export const load: PageServerLoad = async ({ fetch }) => {
	const resp = await fetch('/api/v1/hi-wall');
	if (!resp.ok) return { wall: null };
	const wall = await resp.json();
	return { wall };
};
