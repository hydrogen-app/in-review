import type { PageServerLoad } from './$types';

export const load: PageServerLoad = async ({ fetch }) => {
	const resp = await fetch('/api/v1/blog');
	if (!resp.ok) return { blog: null };
	const blog = await resp.json();
	return { blog };
};
