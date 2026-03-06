import { PRIVATE_API_URL } from '$env/static/private';
import type { Handle, HandleFetch } from '@sveltejs/kit';

export const handle: Handle = async ({ event, resolve }) => {
	return resolve(event);
};

// Rewrite /api/* fetch calls in server-side load functions to go directly to
// the Go backend, bypassing the Vite proxy (which only works in the browser).
export const handleFetch: HandleFetch = async ({ request, fetch }) => {
	const url = new URL(request.url);
	if (url.pathname.startsWith('/api/') || url.pathname.startsWith('/auth/')) {
		const newUrl = PRIVATE_API_URL + url.pathname + url.search;
		return fetch(new Request(newUrl, request));
	}
	return fetch(request);
};
