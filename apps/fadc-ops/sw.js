// Minimal service worker: makes the app installable and opens the shell instantly.
// Data (the API) is never cached, so numbers are always live.
const SHELL = 'fadc-ops-shell-v1';
self.addEventListener('install', (e) => { e.waitUntil(caches.open(SHELL).then((c) => c.addAll(['/']))); self.skipWaiting(); });
self.addEventListener('activate', (e) => { e.waitUntil(caches.keys().then((ks) => Promise.all(ks.filter((k) => k !== SHELL).map((k) => caches.delete(k)))).then(() => self.clients.claim())); });
self.addEventListener('fetch', (e) => {
  const u = new URL(e.request.url);
  if (u.origin !== location.origin || e.request.method !== 'GET') return;
  e.respondWith(fetch(e.request).then((r) => { if (u.pathname === '/') { const c = r.clone(); caches.open(SHELL).then((x) => x.put('/', c)); } return r; }).catch(() => caches.match(e.request).then((m) => m || caches.match('/'))));
});
