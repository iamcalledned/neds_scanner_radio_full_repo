// In: sw.js (Your complete, corrected file)

const CACHE_NAME = 'scanner-cache-v20260331d';
const OFFLINE_URL = 'offline.html';

// Use relative paths so this worker works under /scanner/ when installed there.
// Use relative paths so this worker works under /scanner/ when installed there.
const ASSETS_TO_CACHE = [
  './',
  OFFLINE_URL,
  'manifest.json',
  'static/icons/icon-192x192.png',
  'static/icons/icon-512x512.png',
  'static/icons/icon-192x192-v2.png',
  'static/icons/icon-512x512-v2.png',
  'static/icons/logo-header.png',
  'static/icons/favicon.ico',

  // Add all critical JS/CSS (without ?v)
  'static/js/scanner_app_new.js',
  'static/js/scanner_view.js',
  'static/js/scanner_archive.js',
  'static/js/scanner_heatmap.js',
  'static/js/pwa.js'
];


// Resilient precache: try to fetch each asset and only cache the ones that succeed.
self.addEventListener('install', (event) => {
  event.waitUntil((async () => {
    const cache = await caches.open(CACHE_NAME);
    const base = new URL('./', self.location);
    await Promise.all(
      ASSETS_TO_CACHE.map(async (asset) => {
        try {
          const url = new URL(asset, base).href;
          const res = await fetch(url, { cache: 'no-cache' });
          if (res && res.ok) {
            await cache.put(url, res.clone());
          } else {
            console.warn('SW: asset not cached (non-OK):', url, res && res.status);
          }
        } catch (err) {
          console.warn('SW: failed to fetch asset', asset, err);
        }
      })
    );
  })());
  self.skipWaiting();
});

// This is the ONLY fetch listener. It is the correct one.
self.addEventListener('fetch', (event) => {
  if (event.request.method !== 'GET') return;
  if (!event.request.url.startsWith('http')) return;

  // --- START: API Request Handling ---
  // If the request is for an API endpoint, always go to the network.
  // This ensures we get live data and that authentication cookies are sent.
  const isApiRequest = event.request.url.includes('/scanner/api/') ||
                       event.request.url.includes('/scanner/me') ||
                       event.request.url.includes('/scanner/logout');
  
  if (isApiRequest) { 
    // Do not cache API calls. Respond with the network request.
    event.respondWith(
      fetch(event.request)
    );
    return;
  }
  // --- END: API Request Handling ---

  // HTML navigations: network-first with offline fallback
  if (event.request.mode === 'navigate') {
    event.respondWith(
      fetch(event.request).catch(() => caches.match(OFFLINE_URL))
    );
    return;
  }

  // Everything else: network-first, cache-on-success
  event.respondWith(
    fetch(event.request)
      .then((response) => {
        if (response.status === 200) {
          const resClone = response.clone();
          caches.open(CACHE_NAME).then((cache) => cache.put(event.request, resClone));
        }
        return response;
      })
      .catch(() => caches.match(event.request))
  );
});

self.addEventListener('activate', (event) => {
  event.waitUntil(
    caches.keys().then((keys) => {
      return Promise.all(
        keys.map((key) => {
          // Delete ALL old caches (different name), and also purge icon/manifest
          // entries from the current cache so they re-fetch with the new logo
          if (key !== CACHE_NAME) {
            return caches.delete(key);
          } else {
            return caches.open(key).then((cache) =>
              cache.keys().then((requests) =>
                Promise.all(
                  requests
                    .filter((r) => r.url.includes('manifest.json') || r.url.includes('/icons/'))
                    .map((r) => cache.delete(r))
                )
              )
            );
          }
        })
      );
    })
  );
  self.clients.claim();
});


// Listen for messages from the page (e.g., SKIP_WAITING)
self.addEventListener('message', (event) => {
  if (!event.data) return;
  if (event.data.type === 'SKIP_WAITING') {
    self.skipWaiting();
  }
});

//
// THE SECOND, BROKEN 'fetch' LISTENER HAS BEEN DELETED
//


// Handle push events (display notifications)
self.addEventListener('push', function(event) {
  let payload = {};
  try {
    if (event.data) payload = event.data.json();
  } catch (e) {
    try { payload = { message: event.data.text() }; } catch (e2) { payload = { message: 'New notification' }; }
  }

  const title = (payload && payload.title) || 'Scanner';
  const options = {
    body: (payload && payload.message) || '',
    icon: 'static/icons/icon-192x192.png',
    badge: 'static/icons/icon-192x192.png',
    data: payload.data || {}
  };

  event.waitUntil(self.registration.showNotification(title, options));
});


// Handle notification click
self.addEventListener('notificationclick', function(event) {
  event.notification.close();
  const urlToOpen = new URL('/scanner/', self.location.origin).href;
  event.waitUntil(
    clients.matchAll({ type: 'window', includeUncontrolled: true }).then( windowClients => {
      for (let i = 0; i < windowClients.length; i++) {
        const client = windowClients[i];
        if (client.url === urlToOpen && 'focus' in client) return client.focus();
      }
      if (clients.openWindow) return clients.openWindow(urlToOpen);
    })
  );
});