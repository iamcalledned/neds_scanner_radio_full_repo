
// --- Service Worker Registration ---
// Register on every page load so push works in both web and PWA contexts.
if ('serviceWorker' in navigator) {
    navigator.serviceWorker.register('/scanner/sw.js', { scope: '/scanner/' })
        .then(reg => {
            console.log('[SW] Registered, scope:', reg.scope);
            // Force the new service worker to activate immediately
            reg.update();
            if (reg.waiting) {
                reg.waiting.postMessage({ type: 'SKIP_WAITING' });
            }
            reg.addEventListener('updatefound', () => {
                const newWorker = reg.installing;
                if (newWorker) {
                    newWorker.addEventListener('statechange', () => {
                        if (newWorker.state === 'installed' && navigator.serviceWorker.controller) {
                            newWorker.postMessage({ type: 'SKIP_WAITING' });
                        }
                    });
                }
            });
        })
        .catch(err => console.warn('[SW] Registration failed:', err));
}

/**
 * Returns navigator.serviceWorker.ready with a timeout.
 * Avoids hanging forever when the SW hasn't activated yet.
 */
function swReady(timeoutMs = 10000) {
    return Promise.race([
        navigator.serviceWorker.ready,
        new Promise((_, reject) =>
            setTimeout(() => reject(new Error('Service worker not ready within timeout')), timeoutMs)
        ),
    ]);
}

// --- PWA Installation Logic ---
window.addEventListener('beforeinstallprompt', (event) => {
    event.preventDefault();
    deferredInstallPrompt = event;
    const installButton = document.getElementById('install-btn');
    if (installButton) {
        installButton.style.display = 'inline-flex';
    }
});
window.addEventListener('appinstalled', () => {
    console.log('PWA was installed');
    deferredInstallPrompt = null;
    const installButton = document.getElementById('install-btn');
    if (installButton) {
        installButton.style.display = 'none';
        installButton.classList.add('hidden');
    }
});

document.addEventListener('click', async (event) => {
    const installButton = event.target.closest('#install-btn');
    if (!installButton || !deferredInstallPrompt) return;

    deferredInstallPrompt.prompt();
    const { outcome } = await deferredInstallPrompt.userChoice;
    console.log(`User response to the install prompt: ${outcome}`);
    deferredInstallPrompt = null;
    installButton.style.display = 'none';
    installButton.classList.add('hidden');
});

// --- iOS "Add to Home Screen" Banner ---
(function () {
    const isIos = /iphone|ipad|ipod/i.test(navigator.userAgent);
    const isStandalone = window.navigator.standalone === true;
    const dismissed = localStorage.getItem('ios-install-dismissed');
    if (!isIos || isStandalone || dismissed) return;

    const banner = document.createElement('div');
    banner.id = 'ios-install-banner';
    banner.innerHTML = `
        <span>Install this app: tap the <strong>Share</strong> button &#x2197; then <strong>"Add to Home Screen"</strong></span>
        <button id="ios-install-dismiss" aria-label="Dismiss">&times;</button>
    `;
    banner.className = 'scanner-ios-banner';
    Object.assign(banner.style, {
        position: 'fixed',
        bottom: '0',
        left: '0',
        right: '0',
        zIndex: '9999',
        display: 'flex',
        alignItems: 'center',
        justifyContent: 'space-between',
        gap: '12px',
        padding: '14px 16px',
        background: '#0b1525',
        borderTop: '1px solid rgba(103, 212, 255, 0.35)',
        color: '#e2e8f0',
        fontSize: '14px',
        lineHeight: '1.4',
        boxShadow: '0 -12px 32px rgba(0,0,0,0.42)',
        backdropFilter: 'blur(18px)',
        WebkitBackdropFilter: 'blur(18px)'
    });
    const btn = banner.querySelector('#ios-install-dismiss');
    Object.assign(btn.style, {
        flexShrink: '0',
        background: 'none',
        border: 'none',
        color: '#94a3b8',
        fontSize: '20px',
        cursor: 'pointer',
        padding: '0 4px',
        lineHeight: '1',
    });

    document.addEventListener('DOMContentLoaded', () => {
        document.body.appendChild(banner);
        btn.addEventListener('click', () => {
            banner.remove();
            localStorage.setItem('ios-install-dismissed', '1');
        });
    });
})();

// --- Push Notification Subscribe / Unsubscribe ---

/**
 * Convert a base64url string to a Uint8Array (required by pushManager.subscribe).
 */
function _urlBase64ToUint8Array(base64String) {
    const padding = '='.repeat((4 - base64String.length % 4) % 4);
    const base64 = (base64String + padding).replace(/-/g, '+').replace(/_/g, '/');
    const rawData = atob(base64);
    return Uint8Array.from([...rawData].map(c => c.charCodeAt(0)));
}

/**
 * Fetch the VAPID public key from the server.
 */
async function _getVapidPublicKey() {
    const res = await fetch('/scanner/push/vapid_public');
    if (!res.ok) throw new Error('Could not fetch VAPID public key');
    return (await res.text()).trim();
}

/**
 * Subscribe this browser to push notifications and save to the server.
 * Returns the PushSubscription object on success, null on failure.
 */
async function subscribeToPush() {
    if (!('serviceWorker' in navigator) || !('PushManager' in window)) {
        console.warn('[Push] Push API not supported in this browser.');
        return null;
    }

    const permission = await Notification.requestPermission();
    if (permission !== 'granted') {
        console.warn('[Push] Notification permission denied.');
        return null;
    }

    try {
        const reg = await swReady();
        const vapidKey = await _getVapidPublicKey();
        const applicationServerKey = _urlBase64ToUint8Array(vapidKey);

        const subscription = await reg.pushManager.subscribe({
            userVisibleOnly: true,
            applicationServerKey,
        });

        // Send subscription to server
        await fetch('/scanner/push/subscribe', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(subscription.toJSON()),
        });

        localStorage.setItem('push_subscribed', '1');
        console.log('[Push] Subscribed successfully.');
        return subscription;
    } catch (err) {
        console.error('[Push] Subscription failed:', err);
        return null;
    }
}

/**
 * Unsubscribe this browser from push notifications.
 */
async function unsubscribeFromPush() {
    if (!('serviceWorker' in navigator)) return;
    try {
        const reg = await swReady();
        const subscription = await reg.pushManager.getSubscription();
        if (subscription) {
            await fetch('/scanner/push/unsubscribe', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ endpoint: subscription.endpoint }),
            });
            await subscription.unsubscribe();
        }
        localStorage.removeItem('push_subscribed');
        console.log('[Push] Unsubscribed successfully.');
    } catch (err) {
        console.error('[Push] Unsubscribe failed:', err);
    }
}

/**
 * Update the bell button appearance to reflect the current subscription state.
 */
function _updateBellButton(btn, subscribed) {
    if (!btn) return;
    if (subscribed) {
        btn.textContent = '🔔';
        btn.title = 'Push notifications ON — click to disable';
        btn.classList.add('push-active');
    } else {
        btn.textContent = '🔕';
        btn.title = 'Enable push notifications for new calls';
        btn.classList.remove('push-active');
    }
}

/**
 * Wire up the #push-btn bell button.
 * Call this once the DOM is ready (DOMContentLoaded or after template renders).
 */
async function initPushButton() {
    if (!('Notification' in window) || !('serviceWorker' in navigator) || !('PushManager' in window)) {
        // Push not supported — hide the button
        const btn = document.getElementById('push-btn');
        if (btn) btn.style.display = 'none';
        return;
    }

    const btn = document.getElementById('push-btn');
    if (!btn) return;

    // Detect current subscription state
    let isSubscribed = false;
    try {
        const reg = await swReady();
        const sub = await reg.pushManager.getSubscription();
        isSubscribed = !!sub && Notification.permission === 'granted';
    } catch (_) { /* ignore */ }

    _updateBellButton(btn, isSubscribed);
    btn.style.display = 'inline-flex';

    btn.addEventListener('click', async () => {
        btn.disabled = true;
        if (isSubscribed) {
            await unsubscribeFromPush();
            isSubscribed = false;
        } else {
            const sub = await subscribeToPush();
            isSubscribed = !!sub;
        }
        _updateBellButton(btn, isSubscribed);
        btn.disabled = false;
    });
}

// Auto-initialise when DOM is ready
// Use both DOMContentLoaded AND window load as a fallback so initNotifOverlay
// always runs regardless of which defer script executes first.
function _initAll() {
    initPushButton();
    initNotifOverlay();
}
if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', _initAll);
} else {
    // Already past DOMContentLoaded — run now but also schedule via
    // window.load in case the other defer script hasn't fired yet.
    _initAll();
}
// Safety net: re-run on load so event listeners are always attached
window.addEventListener('load', _initAll, { once: true });

// ----------------------------------------------------------------
// Notifications Settings Overlay
// ----------------------------------------------------------------

function ensureNotifOverlayMarkup() {
    let overlay = document.getElementById('notif-overlay');
    if (overlay) return overlay;

    overlay = document.createElement('div');
    overlay.id = 'notif-overlay';
    overlay.className = 'hidden fixed inset-0 z-[100] flex items-end sm:items-center justify-center';
    overlay.innerHTML = `
        <div id="notif-backdrop" class="absolute inset-0 bg-black/65 backdrop-blur-sm"></div>
        <div class="relative w-full sm:max-w-md mx-auto bg-[#0c1525] border border-slate-700/70 rounded-t-2xl sm:rounded-2xl shadow-2xl flex flex-col max-h-[85vh]">
            <div class="flex items-center justify-between px-5 py-4 border-b border-slate-700/70 shrink-0">
                <div>
                    <h2 class="text-base font-semibold text-white">Notification Channels</h2>
                    <p class="text-xs text-slate-400 mt-0.5">Choose which feeds send alerts to this device.</p>
                </div>
                <button id="notif-close" class="text-slate-400 hover:text-white text-xl leading-none">&times;</button>
            </div>

            <div id="notif-permission-banner" class="hidden mx-4 mt-3 px-4 py-3 rounded-lg bg-amber-900/30 border border-amber-700/50 text-amber-300 text-sm shrink-0">
                <strong>Permission required.</strong> Enable browser notifications to receive alerts.
                <button id="notif-grant-btn" class="ml-2 underline hover:text-white">Grant permission</button>
            </div>

            <div id="notif-channel-list" class="overflow-y-auto px-4 py-3 space-y-1 flex-1">
                <p class="text-slate-400 text-sm">Loading channels...</p>
            </div>

            <div class="px-5 py-4 border-t border-slate-700/70 flex items-center justify-between gap-3 shrink-0">
                <div class="flex gap-2">
                    <button id="notif-select-all" class="text-xs text-sky-400 hover:text-sky-300 underline">All</button>
                    <span class="text-slate-600">|</span>
                    <button id="notif-select-none" class="text-xs text-sky-400 hover:text-sky-300 underline">None</button>
                </div>
                <div class="flex gap-2">
                    <button id="notif-save" class="px-4 py-1.5 rounded-md bg-sky-600 hover:bg-sky-500 text-white text-sm font-medium transition">Save</button>
                    <button id="notif-cancel" class="px-4 py-1.5 rounded-md border border-slate-600 text-slate-300 hover:text-white text-sm transition">Cancel</button>
                </div>
            </div>
        </div>
    `;
    document.body.appendChild(overlay);
    return overlay;
}

async function initNotifOverlay() {
    const overlay      = ensureNotifOverlayMarkup();
    const openBtn      = document.getElementById('notif-settings-btn');
    const mobileOpenBtn = document.getElementById('notif-settings-btn-mobile');

    if (!overlay || (!openBtn && !mobileOpenBtn)) return;
    if (overlay.dataset.notifInitialised === '1') return;
    overlay.dataset.notifInitialised = '1';

    const backdrop     = document.getElementById('notif-backdrop');
    const closeBtn     = document.getElementById('notif-close');
    const cancelBtn    = document.getElementById('notif-cancel');
    const saveBtn      = document.getElementById('notif-save');
    const selectAll    = document.getElementById('notif-select-all');
    const selectNone   = document.getElementById('notif-select-none');
    const channelList  = document.getElementById('notif-channel-list');
    const permBanner   = document.getElementById('notif-permission-banner');
    const grantBtn     = document.getElementById('notif-grant-btn');

    let channels = [];   // populated on first open
    let currentEndpoint = null;

    // ---- helpers ----
    const openOverlay  = () => overlay.classList.remove('hidden');
    const closeOverlay = () => overlay.classList.add('hidden');

    function _toggleIds() {
        return [...overlay.querySelectorAll('.notif-toggle-input')]
            .filter(cb => cb.checked)
            .map(cb => cb.dataset.feed);
    }

    function _renderChannels(channels, savedFeeds) {
        if (!channelList) return;
        channelList.innerHTML = '';

        // Group by town
        const towns = {};
        for (const ch of channels) {
            if (!towns[ch.town]) towns[ch.town] = [];
            towns[ch.town].push(ch);
        }

        const allSelected = savedFeeds.length === 0; // empty = all

        for (const [town, feeds] of Object.entries(towns)) {
            const header = document.createElement('p');
            header.className = 'notif-town-header';
            header.textContent = town;
            channelList.appendChild(header);

            for (const ch of feeds) {
                const isChecked = allSelected || savedFeeds.includes(ch.id);
                const row = document.createElement('div');
                row.className = 'notif-channel-row';
                row.innerHTML = `
                    <label class="notif-channel-label" for="notif-ch-${ch.id}">
                        <span class="notif-type-dot ${ch.type === 'police' ? 'dot-police' : 'dot-fire'}"></span>
                        ${ch.label}
                    </label>
                    <label class="notif-toggle">
                        <input type="checkbox" id="notif-ch-${ch.id}"
                               class="notif-toggle-input"
                               data-feed="${ch.id}"
                               ${isChecked ? 'checked' : ''}>
                        <span class="notif-toggle-track"></span>
                    </label>`;
                channelList.appendChild(row);
            }
        }
    }

    async function _loadAndRender() {
        channelList.innerHTML = '<p class="text-slate-400 text-sm">Loading channels…</p>';

        // Fetch channel list
        try {
            const res = await fetch('/scanner/push/channels');
            const data = await res.json();
            channels = data.channels || [];
        } catch (e) {
            channelList.innerHTML = '<p class="text-red-400 text-sm">Could not load channels.</p>';
            return;
        }

        // Get current endpoint
        currentEndpoint = null;
        try {
            const reg = await swReady();
            const sub = await reg.pushManager.getSubscription();
            if (sub) currentEndpoint = sub.endpoint;
        } catch (_) {}

        // Fetch saved prefs
        let savedFeeds = [];
        if (currentEndpoint) {
            try {
                const r = await fetch(`/scanner/push/prefs?endpoint=${encodeURIComponent(currentEndpoint)}`);
                const d = await r.json();
                savedFeeds = d.feeds || [];
            } catch (_) {}
        }

        _renderChannels(channels, savedFeeds);
    }

    // ---- permission banner ----
    function _updatePermBanner() {
        if (!permBanner) return;
        const notSupported = !('Notification' in window);
        const granted = Notification.permission === 'granted';
        permBanner.classList.toggle('hidden', notSupported || granted);
    }

    async function openSettingsOverlay(e) {
        e?.stopPropagation?.();
        const dd = document.getElementById('menu-dropdown');
        if (dd) dd.classList.add('hidden');

        openOverlay();
        _updatePermBanner();
        await _loadAndRender();
    }

    if (grantBtn) {
        grantBtn.addEventListener('click', async () => {
            await subscribeToPush();
            _updatePermBanner();
            await _loadAndRender();
        });
    }

    // ---- open ----
    openBtn?.addEventListener('click', openSettingsOverlay);
    mobileOpenBtn?.addEventListener('click', openSettingsOverlay);

    // ---- close ----
    closeBtn?.addEventListener('click', closeOverlay);
    cancelBtn?.addEventListener('click', closeOverlay);
    backdrop?.addEventListener('click', closeOverlay);

    // ---- select all / none ----
    selectAll?.addEventListener('click', () => {
        overlay.querySelectorAll('.notif-toggle-input').forEach(cb => cb.checked = true);
    });
    selectNone?.addEventListener('click', () => {
        overlay.querySelectorAll('.notif-toggle-input').forEach(cb => cb.checked = false);
    });

    // ---- save ----
    saveBtn?.addEventListener('click', async () => {
        saveBtn.disabled = true;
        saveBtn.textContent = 'Subscribing…';

        try {
            // If we don't have an endpoint yet, create a push subscription now.
            if (!currentEndpoint) {
                // Check permission first — request it if needed
                if (Notification.permission === 'denied') {
                    saveBtn.textContent = 'Notifications blocked';
                    setTimeout(() => { saveBtn.textContent = 'Save'; saveBtn.disabled = false; }, 2500);
                    return;
                }
                if (Notification.permission !== 'granted') {
                    const perm = await Notification.requestPermission();
                    if (perm !== 'granted') {
                        saveBtn.textContent = 'Permission denied';
                        setTimeout(() => { saveBtn.textContent = 'Save'; saveBtn.disabled = false; }, 2500);
                        return;
                    }
                }
                // Permission is granted — create the push subscription
                try {
                    const reg = await swReady();
                    const vapidKey = await _getVapidPublicKey();
                    const appServerKey = _urlBase64ToUint8Array(vapidKey);
                    const newSub = await reg.pushManager.subscribe({
                        userVisibleOnly: true,
                        applicationServerKey: appServerKey,
                    });
                    await fetch('/scanner/push/subscribe', {
                        method: 'POST',
                        headers: { 'Content-Type': 'application/json' },
                        body: JSON.stringify(newSub.toJSON()),
                    });
                    localStorage.setItem('push_subscribed', '1');
                    currentEndpoint = newSub.endpoint;
                } catch (subErr) {
                    console.error('[NotifOverlay] pushManager.subscribe failed:', subErr);
                    saveBtn.textContent = `Subscribe failed: ${subErr.message || subErr}`;
                    setTimeout(() => { saveBtn.textContent = 'Save'; saveBtn.disabled = false; }, 4000);
                    return;
                }
            }

            saveBtn.textContent = 'Saving…';

            const selected = _toggleIds();
            // empty array = "all feeds" semantics
            const toSave = selected.length === channels.length ? [] : selected;

            await fetch('/scanner/push/prefs', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ endpoint: currentEndpoint, feeds: toSave }),
            });

            // Update bell button to reflect active subscription
            const bellBtn = document.getElementById('push-btn');
            _updateBellButton(bellBtn, true);

            saveBtn.textContent = 'Saved ✓';
            setTimeout(() => {
                saveBtn.textContent = 'Save';
                saveBtn.disabled = false;
                closeOverlay();
            }, 900);
        } catch (e) {
            console.error('[NotifOverlay] Save failed:', e);
            saveBtn.textContent = 'Error — check console';
            setTimeout(() => { saveBtn.textContent = 'Save'; saveBtn.disabled = false; }, 2500);
        }
    });
}
