// ==========================================================
// Ned’s Scanner Network - Main App Logic (Handles Auth, Socket, PWA Install)
// ==========================================================

// --- Globals ---
let socket;
let isLoggedIn = false;
let deferredInstallPrompt = null; // For PWA installation

const FEED_TOWN_MAP = {
    pd: "Hopedale", fd: "Hopedale",
    mpd: "Milford", mfd: "Milford",
    bpd: "Bellingham", bfd: "Bellingham",
    mndpd: "Mendon", mndfd: "Mendon",
    uptpd: "Upton", uptfd: "Upton",
    blkpd: "Blackstone", blkfd: "Blackstone",
    frkpd: "Franklin", frkfd: "Franklin"
};

const HTML_ESCAPES = { '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' };

const CALL_BOARD_REFRESH_INTERVAL = 45000; // 45 seconds
const CALL_BOARD_MIN_REFRESH = 20000; // Debounce frequent socket updates
let lastCallBoardRefresh = 0;
let callBoardIntervalId = null;
const LISTENER_HEARTBEAT_INTERVAL_MS = 45000;
const LISTENER_CLIENT_ID_KEY = 'scanner_listener_client_id';
let listenerHeartbeatTimer = null;
let listenerHeartbeatStarted = false;

const API_CACHE_DEFAULT_TTL = 30000;
const API_CACHE_KEYS = {
    stats: 'scanner_api_stats',
    wsUsers: 'scanner_api_listeners',
    latest: 'scanner_api_latest',
    homeLiveCalls: 'scanner_api_home_live_calls',
    todayCounts: 'scanner_api_today_counts'
};

function readCache(cacheKey, maxAgeMs) {
    try {
        const raw = localStorage.getItem(cacheKey);
        if (!raw) return null;
        const parsed = JSON.parse(raw);
        if (!parsed?.timestamp || !('data' in parsed)) return null;
        if (Date.now() - parsed.timestamp > maxAgeMs) return null;
        return parsed.data;
    } catch (err) {
        console.warn('[Cache] Failed to read cache:', cacheKey, err);
        return null;
    }
}

function writeCache(cacheKey, data) {
    try {
        localStorage.setItem(cacheKey, JSON.stringify({ timestamp: Date.now(), data }));
    } catch (err) {
        console.warn('[Cache] Failed to write cache:', cacheKey, err);
    }
}

async function fetchJsonWithCache(url, cacheKey, ttlMs, onData) {
    const cached = readCache(cacheKey, ttlMs);
    if (cached) onData(cached, true);

    try {
        const res = await fetch(url, { cache: 'no-store' });
        if (!res.ok) throw new Error(`Fetch failed: ${res.status}`);
        const data = await res.json();
        writeCache(cacheKey, data);
        onData(data, false);
    } catch (err) {
        console.warn(`[Cache] Network fetch failed for ${url}:`, err);
    }
}

function getListenerClientId() {
    try {
        let clientId = localStorage.getItem(LISTENER_CLIENT_ID_KEY);
        if (!clientId) {
            clientId = (window.crypto && typeof window.crypto.randomUUID === 'function')
                ? window.crypto.randomUUID()
                : `listener-${Date.now()}-${Math.random().toString(16).slice(2)}`;
            localStorage.setItem(LISTENER_CLIENT_ID_KEY, clientId);
        }
        return clientId;
    } catch (err) {
        return `listener-${Date.now()}-${Math.random().toString(16).slice(2)}`;
    }
}

async function sendListenerHeartbeat(active = true) {
    try {
        await fetch('/scanner/_heartbeat', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            credentials: 'same-origin',
            keepalive: true,
            body: JSON.stringify({
                client_id: getListenerClientId(),
                page: `${window.location.pathname}${window.location.search}`,
                active
            })
        });
    } catch (err) {
        console.warn('[Presence] Heartbeat failed:', err);
    }
}

function initListenerHeartbeat() {
    if (listenerHeartbeatStarted) return;
    listenerHeartbeatStarted = true;

    sendListenerHeartbeat(true);
    listenerHeartbeatTimer = window.setInterval(() => {
        sendListenerHeartbeat(true);
    }, LISTENER_HEARTBEAT_INTERVAL_MS);

    document.addEventListener('visibilitychange', () => {
        if (document.visibilityState === 'visible') {
            sendListenerHeartbeat(true);
        }
    });

    window.addEventListener('focus', () => sendListenerHeartbeat(true));
}

// --- Inject CSS for shared styles (live indicators) ---
function injectSocketStyles() {
    const style = document.createElement('style');
    style.textContent = `
    .live-indicator {
      display: inline-block;
      width: 10px;
      height: 10px;
      background-color: #64748b; /* slate-500 */
      border-radius: 50%;
      margin-right: 6px;
      transition: background-color 0.3s ease;
      vertical-align: middle;
    }
    .live-indicator.live {
      background-color: #f87171; /* red-400 - Brighter base */
      animation: pulse-bg 1.5s cubic-bezier(0.4, 0, 0.6, 1) infinite;
      box-shadow: 0 0 6px #ef4444, 0 0 10px #f87171; /* red-500, red-400 glow */
    }
    @keyframes pulse-bg {
       0%, 100% { 
         background-color: #f87171; /* red-400 */
         transform: scale(1); 
       }
     50% { 
         background-color: #dc2626; /* red-600 - Darker pulse */
         transform: scale(1.1); /* Optional: slight scale */
       } 
    }
    /* === UPDATED: Generic selector for new <a> tags === */
    [data-feed] .live-indicator {
      position: absolute;
      top: 8px; 
      left: 12px;
      width: 8px; 
      height: 8px;
    }
    /* Styles for positioning next to text (used on scanner_stats.html) */
    #live-status-grid .live-indicator {
      width: 10px;
      height: 10px;
    }

    /* Flash effect for updated times */
    .time-updated-flash {
        color: #34d399 !important; /* emerald-400 */
        transition: color 0.1s ease-in-out;
    }
  `;
    document.head.appendChild(style);
}

// --- Service Worker registration is handled by pwa.js ---


// --- AUTH LOGIC (No changes) ---
async function checkAuth() {
    const authButtonContainer = document.getElementById("auth-status-button");
    if (!authButtonContainer) {
        console.warn("[Auth] auth-status-button container not found.");
        return;
    }
    try {
        const resp = await fetch("/scanner/me");
        if (!resp.ok) {
             if (resp.status === 401) throw new Error("Not authenticated");
             else throw new Error(`Server error: ${resp.status}`);
        }
        const data = await resp.json();
        console.log("[Auth] /scanner/me →", data);
        if (data?.userInfo?.username) {
            const name = data.userInfo.username;
            authButtonContainer.innerHTML = userButtonHTML(name);
            isLoggedIn = true;
            const logoutBtn = document.getElementById("logout-button");
            if(logoutBtn) logoutBtn.addEventListener("click", logout);
        } else {
            throw new Error("Invalid user info in response");
        }
    } catch (err) {
        isLoggedIn = false;
        if (err.message === "Not authenticated") console.info("[Auth] User is not logged in.");
        else console.warn("[Auth] Auth check failed:", err.message);
        authButtonContainer.innerHTML = loginButtonHTML();
    }
}
async function logout() {
    try {
        const resp = await fetch("/scanner/logout", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({}),
        });
         if (!resp.ok) console.warn(`Logout request failed with status: ${resp.status}`);
         else console.log("[Auth] Logout successful via API.");
    } catch (e) {
        console.error("Logout network request failed:", e);
    } finally {
        isLoggedIn = false;
        const authButtonContainer = document.getElementById("auth-status-button");
         if (authButtonContainer) authButtonContainer.innerHTML = loginButtonHTML();
    }
}
function loginButtonHTML() {
    return `<a href="/scanner/login" class="menu-item">Login</a>`;
}
function userButtonHTML(name) {
    const displayName = name.length > 10 ? name.substring(0, 8) + '...' : name;
    return `<button id="logout-button" class="menu-item w-full text-left" title="Log out ${name}">Logout (${displayName})</button>`;
}

// ==========================================================
// === SOCKET.IO LOGIC ======================================
// ==========================================================
function initSocketIO() {
    const serverURL = window.location.origin.includes('iamcalledned.ai')
      ? "https://iamcalledned.ai" // Production
      : "http://localhost:5005"; // Development
      
    console.log(`[Socket] Attempting to connect to ${serverURL}`);
    socket = io(serverURL, { transports: ['websocket', 'polling'] });

    socket.on('connect', () => console.log('[Socket] Connected to server!'));
    socket.on('disconnect', (reason) => console.warn(`[Socket] Disconnected from server. Reason: ${reason}`));
    socket.on('connect_error', (err) => console.error('[Socket] Connection Error:', err.message));
    socket.on('connection_response', (msg) => console.log('[Socket] Server says:', msg.data));

    socket.on('transmitting_update', (statusObject) => {
        console.log('[Socket] Received transmitting_update:', statusObject);
        if (statusObject && typeof statusObject === 'object') {
            Object.entries(statusObject).forEach(([deptId, status]) => {
                updateTransmittingStatus(deptId, status === 'Y');
            });
        } else {
             console.warn('[Socket] Received invalid transmitting_update data:', statusObject);
        }
    });

    // === 1. LISTENER FOR INITIAL TIME SNAPSHOT ===
    socket.on('initial_time_snapshot', (allTimes) => {
        console.log('[Socket] Received initial_time_snapshot:', allTimes);
        for (const [feedId, formattedTime] of Object.entries(allTimes)) {
            if (!formattedTime) continue; 
            
            // === UPDATED: Find <a> tag and query *within* it ===
            const feedElement = document.querySelector(`[data-feed="${feedId}"]`);
            if (feedElement) {
                const timeElement = feedElement.querySelector('div.text-xs.text-slate-500, div.text-xs.text-slate-600');
                if (timeElement) {
                    timeElement.textContent = formattedTime;
                    if (timeElement.classList.contains('text-slate-600')) {
                        timeElement.classList.remove('text-slate-600', 'italic');
                        timeElement.classList.add('text-slate-500');
                    }
                } else {
                    console.log(`[Socket] Snapshot: Could not find time element for feed: ${feedId}.`);
                }
            }
        }
    });

    // === 2. LISTENER FOR LIVE TIME UPDATES ===
    socket.on('latest_time_update', (data) => {
        console.log('[Socket] Received latest_time_update:', data);
        
        const feedId = data.feed;
        const formattedTime = data.time;

        if (!feedId || typeof formattedTime === 'undefined') {
            console.warn('[Socket] Invalid time update data received:', data);
            return;
        }

        // === UPDATED: Find <a> tag and query *within* it ===
        const feedElement = document.querySelector(`[data-feed="${feedId}"]`);
        if (feedElement) {
            const timeElement = feedElement.querySelector('div.text-xs.text-slate-500, div.text-xs.text-slate-600');
            if (timeElement) {
                timeElement.textContent = formattedTime;
                if (timeElement.classList.contains('text-slate-600')) {
                    timeElement.classList.remove('text-slate-600', 'italic');
                    timeElement.classList.add('text-slate-500');
                }
                // Add flash effect
                timeElement.classList.add('time-updated-flash');
                setTimeout(() => {
                    if (timeElement) timeElement.classList.remove('time-updated-flash');
                }, 500);
            } else {
                 console.warn(`[Socket] Could not find time element for feed: ${feedId}`);
            }
            refreshCallBoard();
        }
    });
}

// Cache last state to prevent flicker
const lastTransmittingState = {};

// --- Live Now Bar ---
const liveNowFeeds = new Set();

function updateLiveNowBar(dept, isTransmitting) {
    const chips = document.getElementById('live-now-chips');
    const quiet = document.getElementById('live-bar-quiet');
    if (!chips) return;

    if (isTransmitting) {
        liveNowFeeds.add(dept);
    } else {
        liveNowFeeds.delete(dept);
    }

    // Rebuild chips
    chips.innerHTML = '';
    if (liveNowFeeds.size === 0) {
        chips.appendChild(Object.assign(document.createElement('span'), {
            id: 'live-bar-quiet',
            className: 'text-slate-600 text-xs italic',
            textContent: 'All quiet'
        }));
        return;
    }

    liveNowFeeds.forEach(feed => {
        const isFire = feed.toLowerCase().includes('fd');
        const townName = resolveTownFromFeed(feed);
        const chip = document.createElement('span');
        chip.className = `inline-flex items-center gap-1.5 px-2.5 py-0.5 rounded-full text-xs font-medium border ${
            isFire
                ? 'bg-red-900/40 border-red-700/50 text-red-300'
                : 'bg-blue-900/40 border-blue-700/50 text-blue-300'
        }`;
        chip.innerHTML = `<span class="w-1.5 h-1.5 rounded-full ${isFire ? 'bg-red-400' : 'bg-blue-400'} animate-ping"></span>${townName} ${isFire ? 'Fire' : 'PD'}`;
        chips.appendChild(chip);
    });
}

function updateTransmittingStatus(dept, isTransmitting) {
  if (lastTransmittingState[dept] === isTransmitting) return;
  lastTransmittingState[dept] = isTransmitting;
  updateLiveNowBar(dept, isTransmitting);

  // === UPDATED: Generic selector finds <a> tag now ===
  const feedElement = document.querySelector(`[data-feed="${dept}"]`);
  if (!feedElement) {
    console.warn(`[Socket] No element found for feed: ${dept}`);
    return;
  }

  let indicator = feedElement.querySelector('.live-indicator');
  if (!indicator) {
    indicator = document.createElement('span');
    indicator.classList.add('live-indicator');
    feedElement.insertBefore(indicator, feedElement.firstChild);
  }

  indicator.classList.toggle('live', isTransmitting);
  console.log(`[Socket] ${dept} → ${isTransmitting ? 'ON 🔴' : 'OFF ⚫'}`);
}

// --- Header Init (No changes) ---
function initHeader() {
    const REFRESH_INTERVAL_MS = 30000;
    const menuBtn = document.getElementById('menu-btn');
    const menuDropdown = document.getElementById('menu-dropdown');

    if (menuBtn && menuDropdown) {
        console.log("[Header] Initializing menu dropdown.");
        document.addEventListener('click', (e) => {
            if (menuBtn.contains(e.target)) {
                menuDropdown.classList.toggle('hidden');
            } else if (!menuDropdown.contains(e.target)) {
                menuDropdown.classList.add('hidden');
            }
        });
    } else {
        console.warn("[Header] Menu button or dropdown not found.");
    }

    const wsUserCountEl = document.getElementById('ws-user-count');
    const activeUserCountEl = document.getElementById('active-user-count');

    if (!wsUserCountEl) {
        console.warn("[Header] Listener count element not found.");
        return;
    }
    initListenerHeartbeat();
    console.log("[Header] Initializing live user counts.");
    const updateHeaderCounts = async () => {
        try {
            await sendListenerHeartbeat(true);
            const res = await fetch('/scanner/api/listeners', { cache: 'no-store' });
            if (res.ok) {
                const data = await res.json();
                const listenerCount = data.connected_users ?? data.active_count ?? 0;
                wsUserCountEl.textContent = `${listenerCount} listener${listenerCount === 1 ? '' : 's'}`;
                if (activeUserCountEl) {
                    activeUserCountEl.textContent = `${data.active_users ?? 0} Logged-in`;
                }
                console.log("[Header] User status updated:", data);
            }
        } catch (e) {
            console.warn('User status update failed:', e);
        }
    };

    updateHeaderCounts();
    setInterval(updateHeaderCounts, REFRESH_INTERVAL_MS);
}


// ==========================================================
// === NOW PLAYING + TOWN GRID (Logic specific to scanner.html) ==
// ==========================================================
let activeTownFilter = 'all';

function applyTownFilter(town) {
    activeTownFilter = town;
    const grid = document.getElementById('call-card-grid');
    if (!grid) return;
    grid.querySelectorAll('a.call-card-link').forEach(card => {
        const cardTown = card.dataset.town || 'unknown';
        card.style.display = (town === 'all' || cardTown === town) ? '' : 'none';
    });
}

function initTownFilterTabs() {
    const tabs = document.querySelectorAll('.town-filter-tab');
    if (!tabs.length) return;

    tabs.forEach(tab => {
        tab.addEventListener('click', () => {
            tabs.forEach(t => {
                t.className = 'town-filter-tab px-3 py-1 rounded-full text-xs font-medium bg-slate-800 text-slate-400 hover:text-slate-200 border border-slate-700 transition';
            });
            tab.className = 'town-filter-tab px-3 py-1 rounded-full text-xs font-medium bg-scannerBlue/20 text-scannerBlue border border-scannerBlue/40 transition';
            applyTownFilter(tab.dataset.town);
        });
    });
}

function initScannerHomepage() {
    const townGridEl = document.getElementById("town-grid");

    if (!townGridEl) return;

    console.log("Initializing scanner homepage elements.");
    initTownFilterTabs();
    loadTownGrid();
    initCallBoard();
    loadHomepageSummary();

    setInterval(loadHomepageSummary, 30000);
}

async function loadHomepageSummary() {
    const totalCallsEl = document.getElementById("total-calls");
    const totalMinutesEl = document.getElementById("active-feeds");
    const listenersEl = document.getElementById("listeners");
    const hooksTodayEl = document.getElementById("hooks-today");

    if (!totalCallsEl || !totalMinutesEl || !listenersEl) return;

    fetchJsonWithCache("/scanner/api/stats", API_CACHE_KEYS.stats, API_CACHE_DEFAULT_TTL, (stats) => {
        totalCallsEl.textContent = stats.total_calls ?? '--';
        totalMinutesEl.textContent = stats.total_minutes ?? '--';
        if (hooksTodayEl) hooksTodayEl.textContent = stats.total_hooks_today ?? '--';
    });

    initListenerHeartbeat();
    fetchJsonWithCache("/scanner/api/listeners", API_CACHE_KEYS.wsUsers, 15000, (wsData) => {
        listenersEl.textContent = wsData.connected_users ?? wsData.active_count ?? 0;
    });
}

const CALL_CARD_LIMIT = 6;

function escapeHTML(value) {
    if (value === undefined || value === null) return '';
    return String(value).replace(/[&<>"']/g, (char) => HTML_ESCAPES[char] || char);
}

function resolveTownFromFeed(feedId) {
    if (!feedId) return 'Unknown';
    return FEED_TOWN_MAP[feedId] || 'Unknown';
}

function deriveDepartmentMeta(feedId) {
    const normalized = (feedId || '').toLowerCase();
    const isFire = normalized.includes('fd');
    return {
        label: isFire ? 'Fire Desk' : 'Police Desk',
        colorClass: isFire ? 'text-red-200' : 'text-blue-200',
        waveformClass: isFire ? 'waveform waveform-fire' : 'waveform waveform-police',
        icon: isFire ? '🚒' : '🚓',
        pillClass: isFire ? 'pill-fire' : 'pill-police',
        dotClass: isFire ? 'dot-fire' : ''
    };
}

function buildCallCardHTML(entry) {
    const deptMeta = deriveDepartmentMeta(entry.feed);
    const townName = resolveTownFromFeed(entry.feed);
    const timestamp = extractTimeFromFile(entry.file) || 'Pending';
    const transcriptSource = entry.transcript || 'Awaiting transcript capture...';
    const clippedTranscript = transcriptSource.length > 250 ? `${transcriptSource.slice(0, 250)}...` : transcriptSource;
    const preview = escapeHTML(clippedTranscript);
    const isFire = deptMeta.waveformClass.includes('fire');
    const audioSrc = entry.file ? `/scanner/audio/${entry.file}` : '';
    const wavePlayerHTML = audioSrc ? `
        <div class="home-wave-player mt-2" data-src="${audioSrc}" data-feed="${entry.feed}">
          <div class="flex items-center gap-2">
            <button class="home-wave-btn w-7 h-7 rounded-full flex items-center justify-center flex-shrink-0 border transition
                ${isFire ? 'bg-red-900/40 border-red-700/50 text-red-300 hover:bg-red-800/50'
                         : 'bg-blue-900/40 border-blue-700/50 text-blue-300 hover:bg-blue-800/50'}">
              <span class="home-wave-icon" style="font-size:9px">▶</span>
            </button>
            <div class="flex-1 home-wave-scrub cursor-pointer">
              <canvas class="home-wave-canvas block w-full rounded" height="28"></canvas>
            </div>
            <span class="home-wave-time text-xs text-slate-600 tabular-nums w-9 text-right shrink-0">${entry.duration > 0 ? _homeFmtTime(entry.duration) : '--:--'}</span>
          </div>
        </div>` : `<div class="call-card-wave ${deptMeta.waveformClass}"></div>`;
        return `
            <a href="/scanner/view?feed=${entry.feed}" class="call-card-link" data-town="${townName.toLowerCase()}">
                <article role="article" data-feed="${entry.feed}" class="call-card-entry panel p-5 flex flex-col">
                    <div class="call-card-meta">
                        <span class="call-card-pill ${deptMeta.pillClass}">
                            <span class="call-card-dot ${deptMeta.dotClass}"></span>
                            ${deptMeta.label}
                        </span>
                        <span class="call-card-time">${timestamp}</span>
                    </div>
                    <h3 class="call-card-title">${townName}</h3>
                    <p class="call-card-transcript">${preview || 'Awaiting transcript capture...'}</p>
                    <div class="call-card-footer">
                        <span>${entry.feed.toUpperCase()}</span>
                        <span class="${deptMeta.colorClass}">${deptMeta.icon}</span>
                    </div>
                    ${wavePlayerHTML}
                </article>
            </a>
        `;
}

async function refreshCallBoard(force = false) {
    const grid = document.getElementById("call-card-grid");
    const placeholder = document.getElementById("call-board-empty");
    if (!grid) return;

    const now = Date.now();
    if (!force && (now - lastCallBoardRefresh) < CALL_BOARD_MIN_REFRESH) return;
    lastCallBoardRefresh = now;

    const defaultPlaceholder = placeholder?.dataset?.defaultText || placeholder?.textContent || "";

    const revealPlaceholder = (message) => {
        if (!placeholder) return;
        placeholder.textContent = message || defaultPlaceholder;
        placeholder.classList.remove('hidden');
        if (!grid.contains(placeholder)) {
            grid.replaceChildren(placeholder);
        }
    };

    const renderEntries = (data) => {
        const entries = (data?.calls || []).slice(0, CALL_CARD_LIMIT);

        if (!entries.length) {
            grid.innerHTML = '';
            revealPlaceholder('Waiting for live transcripts...');
            return;
        }

        grid.innerHTML = '';
        entries.forEach(entry => grid.insertAdjacentHTML('beforeend', buildCallCardHTML(entry)));
        if (activeTownFilter && activeTownFilter !== 'all') applyTownFilter(activeTownFilter);
        initAllHomeWaveformPlayers(grid);
    };

    fetchJsonWithCache("/scanner/api/home_live_calls", API_CACHE_KEYS.homeLiveCalls, 15000, renderEntries);
}

function initCallBoard() {
    refreshCallBoard(true);
    if (callBoardIntervalId) clearInterval(callBoardIntervalId);
    callBoardIntervalId = setInterval(() => refreshCallBoard(), CALL_BOARD_REFRESH_INTERVAL);
}

// --- Helper Functions (No changes) ---
function formatISOTime(isoString) {
    if (!isoString || typeof isoString !== 'string') return '';
    try {
        const d = new Date(isoString);
        if (isNaN(d.getTime())) return '';
        return d.toLocaleTimeString('en-US', { hour: 'numeric', minute: '2-digit' });
    } catch { return ''; }
}
function extractTimeFromFile(file) {
    if (!file || typeof file !== 'string') return '';
    const match = file.match(/rec_(\d{4}-\d{2}-\d{2})_(\d{2})-(\d{2})-(\d{2})_/);
    if (!match) return '';
    const [_, date, hh, mm, ss] = match;
    try {
      const d = new Date(`${date}T${hh}:${mm}:${ss}`);
      if (isNaN(d.getTime())) return ''; 
      return d.toLocaleTimeString('en-US', { hour: 'numeric', minute: '2-digit' });
    } catch { return ''; }
}

async function loadTownGrid() {
    const grid = document.getElementById("town-grid");
    if (!grid) return;

    const towns = [
        { name: "Hopedale", slug: "hopedale", pd: "pd", fd: "fd" },
        { name: "Milford", slug: "milford", pd: "mpd", fd: "mfd" },
        { name: "Bellingham", slug: "bellingham", pd: "bpd", fd: "bfd" },
        { name: "Mendon", slug: "mendon", pd: "mndpd", fd: "mndfd" },
        { name: "Franklin", slug: "franklin",fd: "frkfd" },
        { name: "Blackstone", slug: "blackstone", pd: "blkpd", fd: "blkfd" },
        { name: "Millville", slug: "millville", pd: "mllpd", fd: "mllfd" }
    ];

    const renderGrid = (feedData) => {
        grid.innerHTML = '';

        towns.forEach(t => {
                const pdCount = t.pd ? (feedData[t.pd]?.count || 0) : 0;
                const fdCount = t.fd ? (feedData[t.fd]?.count || 0) : 0;
                const pdIsoTime = feedData[t.pd]?.latest_time || '';
                const fdIsoTime = feedData[t.fd]?.latest_time || '';
                const pdTime = formatISOTime(pdIsoTime);
                const fdTime = formatISOTime(fdIsoTime);
                const pdHooks = t.pd ? (feedData[t.pd]?.hooks_count || 0) : 0;
                const hasPD = !!t.pd;
                const hasFD = !!t.fd;
                const justifyClass = (hasPD && hasFD) ? 'justify-around' : 'justify-center';
                const isHotTown = pdCount + fdCount >= 6;
                const outerClasses = `glass border border-slate-800 rounded-xl p-5 hover:border-slate-600 transition flex flex-col justify-between${isHotTown ? ' town-tab-active' : ''}`;

                const cardHTML = `
                    <div class="${outerClasses}">
                        <h3 class="font-semibold text-lg text-center mb-4">
                            <a href="/scanner/town?town=${t.slug}" class="hover:text-scannerBlue transition">${t.name}</a>
                        </h3>
                        <div class="flex ${justifyClass} items-stretch gap-3">
                            ${hasPD ? `
                            <a href="/scanner/view?feed=${t.pd}"
                                 data-feed="${t.pd}"
                                 class="flex-1 flex flex-col text-center bg-blue-900/30 border border-blue-800/50 rounded-md hover:bg-blue-900/50 transition p-3 max-w-[130px]">
                                <div class="relative font-semibold text-sm text-blue-200 mb-1.5">
                                    <span class="live-indicator"></span>
                                    🚓 Police
                                </div>
                                <div class="flex-1"></div>
                                <div class="text-xs text-slate-400">${pdCount} call${pdCount !== 1 ? 's' : ''}</div>
                                ${pdTime ? `<div class="text-xs text-slate-500 mt-0.5">${pdTime}</div>` : '<div class="text-xs text-slate-600 mt-0.5 italic">No calls</div>'}
                                ${pdHooks > 0 ? `<a href="/scanner/view?feed=${t.pd}&goto=hooks" class="text-xs text-amber-400 mt-0.5 hover:text-amber-300 hover:underline" onclick="event.stopPropagation()">🪝 ${pdHooks} hook${pdHooks !== 1 ? 's' : ''}</a>` : ''}
                            </a>` : ''}
                            ${hasFD ? `
                            <a href="/scanner/view?feed=${t.fd}" 
                                 data-feed="${t.fd}"
                                 class="flex-1 flex flex-col text-center bg-red-900/30 border border-red-800/50 rounded-md hover:bg-red-900/50 transition p-3 max-w-[130px]">
                                <div class="relative font-semibold text-sm text-red-200 mb-1.5">
                                    <span class="live-indicator"></span>
                                    🚒 Fire
                                </div>
                                <div class="flex-1"></div>
                                <div class="text-xs text-slate-400">${fdCount} call${fdCount !== 1 ? 's' : ''}</div>
                                ${fdTime ? `<div class="text-xs text-slate-500 mt-0.5">${fdTime}</div>` : '<div class="text-xs text-slate-600 mt-0.5 italic">No calls</div>'}
                            </a>` : ''}
                        </div>
                    </div>
                `;

                grid.insertAdjacentHTML('beforeend', cardHTML);
        });
    };

    fetchJsonWithCache("/scanner/api/today_counts", API_CACHE_KEYS.todayCounts, API_CACHE_DEFAULT_TTL, (data) => {
        renderGrid(data || {});
    });
}


// ==========================================================
// === HOME PAGE WAVEFORM ENGINE ============================
// ==========================================================
let _homeAudioCtx = null;
const _homeWaveCache = new Map();
let _homeActiveStop = null;

function _homeGetCtx() {
    if (!_homeAudioCtx || _homeAudioCtx.state === 'closed') {
        _homeAudioCtx = new (window.AudioContext || window.webkitAudioContext)();
    }
    return _homeAudioCtx;
}

async function _homeFetchDecode(src) {
    if (_homeWaveCache.has(src)) return _homeWaveCache.get(src);
    const ctx = _homeGetCtx();
    const res = await fetch(src);
    const buf = await res.arrayBuffer();
    const decoded = await ctx.decodeAudioData(buf);
    _homeWaveCache.set(src, decoded);
    return decoded;
}

function _homeFmtTime(s) {
    if (!isFinite(s) || s < 0) return '--:--';
    const m = Math.floor(s / 60);
    const sec = Math.floor(s % 60);
    return `${m}:${sec.toString().padStart(2, '0')}`;
}

function _homeDrawWave(canvas, buffer, progress) {
    const ctx = canvas.getContext('2d');
    const W = canvas.width = canvas.offsetWidth || canvas.parentElement.offsetWidth || 200;
    const H = canvas.height;
    ctx.clearRect(0, 0, W, H);
    const data = buffer.getChannelData(0);
    const step = Math.ceil(data.length / W);
    const mid = H / 2;
    const playX = Math.floor(W * (progress || 0));
    for (let i = 0; i < W; i++) {
        let max = 0;
        for (let j = 0; j < step; j++) {
            const v = Math.abs(data[i * step + j] || 0);
            if (v > max) max = v;
        }
        const barH = Math.max(2, max * H * 0.85);
        ctx.fillStyle = i < playX ? '#38bdf8' : '#334155';
        ctx.fillRect(i, mid - barH / 2, 1, barH);
    }
}

function _homeDrawPlaceholder(canvas, isFire) {
    requestAnimationFrame(() => {
        const ctx = canvas.getContext('2d');
        const W = canvas.width = canvas.offsetWidth || canvas.parentElement.offsetWidth || 200;
        const H = canvas.height;
        ctx.clearRect(0, 0, W, H);
        const mid = H / 2;
        const color = isFire ? '#7f1d1d' : '#1e3a5f';
        ctx.fillStyle = color;
        for (let i = 0; i < W; i += 3) {
            const barH = Math.max(2, Math.random() * H * 0.5);
            ctx.fillRect(i, mid - barH / 2, 2, barH);
        }
    });
}

function initHomeWaveformPlayer(playerEl) {
    const src = playerEl.dataset.src;
    const isFire = (playerEl.dataset.feed || '').toLowerCase().includes('fd');
    if (!src) return;

    const btn = playerEl.querySelector('.home-wave-btn');
    const icon = playerEl.querySelector('.home-wave-icon');
    const scrub = playerEl.querySelector('.home-wave-scrub');
    const canvas = playerEl.querySelector('.home-wave-canvas');
    const timeEl = playerEl.querySelector('.home-wave-time');
    if (!btn || !canvas) return;

    _homeDrawPlaceholder(canvas, isFire);

    // Hidden audio element for playback
    let audioEl = playerEl._homeAudio;
    if (!audioEl) {
        audioEl = new Audio(src);
        audioEl.preload = 'none';
        playerEl._homeAudio = audioEl;
    }

    let buffer = null;
    let rafId = null;
    let isPlaying = false;

    function stopPlayback() {
        if (rafId) { cancelAnimationFrame(rafId); rafId = null; }
        audioEl.pause();
        isPlaying = false;
        icon.textContent = '▶';
        if (buffer) _homeDrawWave(canvas, buffer, audioEl.currentTime / (audioEl.duration || 1));
    }

    function tick() {
        if (!isPlaying) return;
        const progress = audioEl.duration ? audioEl.currentTime / audioEl.duration : 0;
        if (buffer) _homeDrawWave(canvas, buffer, progress);
        if (timeEl) timeEl.textContent = _homeFmtTime(audioEl.currentTime);
        rafId = requestAnimationFrame(tick);
    }

    audioEl.addEventListener('ended', () => {
        isPlaying = false;
        icon.textContent = '▶';
        if (buffer) _homeDrawWave(canvas, buffer, 0);
        if (timeEl) timeEl.textContent = _homeFmtTime(0);
    });

    btn.addEventListener('click', async (e) => {
        e.preventDefault();
        e.stopPropagation();
        if (isPlaying) { stopPlayback(); return; }

        // Stop any other home player
        if (_homeActiveStop) _homeActiveStop();
        _homeActiveStop = stopPlayback;

        try {
            if (!buffer) {
                icon.textContent = '…';
                buffer = await _homeFetchDecode(src);
                _homeDrawWave(canvas, buffer, 0);
                if (timeEl) timeEl.textContent = _homeFmtTime(audioEl.duration || 0);
            }
            if (_homeAudioCtx && _homeAudioCtx.state === 'suspended') await _homeAudioCtx.resume();
            audioEl.currentTime = 0;
            await audioEl.play();
            isPlaying = true;
            icon.textContent = '⏸';
            tick();
        } catch (err) {
            console.warn('[HomeWave] Play error:', err);
            icon.textContent = '▶';
        }
    });

    if (scrub) {
        scrub.addEventListener('click', (e) => {
            e.preventDefault();
            e.stopPropagation();
            const rect = scrub.getBoundingClientRect();
            const ratio = Math.max(0, Math.min(1, (e.clientX - rect.left) / rect.width));
            if (audioEl.duration) {
                audioEl.currentTime = ratio * audioEl.duration;
                if (buffer) _homeDrawWave(canvas, buffer, ratio);
                if (timeEl) timeEl.textContent = _homeFmtTime(audioEl.currentTime);
            }
        });
    }
}

function initAllHomeWaveformPlayers(root) {
    (root || document).querySelectorAll('.home-wave-player').forEach(initHomeWaveformPlayer);
}

// ==========================================================
// === ASK NED CHAT =========================================
// ==========================================================
const ASK_NED_ENDPOINT = '/scanner/api/chat/local';
let askNedMessages = [];
let askNedInitialized = false;

function injectAskNedStyles() {
    if (document.getElementById('ask-ned-styles')) return;
    const style = document.createElement('style');
    style.id = 'ask-ned-styles';
    style.textContent = `
    #ask-ned-overlay {
      color: #e2e8f0;
    }
    #ask-ned-panel {
      background: rgba(15, 23, 42, 0.98);
      border: 1px solid rgba(148, 163, 184, 0.2);
      box-shadow: 0 24px 80px rgba(2, 6, 23, 0.7);
      max-height: min(760px, calc(100vh - 24px));
    }
    .ask-ned-message {
      max-width: 86%;
      border-radius: 14px;
      padding: 0.7rem 0.85rem;
      font-size: 0.9rem;
      line-height: 1.45;
      white-space: pre-wrap;
      overflow-wrap: anywhere;
    }
    .ask-ned-message-user {
      margin-left: auto;
      background: rgba(14, 165, 233, 0.16);
      border: 1px solid rgba(56, 189, 248, 0.24);
      color: #f8fafc;
    }
    .ask-ned-message-assistant {
      margin-right: auto;
      background: rgba(30, 41, 59, 0.72);
      border: 1px solid rgba(148, 163, 184, 0.14);
      color: #e2e8f0;
    }
    .ask-ned-citation {
      display: block;
      margin-top: 0.5rem;
      color: #94a3b8;
      font-size: 0.76rem;
    }
    #ask-ned-input {
      resize: none;
      min-height: 46px;
      max-height: 120px;
    }
    @media (max-width: 640px) {
      #ask-ned-panel {
        width: 100%;
        height: calc(100vh - env(safe-area-inset-top));
        max-height: none;
        border-radius: 18px 18px 0 0;
      }
      .ask-ned-message {
        max-width: 94%;
      }
    }
  `;
    document.head.appendChild(style);
}

function ensureAskNedMarkup() {
    let overlay = document.getElementById('ask-ned-overlay');
    if (overlay) return overlay;

    overlay = document.createElement('div');
    overlay.id = 'ask-ned-overlay';
    overlay.className = 'hidden fixed inset-0 z-[120] flex items-end sm:items-center justify-center';
    overlay.innerHTML = `
      <div id="ask-ned-backdrop" class="absolute inset-0 bg-black/65 backdrop-blur-sm"></div>
      <section id="ask-ned-panel" class="relative flex w-full sm:w-[min(92vw,720px)] flex-col rounded-t-2xl sm:rounded-2xl overflow-hidden">
        <header class="flex items-center justify-between gap-3 border-b border-slate-800 px-4 py-3 shrink-0">
          <div class="min-w-0">
            <h2 class="text-base font-semibold text-white">Ask Ned</h2>
            <p class="text-xs text-slate-400 mt-0.5">Scanner questions answered from the local call database.</p>
          </div>
          <button id="ask-ned-close" type="button" class="h-9 w-9 rounded-md border border-slate-700 text-slate-300 hover:text-white hover:border-slate-500 transition" aria-label="Close Ask Ned">x</button>
        </header>
        <div id="ask-ned-messages" class="flex-1 overflow-y-auto px-4 py-4 space-y-3 bg-slate-950/35">
          <div class="ask-ned-message ask-ned-message-assistant">Ask about recent calls, towns, departments, warnings, citations, fire recalls, coverage, addresses, or call IDs.</div>
        </div>
        <form id="ask-ned-form" class="border-t border-slate-800 p-3 bg-slate-950/70">
          <div class="flex items-end gap-2">
            <textarea id="ask-ned-input" rows="1" class="flex-1 rounded-lg border border-slate-700 bg-slate-900 px-3 py-2 text-sm text-slate-100 placeholder:text-slate-500 focus:border-scannerBlue focus:outline-none" placeholder="Ask about scanner calls..."></textarea>
            <button id="ask-ned-send" type="submit" class="rounded-lg bg-sky-600 px-4 py-2 text-sm font-semibold text-white hover:bg-sky-500 disabled:cursor-not-allowed disabled:bg-slate-700 disabled:text-slate-400 transition">Send</button>
          </div>
          <div id="ask-ned-status" class="mt-2 min-h-[1rem] text-xs text-slate-500"></div>
        </form>
      </section>
    `;
    document.body.appendChild(overlay);
    return overlay;
}

function setAskNedOpen(isOpen) {
    const overlay = ensureAskNedMarkup();
    overlay.classList.toggle('hidden', !isOpen);
    document.body.classList.toggle('scanner-overlay-open', isOpen);
    document.querySelector('.main-content-area')?.classList.toggle('overflow-hidden', isOpen);
    if (isOpen) {
        const input = document.getElementById('ask-ned-input');
        if (input) setTimeout(() => input.focus(), 0);
    }
}

function appendAskNedMessage(role, text, citations) {
    const messagesEl = document.getElementById('ask-ned-messages');
    if (!messagesEl) return;

    const bubble = document.createElement('div');
    bubble.className = `ask-ned-message ${role === 'user' ? 'ask-ned-message-user' : 'ask-ned-message-assistant'}`;
    bubble.textContent = text || '';

    if (role !== 'user' && Array.isArray(citations) && citations.length) {
        const citationLine = document.createElement('span');
        citationLine.className = 'ask-ned-citation';
        citationLine.textContent = citations
            .slice(0, 4)
            .map((citation) => {
                const id = citation.call_id ? `#${citation.call_id}` : 'call';
                return citation.timestamp ? `${id} ${citation.timestamp}` : id;
            })
            .join(' | ');
        bubble.appendChild(citationLine);
    }

    messagesEl.appendChild(bubble);
    messagesEl.scrollTop = messagesEl.scrollHeight;
}

function setAskNedBusy(isBusy, statusText) {
    const sendBtn = document.getElementById('ask-ned-send');
    const input = document.getElementById('ask-ned-input');
    const status = document.getElementById('ask-ned-status');
    if (sendBtn) sendBtn.disabled = isBusy;
    if (input) input.disabled = isBusy;
    if (status) status.textContent = statusText || '';
}

function extractAskNedAnswer(data) {
    if (!data) return 'No response was returned.';
    if (data.answer) return data.answer;
    if (data.error) return data.error;
    if (data.tool_result?.error) return data.tool_result.error;
    if (data.tool_result) return JSON.stringify(data.tool_result, null, 2);
    return 'No answer was returned.';
}

async function submitAskNedQuestion() {
    const input = document.getElementById('ask-ned-input');
    if (!input) return;
    const question = input.value.trim();
    if (!question) return;

    input.value = '';
    appendAskNedMessage('user', question);
    askNedMessages.push({ role: 'user', content: question });
    setAskNedBusy(true, 'Asking Ned...');

    try {
        const res = await fetch(ASK_NED_ENDPOINT, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ messages: askNedMessages.slice(-12) })
        });
        const data = await res.json().catch(() => ({}));
        if (!res.ok || data.ok === false) {
            throw new Error(data.error || `Chat request failed with status ${res.status}`);
        }

        const answer = extractAskNedAnswer(data);
        appendAskNedMessage('assistant', answer, data.citations);
        askNedMessages.push({ role: 'assistant', content: answer });
        setAskNedBusy(false, '');
    } catch (err) {
        console.warn('[AskNed] Request failed:', err);
        const message = err?.message || 'Ask Ned is unavailable right now.';
        appendAskNedMessage('assistant', message);
        setAskNedBusy(false, '');
    }
}

function initAskNedChat() {
    if (askNedInitialized) return;
    askNedInitialized = true;
    injectAskNedStyles();
    ensureAskNedMarkup();

    document.querySelectorAll('[data-ask-ned-open]').forEach((button) => {
        button.addEventListener('click', (event) => {
            event.preventDefault();
            const menuDropdown = document.getElementById('menu-dropdown');
            if (menuDropdown) menuDropdown.classList.add('hidden');
            setAskNedOpen(true);
        });
    });

    document.getElementById('ask-ned-close')?.addEventListener('click', () => setAskNedOpen(false));
    document.getElementById('ask-ned-backdrop')?.addEventListener('click', () => setAskNedOpen(false));
    document.getElementById('ask-ned-form')?.addEventListener('submit', (event) => {
        event.preventDefault();
        submitAskNedQuestion();
    });
    document.getElementById('ask-ned-input')?.addEventListener('keydown', (event) => {
        if (event.key === 'Enter' && !event.shiftKey) {
            event.preventDefault();
            submitAskNedQuestion();
        }
    });
    document.addEventListener('keydown', (event) => {
        if (event.key === 'Escape' && !document.getElementById('ask-ned-overlay')?.classList.contains('hidden')) {
            setAskNedOpen(false);
        }
    });
}

// --- INIT ---
window.addEventListener("load", async () => {
    console.log("Window loaded.");
    try {
        injectSocketStyles();
        initHeader();
        initAskNedChat();
        initSocketIO();
        await checkAuth();
        initScannerHomepage();
    } catch (err) {
        console.error('[Init] Error during startup:', err);
    }
});
