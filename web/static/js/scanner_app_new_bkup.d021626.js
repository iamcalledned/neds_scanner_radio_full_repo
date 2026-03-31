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

const API_CACHE_DEFAULT_TTL = 30000;
const API_CACHE_KEYS = {
    stats: 'scanner_api_stats',
    wsUsers: 'scanner_api_ws_users',
    latest: 'scanner_api_latest',
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

// --- Register the Service Worker ---
if ('serviceWorker' in navigator) {
  window.addEventListener('load', () => {
    navigator.serviceWorker.register('/scanner/sw.js')
      .then(reg => console.log('✅ Service Worker registered for scope:', reg.scope))
      .catch(err => console.error('❌ Service Worker registration failed:', err));
  });
}


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

function updateTransmittingStatus(dept, isTransmitting) {
  if (lastTransmittingState[dept] === isTransmitting) return;
  lastTransmittingState[dept] = isTransmitting;

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

    if (!wsUserCountEl || !activeUserCountEl) {
        console.warn("[Header] User count elements not found.");
        return;
    }
    console.log("[Header] Initializing live user counts.");
    const updateHeaderCounts = async () => {
        try {
            const res = await fetch('/scanner/api/users');
            if (res.ok) {
                const data = await res.json();
                wsUserCountEl.textContent = `${data.connected_users ?? 0} listener${data.connected_users === 1 ? '' : 's'}`;
                activeUserCountEl.textContent = `${data.active_users ?? 0} Logged-in`;
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
function initScannerHomepage() {
    const townGridEl = document.getElementById("town-grid");

    if (!townGridEl) return; 
    
    console.log("Initializing scanner homepage elements.");
    loadTownGrid();
    initCallBoard();
    loadHomepageSummary();
    
    setInterval(loadHomepageSummary, 30000);
}

async function loadHomepageSummary() {
    const totalCallsEl = document.getElementById("total-calls");
    const totalMinutesEl = document.getElementById("active-feeds");
    const listenersEl = document.getElementById("listeners");

    if (!totalCallsEl || !totalMinutesEl || !listenersEl) return;

    fetchJsonWithCache("/scanner/api/stats", API_CACHE_KEYS.stats, API_CACHE_DEFAULT_TTL, (stats) => {
        totalCallsEl.textContent = stats.total_calls ?? '--';
        totalMinutesEl.textContent = stats.total_minutes ?? '--';
    });

    fetchJsonWithCache("/scanner/api/ws_users", API_CACHE_KEYS.wsUsers, API_CACHE_DEFAULT_TTL, (wsData) => {
        listenersEl.textContent = wsData.connected_users ?? 0;
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
        return `
            <a href="/scanner/view?feed=${entry.feed}" class="call-card-link">
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
                    <div class="call-card-wave ${deptMeta.waveformClass}"></div>
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
        const entries = Object.entries(data || {})
            .filter(([_, value]) => value?.file)
            .map(([key, value]) => ({ feed: key, transcript: value?.transcript, file: value.file }))
            .sort((a, b) => b.file.localeCompare(a.file))
            .slice(0, CALL_CARD_LIMIT);

        if (!entries.length) {
            grid.innerHTML = '';
            revealPlaceholder('Waiting for live transcripts...');
            return;
        }

        grid.innerHTML = '';
        entries.forEach(entry => grid.insertAdjacentHTML('beforeend', buildCallCardHTML(entry)));
    };

    fetchJsonWithCache("/scanner/api/latest", API_CACHE_KEYS.latest, 15000, renderEntries);
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
        { name: "Bellingham", slug: "bellingham", pd: "bpd" },
        { name: "Mendon", slug: "mendon", pd: "mndpd", fd: "mndfd" },
        { name: "Franklin", slug: "franklin",fd: "frkfd" }
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


// --- INIT ---
window.addEventListener("load", async () => {
    console.log("Window loaded.");
    injectSocketStyles(); // Inject CSS first
    initHeader();         // Initialize menu and user counts
    initSocketIO();       // Start WebSocket connection
    await checkAuth();    // Run Auth check
    initScannerHomepage(); // Run page-specific logic
});