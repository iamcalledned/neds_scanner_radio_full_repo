// ======================================================
//  Scanner Town Page JS (Unified Theme + Shared Auth)
// ======================================================

document.addEventListener('DOMContentLoaded', () => {

  // --- Town Config ---------------------------------------------------------
  const townConfig = {
    hopedale: {
      title: "Hopedale",
      feeds: [
        { id: 'pd', emoji: '🚓', title: 'Hopedale Police', desc: 'See what the local boys in blue are up to', color: 'blue' },
        { id: 'fd', emoji: '🚒', title: 'Hopedale Fire', desc: 'The only fires we deal with are from the grill', color: 'red' }
      ]
    },
    milford: {
      title: "Milford",
      feeds: [
        { id: 'mpd', emoji: '🚓', title: 'Milford Police', desc: 'Wait, you have a license?', color: 'blue' },
        { id: 'mfd', emoji: '🚒', title: 'Milford Fire', desc: 'Hola, donde esta el fuego?', color: 'red' }
      ]
    },
    bellingham: {
      title: "Bellingham",
      feeds: [
        { id: 'bpd', emoji: '🚓', title: 'Bellingham Police', desc: 'We could have nice things', color: 'blue' },
        { id: 'bfd', emoji: '🚒', title: 'Bellingham Fire', desc: 'We could have nice things', color: 'red' }
      ]
    },
    mendon: {
      title: "Mendon",
      feeds: [
        { id: 'mndpd', emoji: '🚓', title: 'Mendon Police', desc: 'We have nice things too', color: 'blue' },
        { id: 'mndfd', emoji: '🚒', title: 'Mendon Fire', desc: 'We have nice things too', color: 'red' }
      ]
    },
    blackstone: {
      title: "Blackstone",
      feeds: [
        { id: 'blkpd', emoji: '🚓', title: 'Blackstone Police', desc: 'Not much action', color: 'blue' },
        { id: 'blkfd', emoji: '🚒', title: 'Blackstone Fire', desc: 'Not much action', color: 'red' }
      ]
    },
    upton: {
      title: "Upton",
      feeds: [
        { id: 'uptpd', emoji: '🚓', title: 'Upton Police', desc: 'Small town, small problems', color: 'blue' },
        { id: 'uptfd', emoji: '🚒', title: 'Upton Fire', desc: 'Small town, small problems', color: 'red' }
      ]
    }
    // Add other towns like franklin here if needed
  };

  let feeds = [];
  const lastTimestamps = {};

  // --- Helpers -------------------------------------------------------------
  function extractTimestamp(filename) {
    if (!filename) return null; // This check is good
    // This .match() call is what fails if filename is not a string
    const m = filename.match(/rec_(\d{4}-\d{2}-\d{2})_(\d{2}[-:]\d{2}[-:]\d{2})/);
    if (!m) return null;
    const iso = `${m[1]}T${m[2].replace(/-/g, ':')}`;
    return new Date(iso).getTime();
  }

  function formatTimeAgo(ts) {
    if (!ts) return "—";
    const diff = Math.floor((Date.now() - ts) / 1000);
    if (diff < 60) return `${diff}s ago`;
    const mins = Math.floor(diff / 60);
    if (mins < 60) return `${mins}m ${diff % 60}s ago`;
    const hrs = Math.floor(mins / 60);
    return `${hrs}h ${mins % 60}m ago`;
  }

  function formatDate(ts) {
    if (!ts) return "—";
    return new Date(ts).toLocaleString([], { dateStyle: "medium", timeStyle: "short" });
  }

  // --- Page Builder --------------------------------------------------------
  function buildTownPage() {
    const params = new URLSearchParams(window.location.search);
    const town = params.get('town');
    const config = townConfig[town];
    const grid = document.getElementById('feed-grid');
    const titleEl = document.getElementById('town-title');

    if (!config) {
      titleEl.textContent = 'Error';
      grid.innerHTML = `<p class="text-center text-red-400 col-span-full">Town not found. <a href="/scanner" class="underline">Go back</a>.</p>`;
      return;
    }

    document.title = `${config.title} Scanner Archive`;
    titleEl.textContent = `📻 ${config.title} Scanner`;
    feeds = config.feeds.map(f => f.id);

    grid.innerHTML = '';
    for (const feed of config.feeds) {
      const isFire = feed.color === 'red';
      const titleColor = isFire ? 'text-red-300' : 'text-blue-300';
      const ringBase = isFire ? 'ring-red-400/20' : 'ring-blue-400/20';

      const card = `
        <div class="feed-card relative flex flex-col rounded-2xl overflow-hidden transition-all duration-500
            ring-1 ${ringBase} bg-slate-900/60 backdrop-blur-sm shadow-lg"
            data-dept="${feed.id}">

          <!-- On Air badge (hidden until transmitting) -->
          <div class="on-air-badge hidden absolute top-3 right-3 items-center gap-1.5 px-2.5 py-1 rounded-full
              bg-red-500/20 border border-red-500/40 text-red-400 text-[10px] font-bold tracking-widest">
            <span class="w-1.5 h-1.5 rounded-full bg-red-400 animate-ping inline-block"></span>
            ON AIR
          </div>

          <!-- Clickable card header -->
          <a href="/scanner/view?feed=${feed.id}" class="block p-6 hover:bg-white/[0.03] transition-colors">
            <div class="flex items-start gap-4 mb-4">
              <span class="text-5xl leading-none">${feed.emoji}</span>
              <div class="flex-1 min-w-0">
                <h2 class="text-xl font-bold ${titleColor} flex items-center gap-2">
                  <span class="live-indicator" data-dept="${feed.id}"></span>
                  ${feed.title}
                </h2>
                <p class="text-slate-500 text-xs mt-1">${feed.desc}</p>
              </div>
            </div>
            <div class="flex items-baseline gap-2 text-sm">
              <span class="font-bold text-scannerBlue text-xl" id="${feed.id}-total-calls">--</span>
              <span class="text-slate-500 text-xs">calls today</span>
              <span class="text-slate-600 text-xs ml-auto" id="${feed.id}-last-time">--</span>
            </div>
          </a>

          <!-- Recent calls preview -->
          <div class="border-t border-slate-800 px-5 pb-4 pt-3 space-y-2.5 flex-1" id="${feed.id}-recent">
            <p class="text-xs text-slate-600 italic">Loading recent calls…</p>
          </div>
        </div>`;
      grid.insertAdjacentHTML('beforeend', card);
    }
  }

  // --- Data Loader ---------------------------------------------------------
  async function loadFeedData() {
    if (feeds.length === 0) return;
    try {
      const [r1, r2] = await Promise.all([
        fetch("/scanner/api/latest"),
        fetch("/scanner/api/today_counts"),
      ]);
      const latest = await r1.json();
      const counts = await r2.json();

      let townTotal = 0;
      let latestTs = null;
      let mostActiveLabel = '—';
      let mostActiveCount = -1;

      for (const id of feeds) {
        const latestData = latest[id];
        const file = latestData ? latestData.file : null;
        const ts = extractTimestamp(file);
        lastTimestamps[id] = ts;

        // counts[id] may be a number or {count, latest_time} object
        const raw = counts[id];
        const count = (raw && typeof raw === 'object') ? (raw.count ?? 0) : (raw ?? 0);
        townTotal += count;

        if (count > mostActiveCount) {
          mostActiveCount = count;
          mostActiveLabel = id.toLowerCase().includes('fd') ? '🔴 Fire' : '🔵 Police';
        }
        if (ts && (!latestTs || ts > latestTs)) latestTs = ts;

        const timeEl = document.getElementById(`${id}-last-time`);
        const totalEl = document.getElementById(`${id}-total-calls`);
        if (totalEl) totalEl.textContent = count;
        if (timeEl) timeEl.textContent = ts ? formatTimeAgo(ts) : '—';

        loadRecentCalls(id);
      }

      // Update town stat bar
      const statToday = document.getElementById('town-stat-today');
      const statLastActive = document.getElementById('town-stat-last-active');
      const statMostActive = document.getElementById('town-stat-most-active');
      if (statToday) statToday.textContent = townTotal;
      if (statLastActive) statLastActive.textContent = latestTs ? formatTimeAgo(latestTs) : '—';
      if (statMostActive) statMostActive.textContent = townTotal > 0 ? mostActiveLabel : '—';

    } catch (e) {
      console.error("[Town] Failed to load feed data:", e);
    }
  }

  // --- Recent Calls Preview -----------------------------------------------
  const HTML_ESC = {'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'};
  function escHtml(str) { return String(str).replace(/[&<>"']/g, c => HTML_ESC[c]); }

  async function loadRecentCalls(feedId) {
    const container = document.getElementById(`${feedId}-recent`);
    if (!container) return;
    const isFire = feedId.toLowerCase().includes('fd');

    try {
      const res = await fetch(`/scanner/api/archive_calls?feed=${feedId}&offset=0&limit=2`);
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      const data = await res.json();
      const calls = data.calls || [];

      if (!calls.length) {
        container.innerHTML = '<p class="text-xs text-slate-600 italic">No recent calls</p>';
        return;
      }

      container.innerHTML = calls.map((call, i) => {
        const ts = extractTimestamp(call.file);
        const timeAgo = ts ? formatTimeAgo(ts) : '—';
        const raw = call.transcript || '';
        const snippet = raw.length > 100 ? raw.slice(0, 100) + '…' : raw || 'No transcript';
        const audioPath = call.path || '';
        return `${i > 0 ? '<div class="border-t border-slate-800/50 my-1"></div>' : ''}
          <div>
            <p class="text-[10px] text-slate-600 uppercase tracking-wide mb-0.5">${timeAgo}</p>
            <p class="text-xs text-slate-400 leading-relaxed mb-2">${escHtml(snippet)}</p>
            ${audioPath ? `
            <div class="relative group/wave cursor-pointer" data-audio="${audioPath}" data-wave-index="${i}">
              <canvas class="wave-canvas block w-full rounded" height="36"
                style="image-rendering:pixelated"></canvas>
              <div class="wave-hint absolute inset-0 flex items-center justify-center
                opacity-0 group-hover/wave:opacity-100 transition-opacity pointer-events-none">
                <span class="text-[10px] text-white/50 uppercase tracking-widest">▶ tap to play</span>
              </div>
            </div>` : ''}
          </div>`;
      }).join('');

      // Wire up each canvas after HTML is in the DOM
      calls.forEach((call, i) => {
        if (!call.path) return;
        const wrapper = container.querySelector(`[data-wave-index="${i}"]`);
        if (!wrapper) return;
        const canvas = wrapper.querySelector('.wave-canvas');
        if (!canvas) return;
        // Draw placeholder immediately
        requestAnimationFrame(() => drawPlaceholderWave(canvas, isFire));
        initWaveformPlayer(wrapper, canvas, call.path, isFire);
      });

    } catch (e) {
      container.innerHTML = '<p class="text-xs text-slate-600 italic">—</p>';
    }
  }

  // --- Waveform Engine ---------------------------------------------------

  let _audioCtx = null;
  function getAudioCtx() {
    if (!_audioCtx || _audioCtx.state === 'closed') {
      _audioCtx = new (window.AudioContext || window.webkitAudioContext)();
    }
    return _audioCtx;
  }

  const _waveCache = {};
  async function fetchAndDecode(url) {
    if (_waveCache[url]) return _waveCache[url];
    const res = await fetch(url);
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    const buf = await res.arrayBuffer();
    const actx = getAudioCtx();
    const decoded = await actx.decodeAudioData(buf);
    _waveCache[url] = decoded;
    return decoded;
  }

  function drawPlaceholderWave(canvas, isFire) {
    const W = canvas.parentElement?.offsetWidth || 280;
    canvas.width = W;
    const H = canvas.height;
    const ctx = canvas.getContext('2d');
    ctx.clearRect(0, 0, W, H);
    const color = isFire ? 'rgba(248,113,113,0.18)' : 'rgba(56,189,248,0.18)';
    ctx.fillStyle = color;
    const bars = Math.floor(W / 4);
    for (let i = 0; i < bars; i++) {
      // Sine-wave pattern so it looks like a real (quiet) waveform
      const h = Math.max(2, Math.abs(Math.sin(i * 0.45) * H * 0.45 + Math.sin(i * 0.8) * H * 0.15) + 3);
      ctx.fillRect(i * 4, (H - h) / 2, 3, h);
    }
  }

  function drawRealWave(canvas, audioBuffer, isFire, progress) {
    const W = canvas.parentElement?.offsetWidth || canvas.width || 280;
    canvas.width = W;
    const H = canvas.height;
    const ctx = canvas.getContext('2d');
    ctx.clearRect(0, 0, W, H);

    const data = audioBuffer.getChannelData(0);
    const barW = 3;
    const gap = 1;
    const step = barW + gap;
    const bars = Math.floor(W / step);
    const blockSize = Math.floor(data.length / bars);
    const playedX = progress * W;
    const litColor  = isFire ? 'rgba(248,113,113,0.9)'  : 'rgba(56,189,248,0.9)';
    const dimColor  = isFire ? 'rgba(248,113,113,0.25)' : 'rgba(56,189,248,0.25)';

    for (let i = 0; i < bars; i++) {
      let sum = 0;
      const start = i * blockSize;
      for (let j = 0; j < blockSize; j++) sum += Math.abs(data[start + j] || 0);
      const rms = sum / blockSize;
      const barH = Math.max(2, rms * H * 6);
      const x = i * step;
      ctx.fillStyle = x <= playedX ? litColor : dimColor;
      ctx.fillRect(x, (H - barH) / 2, barW, barH);
    }

    // Playhead line
    if (progress > 0 && progress < 1) {
      ctx.fillStyle = 'rgba(255,255,255,0.75)';
      ctx.fillRect(Math.floor(playedX), 0, 1, H);
    }
  }

  // Track the currently active player so we can stop it when another starts
  let _activePlayer = null;

  function initWaveformPlayer(wrapper, canvas, audioUrl, isFire) {
    let audioEl = null;
    let decoded = null;
    let animId = null;
    let loading = false;

    function stopAnim() {
      if (animId) { cancelAnimationFrame(animId); animId = null; }
    }

    function tick() {
      if (!audioEl || audioEl.paused) { stopAnim(); return; }
      drawRealWave(canvas, decoded, isFire, audioEl.currentTime / (audioEl.duration || 1));
      animId = requestAnimationFrame(tick);
    }

    function stopThis() {
      if (audioEl && !audioEl.paused) audioEl.pause();
      stopAnim();
      if (decoded) drawRealWave(canvas, decoded, isFire,
        audioEl ? audioEl.currentTime / (audioEl.duration || 1) : 0);
    }

    wrapper.addEventListener('click', async () => {
      // Resume AudioContext (required after user gesture)
      const actx = getAudioCtx();
      if (actx.state === 'suspended') await actx.resume();

      // Pause if already playing
      if (audioEl && !audioEl.paused) { stopThis(); return; }

      // Stop whatever else is playing
      if (_activePlayer && _activePlayer !== stopThis) _activePlayer();
      _activePlayer = stopThis;

      // Decode audio first time
      if (!decoded) {
        if (loading) return;
        loading = true;
        const hint = wrapper.querySelector('.wave-hint');
        if (hint) hint.querySelector('span').textContent = '⟳ loading…';
        try {
          decoded = await fetchAndDecode(audioUrl);
          drawRealWave(canvas, decoded, isFire, 0);
          if (hint) hint.style.display = 'none';
        } catch (err) {
          console.error('[Wave] decode failed:', err);
          if (hint) hint.querySelector('span').textContent = '✕ error';
          loading = false;
          return;
        }
        loading = false;
      }

      // Create audio element once
      if (!audioEl) {
        audioEl = new Audio(audioUrl);
        audioEl.addEventListener('ended', () => {
          stopAnim();
          drawRealWave(canvas, decoded, isFire, 1);
          _activePlayer = null;
        });
      }

      audioEl.play().then(() => { animId = requestAnimationFrame(tick); });
    });
  }

  // --- NEW: WebSocket Logic -------------------------------------------------

  /**
   * Connects to the Socket.IO server and sets up event listeners.
   */
// --- NEW: WebSocket Logic -------------------------------------------------
function initSocketIO() {
  const serverURL = window.location.origin.includes('iamcalledned.ai')
    ? "https://iamcalledned.ai"
    : "http://localhost:5005";

  const socket = io(serverURL, { transports: ['websocket', 'polling'] });

  socket.on('connect', () => console.log('[Town] Socket connected.'));
  socket.on('disconnect', () => console.log('[Town] Socket disconnected.'));
  socket.on('connect_error', (err) => console.error('[Town] Socket error:', err.message));

  socket.on('transmitting_update', (msg) => {
    // Support both possible payload types
    if (!msg) return;

    if (msg.dept && msg.status) {
      // Single dept form: { dept: "fd", status: "Y" }
      console.log(`[Town] Received status for ${msg.dept}: ${msg.status}`);
      updateTransmittingStatus(msg.dept, msg.status === 'Y');
    } else if (typeof msg === 'object') {
      // Multi-feed form: { pd: "Y", fd: "N", mfd: "Y" }
      Object.entries(msg).forEach(([dept, status]) => {
        console.log(`[Town] ${dept} → ${status}`);
        updateTransmittingStatus(dept, status === 'Y');
      });
    }
  });
}

// --- Transmitting status + On Air card state ------------------------------
const lastTransmittingState = {};

function updateTransmittingStatus(dept, isTransmitting) {
  if (lastTransmittingState[dept] === isTransmitting) return;
  lastTransmittingState[dept] = isTransmitting;

  // Live dot
  const indicator = document.querySelector(`.live-indicator[data-dept="${dept}"]`);
  if (indicator) indicator.classList.toggle('live', isTransmitting);

  // On Air card state
  const card = document.querySelector(`.feed-card[data-dept="${dept}"]`);
  if (!card) return;

  const badge = card.querySelector('.on-air-badge');
  if (badge) {
    badge.classList.toggle('hidden', !isTransmitting);
    badge.classList.toggle('flex', isTransmitting);
  }

  const isFire = dept.toLowerCase().includes('fd');
  if (isTransmitting) {
    card.classList.remove('ring-1', isFire ? 'ring-red-400/20' : 'ring-blue-400/20');
    card.classList.add('ring-2', isFire ? 'ring-red-500/60' : 'ring-blue-500/60',
                       'shadow-xl', isFire ? 'shadow-red-500/20' : 'shadow-blue-500/20');
  } else {
    card.classList.remove('ring-2', isFire ? 'ring-red-500/60' : 'ring-blue-500/60',
                          'shadow-xl', isFire ? 'shadow-red-500/20' : 'shadow-blue-500/20');
    card.classList.add('ring-1', isFire ? 'ring-red-400/20' : 'ring-blue-400/20');
  }

  console.log(`[Town] ${dept} → ${isTransmitting ? 'ON AIR 🔴' : 'off ⚫'}`);
}

  // --- Update loop for "time ago" and town stat ----------------------------
  setInterval(() => {
    let latestTs = null;
    for (const id of feeds) {
      const ts = lastTimestamps[id];
      const el = document.getElementById(`${id}-last-time`);
      if (el && ts) el.textContent = formatTimeAgo(ts);
      if (ts && (!latestTs || ts > latestTs)) latestTs = ts;
    }
    const statLastActive = document.getElementById('town-stat-last-active');
    if (statLastActive && latestTs) statLastActive.textContent = formatTimeAgo(latestTs);
  }, 1000);

  // --- Init ---------------------------------------------------------------
  buildTownPage();
  loadFeedData();
  setInterval(loadFeedData, 30000);
  initSocketIO(); // <-- NEW: Connect to the websocket
  
  if (typeof checkAuth === 'function') {
    checkAuth();
  } else {
    console.warn('checkAuth() function not found. Is scanner_app_new.js loaded?');
  }
});

