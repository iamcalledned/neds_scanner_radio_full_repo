// ======================================================
//  Scanner Town Page JS (Redesigned – Matches Home Feel)
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
  };

  let feeds = [];
  const lastTimestamps = {};

  // --- Helpers -------------------------------------------------------------
  const HTML_ESC = {'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'};
  function esc(str) { return String(str).replace(/[&<>"']/g, c => HTML_ESC[c]); }

  function extractTimestamp(filename) {
    if (!filename) return null;
    const m = filename.match(/rec_(\d{4}-\d{2}-\d{2})_(\d{2}[-:]\d{2}[-:]\d{2})/);
    if (!m) return null;
    const iso = m[1] + 'T' + m[2].replace(/-/g, ':');
    return new Date(iso).getTime();
  }

  function extractTimeFromFile(file) {
    if (!file || typeof file !== 'string') return '';
    const match = file.match(/rec_(\d{4}-\d{2}-\d{2})_(\d{2})-(\d{2})-(\d{2})_/);
    if (!match) return '';
    try {
      const d = new Date(match[1] + 'T' + match[2] + ':' + match[3] + ':' + match[4]);
      if (isNaN(d.getTime())) return '';
      return d.toLocaleTimeString('en-US', { hour: 'numeric', minute: '2-digit' });
    } catch { return ''; }
  }

  function formatTimeAgo(ts) {
    if (!ts) return "\u2014";
    const diff = Math.floor((Date.now() - ts) / 1000);
    if (diff < 60) return diff + 's ago';
    const mins = Math.floor(diff / 60);
    if (mins < 60) return mins + 'm ' + (diff % 60) + 's ago';
    const hrs = Math.floor(mins / 60);
    return hrs + 'h ' + (mins % 60) + 'm ago';
  }

  // --- Feed metadata helpers -----------------------------------------------
  const FEED_TOWN_MAP = {
    pd: "Hopedale", fd: "Hopedale",
    mpd: "Milford", mfd: "Milford",
    bpd: "Bellingham", bfd: "Bellingham",
    mndpd: "Mendon", mndfd: "Mendon",
    uptpd: "Upton", uptfd: "Upton",
    blkpd: "Blackstone", blkfd: "Blackstone",
    frkpd: "Franklin", frkfd: "Franklin"
  };

  function deriveDeptMeta(feedId) {
    const isFire = (feedId || '').toLowerCase().includes('fd');
    return {
      label: isFire ? 'Fire Desk' : 'Police Desk',
      pillClass: isFire ? 'pill-fire' : 'pill-police',
      dotClass: isFire ? 'dot-fire' : '',
      colorClass: isFire ? 'text-red-200' : 'text-blue-200',
      icon: isFire ? '🚒' : '🚓',
      waveformClass: isFire ? 'waveform waveform-fire' : 'waveform waveform-police',
      isFire: isFire
    };
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
      grid.innerHTML = '<p class="text-center text-red-400 col-span-full">Town not found. <a href="/scanner" class="underline">Go back</a>.</p>';
      return;
    }

    document.title = config.title + ' Scanner \u2013 Ned\'s Scanner Network';
    titleEl.textContent = '\ud83d\udcfb ' + config.title;
    feeds = config.feeds.map(function(f) { return f.id; });

    grid.innerHTML = '';
    for (var fi = 0; fi < config.feeds.length; fi++) {
      var feed = config.feeds[fi];
      var isFire = feed.color === 'red';
      var titleColor = isFire ? 'text-red-300' : 'text-blue-300';

      var card = '<div class="feed-card panel relative flex flex-col overflow-hidden transition-all duration-500"' +
          ' data-dept="' + feed.id + '">' +
          '<div class="on-air-badge hidden absolute top-3 right-3 items-center gap-1.5 px-2.5 py-1 rounded-full' +
          ' bg-red-500/20 border border-red-500/40 text-red-400 text-[10px] font-bold tracking-widest z-10">' +
          '<span class="w-1.5 h-1.5 rounded-full bg-red-400 animate-ping inline-block"></span>' +
          ' ON AIR</div>' +
          '<a href="/scanner/view?feed=' + feed.id + '" class="block p-6 hover:bg-white/[0.03] transition-colors">' +
          '<div class="flex items-start gap-4 mb-4">' +
          '<span class="text-5xl leading-none">' + feed.emoji + '</span>' +
          '<div class="flex-1 min-w-0">' +
          '<h2 class="text-xl font-bold ' + titleColor + ' flex items-center gap-2">' +
          '<span class="live-indicator" data-dept="' + feed.id + '"></span>' +
          feed.title + '</h2>' +
          '<p class="text-slate-500 text-xs mt-1">' + feed.desc + '</p></div></div>' +
          '<div class="flex items-baseline gap-2 text-sm">' +
          '<span class="font-extrabold text-scannerBlue text-2xl" id="' + feed.id + '-total-calls">--</span>' +
          '<span class="text-slate-500 text-xs uppercase tracking-wider">calls today</span>' +
          '<span class="text-slate-600 text-xs ml-auto" id="' + feed.id + '-last-time">--</span>' +
          '</div></a>' +
          '<div class="border-t border-slate-800/60 px-5 pb-4 pt-3 space-y-2.5 flex-1" id="' + feed.id + '-recent">' +
          '<p class="text-xs text-slate-600 italic">Loading recent calls\u2026</p></div></div>';
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
      let townHooks = 0;
      let latestTs = null;
      let mostActiveLabel = '\u2014';
      let mostActiveCount = -1;

      for (var i = 0; i < feeds.length; i++) {
        var id = feeds[i];
        var latestData = latest[id];
        var file = latestData ? latestData.file : null;
        var ts = extractTimestamp(file);
        lastTimestamps[id] = ts;

        var raw = counts[id];
        var count = (raw && typeof raw === 'object') ? (raw.count || 0) : (raw || 0);
        var hooks = (raw && typeof raw === 'object') ? (raw.hooks_count || 0) : 0;
        townTotal += count;
        townHooks += hooks;

        if (count > mostActiveCount) {
          mostActiveCount = count;
          mostActiveLabel = id.toLowerCase().includes('fd') ? '\ud83d\udd34 Fire' : '\ud83d\udd35 Police';
        }
        if (ts && (!latestTs || ts > latestTs)) latestTs = ts;

        var timeEl = document.getElementById(id + '-last-time');
        var totalEl = document.getElementById(id + '-total-calls');
        if (totalEl) totalEl.textContent = count;
        if (timeEl) timeEl.textContent = ts ? formatTimeAgo(ts) : '\u2014';

        loadRecentCalls(id);
      }

      var statToday = document.getElementById('town-stat-today');
      var statHooks = document.getElementById('town-stat-hooks');
      var statLastActive = document.getElementById('town-stat-last-active');
      var statMostActive = document.getElementById('town-stat-most-active');
      if (statToday) statToday.textContent = townTotal;
      if (statHooks) statHooks.textContent = townHooks;
      if (statLastActive) statLastActive.textContent = latestTs ? formatTimeAgo(latestTs) : '\u2014';
      if (statMostActive) statMostActive.textContent = townTotal > 0 ? mostActiveLabel : '\u2014';

      loadRecentCallCards();

    } catch (e) {
      console.error("[Town] Failed to load feed data:", e);
    }
  }

  // --- Recent Calls Preview (inside feed cards) ----------------------------
  async function loadRecentCalls(feedId) {
    var container = document.getElementById(feedId + '-recent');
    if (!container) return;
    var isFire = feedId.toLowerCase().includes('fd');

    try {
      var res = await fetch('/scanner/api/archive_calls?feed=' + feedId + '&offset=0&limit=2');
      if (!res.ok) throw new Error('HTTP ' + res.status);
      var data = await res.json();
      var calls = data.calls || [];

      if (!calls.length) {
        container.innerHTML = '<p class="text-xs text-slate-600 italic">No recent calls</p>';
        return;
      }

      container.innerHTML = calls.map(function(call, i) {
        var ts = extractTimestamp(call.file);
        var timeAgo = ts ? formatTimeAgo(ts) : '\u2014';
        var rawText = call.transcript || '';
        var snippet = rawText.length > 100 ? rawText.slice(0, 100) + '\u2026' : rawText || 'No transcript';
        var audioPath = call.path || '';

        var meta = call.metadata || {};
        var addr = meta.derived_full_address || '';
        var addrHtml = addr ? '<div class="flex items-center gap-1.5 mt-1">' +
          '<svg xmlns="http://www.w3.org/2000/svg" class="w-3 h-3 text-emerald-400 shrink-0" fill="none" viewBox="0 0 24 24" stroke="currentColor" stroke-width="2">' +
          '<path stroke-linecap="round" stroke-linejoin="round" d="M17.657 16.657L13.414 20.9a1.998 1.998 0 01-2.827 0l-4.244-4.243a8 8 0 1111.314 0z" />' +
          '<path stroke-linecap="round" stroke-linejoin="round" d="M15 11a3 3 0 11-6 0 3 3 0 016 0z" />' +
          '</svg><span class="text-[10px] text-emerald-400/80 truncate">' + esc(addr) + '</span></div>' : '';

        return (i > 0 ? '<div class="border-t border-slate-800/50 my-1"></div>' : '') +
          '<div>' +
          '<p class="text-[10px] text-slate-600 uppercase tracking-wide mb-0.5">' + timeAgo + '</p>' +
          '<p class="text-xs text-slate-400 leading-relaxed">' + esc(snippet) + '</p>' +
          addrHtml +
          (audioPath ? '<div class="relative group/wave cursor-pointer mt-2" data-audio="' + audioPath + '" data-wave-index="' + i + '">' +
            '<canvas class="wave-canvas block w-full rounded" height="36" style="image-rendering:pixelated"></canvas>' +
            '<div class="wave-hint absolute inset-0 flex items-center justify-center opacity-0 group-hover/wave:opacity-100 transition-opacity pointer-events-none">' +
            '<span class="text-[10px] text-white/50 uppercase tracking-widest">\u25b6 tap to play</span></div></div>' : '') +
          '</div>';
      }).join('');

      calls.forEach(function(call, i) {
        if (!call.path) return;
        var wrapper = container.querySelector('[data-wave-index="' + i + '"]');
        if (!wrapper) return;
        var canvas = wrapper.querySelector('.wave-canvas');
        if (!canvas) return;
        requestAnimationFrame(function() { drawPlaceholderWave(canvas, isFire); });
        initWaveformPlayer(wrapper, canvas, call.path, isFire);
      });

    } catch (e) {
      container.innerHTML = '<p class="text-xs text-slate-600 italic">\u2014</p>';
    }
  }

  // --- Recent Call Cards section (full-width below feeds) ------------------
  async function loadRecentCallCards() {
    var grid = document.getElementById('recent-call-grid');
    var empty = document.getElementById('recent-calls-empty');
    if (!grid) return;

    try {
      var allCalls = [];
      var promises = feeds.map(async function(feedId) {
        try {
          var res = await fetch('/scanner/api/archive_calls?feed=' + feedId + '&offset=0&limit=3');
          if (!res.ok) return;
          var data = await res.json();
          (data.calls || []).forEach(function(c) {
            c._feedId = feedId;
            allCalls.push(c);
          });
        } catch (e) { /* skip */ }
      });
      await Promise.all(promises);

      allCalls.sort(function(a, b) { return (b.file || '').localeCompare(a.file || ''); });
      var top = allCalls.slice(0, 6);

      if (!top.length) {
        if (empty) empty.textContent = 'No calls yet today';
        return;
      }

      grid.innerHTML = '';
      top.forEach(function(call, idx) {
        var feedId = call._feedId || call.feed || '';
        var dept = deriveDeptMeta(feedId);
        var timestamp = extractTimeFromFile(call.file) || 'Pending';
        var rawText = call.transcript || 'Awaiting transcript\u2026';
        var preview = rawText.length > 200 ? rawText.slice(0, 200) + '\u2026' : rawText;
        var meta = call.metadata || {};
        var addr = meta.derived_full_address || '';
        var audioSrc = call.path || '';

        var addrHtml = addr ? '<div class="call-card-address">' +
          '<svg xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" stroke="currentColor" stroke-width="2">' +
          '<path stroke-linecap="round" stroke-linejoin="round" d="M17.657 16.657L13.414 20.9a1.998 1.998 0 01-2.827 0l-4.244-4.243a8 8 0 1111.314 0z" />' +
          '<path stroke-linecap="round" stroke-linejoin="round" d="M15 11a3 3 0 11-6 0 3 3 0 016 0z" />' +
          '</svg><span class="truncate">' + esc(addr) + '</span></div>' : '';

        var waveHtml = audioSrc ?
          '<div class="town-card-player mt-2" data-src="' + audioSrc + '" data-feed="' + feedId + '">' +
          '<div class="flex items-center gap-2">' +
          '<button class="town-card-btn w-7 h-7 rounded-full flex items-center justify-center flex-shrink-0 border transition ' +
            (dept.isFire ? 'bg-red-900/40 border-red-700/50 text-red-300 hover:bg-red-800/50' :
                           'bg-blue-900/40 border-blue-700/50 text-blue-300 hover:bg-blue-800/50') + '">' +
          '<span class="town-card-icon" style="font-size:9px">\u25b6</span></button>' +
          '<div class="flex-1 town-card-scrub cursor-pointer">' +
          '<canvas class="town-card-canvas block w-full rounded" height="28"></canvas></div>' +
          '<span class="town-card-time text-xs text-slate-600 tabular-nums w-9 text-right shrink-0">--:--</span>' +
          '</div></div>'
          : '<div class="call-card-wave ' + dept.waveformClass + '"></div>';

        var cardHtml = '<a href="/scanner/view?feed=' + feedId + '" class="call-card-link" style="animation-delay: ' + (idx * 0.07) + 's">' +
          '<article class="call-card-entry panel p-5 flex flex-col">' +
          '<div class="call-card-meta">' +
          '<span class="call-card-pill ' + dept.pillClass + '">' +
          '<span class="call-card-dot ' + dept.dotClass + '"></span> ' +
          dept.label + '</span>' +
          '<span class="call-card-time">' + timestamp + '</span></div>' +
          '<h3 class="call-card-title">' + esc(FEED_TOWN_MAP[feedId] || feedId.toUpperCase()) + '</h3>' +
          '<p class="call-card-transcript">' + esc(preview) + '</p>' +
          addrHtml +
          '<div class="call-card-footer"><span>' + feedId.toUpperCase() + '</span>' +
          '<span class="' + dept.colorClass + '">' + dept.icon + '</span></div>' +
          waveHtml +
          '</article></a>';

        grid.insertAdjacentHTML('beforeend', cardHtml);
      });

      initAllTownCardPlayers(grid);

    } catch (e) {
      console.error('[Town] loadRecentCallCards error:', e);
    }
  }

  // --- Waveform Engine -----------------------------------------------------

  var _audioCtx = null;
  function getAudioCtx() {
    if (!_audioCtx || _audioCtx.state === 'closed') {
      _audioCtx = new (window.AudioContext || window.webkitAudioContext)();
    }
    return _audioCtx;
  }

  var _waveCache = {};
  async function fetchAndDecode(url) {
    if (_waveCache[url]) return _waveCache[url];
    var res = await fetch(url);
    if (!res.ok) throw new Error('HTTP ' + res.status);
    var buf = await res.arrayBuffer();
    var actx = getAudioCtx();
    var decoded = await actx.decodeAudioData(buf);
    _waveCache[url] = decoded;
    return decoded;
  }

  function drawPlaceholderWave(canvas, isFire) {
    var W = (canvas.parentElement && canvas.parentElement.offsetWidth) || 280;
    canvas.width = W;
    var H = canvas.height;
    var ctx = canvas.getContext('2d');
    ctx.clearRect(0, 0, W, H);
    ctx.fillStyle = isFire ? 'rgba(248,113,113,0.18)' : 'rgba(56,189,248,0.18)';
    var bars = Math.floor(W / 4);
    for (var i = 0; i < bars; i++) {
      var h = Math.max(2, Math.abs(Math.sin(i * 0.45) * H * 0.45 + Math.sin(i * 0.8) * H * 0.15) + 3);
      ctx.fillRect(i * 4, (H - h) / 2, 3, h);
    }
  }

  function drawRealWave(canvas, audioBuffer, isFire, progress) {
    var W = (canvas.parentElement && canvas.parentElement.offsetWidth) || canvas.width || 280;
    canvas.width = W;
    var H = canvas.height;
    var ctx = canvas.getContext('2d');
    ctx.clearRect(0, 0, W, H);

    var data = audioBuffer.getChannelData(0);
    var barW = 3, gap = 1, step = barW + gap;
    var bars = Math.floor(W / step);
    var blockSize = Math.floor(data.length / bars);
    var playedX = progress * W;
    var litColor  = isFire ? 'rgba(248,113,113,0.9)'  : 'rgba(56,189,248,0.9)';
    var dimColor  = isFire ? 'rgba(248,113,113,0.25)' : 'rgba(56,189,248,0.25)';

    for (var i = 0; i < bars; i++) {
      var sum = 0;
      var start = i * blockSize;
      for (var j = 0; j < blockSize; j++) sum += Math.abs(data[start + j] || 0);
      var rms = sum / blockSize;
      var barH = Math.max(2, rms * H * 6);
      var x = i * step;
      ctx.fillStyle = x <= playedX ? litColor : dimColor;
      ctx.fillRect(x, (H - barH) / 2, barW, barH);
    }

    if (progress > 0 && progress < 1) {
      ctx.fillStyle = 'rgba(255,255,255,0.75)';
      ctx.fillRect(Math.floor(playedX), 0, 1, H);
    }
  }

  var _activePlayer = null;

  function initWaveformPlayer(wrapper, canvas, audioUrl, isFire) {
    var audioEl = null;
    var decoded = null;
    var animId = null;
    var loading = false;

    function stopAnim() { if (animId) { cancelAnimationFrame(animId); animId = null; } }

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

    wrapper.addEventListener('click', async function() {
      var actx = getAudioCtx();
      if (actx.state === 'suspended') await actx.resume();
      if (audioEl && !audioEl.paused) { stopThis(); return; }
      if (_activePlayer && _activePlayer !== stopThis) _activePlayer();
      _activePlayer = stopThis;

      if (!decoded) {
        if (loading) return;
        loading = true;
        var hint = wrapper.querySelector('.wave-hint');
        if (hint) hint.querySelector('span').textContent = '\u27f3 loading\u2026';
        try {
          decoded = await fetchAndDecode(audioUrl);
          drawRealWave(canvas, decoded, isFire, 0);
          if (hint) hint.style.display = 'none';
        } catch (err) {
          console.error('[Wave] decode failed:', err);
          if (hint) hint.querySelector('span').textContent = '\u2715 error';
          loading = false;
          return;
        }
        loading = false;
      }

      if (!audioEl) {
        audioEl = new Audio(audioUrl);
        audioEl.addEventListener('ended', function() {
          stopAnim();
          drawRealWave(canvas, decoded, isFire, 1);
          _activePlayer = null;
        });
      }

      audioEl.play().then(function() { animId = requestAnimationFrame(tick); });
    });
  }

  // --- Call-card waveform players (for Recent Calls section) ---------------
  function fmtTime(s) {
    if (!isFinite(s) || s < 0) return '--:--';
    var m = Math.floor(s / 60);
    var sec = Math.floor(s % 60);
    return m + ':' + sec.toString().padStart(2, '0');
  }

  function drawCardWave(canvas, buffer, progress) {
    var ctx = canvas.getContext('2d');
    var W = canvas.width = canvas.offsetWidth || (canvas.parentElement && canvas.parentElement.offsetWidth) || 200;
    var H = canvas.height;
    ctx.clearRect(0, 0, W, H);
    var data = buffer.getChannelData(0);
    var step = Math.ceil(data.length / W);
    var mid = H / 2;
    var playX = Math.floor(W * (progress || 0));
    for (var i = 0; i < W; i++) {
      var max = 0;
      for (var j = 0; j < step; j++) {
        var v = Math.abs(data[i * step + j] || 0);
        if (v > max) max = v;
      }
      var h = Math.max(1, max * H * 0.85);
      ctx.fillStyle = i <= playX ? 'rgba(56,189,248,0.85)' : 'rgba(56,189,248,0.2)';
      ctx.fillRect(i, mid - h / 2, 1, h);
    }
  }

  function initAllTownCardPlayers(container) {
    container.querySelectorAll('.town-card-player').forEach(function(player) {
      var src = player.dataset.src;
      if (!src) return;
      var btn = player.querySelector('.town-card-btn');
      var icon = player.querySelector('.town-card-icon');
      var canvas = player.querySelector('.town-card-canvas');
      var timeEl = player.querySelector('.town-card-time');
      var scrub = player.querySelector('.town-card-scrub');
      if (!btn || !canvas) return;

      var audioEl = null, decoded = null, animId = null, loading = false;

      function stop() {
        if (audioEl && !audioEl.paused) audioEl.pause();
        if (animId) { cancelAnimationFrame(animId); animId = null; }
        if (icon) icon.textContent = '\u25b6';
      }

      function tick() {
        if (!audioEl || audioEl.paused) { stop(); return; }
        var prog = audioEl.currentTime / (audioEl.duration || 1);
        drawCardWave(canvas, decoded, prog);
        if (timeEl) timeEl.textContent = fmtTime(audioEl.duration - audioEl.currentTime);
        animId = requestAnimationFrame(tick);
      }

      async function toggle() {
        var actx = getAudioCtx();
        if (actx.state === 'suspended') await actx.resume();
        if (audioEl && !audioEl.paused) { stop(); return; }
        if (_activePlayer && _activePlayer !== stop) _activePlayer();
        _activePlayer = stop;

        if (!decoded) {
          if (loading) return;
          loading = true;
          if (icon) icon.textContent = '\u27f3';
          try {
            decoded = await fetchAndDecode(src);
            drawCardWave(canvas, decoded, 0);
          } catch (e) {
            console.error('[Town] card decode fail:', e);
            if (icon) icon.textContent = '\u2715';
            loading = false;
            return;
          }
          loading = false;
        }

        if (!audioEl) {
          audioEl = new Audio(src);
          audioEl.addEventListener('ended', function() {
            stop();
            drawCardWave(canvas, decoded, 1);
            _activePlayer = null;
          });
          audioEl.addEventListener('loadedmetadata', function() {
            if (timeEl) timeEl.textContent = fmtTime(audioEl.duration);
          });
        }

        audioEl.play().then(function() {
          if (icon) icon.textContent = '\u23f8';
          animId = requestAnimationFrame(tick);
        });
      }

      btn.addEventListener('click', function(e) { e.preventDefault(); e.stopPropagation(); toggle(); });
      if (scrub) scrub.addEventListener('click', function(e) {
        e.preventDefault(); e.stopPropagation();
        if (!audioEl || !decoded) { toggle(); return; }
        var rect = canvas.getBoundingClientRect();
        var frac = Math.max(0, Math.min(1, (e.clientX - rect.left) / rect.width));
        audioEl.currentTime = frac * audioEl.duration;
        drawCardWave(canvas, decoded, frac);
      });
    });
  }

  // --- WebSocket Logic (Live Now bar + On-Air) -----------------------------
  var liveNowFeeds = new Set();
  var lastTransmittingState = {};

  function initSocketIO() {
    var serverURL = window.location.origin.includes('iamcalledned.ai')
      ? "https://iamcalledned.ai"
      : "http://localhost:5005";

    var socket = io(serverURL, { transports: ['websocket', 'polling'] });

    socket.on('connect', function() { console.log('[Town] Socket connected.'); });
    socket.on('disconnect', function() { console.log('[Town] Socket disconnected.'); });
    socket.on('connect_error', function(err) { console.error('[Town] Socket error:', err.message); });

    socket.on('transmitting_update', function(msg) {
      if (!msg) return;
      if (msg.dept && msg.status) {
        updateTransmittingStatus(msg.dept, msg.status === 'Y');
      } else if (typeof msg === 'object') {
        Object.entries(msg).forEach(function(pair) {
          updateTransmittingStatus(pair[0], pair[1] === 'Y');
        });
      }
    });
  }

  function updateTransmittingStatus(dept, isTransmitting) {
    if (lastTransmittingState[dept] === isTransmitting) return;
    lastTransmittingState[dept] = isTransmitting;

    var indicator = document.querySelector('.live-indicator[data-dept="' + dept + '"]');
    if (indicator) indicator.classList.toggle('live', isTransmitting);

    var card = document.querySelector('.feed-card[data-dept="' + dept + '"]');
    if (card) {
      var badge = card.querySelector('.on-air-badge');
      if (badge) {
        badge.classList.toggle('hidden', !isTransmitting);
        badge.classList.toggle('flex', isTransmitting);
      }

      var isFire = dept.toLowerCase().includes('fd');
      if (isTransmitting) {
        card.style.boxShadow = isFire
          ? '0 0 30px rgba(239,68,68,0.15), 0 8px 32px rgba(0,0,0,0.25)'
          : '0 0 30px rgba(59,130,246,0.15), 0 8px 32px rgba(0,0,0,0.25)';
        card.style.borderColor = isFire
          ? 'rgba(239,68,68,0.3)' : 'rgba(59,130,246,0.3)';
      } else {
        card.style.boxShadow = '';
        card.style.borderColor = '';
      }
    }

    updateLiveNowBar(dept, isTransmitting);
    console.log('[Town] ' + dept + ' \u2192 ' + (isTransmitting ? 'ON AIR \ud83d\udd34' : 'off \u26ab'));
  }

  function updateLiveNowBar(dept, isTransmitting) {
    var chips = document.getElementById('live-now-chips');
    if (!chips) return;

    if (isTransmitting) liveNowFeeds.add(dept);
    else liveNowFeeds.delete(dept);

    chips.innerHTML = '';
    if (liveNowFeeds.size === 0) {
      var quiet = document.createElement('span');
      quiet.id = 'live-bar-quiet';
      quiet.className = 'text-slate-600 text-xs italic';
      quiet.textContent = 'All quiet';
      chips.appendChild(quiet);
      return;
    }

    liveNowFeeds.forEach(function(feed) {
      var isFire = feed.toLowerCase().includes('fd');
      var townName = FEED_TOWN_MAP[feed] || feed.toUpperCase();
      var chip = document.createElement('span');
      chip.className = 'inline-flex items-center gap-1.5 px-2.5 py-0.5 rounded-full text-xs font-medium border ' +
        (isFire
          ? 'bg-red-900/40 border-red-700/50 text-red-300'
          : 'bg-blue-900/40 border-blue-700/50 text-blue-300');
      chip.innerHTML = '<span class="w-1.5 h-1.5 rounded-full ' + (isFire ? 'bg-red-400' : 'bg-blue-400') + ' animate-ping"></span>' +
        townName + ' ' + (isFire ? 'Fire' : 'PD');
      chips.appendChild(chip);
    });
  }

  // --- Time-ago refresh loop -----------------------------------------------
  setInterval(function() {
    var latestTs = null;
    for (var i = 0; i < feeds.length; i++) {
      var id = feeds[i];
      var ts = lastTimestamps[id];
      var el = document.getElementById(id + '-last-time');
      if (el && ts) el.textContent = formatTimeAgo(ts);
      if (ts && (!latestTs || ts > latestTs)) latestTs = ts;
    }
    var statLastActive = document.getElementById('town-stat-last-active');
    if (statLastActive && latestTs) statLastActive.textContent = formatTimeAgo(latestTs);
  }, 1000);

  // --- Init ---------------------------------------------------------------
  buildTownPage();
  loadFeedData();
  setInterval(loadFeedData, 30000);
  initSocketIO();

  if (typeof checkAuth === 'function') {
    checkAuth();
  } else {
    console.warn('checkAuth() function not found. Is scanner_app_new.js loaded?');
  }
});
