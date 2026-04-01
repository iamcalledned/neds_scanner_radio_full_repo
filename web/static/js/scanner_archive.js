// ===============================================================
// Scanner Archive — Redesigned v2 (modern glass panels)
// ===============================================================

const _archiveFeedConfig = {
  pd:    { title: 'Hopedale Police', town: 'Hopedale', type: 'police' },
  fd:    { title: 'Hopedale Fire',   town: 'Hopedale', type: 'fire'   },
  mpd:   { title: 'Milford Police',  town: 'Milford',  type: 'police' },
  mfd:   { title: 'Milford Fire',    town: 'Milford',  type: 'fire'   },
  bpd:   { title: 'Bellingham Police', town: 'Bellingham', type: 'police' },
  bfd:   { title: 'Bellingham Fire',   town: 'Bellingham', type: 'fire'   },
  mndpd: { title: 'Mendon Police',  town: 'Mendon',  type: 'police' },
  mndfd: { title: 'Mendon Fire',    town: 'Mendon',  type: 'fire'   },
  uptpd: { title: 'Upton Police',   town: 'Upton',   type: 'police' },
  uptfd: { title: 'Upton Fire',     town: 'Upton',   type: 'fire'   },
  blkpd: { title: 'Blackstone Police', town: 'Blackstone', type: 'police' },
  blkfd: { title: 'Blackstone Fire',   town: 'Blackstone', type: 'fire'   },
  frkpd: { title: 'Franklin Police', town: 'Franklin', type: 'police' },
  frkfd: { title: 'Franklin Fire',   town: 'Franklin', type: 'fire'   },
  milpd: { title: 'Millis Police',  town: 'Millis',  type: 'police' },
  milfd: { title: 'Millis Fire',    town: 'Millis',  type: 'fire'   },
  medpd: { title: 'Medway Police',  town: 'Medway',  type: 'police' },
  medfd: { title: 'Medway Fire',    town: 'Medway',  type: 'fire'   },
  foxpd: { title: 'Foxboro Police', town: 'Foxboro', type: 'police' },
  mllpd: { title: 'Millville Police', town: 'Millville', type: 'police' },
  mllfd: { title: 'Millville Fire',   town: 'Millville', type: 'fire'   },
};

const _archiveTowns = [
  { name: 'Hopedale',    pd: 'pd',    fd: 'fd'    },
  { name: 'Milford',     pd: 'mpd',   fd: 'mfd'   },
  { name: 'Bellingham',  pd: 'bpd',   fd: 'bfd'   },
  { name: 'Mendon',      pd: 'mndpd', fd: 'mndfd' },
  { name: 'Upton',       pd: 'uptpd', fd: 'uptfd' },
  { name: 'Blackstone',  pd: 'blkpd', fd: 'blkfd' },
  { name: 'Franklin',    pd: 'frkpd', fd: 'frkfd' },
];

function _esc(s) {
  if (s == null) return '';
  return String(s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;');
}

// ── State ──
let _currentDaysBack = 7;
let _summaryData = null;  // { days:[], call_totals:{} }
const CALLS_PER_PAGE = 10;

// ── Waveform engine (compact version for archive) ──
let _archAudioCtx = null;
function _getACtx() {
  if (!_archAudioCtx || _archAudioCtx.state === 'closed')
    _archAudioCtx = new (window.AudioContext || window.webkitAudioContext)();
  return _archAudioCtx;
}
const _archWaveCache = {};
async function _fetchDecode(url) {
  if (_archWaveCache[url]) return _archWaveCache[url];
  const res = await fetch(url);
  if (!res.ok) throw new Error(`HTTP ${res.status}`);
  const decoded = await _getACtx().decodeAudioData(await res.arrayBuffer());
  _archWaveCache[url] = decoded;
  return decoded;
}
function _fmtTime(s) {
  if (!isFinite(s) || s < 0) return '--:--';
  return `${Math.floor(s/60)}:${Math.floor(s%60).toString().padStart(2,'0')}`;
}
function _isFire(feed) { return (feed || '').toLowerCase().includes('fd'); }

function _drawPlaceholder(canvas, fire) {
  const W = canvas.parentElement?.offsetWidth || 280;
  canvas.width = W; const H = canvas.height;
  const ctx = canvas.getContext('2d');
  ctx.clearRect(0,0,W,H);
  ctx.fillStyle = fire ? 'rgba(248,113,113,0.15)' : 'rgba(56,189,248,0.15)';
  const bars = Math.floor(W/4);
  for (let i=0;i<bars;i++){
    const h = Math.max(2, Math.abs(Math.sin(i*0.45)*H*0.45 + Math.sin(i*0.8)*H*0.15)+3);
    ctx.fillRect(i*4,(H-h)/2,3,h);
  }
}
function _drawWave(canvas, buf, fire, prog) {
  const W = canvas.parentElement?.offsetWidth || canvas.width || 280;
  canvas.width = W; const H = canvas.height;
  const ctx = canvas.getContext('2d'); ctx.clearRect(0,0,W,H);
  const data = buf.getChannelData(0);
  const step=4, bars=Math.floor(W/step), blockSize=Math.floor(data.length/bars);
  const playedX = prog*W;
  const lit = fire ? 'rgba(248,113,113,0.9)' : 'rgba(56,189,248,0.9)';
  const dim = fire ? 'rgba(248,113,113,0.22)' : 'rgba(56,189,248,0.22)';
  for(let i=0;i<bars;i++){
    let sum=0; const start=i*blockSize;
    for(let j=0;j<blockSize;j++) sum += Math.abs(data[start+j]||0);
    const bH = Math.max(2,(sum/blockSize)*H*6);
    ctx.fillStyle = i*step <= playedX ? lit : dim;
    ctx.fillRect(i*step,(H-bH)/2,3,bH);
  }
  if(prog>0&&prog<1){ctx.fillStyle='rgba(255,255,255,0.7)';ctx.fillRect(Math.floor(playedX),0,1,H);}
}

let _archActiveStop = null;
function initWaveformPlayer(el) {
  const url = el.dataset.src, feed = el.dataset.feed || '';
  if(!url) return;
  const fire = _isFire(feed);
  const canvas = el.querySelector('.wave-canvas');
  const playBtn = el.querySelector('.wave-play-btn');
  const playIcon = el.querySelector('.wave-play-icon');
  const timeEl = el.querySelector('.wave-time');
  const scrub = el.querySelector('.wave-scrub');
  if(!canvas) return;

  let audioEl = new Audio(url);
  let decoded=null, animId=null, loading=false;
  requestAnimationFrame(()=>_drawPlaceholder(canvas,fire));

  function stopAnim(){if(animId){cancelAnimationFrame(animId);animId=null;}}
  function tick(){
    if(!audioEl||audioEl.paused){stopAnim();return;}
    _drawWave(canvas,decoded,fire,audioEl.currentTime/(audioEl.duration||1));
    if(timeEl) timeEl.textContent=_fmtTime(audioEl.currentTime);
    animId=requestAnimationFrame(tick);
  }
  function stopThis(){
    if(!audioEl.paused) audioEl.pause();
    stopAnim();
    if(playIcon) playIcon.textContent='▶';
    if(decoded) _drawWave(canvas,decoded,fire,audioEl.currentTime/(audioEl.duration||1));
  }
  audioEl.addEventListener('ended',()=>{
    stopAnim(); if(playIcon) playIcon.textContent='▶';
    if(decoded) _drawWave(canvas,decoded,fire,1);
    if(timeEl&&audioEl.duration) timeEl.textContent=_fmtTime(audioEl.duration);
    _archActiveStop=null;
  });
  audioEl.addEventListener('timeupdate',()=>{if(timeEl&&!animId) timeEl.textContent=_fmtTime(audioEl.currentTime);});

  async function startPlay(){
    const actx=_getACtx(); if(actx.state==='suspended') await actx.resume();
    if(!audioEl.paused){stopThis();return;}
    if(_archActiveStop&&_archActiveStop!==stopThis) _archActiveStop();
    _archActiveStop=stopThis;
    if(!decoded){
      if(loading) return; loading=true;
      if(playIcon) playIcon.textContent='⟳';
      try{decoded=await _fetchDecode(url);_drawWave(canvas,decoded,fire,0);if(timeEl&&audioEl.duration) timeEl.textContent=_fmtTime(audioEl.duration);}
      catch(e){console.error('[Wave]',e);if(playIcon) playIcon.textContent='✕';loading=false;return;}
      loading=false;
    }
    audioEl.play().then(()=>{if(playIcon) playIcon.textContent='⏸';animId=requestAnimationFrame(tick);});
  }
  if(playBtn) playBtn.addEventListener('click',startPlay);
  scrub?.addEventListener('click',(e)=>{
    if(!decoded||!audioEl.duration){startPlay();return;}
    const ratio=(e.clientX-canvas.getBoundingClientRect().left)/canvas.getBoundingClientRect().width;
    audioEl.currentTime=ratio*audioEl.duration;
    _drawWave(canvas,decoded,fire,ratio);
    if(timeEl) timeEl.textContent=_fmtTime(audioEl.currentTime);
    if(audioEl.paused) startPlay();
  });
}

// ── Format a day string nicely ──
function _formatDay(dayStr) {
  try {
    const d = new Date(dayStr + 'T12:00:00');
    const today = new Date(); today.setHours(12,0,0,0);
    const yesterday = new Date(today); yesterday.setDate(yesterday.getDate()-1);
    if (d.toDateString() === today.toDateString()) return 'Today';
    if (d.toDateString() === yesterday.toDateString()) return 'Yesterday';
    return d.toLocaleDateString('en-US', { weekday: 'long', month: 'long', day: 'numeric', year: 'numeric' });
  } catch { return dayStr; }
}

// ── Render a single call card ──
function _renderCallCard(call, feed) {
  const fire = _isFire(feed);
  const isHook = call.metadata && call.metadata.hook_request === '1';
  const playCount = (call.metadata && call.metadata.play_count) || 0;
  const address = (call.metadata && call.metadata.derived_address) || '';
  const addrConf = (call.metadata && call.metadata.address_confidence) || 'none';
  const enhanced = call.enhanced_transcript || '';
  const transcript = call.transcript || '(no transcript)';
  const timestamp = call.timestamp_human || '';

  // Format timestamp nicely
  let timeDisplay = timestamp;
  const match = timestamp.match(/(\d{4}-\d{2}-\d{2})\s+(\d{2})-(\d{2})-(\d{2})/);
  if (match) {
    try {
      const d = new Date(`${match[1]}T${match[2]}:${match[3]}:${match[4]}`);
      timeDisplay = d.toLocaleTimeString('en-US', { hour: 'numeric', minute: '2-digit' });
    } catch { /* keep original */ }
  }

  let pillHTML;
  if (isHook) pillHTML = '<span class="call-card-pill pill-hook"><span class="call-card-dot dot-hook"></span>Hook</span>';
  else if (fire) pillHTML = '<span class="call-card-pill pill-fire"><span class="call-card-dot dot-fire"></span>Fire</span>';
  else pillHTML = '<span class="call-card-pill pill-police"><span class="call-card-dot"></span>Police</span>';

  let addressHTML = '';
  if (address && addrConf !== 'none') {
    addressHTML = `<div class="call-card-address"><svg viewBox="0 0 20 20" fill="currentColor"><path fill-rule="evenodd" d="M5.05 4.05a7 7 0 119.9 9.9L10 18.9l-4.95-4.95a7 7 0 010-9.9zM10 11a2 2 0 100-4 2 2 0 000 4z" clip-rule="evenodd"/></svg><span>${_esc(address)}</span></div>`;
  }

  let transcriptHTML = '';
  if (enhanced) {
    transcriptHTML += `<div><div class="transcript-label text-purple-400">✨ Enhanced</div><div class="transcript-block text-purple-100/90">${_esc(enhanced)}</div></div>`;
  }
  transcriptHTML += `<div><div class="transcript-label text-slate-500">🎧 Original</div><pre class="transcript-block text-slate-200">${_esc(transcript)}</pre></div>`;

  const playCountHTML = playCount > 0 ? `<span class="play-count-badge">👂 ${playCount}</span>` : '';

  const div = document.createElement('div');
  div.className = 'archive-call-card';
  div.innerHTML = `
    <div class="flex items-center justify-between mb-2">
      <div class="flex items-center gap-2">${pillHTML}</div>
      <div class="flex items-center gap-3">
        ${playCountHTML}
        <span class="text-xs text-slate-500 uppercase tracking-wider font-medium">${_esc(timeDisplay)}</span>
      </div>
    </div>
    ${addressHTML}
    <div class="wave-player mt-2" data-src="${call.path}" data-feed="${_esc(feed)}">
      <div class="wave-player-row">
        <button class="wave-play-btn w-8 h-8 rounded-full flex items-center justify-center transition flex-shrink-0
            ${fire ? 'bg-red-900/40 border border-red-700/50 text-red-300 hover:bg-red-800/50'
                   : 'bg-blue-900/40 border border-blue-700/50 text-blue-300 hover:bg-blue-800/50'}">
          <span class="wave-play-icon text-xs">▶</span>
        </button>
        <div class="flex-1 relative wave-scrub cursor-pointer">
          <canvas class="wave-canvas block w-full rounded" height="38"></canvas>
        </div>
        <span class="wave-time text-xs text-slate-500 tabular-nums w-10 text-right shrink-0">${call.duration > 0 ? _fmtTime(call.duration) : '--:--'}</span>
      </div>
    </div>
    <div class="space-y-2 mt-2">${transcriptHTML}</div>
  `;
  return div;
}

// ── Build overview (no feed selected) ──
function buildOverview() {
  const header = document.getElementById('page-header');
  const container = document.getElementById('archive-container');

  header.innerHTML = `
    <h2 class="text-3xl sm:text-4xl font-extrabold text-transparent bg-clip-text bg-gradient-to-r from-scannerBlue to-purple-400">
      Scanner Archive
    </h2>
    <p class="text-slate-500 text-sm mt-1">Select a department to browse archived calls</p>
  `;

  container.innerHTML = '';
  const grid = document.createElement('div');
  grid.className = 'grid grid-cols-1 sm:grid-cols-2 gap-4';

  _archiveTowns.forEach((t, i) => {
    const card = document.createElement('div');
    card.className = 'town-card';
    card.style.animationDelay = `${i * 0.07}s`;
    card.innerHTML = `
      <h3 class="text-lg font-bold text-transparent bg-clip-text bg-gradient-to-r from-scannerBlue to-blue-300 mb-3">${t.name}</h3>
      <div class="space-y-1">
        <a href="/scanner/archive?feed=${t.pd}" class="dept-link">
          <span class="call-card-dot"></span>
          <span>Police Department</span>
        </a>
        <a href="/scanner/archive?feed=${t.fd}" class="dept-link dept-link-fire">
          <span class="call-card-dot dot-fire"></span>
          <span>Fire Department</span>
        </a>
      </div>
    `;
    grid.appendChild(card);
  });
  container.appendChild(grid);
}

// ── Build feed view ──
async function buildFeedView(feed, daysBack) {
  const cfg = _archiveFeedConfig[feed] || { title: feed.toUpperCase(), town: '', type: 'police' };
  const fire = cfg.type === 'fire';
  const header = document.getElementById('page-header');
  const container = document.getElementById('archive-container');
  const statsPanel = document.getElementById('archive-stats-panel');
  const rangeSelector = document.getElementById('range-selector');

  header.innerHTML = `
    <h2 class="text-3xl sm:text-4xl font-extrabold text-transparent bg-clip-text bg-gradient-to-r
        ${fire ? 'from-red-400 to-orange-400' : 'from-scannerBlue to-purple-400'}">
      ${_esc(cfg.title)} Archive
    </h2>
    <p class="text-slate-500 text-sm mt-1">${_esc(cfg.town)} — ${daysBack > 0 ? `Past ${daysBack} days` : 'All time'}</p>
  `;

  rangeSelector.classList.remove('hidden');
  container.innerHTML = `
    <div class="panel p-8 text-center">
      <svg class="loading-spinner w-6 h-6 inline-block text-scannerBlue mb-2" xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" stroke-width="2" stroke="currentColor">
        <path stroke-linecap="round" stroke-linejoin="round" d="M16.023 9.348h4.992v-.001M2.985 19.644v-4.992m0 0h4.992m-4.993 0l3.181 3.183a8.25 8.25 0 0013.803-3.7M4.031 9.865a8.25 8.25 0 0113.803-3.7l3.181 3.182m0-4.991v4.99"/>
      </svg>
      <p class="text-slate-500 text-sm">Loading archive...</p>
    </div>`;

  // Fetch summary
  try {
    const daysParam = daysBack > 0 ? daysBack : 3650; // "All Time" = ~10 years
    const resp = await fetch(`/scanner/archive?feed=${feed}&json=1&days_back=${daysParam}`);
    if (!resp.ok) throw new Error(resp.statusText);
    _summaryData = await resp.json();
  } catch (err) {
    container.innerHTML = `<div class="panel p-6 text-center text-red-400 text-sm">Failed to load archive: ${_esc(err.message)}</div>`;
    return;
  }

  const days = _summaryData.days || [];
  const totals = _summaryData.call_totals || {};

  if (days.length === 0) {
    container.innerHTML = '<div class="panel-soft px-6 py-8 text-center text-sm text-slate-400">No archived calls found for this range.</div>';
    statsPanel.classList.add('hidden');
    return;
  }

  // Stats
  const totalCalls = days.reduce((s, d) => s + (totals[d] || 0), 0);
  const avgPerDay = days.length > 0 ? Math.round(totalCalls / days.length) : 0;
  document.getElementById('stat-total-days').textContent = days.length;
  document.getElementById('stat-total-calls').textContent = totalCalls.toLocaleString();
  document.getElementById('stat-avg-day').textContent = avgPerDay;
  statsPanel.classList.remove('hidden');

  // Build day accordions
  container.innerHTML = '';
  days.forEach((day, i) => {
    const count = totals[day] || 0;
    const details = document.createElement('details');
    details.className = 'day-panel';
    details.style.animationDelay = `${i * 0.04}s`;
    details.innerHTML = `
      <summary class="day-summary" role="button">
        <div class="flex items-center gap-3">
          <span class="text-sm font-semibold text-slate-200">${_formatDay(day)}</span>
          <span class="text-xs px-2 py-0.5 rounded-full ${fire ? 'bg-red-900/30 text-red-300 border border-red-800/30' : 'bg-blue-900/30 text-blue-300 border border-blue-800/30'} font-semibold">${count} call${count !== 1 ? 's' : ''}</span>
        </div>
        <svg class="day-chevron" xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" stroke-width="2.5" stroke="currentColor">
          <path stroke-linecap="round" stroke-linejoin="round" d="M19.5 8.25l-7.5 7.5-7.5-7.5"/>
        </svg>
      </summary>
      <div class="p-4 space-y-3 call-list" data-day="${day}" data-feed="${_esc(feed)}">
        <div class="text-center py-4">
          <svg class="loading-spinner w-5 h-5 inline-block text-slate-500" xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" stroke-width="2" stroke="currentColor">
            <path stroke-linecap="round" stroke-linejoin="round" d="M16.023 9.348h4.992v-.001M2.985 19.644v-4.992m0 0h4.992m-4.993 0l3.181 3.183a8.25 8.25 0 0013.803-3.7M4.031 9.865a8.25 8.25 0 0113.803-3.7l3.181 3.182m0-4.991v4.99"/>
          </svg>
          <p class="text-slate-500 text-xs mt-1">Loading calls...</p>
        </div>
      </div>
    `;

    details.addEventListener('toggle', () => {
      if (!details.open) return;
      const callList = details.querySelector('.call-list');
      if (callList.dataset.loaded === '1') return;
      loadDayCalls(callList, feed, day, 1);
    });

    container.appendChild(details);
  });
}

// ── Load calls for a specific day ──
async function loadDayCalls(callList, feed, day, page) {
  callList.dataset.loaded = '1';
  try {
    const resp = await fetch(`/scanner/archive?feed=${feed}&day=${encodeURIComponent(day)}&page=${page}&json=1`);
    if (!resp.ok) throw new Error(resp.statusText);
    const data = await resp.json();

    if (page === 1) callList.innerHTML = '';

    if (!data.calls || data.calls.length === 0) {
      if (page === 1) callList.innerHTML = '<p class="text-slate-500 text-sm text-center py-2">No calls found.</p>';
      return;
    }

    data.calls.forEach(call => {
      const card = _renderCallCard(call, feed);
      callList.appendChild(card);
      const wp = card.querySelector('.wave-player');
      if (wp) initWaveformPlayer(wp);
    });

    // "Load more" if a full page was returned
    if (data.calls.length >= CALLS_PER_PAGE) {
      // Remove existing load-more if any
      callList.querySelector('.load-more-day')?.remove();
      const btn = document.createElement('button');
      btn.className = 'load-older-btn load-more-day mt-3';
      btn.textContent = 'Load more calls';
      btn.addEventListener('click', async () => {
        btn.disabled = true;
        btn.innerHTML = '<svg class="loading-spinner w-4 h-4 inline-block" xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" stroke-width="2" stroke="currentColor"><path stroke-linecap="round" stroke-linejoin="round" d="M16.023 9.348h4.992v-.001M2.985 19.644v-4.992m0 0h4.992m-4.993 0l3.181 3.183a8.25 8.25 0 0013.803-3.7M4.031 9.865a8.25 8.25 0 0113.803-3.7l3.181 3.182m0-4.991v4.99"/></svg> Loading...';
        try {
          const nextResp = await fetch(`/scanner/archive?feed=${feed}&day=${encodeURIComponent(day)}&page=${page + 1}&json=1`);
          const nextData = await nextResp.json();
          btn.remove();
          if (nextData.calls && nextData.calls.length > 0) {
            nextData.calls.forEach(c => {
              const card = _renderCallCard(c, feed);
              callList.appendChild(card);
              const wp = card.querySelector('.wave-player');
              if (wp) initWaveformPlayer(wp);
            });
            if (nextData.calls.length >= CALLS_PER_PAGE) {
              // Recursively add another load-more
              const nextBtn = document.createElement('button');
              nextBtn.className = 'load-older-btn load-more-day mt-3';
              nextBtn.textContent = 'Load more calls';
              const nextPage = page + 1;
              nextBtn.addEventListener('click', async function handler() {
                nextBtn.removeEventListener('click', handler);
                nextBtn.remove();
                await loadDayCalls(callList, feed, day, nextPage + 1);
              });
              callList.appendChild(nextBtn);
            }
          }
        } catch (e) {
          btn.textContent = 'Error loading';
          btn.disabled = false;
          setTimeout(() => btn.remove(), 2000);
        }
      });
      callList.appendChild(btn);
    }
  } catch (err) {
    callList.innerHTML = `<p class="text-red-400 text-sm text-center py-2">Error: ${_esc(err.message)}</p>`;
  }
}

// ── Main ──
document.addEventListener('DOMContentLoaded', () => {
  const params = new URLSearchParams(window.location.search);
  const feed = params.get('feed');

  // --- OVERVIEW MODE ---
  if (!feed) {
    buildOverview();
    return;
  }

  // --- FEED MODE ---
  buildFeedView(feed, _currentDaysBack);

  // Range selector
  document.getElementById('range-selector')?.addEventListener('click', (e) => {
    const btn = e.target.closest('.range-btn');
    if (!btn) return;
    const days = parseInt(btn.dataset.days, 10);
    _currentDaysBack = days;

    // Update active state
    document.querySelectorAll('.range-btn').forEach(b => b.classList.remove('active'));
    btn.classList.add('active');

    buildFeedView(feed, days);
  });

  // Load older button
  document.getElementById('load-older-btn')?.addEventListener('click', () => {
    // Find the next range step
    const steps = [7, 14, 30, 60, 90, 0];
    const currentIdx = steps.indexOf(_currentDaysBack);
    const nextDays = currentIdx >= 0 && currentIdx < steps.length - 1 ? steps[currentIdx + 1] : 0;
    _currentDaysBack = nextDays;

    // Update range button
    document.querySelectorAll('.range-btn').forEach(b => {
      b.classList.toggle('active', parseInt(b.dataset.days, 10) === nextDays);
    });

    buildFeedView(feed, nextDays);
  });
});
