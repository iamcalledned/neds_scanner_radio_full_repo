// ===============================================================
// Scanner View Page — Redesigned v2 (matches home/town page feel)
// ===============================================================

// --- FEED CONFIG ---
const feedConfig = {
    'pd': { title: "Hopedale Police", town: "Hopedale" },
    'fd': { title: "Hopedale Fire", town: "Hopedale" },
    'mpd': { title: "Milford Police", town: "Milford" },
    'mfd': { title: "Milford Fire", town: "Milford" },
    'frkfd': { title: "Franklin Fire", town: "Franklin" },
    'frkpd': { title: "Franklin Police", town: "Franklin" },
    'bpd': { title: "Bellingham Police", town: "Bellingham" },
    'bfd': { title: "Bellingham Fire", town: "Bellingham" },
    'mndpd': { title: "Mendon Police", town: "Mendon" },
    'mndfd': { title: "Mendon Fire", town: "Mendon" },
    'blkpd': { title: "Blackstone Police", town: "Blackstone" },
    'blkfd': { title: "Blackstone Fire", town: "Blackstone" },
    'milpd': { title: "Millis Police", town: "Millis" },
    'milfd': { title: "Millis Fire", town: "Millis" },
    'medpd': { title: "Medway Police", town: "Medway" },
    'medfd': { title: "Medway Fire", town: "Medway" },
    'foxpd': { title: "Foxboro Police", town: "Foxboro" },
    'uptpd': { title: "Upton Police", town: "Upton" },
    'uptfd': { title: "Upton Fire", town: "Upton" },
    'mllpd': { title: "Millville Police", town: "Millville" },
    'mllfd': { title: "Millville Fire", town: "Millville" },
};

// --- GLOBAL STATE ---
let callMetadata = {};
let offset = window.initialCallsCount || 0;
const limit = 10;
let isLoading = false;
let allCallsLoaded = false;
let existingCallFiles = new Set();
let nextRenderIndex = 0;

// --- HELPERS ---
function _escHtml(s) {
  if (s == null) return '';
  return String(s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;');
}

function _getCurrentFeed() {
  return new URLSearchParams(window.location.search).get('feed') || '';
}

function _isFire(feed) {
  return (feed || '').toLowerCase().includes('fd');
}

// --- FEED HEADER + STATS ---
function buildPageHeader() {
  const feed = _getCurrentFeed();
  const cfg = feedConfig[feed] || { title: feed.toUpperCase(), town: '' };
  const isFire = _isFire(feed);
  const headerEl = document.getElementById('page-header');
  if (!headerEl) return;

  headerEl.innerHTML = `
    <h2 class="text-3xl sm:text-4xl font-extrabold text-transparent bg-clip-text bg-gradient-to-r
        ${isFire ? 'from-red-400 to-orange-400' : 'from-scannerBlue to-purple-400'}">
      ${_escHtml(cfg.title)}
      <span class="live-badge">LIVE</span>
    </h2>
    <p class="text-slate-500 text-sm mt-1">${_escHtml(cfg.town)} — Today's feed</p>
  `;
}

async function loadFeedStats() {
  const feed = _getCurrentFeed();
  if (!feed) return;
  try {
    const resp = await fetch('/scanner/api/today_counts');
    if (!resp.ok) return;
    const data = await resp.json();
    const feedData = data[feed] || {};

    const callsEl = document.getElementById('stat-calls-today');
    const hooksEl = document.getElementById('stat-hooks-today');
    const lastEl  = document.getElementById('stat-last-active');

    if (callsEl) callsEl.textContent = feedData.count || 0;
    if (hooksEl) hooksEl.textContent = feedData.hooks_count || 0;

    if (lastEl && feedData.latest_time) {
      try {
        const d = new Date(feedData.latest_time);
        lastEl.textContent = d.toLocaleTimeString('en-US', { hour: 'numeric', minute: '2-digit' });
      } catch { lastEl.textContent = '--'; }
    }
  } catch (e) {
    console.warn('Failed to load feed stats:', e);
  }
}

// --- RENDER A SINGLE CALL CARD (JS version for lazy-loaded calls) ---
function renderCall(call, index) {
  const isHook = (call.metadata && call.metadata.hook_request === '1');
  const isFire = _isFire(call.feed);
  const playCount = (call.metadata && call.metadata.play_count) || 0;
  const timestampHuman = call.timestamp_human || 'Unknown time';
  const callPath = call.path || '#';
  const callFile = call.file || '';
  const callFeed = call.feed || '';

  // Address data from metadata
  const derivedAddress = (call.metadata && call.metadata.derived_address) || '';
  const addrConfidence = (call.metadata && call.metadata.address_confidence) || 'none';

  // Transcripts
  const enhancedTranscript = (call.metadata && call.metadata.enhanced_transcript) || '';
  const editedTranscript = (call.metadata && call.metadata.edited_transcript) || '';
  const editPending = call.edit_pending || false;
  const saveForEval = call.save_for_eval || false;
  const originalTranscript = call.transcript || 'Transcript not available';

  // Build pill
  let pillHTML;
  if (isHook) {
    pillHTML = '<span class="call-card-pill pill-hook"><span class="call-card-dot dot-hook"></span>Hook / Tow</span>';
  } else if (isFire) {
    pillHTML = '<span class="call-card-pill pill-fire"><span class="call-card-dot dot-fire"></span>Fire Desk</span>';
  } else {
    pillHTML = '<span class="call-card-pill pill-police"><span class="call-card-dot"></span>Police Desk</span>';
  }

  // Play count
  const playCountHTML = playCount > 0
    ? `<div id="playcount-${index}" class="play-count-badge">👂 ${playCount}</div>`
    : `<div id="playcount-${index}" class="play-count-badge hidden"></div>`;

  // Address badge
  let addressHTML = '';
  if (derivedAddress && addrConfidence !== 'none') {
    addressHTML = `
      <div class="call-card-address">
        <svg viewBox="0 0 20 20" fill="currentColor"><path fill-rule="evenodd" d="M5.05 4.05a7 7 0 119.9 9.9L10 18.9l-4.95-4.95a7 7 0 010-9.9zM10 11a2 2 0 100-4 2 2 0 000 4z" clip-rule="evenodd"/></svg>
        <span>${_escHtml(derivedAddress)}</span>
      </div>`;
  }

  // Transcript sections
  let transcriptHTML = '';
  if (enhancedTranscript) {
    transcriptHTML += `<div><div class="transcript-label text-purple-400">✨ Enhanced</div><div class="transcript-block text-purple-100/90">${_escHtml(enhancedTranscript)}</div></div>`;
  }
  if (editedTranscript) {
    transcriptHTML += `<div class="edited-block"><div class="transcript-label text-green-400">✅ Edited</div><div class="transcript-block text-green-100/90">${_escHtml(editedTranscript)}</div></div>`;
  }

  const el = document.createElement('div');
  el.className = 'call-card-entry panel p-5 call-entry';
  el.id = `call-card-${index}`;
  if (isHook) el.dataset.hook = '1';
  if (callFile) el.dataset.file = callFile;

  el.innerHTML = `
    <div class="call-card-meta">
      <div class="flex items-center gap-2">${pillHTML}</div>
      <div class="flex items-center gap-3">
        ${playCountHTML}
        <span class="call-card-time call-timestamp" data-timestamp="${_escHtml(timestampHuman)}" data-feed="${_escHtml(callFeed)}">${_escHtml(timestampHuman)}</span>
      </div>
    </div>
    ${addressHTML}
    <audio class="call-audio hidden" src="${callPath}" data-filename="${_escHtml(callFile)}" data-feed="${_escHtml(callFeed)}" data-index="${index}"></audio>
    <div class="wave-player mt-3" data-src="${callPath}" data-feed="${_escHtml(callFeed)}">
      <div class="wave-player-row">
        <button class="wave-play-btn w-9 h-9 rounded-full flex items-center justify-center transition flex-shrink-0
            ${isFire ? 'bg-red-900/40 border border-red-700/50 text-red-300 hover:bg-red-800/50'
                     : 'bg-blue-900/40 border border-blue-700/50 text-blue-300 hover:bg-blue-800/50'}">
          <span class="wave-play-icon text-xs">▶</span>
        </button>
        <div class="flex-1 relative wave-scrub cursor-pointer">
          <canvas class="wave-canvas block w-full rounded" height="44"></canvas>
        </div>
        <span class="wave-time text-xs text-slate-500 tabular-nums w-10 text-right shrink-0">${call.duration > 0 ? _fmtTime(call.duration) : '--:--'}</span>
      </div>
    </div>
    <div class="space-y-3 mt-3">
      ${transcriptHTML}
      <div>
        <div id="orig-label-${index}" class="transcript-label text-slate-500">🎧 Original${editedTranscript ? ` — <button class="orig-toggle" onclick="toggleOriginal(${index})">show ▾</button>` : ''}</div>
        <pre id="pre-${index}" class="transcript-block text-slate-200${editedTranscript ? ' hidden' : ''}">${_escHtml(originalTranscript)}</pre>
        <textarea id="edit-${index}" class="w-full intent-input hidden mt-2" rows="4">${_escHtml(editedTranscript || originalTranscript)}</textarea>
        <div class="call-actions">
          <button data-action="edit" data-index="${index}" class="call-action-btn">Edit</button>
          <button data-action="save" data-file="${_escHtml(callFile)}" data-feed="${_escHtml(callFeed)}" data-index="${index}" id="save-${index}" class="call-action-btn hidden">Save</button>
          <button data-action="cancel" data-index="${index}" id="cancel-${index}" class="call-action-btn hidden">Cancel</button>
          <button data-action="mark-edited" data-index="${index}" id="edited-btn-${index}" class="call-action-btn${editedTranscript ? ' btn-edited-active' : ''}">Edited</button>
          <button data-action="approve" data-file="${_escHtml(callFile)}" data-feed="${_escHtml(callFeed)}" data-index="${index}" id="approve-${index}" class="call-action-btn btn-approve${editedTranscript ? ' btn-approve-active' : ''}" title="Mark transcript as good training data">${editedTranscript ? '✅ Looks Good' : 'Looks Good'}</button>
          <button data-action="classify" data-index="${index}" class="call-action-btn">Classify</button>
          <button data-action="address-lookup" data-index="${index}" class="call-action-btn">Address</button>
          <button data-action="save-eval" data-file="${_escHtml(callFile)}" data-feed="${_escHtml(callFeed)}" data-index="${index}" id="save-eval-${index}" class="call-action-btn btn-save-eval${saveForEval ? ' btn-save-eval-active' : ''}" title="Save this call as an evaluation sample">${saveForEval ? '📋 Save for Eval' : 'Save for Eval'}</button>
          <button data-action="share" data-index="${index}" data-feed="${_escHtml(callFeed)}" class="call-action-btn btn-share">Share</button>
        </div>
        <div id="msg-${index}" class="text-green-400 text-sm hidden mt-2">✔️ Thank you for your submission!</div>
      </div>

      <div id="intent-form-${index}" class="hidden intent-panel mt-3">
        <h4 class="text-sm font-semibold text-purple-300 mb-3 uppercase tracking-wide">Classify Call Intent</h4>
        <div class="space-y-4">
          <div>
            <label class="block text-xs font-medium text-slate-400 mb-2 uppercase tracking-wider">Intents</label>
            <div id="intent-options-${index}" class="grid grid-cols-2 sm:grid-cols-3 gap-2"></div>
          </div>
          <div>
            <label class="block text-xs font-medium text-slate-400 mb-2 uppercase tracking-wider">Disposition</label>
            <div id="disposition-options-${index}" class="grid grid-cols-2 sm:grid-cols-3 gap-2"></div>
          </div>
          <div class="grid grid-cols-1 sm:grid-cols-2 gap-3">
            <div>
              <label for="officer-${index}" class="block text-xs font-medium text-slate-400 mb-1">Officer/Unit</label>
              <input type="text" id="officer-${index}" class="intent-input" placeholder="e.g., 303, Car 4">
            </div>
            <div>
              <label for="road-${index}" class="block text-xs font-medium text-slate-400 mb-1">Road/Street</label>
              <input type="text" id="road-${index}" class="intent-input" placeholder="e.g., Main St, Rt 140">
            </div>
          </div>
          <div>
            <label for="notes-${index}" class="block text-xs font-medium text-slate-400 mb-1">Notes</label>
            <textarea id="notes-${index}" rows="2" class="intent-input" placeholder="Any other relevant details..."></textarea>
          </div>
          <div class="flex justify-end gap-3">
            <button data-action="cancel-intent" data-index="${index}" class="call-action-btn">Cancel</button>
            <button data-action="submit-intent" data-file="${_escHtml(callFile)}" data-feed="${_escHtml(callFeed)}" data-index="${index}" class="call-action-btn" style="background:rgba(139,92,246,0.2);border-color:rgba(139,92,246,0.3);color:#c4b5fd;">Submit</button>
          </div>
        </div>
      </div>
    </div>
  `;
  return el;
}

// --- LAZY LOADING ---
async function fetchMoreCalls() {
  if (isLoading || allCallsLoaded) return;
  isLoading = true;
  document.getElementById('loading-indicator')?.classList.remove('hidden');

  let data;
  try {
    const feed = _getCurrentFeed();
    const response = await fetch(`/scanner/api/archive_calls?feed=${feed}&offset=${offset}&limit=${limit}`);
    if (!response.ok) throw new Error('Failed to fetch');
    data = await response.json();

    if (data.calls && data.calls.length > 0) {
      data.calls.forEach((call, i) => {
        const newIndex = offset + i;
        callMetadata[newIndex] = call.metadata || {};
        const el = renderCall(call, newIndex);
        document.getElementById('calls-container')?.appendChild(el);
        const wp = el.querySelector('.wave-player');
        if (wp) initWaveformPlayer(wp);
      });
      offset += data.calls.length;
    } else {
      allCallsLoaded = true;
      const ind = document.getElementById('loading-indicator');
      if (ind) ind.textContent = "No more calls to load.";
    }
  } catch (error) {
    console.error('Error fetching more calls:', error);
    const ind = document.getElementById('loading-indicator');
    if (ind) ind.textContent = "Error loading calls.";
  } finally {
    isLoading = false;
    if (!allCallsLoaded && !(data && data.calls && data.calls.length === 0)) {
      document.getElementById('loading-indicator')?.classList.add('hidden');
    }
  }
}

// --- SEED / PREPEND / AUTO-UPDATE ---
function seedExistingCalls() {
  const audioEls = document.querySelectorAll('.call-audio[data-filename]');
  let maxIndex = 0;
  audioEls.forEach((el) => {
    const file = el.dataset.filename;
    if (file) existingCallFiles.add(file);
    const idx = Number.parseInt(el.dataset.index || '0', 10);
    if (!Number.isNaN(idx)) maxIndex = Math.max(maxIndex, idx);
  });
  nextRenderIndex = Math.max(maxIndex + 1, offset);
}

function prependNewCalls(calls) {
  const container = document.getElementById('calls-container');
  if (!container) return;
  [...calls].reverse().forEach((call) => {
    const newIndex = nextRenderIndex++;
    callMetadata[newIndex] = call.metadata || {};
    const el = renderCall(call, newIndex);
    container.insertBefore(el, container.firstChild);
    const wp = el.querySelector('.wave-player');
    if (wp) initWaveformPlayer(wp);
    if (call.file) existingCallFiles.add(call.file);
  });
  offset += calls.length;
  if (typeof formatAllTimestamps === 'function') formatAllTimestamps();
}

async function checkForNewCalls() {
  if (isLoading) return;
  const feed = _getCurrentFeed();
  if (!feed) return;
  try {
    const resp = await fetch(`/scanner/api/archive_calls?feed=${feed}&offset=0&limit=${limit}`, { cache: 'no-store' });
    if (!resp.ok) return;
    const data = await resp.json();
    if (!data.calls || !data.calls.length) return;
    const newCalls = data.calls.filter((c) => c.file && !existingCallFiles.has(c.file));
    if (newCalls.length) prependNewCalls(newCalls);
  } catch (e) { console.warn('Auto-update failed:', e); }
}

function initAutoUpdate() {
  seedExistingCalls();
  checkForNewCalls();
  setInterval(checkForNewCalls, 30000);
  loadHooksCounter();
  setInterval(loadHooksCounter, 60000);
}

// --- HOOKS COUNTER ---
async function loadHooksCounter() {
  const feed = _getCurrentFeed();
  if (!feed || !feed.toLowerCase().includes('pd')) return;
  const counterEl = document.getElementById('hooks-counter');
  const valueEl = document.getElementById('hooks-counter-value');
  const btn = document.getElementById('hooks-counter-btn');
  if (!counterEl || !valueEl) return;
  try {
    const resp = await fetch('/scanner/api/today_counts');
    if (!resp.ok) return;
    const data = await resp.json();
    const hooksCount = data[feed]?.hooks_count || 0;
    valueEl.textContent = hooksCount;
    if (hooksCount > 0) {
      counterEl.classList.remove('hidden');
      if (btn) btn.addEventListener('click', () => scrollToFirstHook());
    }
  } catch (e) { /* silently fail */ }
}

async function gotoFirstHook() {
  if (document.querySelector('[data-hook="1"]')) { scrollToFirstHook(); return; }
  for (let i = 0; i < 25 && !allCallsLoaded; i++) {
    while (isLoading) await new Promise(r => setTimeout(r, 100));
    if (document.querySelector('[data-hook="1"]')) { scrollToFirstHook(); return; }
    await fetchMoreCalls();
    if (document.querySelector('[data-hook="1"]')) { scrollToFirstHook(); return; }
  }
}

function scrollToFirstHook() {
  const hooks = document.querySelectorAll('[data-hook="1"]');
  if (!hooks.length) return;
  const hookEl = hooks[0];
  const mainArea = document.querySelector('main.main-content-area');
  if (mainArea) {
    const hookRect = hookEl.getBoundingClientRect();
    const mainRect = mainArea.getBoundingClientRect();
    mainArea.scrollTo({ top: mainArea.scrollTop + hookRect.top - mainRect.top - 80, behavior: 'smooth' });
  }
  hookEl.style.transition = 'box-shadow 0.3s';
  hookEl.style.boxShadow = '0 0 0 3px #f59e0b';
  setTimeout(() => { hookEl.style.boxShadow = ''; }, 1500);
}

// --- EVENT DELEGATION ---
function handleCallAction(event) {
  const target = event.target.closest('[data-action]') || event.target;
  const action = target.dataset.action;
  const index = target.dataset.index;
  const file = target.dataset.file;
  const feed = target.dataset.feed;
  const model = target.dataset.model;
  
  if (!action) return;

  switch (action) {
    case 'vote-best': submitVote(file, model, target); break;
    case 'edit': enableEdit(index); break;
    case 'save': submitEdit(file, feed, index); break;
    case 'approve': toggleApprove(file, feed, index); break;
    case 'cancel': cancelEdit(index); break;
    case 'mark-edited': /* read-only indicator, no action */ break;
    case 'save-eval': toggleSaveForEval(file, feed, index); break;
    case 'classify': toggleIntentForm(index); break;
    case 'submit-intent': submitIntent(file, feed, index); break;
    case 'cancel-intent': toggleIntentForm(index); break;
    case 'share': shareCall(index, feed); break;
    case 'address-lookup':
      event.preventDefault();
      const modal = document.getElementById("address-modal");
      if (modal) {
        document.getElementById("number-input").value = "";
        document.getElementById("street-input").value = "";
        modal.classList.remove("hidden");
        modal.classList.add("flex");
        document.getElementById("number-input")?.focus();
      }
      break;
  }
}

// --- VOTE BEST ---
async function submitVote(filename, model, btn) {
  if (!filename || !model) return;
  const originalHTML = btn.innerHTML;
  btn.innerHTML = `<span class="animate-pulse">Saving...</span>`;
  btn.disabled = true;

  try {
    const res = await fetch("/scanner/submit_vote", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ filename, model })
    });
    const data = await res.json();
    if (data.success) {
      btn.innerHTML = `✅ Voted`;
      btn.classList.add('bg-emerald-900/60', 'text-emerald-400', 'border-emerald-700/50');
      btn.classList.remove('bg-slate-800/50', 'bg-indigo-950/40', 'hover:bg-emerald-900/60');
    } else {
      btn.innerHTML = `❌ Error`;
      setTimeout(() => { btn.innerHTML = originalHTML; btn.disabled = false; }, 2000);
    }
  } catch (err) {
    console.error("Vote failed", err);
    btn.innerHTML = `❌ Error`;
    setTimeout(() => { btn.innerHTML = originalHTML; btn.disabled = false; }, 2000);
  }
}

// --- PLAY COUNT ---
async function handleAudioPlay(event) {
  const audio = event.target;
  if (!audio.matches('audio.call-audio')) return;
  if (audio.dataset.incremented) return;
  audio.dataset.incremented = 'true';
  const { filename, feed, index } = audio.dataset;
  const playCountEl = document.getElementById(`playcount-${index}`);
  try {
    const resp = await fetch("/scanner/increment_play", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ filename, feed })
    });
    if (resp.ok) {
      const data = await resp.json();
      if (data.play_count !== undefined && playCountEl) {
        playCountEl.classList.remove("hidden");
        playCountEl.textContent = `👂 ${data.play_count}`;
        playCountEl.classList.add("text-green-400");
        setTimeout(() => playCountEl.classList.remove("text-green-400"), 1200);
      }
    }
  } catch (err) {
    console.error('Play count error:', err);
  }
}

// --- SHARE ---
async function shareCall(index, feed) {
  const callCard = document.getElementById(`call-card-${index}`);
  if (!callCard) return;
  const audioEl = callCard.querySelector('audio');
  const preEl = callCard.querySelector(`#pre-${index}`);
  if (!audioEl || !preEl) return;

  const fullTranscript = preEl.innerText;
  const cfg = feedConfig[feed] || {};
  const feedTitle = cfg.title || 'Scanner';
  const branding = "\n\n---\nSent from Ned's Scanner Network";
  const msgEl = document.getElementById(`msg-${index}`);

  const showMsg = (msg, isErr = false) => {
    if (!msgEl) return;
    msgEl.textContent = msg;
    msgEl.className = isErr ? 'text-red-400 text-sm' : 'text-green-400 text-sm';
    msgEl.classList.remove('hidden');
    setTimeout(() => { msgEl.classList.add('hidden'); }, 3000);
  };

  const modalHtml = `
    <div id="share-modal-${index}" class="fixed inset-0 bg-black/60 backdrop-blur-sm flex items-center justify-center z-50">
      <div class="panel p-6 w-full max-w-md mx-4">
        <h3 class="text-lg font-semibold mb-4 text-slate-100">Share this Call</h3>
        <div class="space-y-2">
          ${navigator.share && navigator.canShare ? `<button id="share-both-btn" class="w-full text-left p-3 panel-soft hover:bg-white/[0.04] rounded-lg transition flex items-center gap-3"><span class="text-lg">🔊</span><span class="text-slate-200">Share Audio & Transcript</span></button>` : ''}
          ${navigator.share && navigator.canShare ? `<button id="share-audio-btn" class="w-full text-left p-3 panel-soft hover:bg-white/[0.04] rounded-lg transition flex items-center gap-3"><span class="text-lg">🎵</span><span class="text-slate-200">Share Audio Only</span></button>` : ''}
          <button id="copy-text-btn" class="w-full text-left p-3 panel-soft hover:bg-white/[0.04] rounded-lg transition flex items-center gap-3"><span class="text-lg">📝</span><span class="text-slate-200">Copy Transcript</span></button>
        </div>
        <button id="close-modal-btn" class="mt-4 w-full p-2 panel-soft hover:bg-white/[0.04] rounded-lg transition text-slate-400 text-sm">Close</button>
      </div>
    </div>`;
  document.body.insertAdjacentHTML('beforeend', modalHtml);

  const modal = document.getElementById(`share-modal-${index}`);
  if (!modal) return;
  const closeModal = () => modal.remove();

  const shareBothBtn = document.getElementById('share-both-btn');
  if (shareBothBtn) {
    shareBothBtn.onclick = async () => {
      try {
        const r = await fetch(audioEl.src); if (!r.ok) throw new Error('fetch failed');
        const blob = await r.blob();
        const file = new File([blob], `${feed}-call.wav`, { type: 'audio/wav' });
        if (navigator.canShare && navigator.canShare({ files: [file] })) {
          await navigator.share({ title: `Scanner: ${feedTitle}`, text: `${fullTranscript}${branding}`, files: [file] });
        } else { showMsg('Sharing files not supported on this device.', true); }
      } catch (e) { console.error(e); showMsg('❌ Share failed.', true); }
      closeModal();
    };
  }

  const shareAudioBtn = document.getElementById('share-audio-btn');
  if (shareAudioBtn) {
    shareAudioBtn.onclick = async () => {
      try {
        const r = await fetch(audioEl.src); if (!r.ok) throw new Error('fetch failed');
        const blob = await r.blob();
        const file = new File([blob], `${feed}-call.wav`, { type: 'audio/wav' });
        if (navigator.canShare && navigator.canShare({ files: [file] })) {
          await navigator.share({ title: `Scanner Audio: ${feedTitle}`, files: [file] });
        } else { showMsg('Sharing files not supported.', true); }
      } catch (e) { console.error(e); showMsg('❌ Share failed.', true); }
      closeModal();
    };
  }

  const copyBtn = document.getElementById('copy-text-btn');
  if (copyBtn) {
    copyBtn.onclick = async () => {
      try {
        await navigator.clipboard.writeText(`${fullTranscript}${branding}`);
        showMsg('✔️ Copied to clipboard!');
      } catch { showMsg('❌ Failed to copy.', true); }
      closeModal();
    };
  }

  document.getElementById('close-modal-btn')?.addEventListener('click', closeModal);
  modal.onclick = (e) => { if (e.target === modal) closeModal(); };
}

// --- EDIT TRANSCRIPT ---
function enableEdit(id) {
  document.getElementById(`pre-${id}`)?.classList.add("hidden");
  document.getElementById(`edit-${id}`)?.classList.remove("hidden");
  document.getElementById(`save-${id}`)?.classList.remove("hidden");
  document.getElementById(`cancel-${id}`)?.classList.remove("hidden");
  document.getElementById(`msg-${id}`)?.classList.add("hidden");
  // Dim Looks Good while editing is in progress
  document.getElementById(`approve-${id}`)?.classList.add('btn-dimmed');
}

function cancelEdit(id) {
  const pre = document.getElementById(`pre-${id}`);
  const edit = document.getElementById(`edit-${id}`);
  const hasEdited = document.getElementById(`edited-btn-${id}`)?.classList.contains('btn-edited-active');
  // Only show original if there's no edit or user previously expanded it
  if (edit) edit.classList.add('hidden');
  if (pre && !hasEdited) pre.classList.remove('hidden');
  document.getElementById(`save-${id}`)?.classList.add("hidden");
  document.getElementById(`cancel-${id}`)?.classList.add("hidden");
  document.getElementById(`approve-${id}`)?.classList.remove('btn-dimmed');
}

function toggleOriginal(id) {
  const pre = document.getElementById(`pre-${id}`);
  const btn = document.querySelector(`#orig-label-${id} .orig-toggle`);
  if (!pre) return;
  const hidden = pre.classList.toggle('hidden');
  if (btn) btn.textContent = hidden ? 'show ▾' : 'hide ▴';
}

async function toggleSaveForEval(filename, feed, id) {
  const btn = document.getElementById(`save-eval-${id}`);
  const msgEl = document.getElementById(`msg-${id}`);
  const showMsg = (msg, isErr) => {
    if (!msgEl) return;
    msgEl.textContent = msg;
    msgEl.className = isErr ? 'text-red-400 text-sm' : 'text-green-400 text-sm';
    msgEl.classList.remove('hidden');
    setTimeout(() => msgEl.classList.add('hidden'), 3000);
  };
  const save = !(btn && btn.classList.contains('btn-save-eval-active'));
  try {
    const resp = await fetch('/scanner/save_for_eval', {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ filename, feed, save }),
    });
    const result = await resp.json();
    if (resp.ok && result.success) {
      if (btn) {
        if (save) {
          btn.classList.add('btn-save-eval-active');
          btn.textContent = '📋 Save for Eval';
          showMsg('📋 Saved for evaluation set!');
        } else {
          btn.classList.remove('btn-save-eval-active');
          btn.textContent = 'Save for Eval';
          showMsg('↩️ Removed from eval set.');
        }
      }
    } else {
      showMsg('❌ ' + (result.error || 'Failed.'), true);
    }
  } catch (e) {
    console.error(e);
    showMsg('❌ Network error.', true);
  }
}

async function submitEdit(filename, feed, id) {
  const editArea = document.getElementById(`edit-${id}`);
  if (!editArea) return;
  const edited = editArea.value.trim();
  if (!edited) return;
  const msgEl = document.getElementById(`msg-${id}`);
  const showMsg = (msg, isErr) => {
    if (!msgEl) return;
    msgEl.textContent = msg; msgEl.className = isErr ? 'text-red-400 text-sm' : 'text-green-400 text-sm';
    msgEl.classList.remove('hidden'); setTimeout(() => msgEl.classList.add('hidden'), 3000);
  };
  try {
    const resp = await fetch("/scanner/submit_edit", {
      method: "POST", headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ filename, feed, transcript: edited })
    });
    if (resp.ok) {
      // Update or create the ✅ Edited block above the Original section
      const origContainer = document.getElementById(`pre-${id}`)?.closest('div')?.parentElement;
      if (origContainer) {
        let editedBlock = origContainer.querySelector('.edited-block');
        if (!editedBlock) {
          editedBlock = document.createElement('div');
          editedBlock.className = 'edited-block';
          origContainer.insertBefore(editedBlock, origContainer.querySelector('div:has(#pre-' + id + ')') || origContainer.firstChild);
        }
        editedBlock.innerHTML = `<div class="transcript-label text-green-400">✅ Edited</div><div class="transcript-block text-green-100/90">${_escHtml(edited)}</div>`;
      }
      // Activate Edited button
      const editedBtn = document.getElementById(`edited-btn-${id}`);
      if (editedBtn) editedBtn.classList.add('btn-edited-active');
      // Restore Looks Good (no longer dimmed)
      document.getElementById(`approve-${id}`)?.classList.remove('btn-dimmed');
      // Update orig label to show collapse toggle
      const origLabel = document.getElementById(`orig-label-${id}`);
      if (origLabel) origLabel.innerHTML = `🎧 Original — <button class="orig-toggle" onclick="toggleOriginal(${id})">show ▾</button>`;
      // Hide original pre (collapsed by default after edit)
      document.getElementById(`pre-${id}`)?.classList.add('hidden');
      // Seed textarea for future re-edits
      editArea.value = edited;
      showMsg('✔️ Edit saved!');
      document.getElementById(`save-${id}`)?.classList.add("hidden");
      document.getElementById(`cancel-${id}`)?.classList.add("hidden");
      document.getElementById(`edit-${id}`)?.classList.add("hidden");
    } else { showMsg('❌ Submission failed.', true); }
  } catch (e) { console.error(e); showMsg('❌ Network error.', true); }
}

// --- APPROVE (Looks Good) ---
async function toggleApprove(filename, feed, id) {
  const btn = document.getElementById(`approve-${id}`);
  const msgEl = document.getElementById(`msg-${id}`);
  const showMsg = (msg, isErr) => {
    if (!msgEl) return;
    msgEl.textContent = msg;
    msgEl.className = isErr ? 'text-red-400 text-sm' : 'text-green-400 text-sm';
    msgEl.classList.remove('hidden');
    setTimeout(() => msgEl.classList.add('hidden'), 3000);
  };

  const isCurrentlyApproved = btn && btn.classList.contains('btn-approve-active');
  const approve = !isCurrentlyApproved;

  try {
    const resp = await fetch('/scanner/approve_transcript', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ filename, feed, approve }),
    });
    const result = await resp.json();
    if (resp.ok && result.success) {
      if (btn) {
        if (approve) {
          btn.classList.add('btn-approve-active');
          btn.textContent = '✅ Looks Good';
          showMsg('✔️ Marked as good training data!');
        } else {
          btn.classList.remove('btn-approve-active');
          btn.textContent = 'Looks Good';
          showMsg('↩️ Approval removed.');
        }
      }
    } else {
      showMsg('❌ ' + (result.error || 'Failed to update.'), true);
    }
  } catch (e) {
    console.error(e);
    showMsg('❌ Network error.', true);
  }
}

// --- INTENT CLASSIFICATION ---
const policeIntentOptions = [
  "Traffic Stop","Medical","Disturbance","BOLO","Welfare Check",
  "Alarm","Suspicious Activity","MVA","Domestic","Theft","Beeps/Testing","Other"
];
const fireIntentOptions = [
  "Medical Aid","Structure Fire","Brush Fire","Alarm Activation","MVA",
  "Gas Leak","CO Detector","Service Call","Mutual Aid","Wires Down","Beeps/Testing","Other"
];
const dispositionOptions = ["Warning","Citation","Arrest","Taser Used","Shots Fired"];

function getIntentOptions() {
  const feed = _getCurrentFeed();
  return (feed && feed.includes('fd')) ? fireIntentOptions : policeIntentOptions;
}

function populateIntentOptions(index) {
  const container = document.getElementById(`intent-options-${index}`);
  if (!container || container.childElementCount > 0) return;
  populateCheckboxes(container, getIntentOptions());
  const feed = _getCurrentFeed();
  const dispContainer = document.getElementById(`disposition-options-${index}`);
  const dispParent = dispContainer?.closest('div');
  if (feed && !feed.includes('fd') && dispContainer) {
    populateCheckboxes(dispContainer, dispositionOptions);
    if (dispParent) dispParent.style.display = 'block';
  } else if (dispParent) { dispParent.style.display = 'none'; }
}

function populateCheckboxes(container, options) {
  options.forEach(option => {
    const label = document.createElement('label');
    label.className = "intent-checkbox-label";
    const cb = document.createElement('input');
    cb.type = 'checkbox'; cb.value = option;
    cb.className = 'h-4 w-4 rounded border-slate-600 bg-slate-800 text-purple-500 focus:ring-purple-600';
    label.appendChild(cb);
    const span = document.createElement('span');
    span.textContent = option;
    label.appendChild(span);
    container.appendChild(label);
  });
}

function toggleIntentForm(index) {
  const form = document.getElementById(`intent-form-${index}`);
  if (form) {
    populateIntentOptions(index);
    form.classList.toggle('hidden');
    if (!form.classList.contains('hidden')) prepopulateIntentForm(index);
  }
}

function prepopulateIntentForm(index) {
  const meta = callMetadata[index];
  if (!meta) return;
  if (meta.intents && Array.isArray(meta.intents)) {
    document.querySelectorAll(`#intent-options-${index} input[type="checkbox"]`).forEach(cb => {
      cb.checked = meta.intents.includes(cb.value);
    });
  }
  if (meta.dispositions && Array.isArray(meta.dispositions)) {
    document.querySelectorAll(`#disposition-options-${index} input[type="checkbox"]`).forEach(cb => {
      cb.checked = meta.dispositions.includes(cb.value);
    });
  }
  const off = document.getElementById(`officer-${index}`);
  if (off && meta.officer) off.value = meta.officer;
  const rd = document.getElementById(`road-${index}`);
  if (rd && meta.road) rd.value = meta.road;
  const nt = document.getElementById(`notes-${index}`);
  if (nt && meta.notes) nt.value = meta.notes;
}

async function submitIntent(filename, feed, index) {
  const intents = Array.from(document.querySelectorAll(`#intent-options-${index} input:checked`)).map(cb => cb.value);
  const dispositions = Array.from(document.querySelectorAll(`#disposition-options-${index} input:checked`)).map(cb => cb.value);
  const officer = document.getElementById(`officer-${index}`)?.value || "";
  const road = document.getElementById(`road-${index}`)?.value || "";
  const notes = document.getElementById(`notes-${index}`)?.value || "";

  const msgEl = document.getElementById(`msg-${index}`);
  const showMsg = (msg, isErr) => {
    if (!msgEl) return;
    msgEl.textContent = msg; msgEl.className = isErr ? 'text-red-400 text-sm' : 'text-green-400 text-sm';
    msgEl.classList.remove('hidden'); setTimeout(() => msgEl.classList.add('hidden'), 4000);
  };

  try {
    const resp = await fetch('/scanner/submit_intent', {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ filename, feed, intents, dispositions, officer, road, notes })
    });
    if (resp.ok) {
      showMsg('✅ Intent submitted!');
      if (!callMetadata[index]) callMetadata[index] = {};
      Object.assign(callMetadata[index], { intents, dispositions, officer, road, notes });
      toggleIntentForm(index);
    } else { showMsg('❌ Error submitting.', true); }
  } catch (e) { console.error(e); showMsg('❌ Network error.', true); }
}

// --- TIMESTAMP FORMATTING ---
function formatAllTimestamps() {
  document.querySelectorAll('.call-timestamp').forEach(el => {
    const raw = el.dataset.timestamp;
    const feed = el.dataset.feed;
    const match = raw && raw.match(/(\d{4}-\d{2}-\d{2})[ _](\d{2}-\d{2}-\d{2})/);
    if (!match) return;
    const dateStr = match[1];
    const timeStr = match[2].replace(/-/g, ':');
    const d = new Date(`${dateStr}T${timeStr}`);
    if (isNaN(d.getTime())) return;
    const formatted = d.toLocaleString('en-US', {
      month: 'long', day: 'numeric', year: 'numeric', hour: 'numeric', minute: '2-digit', hour12: true
    });
    el.textContent = `${formatted} (${(feed || '').toUpperCase()})`;
  });
}

// --- PULL-TO-REFRESH ---
function initPullToRefresh() {
  const mainEl = document.querySelector('.main-content-area');
  const indicatorEl = document.getElementById('pull-to-refresh-indicator');
  if (!mainEl || !indicatorEl) return;
  const span = indicatorEl.querySelector('span');
  if (!span) return;

  let startY = 0, pullDist = 0, refreshing = false;
  const threshold = 70;

  mainEl.addEventListener('touchstart', (e) => {
    if (mainEl.scrollTop === 0 && !refreshing) { startY = e.touches[0].clientY; indicatorEl.style.transition = 'transform 0s'; }
    else startY = 0;
  }, { passive: true });

  mainEl.addEventListener('touchmove', (e) => {
    if (startY === 0 || refreshing) return;
    pullDist = e.touches[0].clientY - startY;
    if (pullDist > 0) {
      e.preventDefault();
      indicatorEl.style.transform = `translateY(${Math.min(pullDist, threshold + 30)}px)`;
      span.textContent = pullDist > threshold ? 'Release to refresh' : 'Pull to refresh';
    }
  }, { passive: false });

  mainEl.addEventListener('touchend', () => {
    if (refreshing || startY === 0 || pullDist <= 0) return;
    indicatorEl.style.transition = 'transform 0.3s ease-out';
    if (pullDist > threshold) {
      refreshing = true;
      indicatorEl.style.transform = 'translateY(40px)';
      indicatorEl.innerHTML = '<svg class="loading-spinner w-4 h-4 inline-block -mt-1 mr-1" xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" stroke-width="2" stroke="currentColor"><path stroke-linecap="round" stroke-linejoin="round" d="M16.023 9.348h4.992v-.001M2.985 19.644v-4.992m0 0h4.992m-4.993 0l3.181 3.183a8.25 8.25 0 0013.803-3.7M4.031 9.865a8.25 8.25 0 0113.803-3.7l3.181 3.182m0-4.991v4.99"/></svg> Refreshing...';
      window.location.reload();
    } else {
      indicatorEl.style.transform = 'translateY(-100%)';
    }
    startY = 0; pullDist = 0;
  });
}

// --- WAVEFORM ENGINE ---
let _viewAudioCtx = null;
function _getActx() {
  if (!_viewAudioCtx || _viewAudioCtx.state === 'closed')
    _viewAudioCtx = new (window.AudioContext || window.webkitAudioContext)();
  return _viewAudioCtx;
}

const _viewWaveCache = {};
async function _fetchDecode(url) {
  if (_viewWaveCache[url]) return _viewWaveCache[url];
  const res = await fetch(url);
  if (!res.ok) throw new Error(`HTTP ${res.status}`);
  const buf = await res.arrayBuffer();
  const decoded = await _getActx().decodeAudioData(buf);
  _viewWaveCache[url] = decoded;
  return decoded;
}

function _fmtTime(s) {
  if (!isFinite(s) || s < 0) return '--:--';
  const m = Math.floor(s / 60);
  const sec = Math.floor(s % 60);
  return `${m}:${sec.toString().padStart(2, '0')}`;
}

function _drawPlaceholder(canvas, isFire) {
  const W = canvas.parentElement?.offsetWidth || 280;
  canvas.width = W;
  const H = canvas.height;
  const ctx = canvas.getContext('2d');
  ctx.clearRect(0, 0, W, H);
  ctx.fillStyle = isFire ? 'rgba(248,113,113,0.15)' : 'rgba(56,189,248,0.15)';
  const bars = Math.floor(W / 4);
  for (let i = 0; i < bars; i++) {
    const h = Math.max(2, Math.abs(Math.sin(i * 0.45) * H * 0.45 + Math.sin(i * 0.8) * H * 0.15) + 3);
    ctx.fillRect(i * 4, (H - h) / 2, 3, h);
  }
}

function _drawWave(canvas, audioBuffer, isFire, progress) {
  const W = canvas.parentElement?.offsetWidth || canvas.width || 280;
  canvas.width = W;
  const H = canvas.height;
  const ctx = canvas.getContext('2d');
  ctx.clearRect(0, 0, W, H);
  const data = audioBuffer.getChannelData(0);
  const step = 4, bars = Math.floor(W / step), blockSize = Math.floor(data.length / bars);
  const playedX = progress * W;
  const lit = isFire ? 'rgba(248,113,113,0.9)' : 'rgba(56,189,248,0.9)';
  const dim = isFire ? 'rgba(248,113,113,0.22)' : 'rgba(56,189,248,0.22)';
  for (let i = 0; i < bars; i++) {
    let sum = 0;
    const start = i * blockSize;
    for (let j = 0; j < blockSize; j++) sum += Math.abs(data[start + j] || 0);
    const barH = Math.max(2, (sum / blockSize) * H * 6);
    const x = i * step;
    ctx.fillStyle = x <= playedX ? lit : dim;
    ctx.fillRect(x, (H - barH) / 2, 3, barH);
  }
  if (progress > 0 && progress < 1) {
    ctx.fillStyle = 'rgba(255,255,255,0.7)';
    ctx.fillRect(Math.floor(playedX), 0, 1, H);
  }
}

let _viewActiveStop = null;

function initWaveformPlayer(playerEl) {
  const audioUrl = playerEl.dataset.src;
  const feed = playerEl.dataset.feed || '';
  if (!audioUrl) return;
  const isFire = _isFire(feed);
  const canvas = playerEl.querySelector('.wave-canvas');
  const playBtn = playerEl.querySelector('.wave-play-btn');
  const playIcon = playerEl.querySelector('.wave-play-icon');
  const timeEl = playerEl.querySelector('.wave-time');
  const scrub = playerEl.querySelector('.wave-scrub');
  if (!canvas) return;

  const card = playerEl.closest('.call-entry, .call-card-entry');
  let audioEl = card ? card.querySelector('audio.call-audio') : null;
  if (!audioEl) audioEl = new Audio(audioUrl);

  let decoded = null, animId = null, loading = false;
  requestAnimationFrame(() => _drawPlaceholder(canvas, isFire));

  function stopAnim() { if (animId) { cancelAnimationFrame(animId); animId = null; } }
  function tick() {
    if (!audioEl || audioEl.paused) { stopAnim(); return; }
    const prog = audioEl.currentTime / (audioEl.duration || 1);
    _drawWave(canvas, decoded, isFire, prog);
    if (timeEl) timeEl.textContent = _fmtTime(audioEl.currentTime);
    animId = requestAnimationFrame(tick);
  }
  function stopThis() {
    if (!audioEl.paused) audioEl.pause();
    stopAnim();
    if (playIcon) playIcon.textContent = '▶';
    if (decoded) _drawWave(canvas, decoded, isFire, audioEl.currentTime / (audioEl.duration || 1));
  }

  audioEl.addEventListener('ended', () => {
    stopAnim();
    if (playIcon) playIcon.textContent = '▶';
    if (decoded) _drawWave(canvas, decoded, isFire, 1);
    if (timeEl && audioEl.duration) timeEl.textContent = _fmtTime(audioEl.duration);
    _viewActiveStop = null;
  });
  audioEl.addEventListener('timeupdate', () => {
    if (timeEl && !animId) timeEl.textContent = _fmtTime(audioEl.currentTime);
  });

  async function startPlay() {
    const actx = _getActx();
    if (actx.state === 'suspended') await actx.resume();
    if (!audioEl.paused) { stopThis(); return; }
    if (_viewActiveStop && _viewActiveStop !== stopThis) _viewActiveStop();
    _viewActiveStop = stopThis;
    if (!decoded) {
      if (loading) return;
      loading = true;
      if (playIcon) playIcon.textContent = '⟳';
      try {
        decoded = await _fetchDecode(audioUrl);
        _drawWave(canvas, decoded, isFire, 0);
        if (timeEl && audioEl.duration) timeEl.textContent = _fmtTime(audioEl.duration);
      } catch (err) {
        console.error('[Wave]', err);
        if (playIcon) playIcon.textContent = '✕';
        loading = false; return;
      }
      loading = false;
    }
    audioEl.play().then(() => {
      if (playIcon) playIcon.textContent = '⏸';
      animId = requestAnimationFrame(tick);
    });
  }

  if (playBtn) playBtn.addEventListener('click', startPlay);
  scrub?.addEventListener('click', (e) => {
    if (!decoded || !audioEl.duration) { startPlay(); return; }
    const rect = canvas.getBoundingClientRect();
    const ratio = (e.clientX - rect.left) / rect.width;
    audioEl.currentTime = ratio * audioEl.duration;
    _drawWave(canvas, decoded, isFire, ratio);
    if (timeEl) timeEl.textContent = _fmtTime(audioEl.currentTime);
    if (audioEl.paused) startPlay();
  });
}

function initAllWaveformPlayers(root) {
  (root || document).querySelectorAll('.wave-player').forEach(initWaveformPlayer);
}

// --- MAIN DOMContentLoaded ---
document.addEventListener('DOMContentLoaded', () => {
  const mainArea = document.querySelector('main.main-content-area');
  const callsContainer = document.getElementById('calls-container');

  // Build header & stats
  buildPageHeader();
  loadFeedStats();

  // Event delegation
  if (callsContainer) {
    callsContainer.addEventListener('play', handleAudioPlay, true);
    callsContainer.addEventListener('click', handleCallAction);
  }

  // Lazy loading
  if (mainArea) {
    if (mainArea.scrollHeight <= mainArea.clientHeight) fetchMoreCalls();
    mainArea.addEventListener('scroll', () => {
      const { scrollTop, scrollHeight, clientHeight } = mainArea;
      if (scrollTop + clientHeight >= scrollHeight - 150 && !isLoading && !allCallsLoaded) fetchMoreCalls();
    });
  }

  // Address modal
  const modal = document.getElementById("address-modal");
  const closeBtn = document.getElementById("close-modal");
  const lookupBtn = document.getElementById("lookup-btn");
  const resultDiv = document.getElementById("lookup-result");

  if (modal && closeBtn && lookupBtn && resultDiv) {
    closeBtn.addEventListener("click", () => {
      modal.classList.add("hidden"); modal.classList.remove("flex");
      resultDiv.classList.add("hidden"); resultDiv.innerHTML = "";
      document.getElementById("street-input").value = "";
      document.getElementById("number-input").value = "";
    });
    modal.addEventListener('click', (e) => { if (e.target === modal) closeBtn.click(); });

    lookupBtn.addEventListener("click", async () => {
      const street = modal.querySelector("#street-input")?.value.trim();
      const number = modal.querySelector("#number-input")?.value.trim();
      if (!street || !number) {
        resultDiv.innerHTML = '<div class="text-red-400 mt-2">Please enter both street and number.</div>';
        resultDiv.classList.remove("hidden"); return;
      }
      resultDiv.innerHTML = '<div class="mt-3 text-blue-400">Searching...</div>';
      resultDiv.classList.remove("hidden");
      try {
        const res = await fetch(`/scanner/api/property?street=${encodeURIComponent(street)}&number=${encodeURIComponent(number)}&town=hopedale`);
        const data = await res.json();
        if (!res.ok) { resultDiv.innerHTML = `<div class="text-red-400 mt-2">${data.error || "No data found."}</div>`; return; }
        if (Array.isArray(data) && data.length > 0) {
          const prop = data[0];
          const acct = prop.parcel_url.split('=').pop();
          const url = `https://hopedale.patriotproperties.com/SearchResults.asp?SearchBy=Account&Account=Select&acct=${acct}`;
          resultDiv.innerHTML = `
            <div class="mt-3 panel-soft p-3">
              <div><span class="text-slate-400 text-xs uppercase tracking-wider">Owner:</span> <span class="text-white">${prop.owner}</span></div>
              <div class="mt-1"><span class="text-slate-400 text-xs uppercase tracking-wider">Address:</span> <span class="text-white">${prop.location}</span></div>
              <div class="mt-1"><span class="text-slate-400 text-xs uppercase tracking-wider">Value:</span> <span class="text-white">${prop.total_value}</span></div>
              <a href="${url}" target="_blank" class="inline-block mt-2 text-scannerBlue hover:underline text-sm">View full record →</a>
            </div>`;
        } else {
          resultDiv.innerHTML = '<div class="text-red-400 mt-2">No results found.</div>';
        }
      } catch (err) {
        resultDiv.innerHTML = `<div class="text-red-400 mt-2">Error: ${err.message}</div>`;
      }
    });
  }

  // Init everything
  formatAllTimestamps();
  initAutoUpdate();
  initPullToRefresh();
  initAllWaveformPlayers();

  // ?goto=hooks support
  if (new URLSearchParams(window.location.search).get('goto') === 'hooks') gotoFirstHook();
});
