// ===============================================================
// Scanner Review — Review Edited Calls page
// ===============================================================

// ── Helpers ──
function _revEsc(s) {
  if (s == null) return '';
  return String(s)
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;');
}
function _revIsFire(feed) { return (feed || '').toLowerCase().includes('fd'); }
function _revFmtTime(s) {
  if (!isFinite(s) || s < 0) return '--:--';
  return `${Math.floor(s / 60)}:${Math.floor(s % 60).toString().padStart(2, '0')}`;
}

// ── Waveform engine ──
let _revAudioCtx = null;
function _revGetACtx() {
  if (!_revAudioCtx || _revAudioCtx.state === 'closed')
    _revAudioCtx = new (window.AudioContext || window.webkitAudioContext)();
  return _revAudioCtx;
}
const _revWaveCache = {};
async function _revFetchDecode(url) {
  if (_revWaveCache[url]) return _revWaveCache[url];
  const res = await fetch(url);
  if (!res.ok) throw new Error(`HTTP ${res.status}`);
  const buf = await _revGetACtx().decodeAudioData(await res.arrayBuffer());
  _revWaveCache[url] = buf;
  return buf;
}
function _revDrawPlaceholder(canvas, fire) {
  const W = canvas.parentElement?.offsetWidth || 280;
  canvas.width = W;
  const H = canvas.height;
  const ctx = canvas.getContext('2d');
  ctx.clearRect(0, 0, W, H);
  ctx.fillStyle = fire ? 'rgba(248,113,113,0.15)' : 'rgba(56,189,248,0.15)';
  const bars = Math.floor(W / 4);
  for (let i = 0; i < bars; i++) {
    const h = Math.max(2, Math.abs(Math.sin(i * 0.45) * H * 0.45 + Math.sin(i * 0.8) * H * 0.15) + 3);
    ctx.fillRect(i * 4, (H - h) / 2, 3, h);
  }
}
function _revDrawWave(canvas, buf, fire, prog) {
  const W = canvas.parentElement?.offsetWidth || canvas.width || 280;
  canvas.width = W;
  const H = canvas.height;
  const ctx = canvas.getContext('2d');
  ctx.clearRect(0, 0, W, H);
  const data = buf.getChannelData(0);
  const step = 4, bars = Math.floor(W / step);
  const blockSize = Math.floor(data.length / bars);
  const playedX = prog * W;
  const lit = fire ? 'rgba(248,113,113,0.9)' : 'rgba(56,189,248,0.9)';
  const dim = fire ? 'rgba(248,113,113,0.22)' : 'rgba(56,189,248,0.22)';
  for (let i = 0; i < bars; i++) {
    let sum = 0;
    const start = i * blockSize;
    for (let j = 0; j < blockSize; j++) sum += Math.abs(data[start + j] || 0);
    const bH = Math.max(2, (sum / blockSize) * H * 6);
    ctx.fillStyle = i * step <= playedX ? lit : dim;
    ctx.fillRect(i * step, (H - bH) / 2, 3, bH);
  }
  if (prog > 0 && prog < 1) {
    ctx.fillStyle = 'rgba(255,255,255,0.7)';
    ctx.fillRect(Math.floor(playedX), 0, 1, H);
  }
}

let _revActiveStop = null;
function _revInitWaveformPlayer(el) {
  const url = el.dataset.src, feed = el.dataset.feed || '';
  if (!url) return;
  const fire = _revIsFire(feed);
  const canvas = el.querySelector('.wave-canvas');
  const playBtn = el.querySelector('.wave-play-btn');
  const playIcon = el.querySelector('.wave-play-icon');
  const timeEl = el.querySelector('.wave-time');
  const scrub = el.querySelector('.wave-scrub');
  if (!canvas) return;

  let audioEl = new Audio(url);
  let decoded = null, animId = null, loading = false;
  requestAnimationFrame(() => _revDrawPlaceholder(canvas, fire));

  function stopAnim() { if (animId) { cancelAnimationFrame(animId); animId = null; } }
  function tick() {
    if (!audioEl || audioEl.paused) { stopAnim(); return; }
    _revDrawWave(canvas, decoded, fire, audioEl.currentTime / (audioEl.duration || 1));
    if (timeEl) timeEl.textContent = _revFmtTime(audioEl.currentTime);
    animId = requestAnimationFrame(tick);
  }
  function stopThis() {
    if (!audioEl.paused) audioEl.pause();
    stopAnim();
    if (playIcon) playIcon.textContent = '▶';
    if (decoded) _revDrawWave(canvas, decoded, fire, audioEl.currentTime / (audioEl.duration || 1));
  }
  audioEl.addEventListener('ended', () => {
    stopAnim();
    if (playIcon) playIcon.textContent = '▶';
    if (decoded) _revDrawWave(canvas, decoded, fire, 1);
    if (timeEl && audioEl.duration) timeEl.textContent = _revFmtTime(audioEl.duration);
    _revActiveStop = null;
  });
  audioEl.addEventListener('timeupdate', () => {
    if (timeEl && !animId) timeEl.textContent = _revFmtTime(audioEl.currentTime);
  });

  async function startPlay() {
    const actx = _revGetACtx();
    if (actx.state === 'suspended') await actx.resume();
    if (!audioEl.paused) { stopThis(); return; }
    if (_revActiveStop && _revActiveStop !== stopThis) _revActiveStop();
    _revActiveStop = stopThis;
    if (!decoded) {
      if (loading) return;
      loading = true;
      if (playIcon) playIcon.textContent = '⟳';
      try {
        decoded = await _revFetchDecode(url);
        _revDrawWave(canvas, decoded, fire, 0);
        if (timeEl && audioEl.duration) timeEl.textContent = _revFmtTime(audioEl.duration);
      } catch (e) {
        console.error('[RevWave]', e);
        if (playIcon) playIcon.textContent = '✕';
        loading = false;
        return;
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
    const ratio = (e.clientX - canvas.getBoundingClientRect().left) / canvas.getBoundingClientRect().width;
    audioEl.currentTime = ratio * audioEl.duration;
    _revDrawWave(canvas, decoded, fire, ratio);
    if (timeEl) timeEl.textContent = _revFmtTime(audioEl.currentTime);
    if (audioEl.paused) startPlay();
  });
}

// ── State ──
let _revOffset = 0;
const _revLimit = 20;
let _revLoading = false;
let _revHasMore = true;
let _revTotalLoaded = 0;

// ── Render a single review card ──
function renderReviewCard(call) {
  const feed = call.feed || '';
  const fire = _revIsFire(feed);
  const isHook = false; // not tracked for this page
  const transcript = call.transcript || '(no transcript)';
  const editedTranscript = call.edited_transcript || '';
  const enhanced = call.enhanced_transcript || '';
  const timestamp = call.timestamp_human || call.timestamp || '';
  const address = call.derived_address || '';
  const addrConf = call.address_confidence || 'none';

  // Card ID from stem
  const stem = (call.file || '').replace(/^.*[\\/]/, '').replace(/\.[^.]+$/, '');
  const cardId = 'rev-' + stem;

  // Feed label
  let pillHTML;
  if (fire) pillHTML = '<span class="call-card-pill pill-fire"><span class="call-card-dot dot-fire"></span>Fire</span>';
  else pillHTML = '<span class="call-card-pill pill-police"><span class="call-card-dot"></span>Police</span>';

  // Feed name badge
  const feedBadge = feed
    ? `<span class="text-xs text-slate-600 font-mono uppercase">${_revEsc(feed)}</span>`
    : '';

  // Address badge
  let addressHTML = '';
  if (address && addrConf !== 'none') {
    addressHTML = `<div class="call-card-address"><svg viewBox="0 0 20 20" fill="currentColor"><path fill-rule="evenodd" d="M5.05 4.05a7 7 0 119.9 9.9L10 18.9l-4.95-4.95a7 7 0 010-9.9zM10 11a2 2 0 100-4 2 2 0 000 4z" clip-rule="evenodd"/></svg><span>${_revEsc(address)}</span></div>`;
  }

  // Transcripts
  let transcriptHTML = '';
  if (editedTranscript) {
    transcriptHTML += `<div class="rev-edited-block"><div class="transcript-label text-green-400">✅ Edited</div><div class="transcript-block text-green-100/90" style="font-family:inherit">${_revEsc(editedTranscript)}</div></div>`;
  }
  if (enhanced) {
    transcriptHTML += `<div><div class="transcript-label text-purple-400">✨ Enhanced</div><div class="transcript-block text-purple-100/90">${_revEsc(enhanced)}</div></div>`;
  }
  const origCollapseBtn = editedTranscript
    ? ` — <button class="orig-toggle" onclick="revToggleOriginal('${_revEsc(cardId)}')">show ▾</button>`
    : '';
  transcriptHTML += `
    <div>
      <div id="rev-orig-label-${_revEsc(cardId)}" class="transcript-label text-slate-500">🎧 Original${origCollapseBtn}</div>
      <pre id="rev-pre-${_revEsc(cardId)}" class="transcript-block text-slate-200${editedTranscript ? ' hidden' : ''}">${_revEsc(transcript)}</pre>
      <textarea id="rev-edit-${_revEsc(cardId)}" class="w-full intent-input hidden mt-2" rows="4">${_revEsc(editedTranscript || transcript)}</textarea>
    </div>
    <div class="rev-actions">
      <button class="rev-action-btn" onclick="revStartEdit('${_revEsc(cardId)}')">Edit</button>
      <button id="rev-save-${_revEsc(cardId)}" class="rev-action-btn hidden" onclick="revSubmitEdit('${_revEsc(call.file || '')}','${_revEsc(feed)}','${_revEsc(cardId)}')">Save</button>
      <button id="rev-cancel-${_revEsc(cardId)}" class="rev-action-btn hidden" onclick="revCancelEdit('${_revEsc(cardId)}')">Cancel</button>
      <button id="rev-edited-btn-${_revEsc(cardId)}" class="rev-action-btn${editedTranscript ? ' btn-edited-active' : ''}">Edited</button>
      <button id="rev-approve-${_revEsc(cardId)}" class="rev-action-btn btn-approve${editedTranscript ? ' btn-approve-active' : ''}" onclick="revToggleApprove('${_revEsc(call.file || '')}','${_revEsc(feed)}','${_revEsc(cardId)}')" title="Mark as good training data">${editedTranscript ? '✅ Looks Good' : 'Looks Good'}</button>
      <button id="rev-save-eval-${_revEsc(cardId)}" class="rev-action-btn${call.save_for_eval ? ' btn-save-eval-active' : ''}" onclick="revToggleSaveForEval('${_revEsc(call.file || '')}','${_revEsc(feed)}','${_revEsc(cardId)}')" title="Save as evaluation sample">${call.save_for_eval ? '📋 Save for Eval' : 'Save for Eval'}</button>
    </div>
    <div id="rev-msg-${_revEsc(cardId)}" class="text-green-400 text-sm hidden mt-1"></div>`;

  const div = document.createElement('div');
  div.className = 'review-call-card';
  div.id = `card-${_revEsc(cardId)}`;
  div.innerHTML = `
    <div class="flex items-center justify-between mb-2">
      <div class="flex items-center gap-2">${pillHTML} ${feedBadge}</div>
      <span class="text-xs text-slate-500 uppercase tracking-wider font-medium">${_revEsc(timestamp)}</span>
    </div>
    ${addressHTML}
    <div class="wave-player mt-2" data-src="${_revEsc(call.path)}" data-feed="${_revEsc(feed)}">
      <div class="wave-player-row">
        <button class="wave-play-btn w-8 h-8 rounded-full flex items-center justify-center transition flex-shrink-0
            ${fire ? 'bg-red-900/40 border border-red-700/50 text-red-300 hover:bg-red-800/50'
                   : 'bg-blue-900/40 border border-blue-700/50 text-blue-300 hover:bg-blue-800/50'}">
          <span class="wave-play-icon text-xs">▶</span>
        </button>
        <div class="flex-1 relative wave-scrub cursor-pointer">
          <canvas class="wave-canvas block w-full rounded" height="38"></canvas>
        </div>
        <span class="wave-time text-xs text-slate-500 tabular-nums w-10 text-right shrink-0">${call.duration > 0 ? _revFmtTime(call.duration) : '--:--'}</span>
      </div>
    </div>
    <div class="space-y-2 mt-2">${transcriptHTML}</div>
  `;

  // Init waveform player after insertion
  requestAnimationFrame(() => {
    const wp = div.querySelector('.wave-player');
    if (wp) _revInitWaveformPlayer(wp);
  });

  return div;
}

// ── Load a batch of calls ──
async function revLoadMore() {
  if (_revLoading || !_revHasMore) return;
  _revLoading = true;

  const spinner = document.getElementById('review-sentinel');
  if (spinner) spinner.classList.remove('hidden');

  try {
    const resp = await fetch(`/scanner/api/reviewed_calls?offset=${_revOffset}&limit=${_revLimit}`);
    if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
    const data = await resp.json();

    const container = document.getElementById('review-container');

    // Remove initial spinner on first load
    const initEl = document.getElementById('review-initial-spinner');
    if (initEl) initEl.remove();

    if (data.calls && data.calls.length > 0) {
      data.calls.forEach(call => {
        container.appendChild(renderReviewCard(call));
      });
      _revOffset += data.calls.length;
      _revTotalLoaded += data.calls.length;
      _revHasMore = data.has_more;

      // Update count label
      const countRow = document.getElementById('review-count-row');
      const countLabel = document.getElementById('review-count-label');
      if (countRow && countLabel) {
        countRow.classList.remove('hidden');
        countLabel.textContent = `${_revTotalLoaded} call${_revTotalLoaded !== 1 ? 's' : ''} loaded${_revHasMore ? '' : ' (all)'}`;
      }
    } else if (_revTotalLoaded === 0) {
      // No results at all
      const initEl2 = document.getElementById('review-initial-spinner');
      if (initEl2) initEl2.remove();
      container.innerHTML = `
        <div class="py-12 text-center text-slate-500">
          <p class="text-4xl mb-3">📋</p>
          <p class="font-semibold text-slate-400">No edited calls found</p>
          <p class="text-sm mt-1">Calls with edited transcripts since 2026-01-01 will appear here.</p>
        </div>`;
      _revHasMore = false;
    }

    if (!_revHasMore) {
      if (spinner) spinner.classList.add('hidden');
      // Hide the trigger so observer stops firing
      const trigger = document.getElementById('review-trigger');
      if (trigger) trigger.style.display = 'none';
      const endEl = document.getElementById('review-end');
      if (endEl && _revTotalLoaded > 0) endEl.classList.remove('hidden');
    }

  } catch (e) {
    console.error('[ReviewPage] Load error:', e);
    const initSpinner = document.getElementById('review-initial-spinner');
    if (initSpinner) {
      initSpinner.innerHTML = `<p class="text-red-400 text-sm">Failed to load calls. Please refresh.</p>`;
    }
  }

  if (spinner) spinner.classList.add('hidden');
  _revLoading = false;
}

// ── Infinite scroll observer ──
function _revSetupObserver() {
  const trigger = document.getElementById('review-trigger');
  if (!trigger || !('IntersectionObserver' in window)) return;
  // Use the scrollable <main> as the root so the observer fires within it
  const scrollRoot = document.querySelector('main.main-content-area') || null;
  const observer = new IntersectionObserver((entries) => {
    if (entries[0].isIntersecting && _revHasMore && !_revLoading) revLoadMore();
  }, { root: scrollRoot, rootMargin: '300px' });
  observer.observe(trigger);
}

// ── Edit helpers ──
function revStartEdit(cardId) {
  document.getElementById(`rev-pre-${cardId}`)?.classList.add('hidden');
  document.getElementById(`rev-edit-${cardId}`)?.classList.remove('hidden');
  document.getElementById(`rev-save-${cardId}`)?.classList.remove('hidden');
  document.getElementById(`rev-cancel-${cardId}`)?.classList.remove('hidden');
  document.getElementById(`rev-approve-${cardId}`)?.classList.add('btn-dimmed');
}

function revCancelEdit(cardId) {
  const hasEdited = document.getElementById(`rev-edited-btn-${cardId}`)?.classList.contains('btn-edited-active');
  document.getElementById(`rev-edit-${cardId}`)?.classList.add('hidden');
  document.getElementById(`rev-save-${cardId}`)?.classList.add('hidden');
  document.getElementById(`rev-cancel-${cardId}`)?.classList.add('hidden');
  if (!hasEdited) document.getElementById(`rev-pre-${cardId}`)?.classList.remove('hidden');
  document.getElementById(`rev-approve-${cardId}`)?.classList.remove('btn-dimmed');
}

function revToggleOriginal(cardId) {
  const pre = document.getElementById(`rev-pre-${cardId}`);
  const btn = document.querySelector(`#rev-orig-label-${cardId} .orig-toggle`);
  if (!pre) return;
  const hidden = pre.classList.toggle('hidden');
  if (btn) btn.textContent = hidden ? 'show ▾' : 'hide ▴';
}

async function revSubmitEdit(filename, feed, cardId) {
  const editArea = document.getElementById(`rev-edit-${cardId}`);
  if (!editArea) return;
  const edited = editArea.value.trim();
  if (!edited) return;
  const msgEl = document.getElementById(`rev-msg-${cardId}`);
  const showMsg = (msg, isErr) => {
    if (!msgEl) return;
    msgEl.textContent = msg;
    msgEl.style.color = isErr ? '#f87171' : '#4ade80';
    msgEl.classList.remove('hidden');
    setTimeout(() => msgEl.classList.add('hidden'), 3000);
  };
  try {
    const resp = await fetch('/scanner/submit_edit', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ filename, feed, transcript: edited }),
    });
    if (resp.ok) {
      // Update or create the edited block
      const card = document.getElementById(`card-${cardId}`);
      if (card) {
        let editedBlock = card.querySelector('.rev-edited-block');
        if (!editedBlock) {
          editedBlock = document.createElement('div');
          editedBlock.className = 'rev-edited-block';
          const spaceDiv = card.querySelector('.space-y-2');
          if (spaceDiv) spaceDiv.insertBefore(editedBlock, spaceDiv.firstChild);
        }
        editedBlock.innerHTML = `<div class="transcript-label text-green-400">✅ Edited</div><div class="transcript-block text-green-100/90" style="font-family:inherit">${_revEsc(edited)}</div>`;
      }
      document.getElementById(`rev-edited-btn-${cardId}`)?.classList.add('btn-edited-active');
      document.getElementById(`rev-approve-${cardId}`)?.classList.remove('btn-dimmed');
      const origLabel = document.getElementById(`rev-orig-label-${cardId}`);
      if (origLabel) origLabel.innerHTML = `🎧 Original — <button class="orig-toggle" onclick="revToggleOriginal('${cardId}')">show ▾</button>`;
      document.getElementById(`rev-pre-${cardId}`)?.classList.add('hidden');
      editArea.value = edited;
      revCancelEdit(cardId);
      showMsg('✔️ Edit saved!');
    } else {
      showMsg('❌ Save failed.', true);
    }
  } catch (e) {
    console.error(e);
    showMsg('❌ Network error.', true);
  }
}

async function revToggleApprove(filename, feed, cardId) {
  const btn = document.getElementById(`rev-approve-${cardId}`);
  const msgEl = document.getElementById(`rev-msg-${cardId}`);
  const showMsg = (msg, isErr) => {
    if (!msgEl) return;
    msgEl.textContent = msg;
    msgEl.style.color = isErr ? '#f87171' : '#4ade80';
    msgEl.classList.remove('hidden');
    setTimeout(() => msgEl.classList.add('hidden'), 3000);
  };
  const approve = !(btn && btn.classList.contains('btn-approve-active'));
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
      showMsg('❌ ' + (result.error || 'Failed.'), true);
    }
  } catch (e) {
    console.error(e);
    showMsg('❌ Network error.', true);
  }
}

async function revToggleSaveForEval(filename, feed, cardId) {
  const btn = document.getElementById(`rev-save-eval-${cardId}`);
  const msgEl = document.getElementById(`rev-msg-${cardId}`);
  const showMsg = (msg, isErr) => {
    if (!msgEl) return;
    msgEl.textContent = msg;
    msgEl.style.color = isErr ? '#f87171' : '#4ade80';
    msgEl.classList.remove('hidden');
    setTimeout(() => msgEl.classList.add('hidden'), 3000);
  };
  const save = !(btn && btn.classList.contains('btn-save-eval-active'));
  try {
    const resp = await fetch('/scanner/save_for_eval', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
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

// ── Boot ──
document.addEventListener('DOMContentLoaded', () => {
  revLoadMore();
  _revSetupObserver();
});
