// ===============================================================
// Scanner Archive Page Logic
// Merged features from scanner_view.js
// ===============================================================

// --- CONFIG (From scanner_view.js) ---
// Needed for the advanced shareCall function
const feedConfig = {
    'pd': { title: "Hopedale Police Scanner Archive" },
    'fd': { title: "Hopedale Fire Scanner Archive" },
    'mpd': { title: "Milford Police Scanner Archive" },
    'mfd': { title: "Milford Fire Scanner Archive" },
    'frkfd': { title: "Franklin Fire Scanner Archive" },
    'frkpd': { title: "Franklin Police Scanner Archive" },
    'bpd': { title: "Bellingham Police Scanner Archive" },
    'bfd': { title: "Bellingham Fire Scanner Archive" },
    'mndpd': { title: "Mendon Police Scanner Archive" },
    'mndfd': { title: "Mendon Fire Scanner Archive" },
    'blkpd': { title: "Blackstone Police Scanner Archive" },
    'blkfd': { title: "Blackstone Fire Scanner Archive" },
    'milpd': { title: "Millis Police Scanner Archive" },
    'milfd': { title: "Millis Fire Scanner Archive" },
    'medpd': { title: "Medway Police Scanner Archive" },
    'medfd': { title: "Medway Fire Scanner Archive" },
    'foxpd': { title: "Foxboro Police Scanner Archive" },
    'uptpd': { title: "Upton Police Scanner Archive" },
    'uptfd': { title: "Upton Fire Scanner Archive" },
    // ... any other feeds ...
};

// --- GLOBAL STATE ---
let callMetadata = {}; // For caching intent/classification data
let offset = window.initialCallsCount || 0;
const limit = 10;
let isLoading = false;
let allCallsLoaded = false;
let existingCallFiles = new Set();
let nextRenderIndex = 0;

// --- LAZY LOADING (From archive.js) ---
async function fetchMoreCalls() {
  if (isLoading || allCallsLoaded) return;

  isLoading = true;
  document.getElementById('loading-indicator').classList.remove('hidden');

  let data; // <-- FIX 1: Declare data outside the try block

  try {
    const currentFeed = new URLSearchParams(window.location.search).get('feed');
    const response = await fetch(`/scanner/api/archive_calls?feed=${currentFeed}&offset=${offset}&limit=${limit}`);
    if (!response.ok) throw new Error('Failed to fetch more calls');
    
    data = await response.json(); // <-- FIX 1: Assign to the outer variable

    if (data.calls && data.calls.length > 0) {
      data.calls.forEach((call, i) => {
        const newIndex = offset + i;
        callMetadata[newIndex] = call.metadata || {};
        const newCallElement = renderCall(call, newIndex);
        document.getElementById('calls-container').appendChild(newCallElement);
        const wp = newCallElement.querySelector('.wave-player');
        if (wp) initWaveformPlayer(wp);
      });
      offset += data.calls.length;

      // --- FIX 2: ADD A NULL CHECK HERE ---
      const totalCallsEl = document.getElementById('total-calls-count');
      if (totalCallsEl && data.total_count !== undefined) {
          totalCallsEl.textContent = data.total_count;
      }
      // --- END FIX 2 ---

    } else {
      allCallsLoaded = true;
      document.getElementById('loading-indicator').textContent = "No more calls to load.";
    }
  } catch (error) {
    console.error('Error fetching more calls:', error);
    document.getElementById('loading-indicator').textContent = "Error loading calls.";
  } finally {
    isLoading = false;
    // This 'if' block is now safe because 'data' is defined (even if it's 'undefined')
    if (allCallsLoaded || (data && data.calls.length === 0)) {
        // Keep the message visible
    } else {
        document.getElementById('loading-indicator').classList.add('hidden');
    }
  }
}

/**
 * Renders a single call entry.
 * This HTML is now based on scanner_view.js for feature parity
 * but uses data-action attributes for event delegation.
 */
function renderCall(call, index) {
  const isHook = (call.metadata && call.metadata.hook_request === '1');
  const callElement = document.createElement('div');
  callElement.className = isHook
    ? "group block bg-amber-950/40 backdrop-blur-sm p-6 rounded-2xl shadow-lg transition-all duration-300 ring-1 ring-amber-700/50 mb-6 call-entry"
    : "group block bg-gray-800/50 backdrop-blur-sm p-6 rounded-2xl shadow-lg transition-all duration-300 ring-1 ring-white/10 mb-6 call-entry";
  callElement.id = `call-card-${index}`;
  if (isHook) callElement.dataset.hook = '1';
  if (call.file) {
    callElement.dataset.file = call.file;
  }

  const playCount = (call.metadata && call.metadata.play_count) || 0;
  const playCountText = playCount === 1 ? 'time' : 'times';
  const playCountHTML = playCount > 0
      ? `<div id="playcount-${index}" class="transition-all duration-200">👂 Played: ${playCount} ${playCountText}</div>`
      : `<div id="playcount-${index}" class="hidden"></div>`;

  const timestampHuman = call.timestamp_human || 'Unknown time';
  const feedDisplay = call.feed || '';
  const callPath = call.path || '#';
  const callFile = call.file || '';
  const callFeed = call.feed || '';
  const enhancedTranscript = (call.metadata && call.metadata.enhanced_transcript) || '';
  const editedTranscript = (call.metadata && call.metadata.edited_transcript) || '';
  const editPending = call.edit_pending || false;
  const originalTranscript = call.transcript || 'Transcript not available';

  const hookBadgeHTML = isHook
    ? `<div class="flex items-center gap-2 text-amber-400 text-xs font-semibold mb-2"><span>🪝 Hook / Tow Request</span></div>`
    : '';

  // logic for transcript sections
  let transcriptHTML = '';

  // 1. Enhanced Transcript
  if (enhancedTranscript) {
    transcriptHTML += `
      <div>
        <div class="text-purple-400 text-sm">✨ Enhanced Transcript</div>
        <pre class="whitespace-pre-wrap bg-purple-900/50 p-3 rounded-md text-sm text-purple-100 overflow-auto">${enhancedTranscript}</pre>
      </div>`;
  }

  // 2. Pending (Matches Jinja Fix 1/2: if pending AND has transcript)
  if (editPending && editedTranscript) {
    transcriptHTML += `
      <div>
        <div class="text-yellow-400 text-sm">✏️ Edit Pending</div>
        <pre class="whitespace-pre-wrap bg-yellow-900/50 p-3 rounded-md text-sm text-yellow-100 overflow-auto">${editedTranscript}</pre>
      </div>`;
  }
  // 3. Edited (Matches Jinja Fix 3: if has transcript AND NOT pending)
  else if (editedTranscript && !editPending) {
    transcriptHTML += `
      <div>
        <div class="text-green-400 text-sm">✅ Edited Transcript</div>
        <pre class="whitespace-pre-wrap bg-green-900/50 p-3 rounded-md text-sm text-green-100 overflow-auto">${editedTranscript}</pre>
      </div>`;
  }

  callElement.innerHTML = `
    ${hookBadgeHTML}
    <div class="flex justify-between items-center text-sm text-gray-400 mb-1">
      <div>${timestampHuman} ${feedDisplay}</div>
      ${playCountHTML}
    </div>
    <audio class="call-audio hidden" src="${callPath}" data-filename="${callFile}" data-feed="${callFeed}" data-index="${index}"></audio>
    <div class="wave-player mb-3" data-src="${callPath}" data-feed="${callFeed}">
      <div class="flex items-center gap-3 bg-slate-800/60 rounded-xl px-3 py-2">
        <button class="wave-play-btn w-9 h-9 rounded-full flex items-center justify-center transition
            ${callFeed.toLowerCase().includes('fd')
              ? 'bg-red-900/40 border border-red-700/50 text-red-300 hover:bg-red-800/50'
              : 'bg-blue-900/40 border border-blue-700/50 text-blue-300 hover:bg-blue-800/50'}">
          <span class="wave-play-icon text-xs">▶</span>
        </button>
        <div class="flex-1 relative wave-scrub cursor-pointer">
          <canvas class="wave-canvas block w-full rounded" height="44"></canvas>
        </div>
        <span class="wave-time text-xs text-slate-500 tabular-nums w-10 text-right shrink-0">--:--</span>
      </div>
    </div>
    <div class="space-y-4">
      <div class="space-y-4">

        ${transcriptHTML}


      <div>
        <div class="text-gray-400 text-sm">🎧 Original Transcript</div>
        <pre id="pre-${index}" class="whitespace-pre-wrap bg-slate-800 p-3 rounded-md text-sm text-gray-200 overflow-auto">${originalTranscript}</pre>
        <textarea id="edit-${index}" class="w-full bg-gray-800 text-sm p-3 rounded-md text-white border border-gray-600 hidden" rows="4">${originalTranscript}</textarea>
        <div class="flex gap-4 mt-2">
          <button data-action="edit" data-index="${index}" class="text-yellow-400 hover:underline text-sm">Edit</button>
          <button data-action="save" data-file="${callFile}" data-feed="${callFeed}" data-index="${index}" id="save-${index}" class="hidden text-green-400 hover:underline text-sm">Submit</button>
          <button data-action="cancel" data-index="${index}" id="cancel-${index}" class="hidden text-red-400 hover:underline text-sm">Cancel</button>
          <button data-action="classify" data-index="${index}" class="text-purple-400 hover:underline text-sm">Classify</button>
          <button data-action="address-lookup" data-index="${index}" class="text-cyan-400 hover:underline text-sm">Address</button>
          <button data-action="share" data-index="${index}" data-feed="${callFeed}" class="text-blue-400 hover:underline text-sm ml-auto">Share</button>
        </div>
        <div id="msg-${index}" class="text-green-400 text-sm hidden">✔️ Thank you for your submission!</div>
      </div>

      <div id="intent-form-${index}" class="hidden mt-4 pt-4 border-t border-gray-700/50">
        <h4 class="text-md font-semibold text-purple-300 mb-3">Classify Call Intent</h4>
        <div class="space-y-4">
          <div>
            <label class="block text-sm font-medium text-gray-300 mb-2">Intents</label>
            <div id="intent-options-${index}" class="grid grid-cols-2 sm:grid-cols-3 gap-2">
            </div>
          </div>
          <div>
            <label class="block text-sm font-medium text-gray-300 mb-2">Disposition</label>
            <div id="disposition-options-${index}" class="grid grid-cols-2 sm:grid-cols-3 gap-2">
            </div>
          </div>

          <div class="grid grid-cols-1 sm:grid-cols-2 gap-4">
            <div>
              <label for="officer-${index}" class="block text-sm font-medium text-gray-300">Officer/Unit Tag</label>
              <input type="text" id="officer-${index}" class="mt-1 block w-full bg-gray-900/50 border-gray-700 rounded-md shadow-sm py-2 px-3 text-white focus:ring-purple-500 focus:border-purple-500 sm:text-sm" placeholder="e.g., 303, Car 4">
            </div>
            <div>
              <label for="road-${index}" class="block text-sm font-medium text-gray-300">Road/Street</label>
              <input type="text" id="road-${index}" class="mt-1 block w-full bg-gray-900/50 border-gray-700 rounded-md shadow-sm py-2 px-3 text-white focus:ring-purple-500 focus:border-purple-500 sm:text-sm" placeholder="e.g., Main St, Rt 140">
            </div>
          </div>
          <div>
            <label for="notes-${index}" class="block text-sm font-medium text-gray-300">Additional Notes</label>
            <textarea id="notes-${index}" rows="2" class="mt-1 block w-full bg-gray-900/50 border-gray-700 rounded-md shadow-sm py-2 px-3 text-white focus:ring-purple-500 focus:border-purple-500 sm:text-sm" placeholder="Any other relevant details..."></textarea>
          </div>
          <div class="flex justify-end gap-3">
            <button data-action="cancel-intent" data-index="${index}" class="px-4 py-2 text-sm font-medium text-gray-300 bg-gray-700 rounded-md hover:bg-gray-600 transition">Cancel</button>
            <button data-action="submit-intent" data-file="${callFile}" data-feed="${callFeed}" data-index="${index}" class="px-4 py-2 text-sm font-medium text-white bg-purple-600 rounded-md hover:bg-purple-700 transition">Submit Intent</button>
          </div>
        </div>
      </div>
    </div>
  `;
  return callElement;
}

function seedExistingCalls() {
  const audioEls = document.querySelectorAll('.call-audio[data-filename]');
  let maxIndex = 0;
  audioEls.forEach((el) => {
    const file = el.dataset.filename;
    if (file) existingCallFiles.add(file);
    const index = Number.parseInt(el.dataset.index || '0', 10);
    if (!Number.isNaN(index)) maxIndex = Math.max(maxIndex, index);
  });
  nextRenderIndex = Math.max(maxIndex + 1, offset);
}

function prependNewCalls(calls) {
  const container = document.getElementById('calls-container');
  if (!container) return;
  const ordered = [...calls].reverse();
  ordered.forEach((call) => {
    const newIndex = nextRenderIndex++;
    callMetadata[newIndex] = call.metadata || {};
    const newCallElement = renderCall(call, newIndex);
    container.insertBefore(newCallElement, container.firstChild);
    if (call.file) existingCallFiles.add(call.file);
  });
  offset += calls.length;
  if (typeof formatAllTimestamps === 'function') {
    formatAllTimestamps();
  }
}

async function checkForNewCalls() {
  if (isLoading) return;
  const currentFeed = new URLSearchParams(window.location.search).get('feed');
  if (!currentFeed) return;

  try {
    const response = await fetch(`/scanner/api/archive_calls?feed=${currentFeed}&offset=0&limit=${limit}`, { cache: 'no-store' });
    if (!response.ok) return;
    const data = await response.json();
    if (!data.calls || !data.calls.length) return;

    const newCalls = data.calls.filter((call) => call.file && !existingCallFiles.has(call.file));
    if (newCalls.length) {
      prependNewCalls(newCalls);
    }
  } catch (error) {
    console.warn('Auto-update failed:', error);
  }
}

function initAutoUpdate() {
  seedExistingCalls();
  checkForNewCalls();
  setInterval(checkForNewCalls, 30000);
  loadHooksCounter();
  setInterval(loadHooksCounter, 60000);
}

async function loadHooksCounter() {
  const currentFeed = new URLSearchParams(window.location.search).get('feed');
  if (!currentFeed || !currentFeed.toLowerCase().includes('pd')) return;

  const counterEl = document.getElementById('hooks-counter');
  const valueEl = document.getElementById('hooks-counter-value');
  const btn = document.getElementById('hooks-counter-btn');
  if (!counterEl || !valueEl) return;

  try {
    const resp = await fetch('/scanner/api/today_counts');
    if (!resp.ok) return;
    const data = await resp.json();
    const hooksCount = data[currentFeed]?.hooks_count || 0;
    valueEl.textContent = hooksCount;
    if (hooksCount > 0) {
      counterEl.classList.remove('hidden');
      if (btn) btn.addEventListener('click', () => scrollToFirstHook());
    }
  } catch (e) {
    // silently fail — counter stays hidden
  }
}

async function gotoFirstHook() {
  // Check initial server-rendered cards first
  if (document.querySelector('[data-hook="1"]')) {
    scrollToFirstHook();
    return;
  }
  // Lazy-load pages of calls until a hook card appears (max 25 pages).
  // Wait for any in-progress fetch to finish before each attempt so we
  // don't hit the isLoading guard and spin through iterations empty-handed.
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
    // getBoundingClientRect is relative to viewport; convert to scroll position
    const hookRect = hookEl.getBoundingClientRect();
    const mainRect = mainArea.getBoundingClientRect();
    mainArea.scrollTo({ top: mainArea.scrollTop + hookRect.top - mainRect.top - 80, behavior: 'smooth' });
  }
  // briefly flash the card to draw the eye
  hookEl.style.transition = 'box-shadow 0.3s';
  hookEl.style.boxShadow = '0 0 0 3px #f59e0b';
  setTimeout(() => { hookEl.style.boxShadow = ''; }, 1500);
}

// --- DOMCONTENTLOADED (From archive.js, with additions) ---
document.addEventListener('DOMContentLoaded', () => {
    const mainContentArea = document.querySelector('main.main-content-area');
    const callsContainer = document.getElementById('calls-container');
    const pullToRefreshIndicator = document.getElementById('pull-to-refresh-indicator');

    if (!mainContentArea || !callsContainer || !pullToRefreshIndicator) {
        console.error("Critical DOM elements for scanner_archive.js not found. Aborting script.");
        return;
    }
    
    // **ADDED**: Play count listener (from scanner_view.js)
    if (callsContainer) {
        callsContainer.addEventListener('play', handleAudioPlay, true);
    }
    
    // **ADDED**: Master click listener (from archive.js)
    if (callsContainer) {
        callsContainer.addEventListener('click', handleCallAction);
    }

    // --- LAZY LOADING LOGIC (From archive.js) ---
    if (mainContentArea.scrollHeight <= mainContentArea.clientHeight) {
        fetchMoreCalls();
    }
    mainContentArea.addEventListener('scroll', () => {
        const { scrollTop, scrollHeight, clientHeight } = mainContentArea;
        if (scrollTop + clientHeight >= scrollHeight - 150 && !isLoading && !allCallsLoaded) {
            fetchMoreCalls();
        }
    });

    // --- PULL-TO-REFRESH LOGIC (From archive.js) ---
    let startY = 0;
    let pullDistance = 0;
    let refreshing = false;
    const pullThreshold = 80;

    mainContentArea.addEventListener('touchstart', (e) => {
        if (mainContentArea.scrollTop === 0 && !refreshing) {
            startY = e.touches[0].clientY;
            pullToRefreshIndicator.style.transition = 'none';
        }
    }, { passive: true });

    mainContentArea.addEventListener('touchmove', (e) => {
        if (mainContentArea.scrollTop === 0 && !refreshing) {
            const currentY = e.touches[0].clientY;
            pullDistance = currentY - startY;

            if (pullDistance > 0) {
                e.preventDefault(); 
                pullToRefreshIndicator.style.transform = `translateY(${pullDistance}px)`;
                const indicatorText = pullToRefreshIndicator.querySelector('span');
                if (indicatorText) {
                    if (pullDistance > pullThreshold) {
                        indicatorText.textContent = 'Release to refresh';
                    } else {
                        indicatorText.textContent = 'Pull to refresh';
                    }
                }
            }
        }
    }, { passive: false }); 

    mainContentArea.addEventListener('touchend', async () => {
        if (refreshing) return;
        pullToRefreshIndicator.style.transition = 'transform 0.3s ease-out';
        if (pullDistance > pullThreshold) {
            refreshing = true;
            pullToRefreshIndicator.innerHTML = '<svg class="loading-spinner h-5 w-5 animate-spin" xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" stroke="currentColor"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M4 4v5h.582m15.356 2A8.001 8.001 0 0020 13a8.001 8.001 0 00-2.928-6.116L16 10V4m-4 4l-1.5 1.5M12 12l-1.5 1.5M12 12l1.5 1.5M12 12l-1.5-1.5M12 12l1.5-1.5"/></svg> <span>Refreshing...</span>';
            pullToRefreshIndicator.style.transform = 'translateY(0)';
            
            callsContainer.innerHTML = '';
            offset = 0;
            allCallsLoaded = false;
            await fetchMoreCalls();
            refreshing = false;
        }
        pullToRefreshIndicator.style.transform = 'translateY(-100%)';
        // Reset indicator text after animation
        setTimeout(() => {
            pullToRefreshIndicator.innerHTML = '<span>Pull to refresh</span>';
        }, 300);
        pullDistance = 0;
    });

// === ADDRESS LOOKUP MODAL ===
    const modal = document.getElementById("address-modal");
    // const openBtn = document.getElementById("address-lookup-btn"); // <-- We remove this line
    const closeBtn = document.getElementById("close-modal");
    const lookupBtn = document.getElementById("lookup-btn");
    const resultDiv = document.getElementById("lookup-result");

    // Check for the modal and its core parts
    if (modal && closeBtn && lookupBtn && resultDiv) {

        
        // --- YOUR EXISTING MODAL LOGIC (UNCHANGED) ---
// --- YOUR EXISTING MODAL LOGIC (UNCHANGED) ---
        closeBtn.addEventListener("click", () => {
          modal.classList.add("hidden");
          modal.classList.remove("flex");
          resultDiv.classList.add("hidden");
          resultDiv.innerHTML = "";
          // Clear inputs too
          document.getElementById("street-input").value = "";
          document.getElementById("number-input").value = "";
          document.getElementById("town-select").selectedIndex = 0; // Resets to Hopedale
        });

        // Close by clicking the background overlay
        modal.addEventListener('click', (e) => {
          if (e.target === modal) {
            closeBtn.click(); // Just trigger the close button's logic
          }
        });

      lookupBtn.addEventListener("click", async () => {
          // --- FIX 3: Search INSIDE the modal element ---
          const streetInput = modal.querySelector("#street-input");
          const numberInput = modal.querySelector("#number-input");

          // Add a check in case they're still not found
          if (!streetInput || !numberInput) {
            console.error("Could not find street or number input inside the modal!");
            resultDiv.innerHTML = `<div class="text-red-400 mt-2">Error: Page element missing.</div>`;
            resultDiv.classList.remove("hidden");
            return;
          }

          const street = streetInput.value.trim();
          const number = numberInput.value.trim();
          // --- END FIX 3 ---

          if (!street || !number) {
            resultDiv.innerHTML = `<div class="text-red-400 mt-2">Please enter both street and number.</div>`;
            resultDiv.classList.remove("hidden");
            return;
          }

          resultDiv.innerHTML = `<div class"mt-3 text-blue-400">Searching...</div>`;
          resultDiv.classList.remove("hidden");
          try {
            // Fetch is hardcoded to 'hopedale'
            const res = await fetch(`/scanner/api/property?street=${encodeURIComponent(street)}&number=${encodeURIComponent(number)}&town=hopedale`);
            const data = await res.json();
            if (!res.ok) {
              resultDiv.innerHTML = `<div class="text-red-400 mt-2">${data.error || "No data found."}</div>`;
              return;
            }
          if (Array.isArray(data) && data.length > 0) {
              const prop = data[0];
              
              // 1. Get the account number from their URL
              // (e.g., "https://.../Summary.asp?AccountNumber=1688")
              const accountNumber = prop.parcel_url.split('=').pop();

              // 2. Build the NEW search-friendly URL
              // (Replace this with the URL you just found)
              const searchUrl = `https://hopedale.patriotproperties.com/SearchResults.asp?SearchBy=Account&Account=Select&acct=${accountNumber}`;

              resultDiv.innerHTML = `
                <div class="mt-3 bg-[#111827] p-3 rounded-lg border border-slate-700">
                  <div><span class="text-slate-400">Owner:</span> ${prop.owner}</div>
                  <div><span class="text-slate-400">Address:</span> ${prop.location}</div>
                  <div><span class="text-slate-400">Value:</span> ${prop.total_value}</div>
  
                  <div>
                    <a href="${searchUrl}" 
                       target="_blank" 
                       class="text-blue-400 hover:underline">
                      View full record →
                    </a>
                  </div>
                </div>`;
            } else {
              resultDiv.innerHTML = `<div class="text-red-400 mt-2">No results found.</div>`;
            }
          } catch (err) {
            resultDiv.innerHTML = `<div class"text-red-400 mt-2">Error: ${err.message}</div>`;
          }
        });
    }
});

// --- EVENT DELEGATION (From archive.js) ---
function handleCallAction(event) {
  const target = event.target;
  const action = target.dataset.action;
  const index = target.dataset.index;
  const file = target.dataset.file;
  const feed = target.dataset.feed;

  if (!action) return; // Not a button we care about

  switch (action) {
    case 'edit':
      enableEdit(index);
      break;
    case 'save':
      submitEdit(file, feed, index);
      break;
    case 'cancel':
      cancelEdit(index);
      break;
    case 'classify':
      toggleIntentForm(index);
      break;
    case 'submit-intent':
      submitIntent(file, feed, index);
      break;
    case 'share':
      shareCall(index, feed);
      break;
    
    // ADD THIS NEW CASE address lookup
case 'address-lookup':
      const modal = document.getElementById("address-modal");
      if (modal) {
        event.preventDefault();
        
        const index = target.dataset.index;
        const transcriptEl = document.getElementById(`pre-${index}`);
        const numberInput = document.getElementById("number-input");
        const streetInput = document.getElementById("street-input");

        // 1. Clear old values
        numberInput.value = "";
        streetInput.value = "";


        modal.classList.remove("hidden");
        modal.classList.add("flex");
        
        // Focus the first empty field
        if (numberInput.value === "") {
            numberInput.focus();
        } else if (streetInput.value === "") {
            streetInput.focus();
        }
      }
      break;
  }
}

// --- PLAY COUNT (From scanner_view.js) ---
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
                const unit = data.play_count === 1 ? "time" : "times";
                playCountEl.textContent = `👂 Played: ${data.play_count} ${unit}`;
                playCountEl.classList.add("text-green-400");
                setTimeout(() => playCountEl.classList.remove("text-green-400"), 1200);
            }
        } else {
            console.error(`Server error on incrementing play count for ${filename}:`, await resp.text());
        }
    } catch (err) {
        console.error(`Network or client-side parsing error for play counter on ${filename}:`, err);
    }
}

// --- SHARE FUNCTIONALITY (From scanner_view.js) ---
async function shareCall(index, feed) {
    const callCard = document.getElementById(`call-card-${index}`);
    if (!callCard) return; 
    const audioEl = callCard.querySelector('audio');
    const preEl = callCard.querySelector(`#pre-${index}`);
    if (!audioEl || !preEl) return; 

    const fullTranscript = preEl.innerText;
    const feedTitle = feedConfig[feed]?.title || 'Scanner';
    const branding = "\n\n---\nSent from the Command Center";
    const msgEl = document.getElementById(`msg-${index}`);

    const showMessage = (message, isError = false) => {
        if (!msgEl) return; 
        msgEl.textContent = message;
        msgEl.className = isError ? 'text-red-400 text-sm' : 'text-green-400 text-sm';
        msgEl.classList.remove('hidden');
        setTimeout(() => {
            msgEl.classList.add('hidden');
            msgEl.textContent = '✔️ Thank you for your submission!'; // Reset text
            msgEl.className = 'text-green-400 text-sm hidden'; // Reset class
        }, 3000);
    };

    const modalHtml = `
      <div id="share-modal-${index}" class="fixed inset-0 bg-black/60 backdrop-blur-sm flex items-center justify-center z-50">
        <div class="bg-gray-800 rounded-xl p-6 w-full max-w-md shadow-2xl ring-1 ring-white/10">
          <h3 class="text-lg font-semibold mb-4 text-gray-100">Share this Call</h3>
          <div class="space-y-2">
            ${navigator.share && navigator.canShare ? `<button id="share-both-btn" class="w-full text-left p-3 bg-gray-700/50 hover:bg-gray-700 rounded-lg transition flex items-center gap-3"><span class="text-lg">🔊</span> <span class="text-gray-200">Share Audio & Transcript</span></button>` : ''}
            ${navigator.share && navigator.canShare ? `<button id="share-audio-btn" class="w-full text-left p-3 bg-gray-700/50 hover:bg-gray-700 rounded-lg transition flex items-center gap-3"><span class="text-lg">🎵</span> <span class="text-gray-200">Share Audio Only</span></button>` : ''}
            <button id="copy-text-btn" class="w-full text-left p-3 bg-gray-700/50 hover:bg-gray-700 rounded-lg transition flex items-center gap-3"><span class="text-lg">📝</span> <span class="text-gray-200">Copy Transcript</span></button>
          </div>
          <button id="close-modal-btn" class="mt-6 w-full p-2 bg-gray-900/50 hover:bg-gray-900/80 rounded-lg transition text-gray-400">Close</button>
        </div>
      </div>
    `;
    document.body.insertAdjacentHTML('beforeend', modalHtml);

    const modal = document.getElementById(`share-modal-${index}`);
    if (!modal) return; 
    
    const closeModal = () => modal.remove();

    const shareBothBtn = document.getElementById('share-both-btn');
    if (shareBothBtn) {
        shareBothBtn.onclick = async () => {
            try {
                const response = await fetch(audioEl.src);
                if (!response.ok) throw new Error(`Failed to fetch audio: ${response.statusText}`);
                const blob = await response.blob();
                const audioFile = new File([blob], `${feed}-call.wav`, { type: 'audio/wav' });
                if (navigator.canShare && navigator.canShare({ files: [audioFile] })) {
                    await navigator.share({
                        title: `Scanner Call: ${feedTitle}`,
                        text: `${fullTranscript}${branding}`,
                        files: [audioFile],
                    });
                } else {
                    showMessage('Sharing files is not supported on this device.', true);
                }
            } catch (err) {
                console.error('Share both failed:', err);
                showMessage('❌ Share failed.', true);
            }
            closeModal();
        };
    }

    const shareAudioBtn = document.getElementById('share-audio-btn');
    if (shareAudioBtn) {
        shareAudioBtn.onclick = async () => {
            try {
                const response = await fetch(audioEl.src);
                 if (!response.ok) throw new Error(`Failed to fetch audio: ${response.statusText}`);
                const blob = await response.blob();
                const audioFile = new File([blob], `${feed}-call.wav`, { type: 'audio/wav' });
                if (navigator.canShare && navigator.canShare({ files: [audioFile] })) {
                    await navigator.share({
                        title: `Scanner Audio: ${feedTitle}`,
                        files: [audioFile],
                    });
                } else {
                    showMessage('Sharing files is not supported on this device.', true);
                }
            } catch (err) {
                console.error('Share audio failed:', err);
                showMessage('❌ Share failed.', true);
            }
            closeModal();
        };
    }

    const copyTextBtn = document.getElementById('copy-text-btn');
    if (copyTextBtn) {
        copyTextBtn.onclick = async () => {
            try {
                if (navigator.clipboard && navigator.clipboard.writeText) {
                    await navigator.clipboard.writeText(`${fullTranscript}${branding}`);
                    showMessage('✔️ Transcript copied to clipboard!');
                } else {
                    const textArea = document.createElement("textarea");
                    textArea.value = `${fullTranscript}${branding}`;
                    textArea.style.position = "fixed"; 
                    document.body.appendChild(textArea);
                    textArea.focus();
                    textArea.select();
                    try {
                        document.execCommand('copy');
                        showMessage('✔️ Transcript copied to clipboard!');
                    } catch (err) {
                         showMessage('❌ Failed to copy transcript.', true);
                    }
                    document.body.removeChild(textArea);
                }
            } catch (err) {
                 showMessage('❌ Failed to copy transcript.', true);
            }
            closeModal();
        };
    }

    const closeModalBtn = document.getElementById('close-modal-btn');
    if (closeModalBtn) closeModalBtn.onclick = closeModal;
    
    modal.onclick = (e) => {
        if (e.target === modal) {
            closeModal();
        }
    };
}

// --- AUTH HELPER (From scanner_view.js) ---
// Assumes isLoggedIn is a global variable set by another script
function showAuthMessage(index) {
    const msgEl = document.getElementById(`msg-${index}`);
    if (msgEl) {
     //   msgEl.textContent = "❌ You must be logged in to do that.";
        msgEl.className = "text-red-400 text-sm";
        msgEl.classList.remove("hidden");
        setTimeout(() => {
            msgEl.classList.add("hidden");
             msgEl.textContent = "✔️ Thank you for your submission!"; // Reset
             msgEl.className = "text-green-400 text-sm hidden"; // Reset
        }, 3000);
    }
}


// --- EDIT TRANSCRIPT (From scanner_view.js) ---
function enableEdit(id) {
  // remove this check if you always want to allow edits
     showAuthMessage(id);
  //if (typeof isLoggedIn !== 'undefined' && !isLoggedIn) {
  //     showAuthMessage(id);
  //      return;
    
    document.getElementById(`pre-${id}`)?.classList.add("hidden");
    document.getElementById(`edit-${id}`)?.classList.remove("hidden");
    document.getElementById(`save-${id}`)?.classList.remove("hidden");
    document.getElementById(`cancel-${id}`)?.classList.remove("hidden");
    document.getElementById(`msg-${id}`)?.classList.add("hidden");
}

function cancelEdit(id) {
    const pre = document.getElementById(`pre-${id}`);
    const edit = document.getElementById(`edit-${id}`);
    if (pre && edit) {
      edit.value = pre.innerText.trim();
      edit.classList.add("hidden");
      pre.classList.remove("hidden");
    }
    document.getElementById(`save-${id}`)?.classList.add("hidden");
    document.getElementById(`cancel-${id}`)?.classList.add("hidden");
}

async function submitEdit(filename, feed, id) {
    const editArea = document.getElementById(`edit-${id}`);
    if (!editArea) return; 
    const edited = editArea.value;
    const msgEl = document.getElementById(`msg-${id}`);
    
    const showMessage = (message, isError = false) => {
       if (!msgEl) return;
        msgEl.textContent = message;
        msgEl.className = isError ? 'text-red-400 text-sm' : 'text-green-400 text-sm';
        msgEl.classList.remove('hidden');
        setTimeout(() => {
            msgEl.classList.add('hidden');
             msgEl.textContent = '✔️ Thank you for your submission!'; // Reset
             msgEl.className = 'text-green-400 text-sm hidden'; // Reset
        }, 3000);
    };

    try {
        const resp = await fetch("/scanner/submit_edit", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ filename, feed, transcript: edited })
        });
        if (resp.ok) {
            const originalTranscriptContainer = document.getElementById(`pre-${id}`)?.parentElement;
            if (originalTranscriptContainer) {
                // Remove old edited block if one exists
                const oldEditedBlock = originalTranscriptContainer.parentNode.querySelector('.bg-green-900\\/50');
                if (oldEditedBlock) oldEditedBlock.parentElement.remove();

                const newEditedBlock = document.createElement('div');
                newEditedBlock.innerHTML = `
              <div class="text-green-400 text-sm">✅ Edited Transcript</div>
              <pre class="whitespace-pre-wrap bg-green-900/50 p-3 rounded-md text-sm text-green-100 overflow-auto">${edited}</pre>
            `;
                originalTranscriptContainer.parentNode.insertBefore(newEditedBlock, originalTranscriptContainer);
                originalTranscriptContainer.classList.add('hidden');
            }

            localStorage.setItem(`edit_${filename}`, edited);
            showMessage('✔️ Edit submitted and applied!');
            
             document.getElementById(`save-${id}`)?.classList.add("hidden");
             document.getElementById(`cancel-${id}`)?.classList.add("hidden");
             document.getElementById(`edit-${id}`)?.classList.add("hidden");


        } else {
             const errorData = await resp.json().catch(() => ({ error: 'Unknown server error' }));
            showMessage(`❌ Submission failed: ${errorData.error || 'Server error'}`, true);
        }
    } catch (error) {
        console.error('Submit edit error:', error);
        showMessage('❌ Network error during submission.', true);
    }
}


// --- INTENT CLASSIFICATION (From scanner_view.js) ---
const policeIntentOptions = [
    "Traffic Stop", "Medical", "Disturbance", "BOLO", "Welfare Check",
    "Alarm", "Suspicious Activity", "MVA", "Domestic", "Theft", "Beeps/Testing", "Other"
];
const fireIntentOptions = [
    "Medical Aid", "Structure Fire", "Brush Fire", "Alarm Activation", "MVA",
    "Gas Leak", "CO Detector", "Service Call", "Mutual Aid", "Wires Down", "Beeps/Testing", "Other"
];
const dispositionOptions = [
    "Warning", "Citation", "Arrest", "Taser Used", "Shots Fired"
];  

function getIntentOptions() {
    const urlParams = new URLSearchParams(window.location.search);
    const feed = urlParams.get('feed');
    if (feed && feed.includes('fd')) { 
        return fireIntentOptions;
    }
    return policeIntentOptions;
}

function populateIntentOptions(index) {
    const container = document.getElementById(`intent-options-${index}`);
    if (!container || container.childElementCount > 0) return;

    const options = getIntentOptions();
    populateCheckboxes(container, options);

    const urlParams = new URLSearchParams(window.location.search);
    const feed = urlParams.get('feed');
    const dispositionContainer = document.getElementById(`disposition-options-${index}`);
    const dispositionLabelParent = dispositionContainer ? dispositionContainer.closest('div') : null;

    if (feed && !feed.includes('fd') && dispositionContainer) {
        populateCheckboxes(dispositionContainer, dispositionOptions);
        if (dispositionLabelParent) dispositionLabelParent.style.display = 'block';
    } else if (dispositionLabelParent) {
         dispositionLabelParent.style.display = 'none';
    }
}

function populateCheckboxes(container, options) {
    options.forEach(option => {
        const label = document.createElement('label');
        label.className = "flex items-center space-x-2 text-sm text-gray-200 cursor-pointer bg-gray-700/50 px-3 py-1.5 rounded-md hover:bg-gray-700 transition";
        const checkbox = document.createElement('input');
        checkbox.type = 'checkbox';
        checkbox.value = option;
        checkbox.className = 'h-4 w-4 rounded border-gray-600 bg-gray-800 text-purple-500 focus:ring-purple-600';
        label.appendChild(checkbox);
        const text = document.createElement('span');
        text.textContent = option;
        label.appendChild(text);
        container.appendChild(label);
    });
}

function toggleIntentForm(index) {
      showAuthMessage(index);  
  // if (typeof isLoggedIn !== 'undefined' && !isLoggedIn) {
  //      showAuthMessage(index);
  //      return;
   // }

    const form = document.getElementById(`intent-form-${index}`);
    if (form) {
        populateIntentOptions(index); // Ensure options are ready
        form.classList.toggle('hidden');
        if (!form.classList.contains('hidden')) {
            prepopulateIntentForm(index);
        }
    }
}

function prepopulateIntentForm(index) {
    const metadata = callMetadata[index];
    if (!metadata) return;

    if (metadata.intents && Array.isArray(metadata.intents)) {
        document.querySelectorAll(`#intent-options-${index} input[type="checkbox"]`).forEach(cb => {
            cb.checked = metadata.intents.includes(cb.value);
        });
    }
    if (metadata.dispositions && Array.isArray(metadata.dispositions)) {
        document.querySelectorAll(`#disposition-options-${index} input[type="checkbox"]`).forEach(cb => {
             cb.checked = metadata.dispositions.includes(cb.value);
        });
    }
    const officerInput = document.getElementById(`officer-${index}`);
    if(officerInput && metadata.officer) officerInput.value = metadata.officer;

    const roadInput = document.getElementById(`road-${index}`);
    if(roadInput && metadata.road) roadInput.value = metadata.road;
    
    const notesInput = document.getElementById(`notes-${index}`);
    if(notesInput && metadata.notes) notesInput.value = metadata.notes;
}

async function submitIntent(filename, feed, index) {
    const selectedIntents = Array.from(document.querySelectorAll(`#intent-options-${index} input[type="checkbox"]:checked`)).map(cb => cb.value);
    const selectedDispositions = Array.from(document.querySelectorAll(`#disposition-options-${index} input[type="checkbox"]:checked`)).map(cb => cb.value);
    const officer = document.getElementById(`officer-${index}`).value || ""; 
    const road = document.getElementById(`road-${index}`).value || ""; 
    const notes = document.getElementById(`notes-${index}`).value || ""; 

    const payload = { filename, feed, intents: selectedIntents, dispositions: selectedDispositions, officer, road, notes };

    const msgEl = document.getElementById(`msg-${index}`);
    const showMessage = (message, isError = false) => {
        if (!msgEl) return;
        msgEl.textContent = message;
        msgEl.className = isError ? 'text-red-400 text-sm' : 'text-green-400 text-sm';
        msgEl.classList.remove('hidden');
        setTimeout(() => msgEl.classList.add('hidden'), 4000);
    };

    try {
        const response = await fetch('/scanner/submit_intent', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(payload)
        });

        if (response.ok) {
            showMessage('✅ Intent submitted successfully!');
            
            if (!callMetadata[index]) callMetadata[index] = {};
            Object.assign(callMetadata[index], { intents: selectedIntents, dispositions: selectedDispositions, officer, road, notes });

            localStorage.setItem(`classify_${filename}`, JSON.stringify(callMetadata[index]));

            toggleIntentForm(index); // Hide form
        } else {
            const errorData = await response.json().catch(() => ({ error: 'Unknown server error' })); 
            showMessage(`❌ Error: ${errorData.error || 'Unknown error'}`, true);
        }
    } catch (error) {
        console.error('Submit intent error:', error);
        showMessage('❌ Network error during submission.', true);
    }

/**
 * Finds all elements with the class 'call-timestamp' and formats
 * their content into a human-readable date and time.
 */
function formatAllTimestamps() {
  const elements = document.querySelectorAll('.call-timestamp');
  
  elements.forEach(el => {
    const rawTimestamp = el.dataset.timestamp; // "2025-10-28 17-59-31"
    const feed = el.dataset.feed;             // "fd"

    // Regex to find the date and time parts
    // Handles "2025-10-28 17-59-31" or "2025-10-28_17-59-31"
    const match = rawTimestamp.match(/(\d{4}-\d{2}-\d{2})[ _](\d{2}-\d{2}-\d{2})/);
    
    if (!match) {
      console.warn('Could not parse timestamp:', rawTimestamp);
      // Leave the default text if parsing fails
      return; 
    }

    const dateStr = match[1]; // "2025-10-28"
    const timeStr = match[2].replace(/-/g, ':'); // "17:59:31"
    const isoString = `${dateStr}T${timeStr}`; // "2025-10-28T17:59:31"
    
    const dateObj = new Date(isoString);
    
    // Check if the date is valid
    if (isNaN(dateObj.getTime())) {
        console.warn('Invalid date created from:', isoString);
        return;
    }

    // Format the date nicely
    const options = {
      month: 'long', 
      day: 'numeric', 
      year: 'numeric', 
      hour: 'numeric', 
      minute: '2-digit', 
      hour12: true
    };
    
    const formattedDate = dateObj.toLocaleString('en-US', options);
    
    // Set the new, beautiful text
    // e.g., "October 28, 2025 at 5:59 PM (FD)"
    el.textContent = `${formattedDate} (${feed.toUpperCase()})`;
  });
}

// Run this function once the page has loaded
document.addEventListener('DOMContentLoaded', formatAllTimestamps);

// If you have infinite scroll, you'll need to call formatAllTimestamps()
// again *after* you load and append new calls.
}
function initPullToRefresh() {
  const mainEl = document.querySelector('.main-content-area');
  const indicatorEl = document.getElementById('pull-to-refresh-indicator');

  // If the elements don't exist, don't do anything
  if (!mainEl || !indicatorEl) return;
  
  const indicatorSpan = indicatorEl.querySelector('span');
  if (!indicatorSpan) return; // Make sure the span for text is there

  let touchStartY = 0;
  let pullDistance = 0;
  const threshold = 70; // How many pixels to pull down to trigger
  let isRefreshing = false;

  // Listen for the first touch
  mainEl.addEventListener('touchstart', (e) => {
    // Only track if we're at the top and not already refreshing
    if (mainEl.scrollTop === 0 && !isRefreshing) {
      touchStartY = e.touches[0].clientY;
      indicatorEl.style.transition = 'transform 0s'; // Disable transition while dragging
    } else {
      touchStartY = 0;
    }
  }, { passive: true }); // We're just reading, so 'passive' is fine here

  // Listen as the finger moves
  mainEl.addEventListener('touchmove', (e) => {
    if (touchStartY === 0 || isRefreshing) return;

    const currentY = e.touches[0].clientY;
    pullDistance = currentY - touchStartY;

    // Only act if we're pulling down
    if (pullDistance > 0) {
      // *** This is critical ***
      // We are now hijacking the scroll, so we must prevent
      // the browser's default "bounce" effect.
      e.preventDefault(); 

      const pullToShow = Math.min(pullDistance, threshold + 30); // Let them pull a little past the threshold
      
      // This overrides the 'translateY(-100%)' and moves the indicator down
      indicatorEl.style.transform = `translateY(${pullToShow}px)`;

      if (pullDistance > threshold) {
        indicatorSpan.textContent = 'Release to refresh';
      } else {
        indicatorSpan.textContent = 'Pull to refresh';
      }
    }
  }, { passive: false }); // *** Not passive ***, because we call e.preventDefault()

  // Listen for when the finger is lifted
  mainEl.addEventListener('touchend', (e) => {
    if (isRefreshing || touchStartY === 0 || pullDistance <= 0) return;
    
    // Re-enable the snap-back transition
    indicatorEl.style.transition = 'transform 0.3s ease-out';

    if (pullDistance > threshold) {
      // --- TRIGGER REFRESH ---
      isRefreshing = true;
      indicatorEl.style.transform = 'translateY(40px)'; // Hold it in view
      // Add a spinner (using Tailwind classes you already have)
      indicatorEl.innerHTML = `
        <svg class="loading-spinner w-4 h-4 inline-block -mt-1 mr-1" xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" stroke-width="2" stroke="currentColor">
          <path stroke-linecap="round" stroke-linejoin="round" d="M16.023 9.348h4.992v-.001M2.985 19.644v-4.992m0 0h4.992m-4.993 0l3.181 3.183a8.25 8.25 0 0013.803-3.7M4.031 9.865a8.25 8.25 0 0113.803-3.7l3.181 3.182m0-4.991v4.99" />
        </svg>
        Refreshing...
      `;
      
      // Do the refresh
      window.location.reload();
      
    } else {
      // --- CANCEL REFRESH ---
      // Snap back to the hidden position
      indicatorEl.style.transform = 'translateY(-100%)';
    }

    // Reset state
    touchStartY = 0;
    pullDistance = 0;
  });
}

// ============================================================
// WAVEFORM ENGINE
// ============================================================

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
  const step = 4;
  const bars = Math.floor(W / step);
  const blockSize = Math.floor(data.length / bars);
  const playedX = progress * W;
  const lit = isFire ? 'rgba(248,113,113,0.9)'  : 'rgba(56,189,248,0.9)';
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
  const feed     = playerEl.dataset.feed || '';
  if (!audioUrl) return;

  const isFire  = feed.toLowerCase().includes('fd');
  const canvas  = playerEl.querySelector('.wave-canvas');
  const playBtn = playerEl.querySelector('.wave-play-btn');
  const playIcon = playerEl.querySelector('.wave-play-icon');
  const timeEl  = playerEl.querySelector('.wave-time');
  const scrub   = playerEl.querySelector('.wave-scrub');

  if (!canvas) return;

  // Reuse the hidden <audio class="call-audio"> in the same card if present,
  // otherwise create a new one (so play-count events still fire).
  const card = playerEl.closest('.call-entry, .group');
  let audioEl = card ? card.querySelector('audio.call-audio') : null;
  if (!audioEl) { audioEl = new Audio(audioUrl); }

  let decoded = null;
  let animId  = null;
  let loading = false;

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

  // Scrub on canvas click
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

document.addEventListener('DOMContentLoaded', () => {
  initAutoUpdate();
  initPullToRefresh();
  initAllWaveformPlayers();

  // If linked here with ?goto=hooks, load calls until a hook card appears then jump
  if (new URLSearchParams(window.location.search).get('goto') === 'hooks') {
    gotoFirstHook();
  }
});

