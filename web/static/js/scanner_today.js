// ======================================================
//  Scanner "Today" Page JS
// ======================================================

document.addEventListener('DOMContentLoaded', () => {

    // --- Configuration ------------------------------------
    // List all department codes and their display names
    // Ensure these 'id' values match the 'dept' keys sent via WebSocket
    const departments = [
        { id: 'pd', name: 'Hopedale PD' },
        { id: 'fd', name: 'Hopedale FD' },
        { id: 'mpd', name: 'Milford PD' },
        { id: 'mfd', name: 'Milford FD' },
        { id: 'bpd', name: 'Bellingham PD' },
        { id: 'bfd', name: 'Bellingham FD' },
        { id: 'mndpd', name: 'Mendon PD' },
        { id: 'mndfd', name: 'Mendon FD' },
        { id: 'blkpd', name: 'Blackstone PD' },
        { id: 'blkfd', name: 'Blackstone FD' },
        { id: 'uptpd', name: 'Upton PD' },
        { id: 'uptfd', name: 'Upton FD' },
        { id: 'frkpd', name: 'Franklin PD' },
        { id: 'frkfd', name: 'Franklin FD' },
        // Add any other departments you monitor here
    ];
    
    // Global variable to hold metadata for edit/classify functions
    let callMetadata = {}; 

    // --- WebSocket Logic ----------------------------------
    let socket = null;

    function initSocketIO() {
        const serverURL = window.location.origin.includes('iamcalledned.ai')
            ? "https://iamcalledned.ai"
            : "http://localhost:5005"; // Match your Flask dev port
        
        socket = io(serverURL, { transports: ['websocket', 'polling'] });

        socket.on('connect', () => console.log('Socket.IO: Connected!'));
        socket.on('disconnect', () => console.log('Socket.IO: Disconnected.'));
        socket.on('connect_error', (err) => console.error('Socket.IO Connection Error:', err.message));

        socket.on('transmitting_update', (msg) => {
            // msg = { dept: 'fd', status: 'Y' }
            if (msg && msg.dept && msg.status) {
                console.log(`Socket.IO: Received status for ${msg.dept}: ${msg.status}`);
                updateLiveIndicator(msg.dept, msg.status);
            }
        });
    }

    /**
     * Builds the initial HTML for the Live Status section.
     */
    function buildLiveStatusSection() {
        const grid = document.getElementById('live-status-grid');
        if (!grid) return;

        grid.innerHTML = ''; // Clear loading message
        departments.forEach(dept => {
            const div = document.createElement('div');
            div.className = 'flex items-center';
            div.innerHTML = `
                <span class="live-indicator" data-dept="${dept.id}"></span>
                <span>${dept.name}</span>
            `;
            grid.appendChild(div);
        });
    }

    /**
     * Updates a specific indicator dot based on WebSocket message.
     * @param {string} deptId - The department ID (e.g., 'pd', 'mfd').
     * @param {string} status - 'Y' or 'N'.
     */
    function updateLiveIndicator(deptId, status) {
        const indicator = document.querySelector(`.live-indicator[data-dept="${deptId}"]`);
        if (indicator) {
            if (status === 'Y') {
                indicator.classList.add('live');
            } else {
                indicator.classList.remove('live');
            }
        }
    }

    // --- Data Loading & Rendering -------------------------

    /**
     * Fetches all calls for today from the new API endpoint.
     */
    async function loadTodaysCalls() {
        const container = document.getElementById('calls-container');
        if (!container) return;

        try {
            // *** IMPORTANT: You need to create this endpoint in Flask ***
            const response = await fetch('/scanner/api/today_all'); 
            
            if (!response.ok) {
                throw new Error(`HTTP error! status: ${response.status}`);
            }
            const data = await response.json();

            if (data.calls && data.calls.length > 0) {
                 // Populate global metadata object BEFORE rendering
                 callMetadata = {}; // Reset metadata
                 data.calls.forEach((call, index) => {
                     // Using index + 1 as the key, matching how cards will be indexed
                     callMetadata[index + 1] = call.metadata || {}; 
                 });
                renderCalls(data.calls);
            } else {
                container.innerHTML = '<p class="text-slate-500 italic text-center py-10">No calls recorded yet today.</p>';
            }
        } catch (error) {
            console.error("Failed to load today's calls:", error);
            container.innerHTML = `<p class="text-red-400 italic text-center py-10">Error loading calls: ${error.message}</p>`;
        }
    }

    /**
     * Renders the fetched calls into the container.
     * @param {Array} calls - Array of call objects from the API.
     */
    function renderCalls(calls) {
        const container = document.getElementById('calls-container');
        if (!container) return;
        container.innerHTML = ''; // Clear loading message

        // NOTE: Uses the same card structure and functions as scanner_view.js
        // Assumes those functions (enableEdit, submitEdit, etc.) are available
        // either globally or defined within this script. 
        // We'll define them below for completeness.

        calls.forEach((call, i) => {
            const index = i + 1; // Use 1-based index for elements
            const div = document.createElement('div');
            div.id = `call-card-${index}`;
            // Reusing styles from scanner_view.html structure
            div.className = `group block bg-gray-800/50 backdrop-blur-sm p-6 rounded-2xl shadow-lg transition-all duration-300 ring-1 ring-white/10 call-entry`;

            const playCount = (call.metadata && call.metadata.play_count) || 0;
            const playCountText = playCount === 1 ? 'time' : 'times';
            const playCountHTML = playCount > 0
                ? `<div id="playcount-${index}" class="transition-all duration-200">👂 Played: ${playCount} ${playCountText}</div>`
                : `<div id="playcount-${index}" class="hidden"></div>`;

            // Find display name for the feed
            const deptInfo = departments.find(d => d.id === call.feed);
            const deptName = deptInfo ? deptInfo.name : call.feed; // Fallback to ID if not found

            // Safely access properties, provide defaults if null/undefined
            const timestampHuman = call.timestamp_human || 'Unknown time';
            const callPath = call.path || '#';
            const callFile = call.file || '';
            const callFeed = call.feed || '';
            const enhancedTranscript = (call.metadata && call.metadata.enhanced_transcript) || '';
            const editedTranscript = (call.metadata && call.metadata.edited_transcript) || '';
            const editPending = call.edit_pending || false;
            const originalTranscript = call.transcript || 'Transcript not available';


            div.innerHTML = `
                <div class="flex justify-between items-center text-sm text-gray-400 mb-1">
                  <div>${timestampHuman} - <span class="font-semibold ${callFeed.includes('fd') ? 'text-red-400' : 'text-blue-400'}">${deptName}</span></div>
                  ${playCountHTML}
                </div>
                <audio class="w-full mb-2 call-audio" controls src="${callPath}" data-filename="${callFile}" data-feed="${callFeed}" data-index="${index}"></audio>
                <div class="space-y-4">
                  ${enhancedTranscript ? `
                    <div>
                      <div class="text-purple-400 text-sm">✨ Enhanced Transcript</div>
                      <pre class="whitespace-pre-wrap bg-purple-900/50 p-3 rounded-md text-sm text-purple-100 overflow-auto">${enhancedTranscript}</pre>
                    </div>` : ''}
                  ${editedTranscript ? `
                    <div>
                      <div class="text-green-400 text-sm">✅ Edited Transcript</div>
                      <pre class="whitespace-pre-wrap bg-green-900/50 p-3 rounded-md text-sm text-green-100 overflow-auto">${editedTranscript}</pre>
                    </div>` : ''}
                  ${editPending && !editedTranscript ? ` 
                    <div>
                      <div class="text-yellow-400 text-sm">✏️ Edit Pending</div>
                      <pre class="whitespace-pre-wrap bg-yellow-900/50 p-3 rounded-md text-sm text-yellow-100 overflow-auto">${call.edited_transcript /* Use potentially pending edit here */}</pre>
                    </div>` : ''}
                  <div>
                    <div class="text-gray-400 text-sm">🎧 Original Transcript</div>
                    <pre id="pre-${index}" class="whitespace-pre-wrap bg-slate-800 p-3 rounded-md text-sm text-gray-200 overflow-auto">${originalTranscript}</pre>
                    <textarea id="edit-${index}" class="w-full bg-gray-800 text-sm p-3 rounded-md text-white border border-gray-600 hidden" rows="4">${originalTranscript}</textarea>
                    <div class="flex gap-4 mt-2">
                      <button onclick="enableEdit(${index})" class="text-yellow-400 hover:underline text-sm">Edit</button>
                      <button onclick="submitEdit('${callFile}', '${callFeed}', ${index})" id="save-${index}" class="hidden text-green-400 hover:underline text-sm">Submit</button>
                      <button onclick="cancelEdit(${index})" id="cancel-${index}" class="hidden text-red-400 hover:underline text-sm">Cancel</button>
                      <button onclick="toggleIntentForm(${index})" class="text-purple-400 hover:underline text-sm">Classify</button>
                      <button onclick="shareCall(${index}, '${callFeed}')" class="text-blue-400 hover:underline text-sm ml-auto">Share</button>
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
                        <button onclick="toggleIntentForm(${index})" class="px-4 py-2 text-sm font-medium text-gray-300 bg-gray-700 rounded-md hover:bg-gray-600 transition">Cancel</button>
                        <button onclick="submitIntent('${callFile}', '${callFeed}', ${index})" class="px-4 py-2 text-sm font-medium text-white bg-purple-600 rounded-md hover:bg-purple-700 transition">Submit Intent</button>
                      </div>
                    </div>
                  </div>
                </div>`;
            container.appendChild(div);
        });
        
        // Attach play listener AFTER cards are added to the DOM
        const callsContainer = document.getElementById('calls-container');
        if (callsContainer) {
             // Remove potential old listener before adding new one
            callsContainer.removeEventListener('play', handleAudioPlay, true);
            callsContainer.addEventListener('play', handleAudioPlay, true);
        }
    }


    // --- Helper Functions (Copied from scanner_view.js logic) ---
    // These need to be defined here or globally available (e.g., via scanner_app_new.js)
    
    // Play Count Increment (Needs access to fetch)
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
            } else { console.error(`Error incrementing play count for ${filename}: ${resp.status}`); }
        } catch (err) { console.error(`Network error incrementing play count for ${filename}:`, err); }
    }

    // --- Share Functionality ---
    async function shareCall(index, feed) {
        const callCard = document.getElementById(`call-card-${index}`);
        if (!callCard) return; 
        const audioEl = callCard.querySelector('audio');
        const preEl = callCard.querySelector(`#pre-${index}`);
        if (!audioEl || !preEl) return; 

        const fullTranscript = preEl.innerText;
        // Need feedConfig available here
        const feedTitle = feedConfig[feed]?.title || 'Scanner'; 
        const branding = "\n\n---\nSent from the Command Center";
        const msgEl = document.getElementById(`msg-${index}`);

        const showMessage = (message, isError = false) => { /* ... same as scanner_view.js ... */ };
        
        // --- Modal & Share Logic ---
        // (Exactly the same complex logic as in scanner_view.js)
        // Ensure feedConfig is defined/accessible within this scope if needed outside this function
        const modalHtml = `...`; // Same modal HTML
        document.body.insertAdjacentHTML('beforeend', modalHtml);
        const modal = document.getElementById(`share-modal-${index}`);
        // ... rest of shareCall logic (event listeners, fetch, navigator.share, clipboard) ...
    }


    // --- Edit Transcript Logic ---
    function enableEdit(id) {
        // Assumes isLoggedIn is global (from scanner_app_new.js)
        if (typeof isLoggedIn !== 'undefined' && !isLoggedIn) {
            showAuthMessage(id); // Needs showAuthMessage definition
            return;
        }
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
        const showMessage = (message, isError = false) => { /* ... same as above ... */ }; // Needs definition

        try {
            const resp = await fetch("/scanner/submit_edit", { /* ... POST request ... */ });
            if (resp.ok) {
                // ... Update UI logic (create new pre, hide old one) ...
                localStorage.setItem(`edit_${filename}`, edited);
                showMessage('✔️ Edit submitted and applied!');
                 document.getElementById(`save-${id}`)?.classList.add("hidden");
                 document.getElementById(`cancel-${id}`)?.classList.add("hidden");
            } else {
                 const errorData = await resp.json().catch(() => ({ error: 'Unknown server error' }));
                 showMessage(`❌ Submission failed: ${errorData.error || 'Server error'}`, true);
            }
        } catch (error) {
            console.error('Submit edit error:', error);
            showMessage('❌ Network error during submission.', true);
        }
    }

    // --- Intent Classification Logic ---
    const policeIntentOptions = [ /* ... */ ];
    const fireIntentOptions = [ /* ... */ ];
    const dispositionOptions = [ /* ... */ ];
    
    // Needs access to feedConfig or current feed context
    function getIntentOptions() { /* ... same as scanner_view.js ... */ } 
    function populateIntentOptions(index) { /* ... same as scanner_view.js ... */ }
    function populateCheckboxes(container, options) { /* ... same as scanner_view.js ... */ }
    function showAuthMessage(index) { /* ... needs definition ... */ }
    function toggleIntentForm(index) { /* ... same as scanner_view.js ... */ }
    function prepopulateIntentForm(index) { /* ... same as scanner_view.js, needs callMetadata */ }
    async function submitIntent(filename, feed, index) { /* ... same as scanner_view.js, needs callMetadata ... */ }
    
     // --- Define showMessage here if not globally available ---
     function showMessage(elId, message, isError = false) {
        const msgEl = document.getElementById(elId);
        if (msgEl) {
            msgEl.textContent = message;
            msgEl.className = isError ? 'text-red-400 text-sm' : 'text-green-400 text-sm';
            msgEl.classList.remove('hidden');
            setTimeout(() => {
                msgEl.classList.add('hidden');
                msgEl.textContent = '✔️ Thank you for your submission!'; // Reset default text
                msgEl.className = 'text-green-400 text-sm hidden'; // Reset class
            }, 3000);
        }
     }
     
    // --- Re-define feedConfig for shareCall function ---
    // (This is redundant if already defined globally, but ensures availability)
    const feedConfig = { 
        'pd': { title: "Hopedale Police" /* ... other props */ }, 
        'fd': { title: "Hopedale Fire" /* ... */ },
        // ... include ALL feed configs needed by shareCall ...
    };


    // --- Init ---------------------------------------------
    buildLiveStatusSection();
    loadTodaysCalls();
    initSocketIO();
    // checkAuth() and setupInstallButton() should be called by scanner_app_new.js

});
