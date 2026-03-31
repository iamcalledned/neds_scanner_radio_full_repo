document.addEventListener('DOMContentLoaded', async () => {
  const params = new URLSearchParams(window.location.search);
  const feed = params.get('feed');
  const title = document.getElementById('archive-title');
  const subtitle = document.getElementById('archive-subtitle');
  const container = document.getElementById('archive-container');
  const callsPerPage = 10;

  // Town definitions
  const towns = [
    { name: "Hopedale",   pd: "pd",     fd: "fd" },
    { name: "Milford",    pd: "mpd",    fd: "mfd" },
    { name: "Bellingham", pd: "bpd",    fd: "bfd" },
    { name: "Mendon",     pd: "mndpd",  fd: "mndfd" },
    { name: "Upton",      pd: "uptpd",  fd: "uptfd" },
    { name: "Blackstone", pd: "blkpd",  fd: "blkfd" },
    { name: "Franklin",   pd: "frkpd",  fd: "frkfd" } // <-- TYPO FIXED
  ];

  const feedMap = {
    pd: ['🚓 Hopedale Police Archive','Past 7 days of Hopedale PD calls'],
    fd: ['🚒 Hopedale Fire Archive','Past 7 days of Hopedale FD calls'],
    mpd: ['🚓 Milford Police Archive','Past 7 days of Milford PD calls'],
    mfd: ['🚒 Milford Fire Archive','Past 7 days of Milford FD calls'],
    bpd: ['🚓 Bellingham Police Archive','Past 7 days of Bellingham PD calls'],
    bfd: ['🚒 Bellingham Fire Archive','Past 7 days of Bellingham FD calls'],
    mndpd: ['🚓 Mendon Police Archive','Past 7 days of Mendon PD calls'],
    mndfd: ['🚒 Mendon Fire Archive','Past 7 days of Mendon FD calls'],
    uptpd: ['🚓 Upton Police Archive','Past 7 days of Upton PD calls'],
    uptfd: ['🚒 Upton Fire Archive','Past 7 days of Upton FD calls'],
    blkpd: ['🚓 Blackstone Police Archive','Past 7 days of Blackstone PD calls'],
    blkfd: ['🚒 Blackstone Fire Archive','Past 7 days of Blackstone FD calls'],
    frkpd: ['🚓 Franklin Police Archive','Past 7 days of Franklin PD calls'],
    frkfd: ['🚒 Franklin Fire Archive','Past 7 days of Franklin FD calls']
  };

  // --------------------------
  // MODE 1: OVERVIEW (no feed)
  // --------------------------
  if (!feed) {
    title.textContent = "📡 Town Archives";
    subtitle.textContent = "Select a department to browse archived calls";
    container.innerHTML = "";

    const grid = document.createElement('div');
    grid.className = "grid grid-cols-1 sm:grid-cols-2 gap-6";
    towns.forEach(t => {
      const card = document.createElement('div');
      card.className = "bg-slate-800/70 rounded-2xl p-6 ring-1 ring-white/10 shadow hover:ring-scannerBlue/50 transition";
      card.innerHTML = `
        <h2 class="text-xl font-semibold mb-4 text-scannerBlue">${t.name}</h2>
        <div class="flex flex-col gap-2">
          <a href="/scanner/archive?feed=${t.pd}" class="flex items-center gap-2 text-gray-300 hover:text-scannerBlue">
            🚓 <span>Police Department</span>
          </a>
          <a href="/scanner/archive?feed=${t.fd}" class="flex items-center gap-2 text-gray-300 hover:text-scannerBlue">
            🚒 <span>Fire Department</span>
          </a>
        </div>
      `;
      grid.appendChild(card);
    });
    container.appendChild(grid);
    return;
  }

  // --------------------------
  // MODE 2: FEED VIEW
  // --------------------------
  if (feedMap[feed]) {
    title.textContent = feedMap[feed][0];
    subtitle.textContent = feedMap[feed][1];
  } else {
    subtitle.textContent = "Past 7 days of all scanner feeds";
  }

  // Fetch summary
  try {
    const summaryResp = await fetch(`/scanner/archive?feed=${feed}&json=1`);
    if (!summaryResp.ok) {
      throw new Error(`Failed to fetch summary: ${summaryResp.statusText}`);
    }
    const summaryData = await summaryResp.json();
    container.innerHTML = "";

    if (!summaryData.days || summaryData.days.length === 0) {
      container.innerHTML = '<p class="text-gray-500 text-center">No archived calls within the past 7 days.</p>';
      return;
    }

    // Build day sections
    for (const day of summaryData.days) {
      const details = document.createElement('details');
      details.className = 'bg-slate-800/60 rounded-2xl shadow-md backdrop-blur-sm ring-1 ring-white/10 overflow-hidden';
      details.innerHTML = `
        <summary class="cursor-pointer px-5 py-4 text-lg font-semibold text-gray-200 bg-slate-700/70 border-b border-slate-600/50">
          📆 ${day} <span class="text-xs text-gray-400 ml-2">(${summaryData.call_totals[day]} calls)</span>
        </summary>
        <div class="p-4 space-y-6 call-list text-gray-300 text-sm" data-day="${day}">
          <p class="text-gray-500 italic">Click to load calls...</p>
        </div>
      `;
      container.appendChild(details);

      details.addEventListener('toggle', async (e) => {
        if (!details.open) return;
        const callList = details.querySelector('.call-list');
        if (callList.dataset.loaded === '1') return;
        callList.innerHTML = '<p class="text-gray-400">Loading...</p>';

        try {
          const resp = await fetch(`/scanner/archive?feed=${feed}&day=${encodeURIComponent(day)}&page=1&json=1`);
          const data = await resp.json();
          callList.innerHTML = '';
          callList.dataset.loaded = '1';

          if (data.calls && data.calls.length > 0) {
            renderCalls(callList, data.calls, day, 1);
          } else {
            callList.innerHTML = '<p class="text-gray-500">No calls for this day.</p>';
          }
        } catch (err) {
          console.error('Error fetching calls for day:', err);
          callList.innerHTML = '<p class="text-red-400">Error loading calls.</p>';
        }
      });
    }
  } catch (err) {
    console.error('Error fetching archive summary:', err);
    container.innerHTML = `<p class="text-red-400 text-center">Failed to load archive data. ${err.message}</p>`;
  }

  // Render calls
  function renderCalls(container, calls, day, page) {
    calls.forEach(call => {
      const div = document.createElement('div');
      div.className = 'bg-slate-900/70 p-4 rounded-xl shadow-sm ring-1 ring-white/10';
      div.innerHTML = `
        <div class="text-sm text-gray-400 mb-1">${call.timestamp_human}</div>
        <audio class="w-full mb-3" controls src="${call.path}"></audio>
        <pre class="whitespace-pre-wrap bg-slate-800/80 p-3 rounded-md text-gray-100 overflow-auto">${call.transcript}</pre>
      `;
      container.appendChild(div);
    });

    if (calls.length >= callsPerPage) {
      const btn = document.createElement('button');
      btn.className = 'load-more bg-scannerBlue text-slate-900 hover:bg-sky-400 px-4 py-2 rounded mt-2 font-semibold';
      btn.dataset.day = day;
      btn.dataset.page = page;
      btn.textContent = 'Load more';
      container.appendChild(btn);
    }
  }

  // Load-more
  document.body.addEventListener('click', async (e) => {
    if (!e.target.classList.contains('load-more')) return;
    const btn = e.target;
    const day = btn.dataset.day;
    let page = parseInt(btn.dataset.page) + 1;
    btn.disabled = true;
    btn.textContent = 'Loading...';

    try {
      const resp = await fetch(`/scanner/archive?feed=${feed}&day=${encodeURIComponent(day)}&page=${page}&json=1`);
      const data = await resp.json();
      if (data.calls && data.calls.length > 0) {
        renderCalls(btn.closest('.call-list'), data.calls, day, page);
        btn.dataset.page = page;
        btn.disabled = false;
        btn.textContent = 'Load more';
        if (data.calls.length < callsPerPage) btn.remove();
      } else {
        btn.remove();
      }
    } catch (err) {
      console.error('Error fetching more calls:', err);
      btn.textContent = 'Error';
      setTimeout(() => btn.remove(), 2000);
    }
  });
});