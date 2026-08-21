(() => {
    const page = document.getElementById('scanner-incident-page');
    if (!page) return;

    const incidentKey = page.dataset.incidentKey;
    const loadingEl = document.getElementById('incident-loading');
    const contentEl = document.getElementById('incident-content');

    function formatDuration(totalSeconds) {
        const seconds = Math.max(0, Math.round(Number(totalSeconds) || 0));
        if (seconds < 60) return `${seconds}s`;
        const minutes = Math.floor(seconds / 60);
        const remainder = seconds % 60;
        if (minutes < 60) return remainder ? `${minutes}m ${remainder}s` : `${minutes}m`;
        const hours = Math.floor(minutes / 60);
        const remaining = minutes % 60;
        return remaining ? `${hours}h ${remaining}m` : `${hours}h`;
    }

    function formatTimestamp(value, includeDate = false) {
        const parsed = value ? new Date(value) : null;
        if (!parsed || Number.isNaN(parsed.getTime())) return value || '';
        return parsed.toLocaleString([], includeDate
            ? { month: 'short', day: 'numeric', hour: 'numeric', minute: '2-digit' }
            : { hour: 'numeric', minute: '2-digit', second: '2-digit' });
    }

    function stageLabel(stage) {
        return {
            dispatched: 'Dispatched',
            responding: 'Responding',
            on_scene: 'On scene',
            returning: 'Returning',
            in_quarters: 'Back in quarters',
            cleared: 'Cleared',
            terminated: 'Command terminated',
            update: 'Radio update'
        }[stage] || 'Radio update';
    }

    function addMetric(container, label, value, tone = 'slate') {
        if (value === null || value === undefined || value === '') return;
        const item = document.createElement('span');
        const toneClass = tone === 'amber'
            ? 'border-amber-500/40 bg-amber-950/30 text-amber-200'
            : tone === 'sky'
                ? 'border-sky-700/50 bg-sky-950/30 text-sky-200'
                : 'border-slate-700 bg-slate-950/40 text-slate-300';
        item.className = `rounded-full border px-3 py-1.5 text-xs ${toneClass}`;
        item.textContent = `${label}: ${value}`;
        container.appendChild(item);
    }

    function renderTransmission(call, index) {
        const item = document.createElement('article');
        item.className = 'relative border-l border-slate-700 pl-5 pb-2';

        const dot = document.createElement('span');
        dot.className = 'absolute -left-1.5 top-1 h-3 w-3 rounded-full border border-slate-950 bg-sky-400';

        const heading = document.createElement('div');
        heading.className = 'flex flex-wrap items-center justify-between gap-2';
        const title = document.createElement('h3');
        title.className = 'text-sm font-semibold text-white';
        title.textContent = `${stageLabel(call.lifecycle_stage)} · Call #${call.call_id}`;
        const time = document.createElement('span');
        time.className = 'text-[10px] uppercase tracking-wider text-slate-500';
        time.textContent = formatTimestamp(call.timestamp);
        heading.append(title, time);

        const originalLabel = document.createElement('p');
        originalLabel.className = 'mt-2 text-[9px] font-semibold uppercase tracking-[0.14em] text-slate-500';
        originalLabel.textContent = 'Original transcript';
        const transcript = document.createElement('p');
        transcript.className = 'mt-1 whitespace-pre-wrap text-sm leading-6 text-slate-300';
        transcript.textContent = call.transcript || 'No transcript available.';

        const audioWrap = document.createElement('div');
        audioWrap.className = 'mt-3 rounded-xl border border-slate-800 bg-slate-950/40 p-3';
        const audio = document.createElement('audio');
        audio.className = 'w-full';
        audio.controls = true;
        audio.preload = 'metadata';
        audio.src = call.audio_url;
        audioWrap.appendChild(audio);

        const footer = document.createElement('div');
        footer.className = 'mt-2 flex flex-wrap items-center justify-between gap-2 text-[11px] text-slate-500';
        footer.textContent = `${formatDuration(call.duration_seconds)} recorded audio`;
        const evidence = document.createElement('a');
        evidence.href = call.archive_url;
        evidence.className = 'font-semibold text-sky-300 hover:text-white';
        evidence.textContent = 'Open source call →';
        footer.appendChild(evidence);

        item.append(dot, heading, originalLabel, transcript);
        item.append(audioWrap, footer);
        item.style.setProperty('--transmission-index', index);
        return item;
    }

    function renderIncident(data) {
        document.getElementById('incident-title').textContent = data.title || 'Scanner incident';
        document.getElementById('incident-kicker').textContent = `${data.town || 'Unknown'} · ${data.call_type || 'Scanner activity'}`;
        document.getElementById('incident-time').textContent =
            `${formatTimestamp(data.started_at, true)} through ${formatTimestamp(data.ended_at)}`;
        document.getElementById('incident-department').textContent = data.department || data.feed || 'Scanner';
        document.getElementById('incident-summary').textContent = data.summary || '';
        document.getElementById('incident-source-count').textContent =
            `${(data.calls || []).length} transmission${(data.calls || []).length === 1 ? '' : 's'}`;

        const backLink = document.getElementById('incident-back-link');
        backLink.href = `/scanner/neds-take/${encodeURIComponent(data.day || 'today')}`;
        backLink.textContent = `← Back to Ned’s Take for ${data.day || 'today'}`;

        const metrics = document.getElementById('incident-metrics');
        metrics.replaceChildren();
        addMetric(metrics, 'Incident span', data.incident_span_seconds
            ? formatDuration(data.incident_span_seconds)
            : 'single transmission');
        addMetric(metrics, 'Recorded audio', formatDuration(data.recorded_audio_seconds));
        addMetric(metrics, 'Response', data.response_time_seconds === null
            ? 'not captured'
            : formatDuration(data.response_time_seconds), 'sky');
        if (data.department === 'police') {
            addMetric(metrics, 'Outcome', data.outcome || 'not heard',
                data.outcome_type === 'citation' ? 'amber' : 'sky');
        }
        addMetric(metrics, 'Status', data.closed ? 'closed on radio' : 'no closure heard');

        const transmissions = document.getElementById('incident-transmissions');
        transmissions.replaceChildren();
        (data.calls || []).forEach((call, index) => {
            transmissions.appendChild(renderTransmission(call, index));
        });

        loadingEl.classList.add('hidden');
        contentEl.classList.remove('hidden');
    }

    fetch(`/scanner/api/incident/${encodeURIComponent(incidentKey)}/take`, {
        cache: 'no-store',
        headers: { 'Accept': 'application/json', 'Cache-Control': 'no-cache' }
    })
        .then(async (response) => {
            const data = await response.json().catch(() => ({}));
            if (!response.ok || data.ok === false) {
                throw new Error(data.error || `Request failed: ${response.status}`);
            }
            return data;
        })
        .then(renderIncident)
        .catch((error) => {
            console.warn('[IncidentTake] Request failed:', error);
            loadingEl.textContent = 'This grouped incident could not be loaded.';
        });
})();
