(() => {
    const page = document.getElementById('daily-take-page');
    if (!page) return;

    const day = page.dataset.takeDay || 'today';
    const loadingEl = document.getElementById('daily-take-loading');
    const contentEl = document.getElementById('daily-take-content');

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

    function totalsLine(totals = {}) {
        return `${Number(totals.transmissions || 0).toLocaleString()} transmissions · ` +
            `${Number(totals.estimated_incidents || 0).toLocaleString()} estimated incidents · ` +
            `${Number(totals.closed_incidents || 0).toLocaleString()} closed on radio`;
    }

    function createIncidentLink(highlight) {
        const card = document.createElement('article');
        card.className = 'block rounded-2xl border border-slate-800 bg-slate-950/35 p-4 transition hover:border-amber-500/35 hover:bg-slate-900/65';

        const heading = document.createElement('div');
        heading.className = 'flex flex-wrap items-start justify-between gap-2';
        const title = document.createElement('a');
        title.href = `/scanner/incident/${encodeURIComponent(highlight.incident_key)}`;
        title.className = 'text-sm font-semibold text-white';
        title.textContent = highlight.title || 'Scanner incident';
        const service = document.createElement('span');
        service.className = 'text-[9px] font-semibold uppercase tracking-wider text-slate-500';
        service.textContent = highlight.service || 'scanner';
        heading.append(title, service);

        const summary = document.createElement('p');
        summary.className = 'mt-2 text-xs leading-5 text-slate-300';
        summary.textContent = highlight.summary || '';

        const details = document.createElement('p');
        details.className = 'mt-3 text-[10px] uppercase tracking-wider text-slate-500';
        const parts = [
            highlight.incident_span_seconds
                ? `span ${formatDuration(highlight.incident_span_seconds)}`
                : 'single transmission',
            `audio ${formatDuration(highlight.recorded_audio_seconds)}`,
            highlight.response_time_seconds === null || highlight.response_time_seconds === undefined
                ? 'response not captured'
                : `response ${formatDuration(highlight.response_time_seconds)}`
        ];
        if (highlight.service === 'police' && highlight.outcome) {
            parts.push(`outcome: ${highlight.outcome}`);
        }
        details.textContent = parts.join(' · ');

        const evidenceList = document.createElement('div');
        evidenceList.className = 'mt-3 space-y-2';
        (highlight.citations || []).slice(0, 2).forEach((source) => {
            const evidence = document.createElement('div');
            evidence.className = 'rounded-xl border border-slate-800 bg-slate-950/55 p-3';
            const label = document.createElement('a');
            label.href = source.archive_url;
            label.className = 'text-[9px] font-semibold uppercase tracking-[0.16em] text-sky-300 hover:text-white';
            label.textContent = `Actual radio · Call #${source.call_id}`;
            const excerpt = document.createElement('p');
            excerpt.className = 'mt-1.5 text-xs leading-5 text-slate-300';
            excerpt.textContent = source.excerpt || 'No transcript available.';
            evidence.append(label, excerpt);
            if (source.audio_url) {
                const audio = document.createElement('audio');
                audio.className = 'mt-2 h-8 w-full';
                audio.controls = true;
                audio.preload = 'none';
                audio.src = source.audio_url;
                evidence.appendChild(audio);
            }
            evidenceList.appendChild(evidence);
        });

        const open = document.createElement('a');
        open.href = `/scanner/incident/${encodeURIComponent(highlight.incident_key)}`;
        open.className = 'mt-3 block text-xs font-semibold text-sky-300';
        open.textContent = 'Open complete incident →';
        card.appendChild(heading);
        const hasActualEvidence = (highlight.citations || []).length > 0;
        const isGenericSummary = /radio traffic was recorded in/i.test(
            highlight.summary || ''
        );
        if (!hasActualEvidence || !isGenericSummary) {
            card.appendChild(summary);
        }
        card.appendChild(details);
        if (evidenceList.childElementCount) card.appendChild(evidenceList);
        card.appendChild(open);
        return card;
    }

    function createTownSection(town) {
        const section = document.createElement('section');
        section.className = 'panel p-5 sm:p-7';

        const heading = document.createElement('div');
        heading.className = 'flex flex-wrap items-start justify-between gap-3';
        const copy = document.createElement('div');
        const title = document.createElement('h2');
        title.className = 'text-xl font-semibold text-white';
        title.textContent = town.scope?.town || 'Town';
        const totals = document.createElement('p');
        totals.className = 'mt-1 text-[10px] uppercase tracking-wider text-slate-500';
        totals.textContent = totalsLine(town.totals);
        copy.append(title, totals);
        heading.appendChild(copy);
        section.appendChild(heading);

        if (town.ned_take) {
            const commentary = document.createElement('blockquote');
            commentary.className = 'mt-4 border-l-2 border-amber-400/55 pl-4 text-sm leading-6 text-amber-100';
            commentary.textContent = town.ned_take;
            section.appendChild(commentary);
        }

        town.departments?.forEach((department) => {
            const departmentSection = document.createElement('div');
            departmentSection.className = 'mt-6 border-t border-slate-800 pt-5';
            const departmentHeading = document.createElement('div');
            departmentHeading.className = 'flex flex-wrap items-center justify-between gap-2';
            const label = document.createElement('h3');
            label.className = 'text-sm font-semibold text-white';
            label.textContent = department.headline || 'Department';
            const counts = document.createElement('span');
            counts.className = 'text-[10px] uppercase tracking-wider text-slate-500';
            counts.textContent = totalsLine(department.totals);
            departmentHeading.append(label, counts);
            departmentSection.appendChild(departmentHeading);

            if (department.ned_take) {
                const note = document.createElement('p');
                note.className = 'mt-2 text-xs leading-5 text-amber-100/80';
                note.textContent = department.ned_take;
                departmentSection.appendChild(note);
            }
            const incidents = document.createElement('div');
            incidents.className = 'mt-4 grid grid-cols-1 gap-3 lg:grid-cols-2';
            (department.highlights || []).slice(0, 4).forEach((highlight) => {
                incidents.appendChild(createIncidentLink(highlight));
            });
            if (incidents.childElementCount) departmentSection.appendChild(incidents);
            section.appendChild(departmentSection);
        });
        return section;
    }

    function render(data) {
        const take = data.take || {};
        const meta = document.getElementById('daily-take-meta');
        const watermark = data.source_watermark || data.fact_pack?.source_watermark || {};
        meta.textContent = `${data.day || day} · through call #${watermark.max_call_id || '—'} · ${data.edition_type || 'rolling'} edition`;

        contentEl.replaceChildren();
        const overview = document.createElement('section');
        overview.className = 'panel p-5 sm:p-7';
        const title = document.createElement('h2');
        title.className = 'text-xl font-semibold text-white';
        title.textContent = take.headline || 'Network overview';
        const summary = document.createElement('p');
        summary.className = 'mt-3 text-sm leading-6 text-slate-300';
        summary.textContent = take.straight_summary || '';
        const commentary = document.createElement('blockquote');
        commentary.className = 'mt-4 border-l-2 border-amber-400/55 pl-4 text-sm leading-6 text-amber-100';
        commentary.textContent = take.ned_take || '';
        overview.append(title, summary, commentary);
        contentEl.appendChild(overview);

        (take.towns || []).forEach((town) => {
            contentEl.appendChild(createTownSection(town));
        });
        loadingEl.classList.add('hidden');
        contentEl.classList.remove('hidden');
    }

    fetch(`/scanner/api/neds-take?date=${encodeURIComponent(day)}`, {
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
        .then(render)
        .catch((error) => {
            console.warn('[DailyTake] Request failed:', error);
            loadingEl.textContent = 'The daily report could not be loaded.';
        });
})();
