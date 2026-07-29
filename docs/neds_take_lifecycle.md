# Ned's Take incident lifecycle

Ned's Take groups individual radio recordings into estimated incident threads.
The grouping is deliberately department-aware because fire and police radio
traffic close incidents differently.

## Fire lifecycle

Typical stages:

1. `dispatched` — tone, station alert, or still-alarm announcement
2. `responding` — apparatus responding or en route
3. `on_scene` — arrival or on-scene traffic
4. `update` — operational traffic, transport, coverage, or status messages
5. `returning` — clear returning or returning to station
6. `in_quarters` — back/in quarters or back in service

`returning` does not close the incident. `in_quarters` closes it. Explicit
command-terminated language also closes it.

A new tone normally starts a new incident even when the same apparatus served
the previous incident. A repeated tone can remain in the same incident when it
has the same address and occurs within two minutes.

Address or apparatus matches can connect fire transmissions over a maximum
three-hour window. Apparatus names are extracted from both classification data
and transcript forms such as `Engine-4`, `Ladder 1`, `Rescue-1`, `A-1`, and
`P-2`.

## Police lifecycle

Typical stages:

1. `dispatched` — respond, responding, head to, or en route
2. `on_scene` — arrival or on-scene traffic
3. `update` — investigation and status traffic
4. `cleared` — clear, show me clear, clear from, or all units clear

`cleared` closes the incident. Phrases that describe radio readability or a
building-search status do not close it, including `loud and clear`,
`clear to copy`, `when you're clear`, `first floor clear`, and `channel clear`.

Generic police unit continuity is limited to 15 minutes because units are
reused across many calls. An explicit clear message can connect back to the
same unit for up to one hour.

## Editorial behavior

Highlights are not filtered by call type or transcript subject. Ranking uses
incident length, recorded duration, tones, hook requests, and play counts.
Every highlight retains source call IDs and links back to its evidence page.

Incident totals remain estimates. The persisted lifecycle stages and source
call IDs are intended to make grouping mistakes inspectable and correctable.

There is no blocked-word or "safe terms" vocabulary filter. The local LLM is
given verified labels and evidence, then asked to write only the variable
commentary. It is not responsible for grouping incidents or calculating facts.
Numeric claims are kept out of generated prose because exact counts and timing
are rendered separately from the verified data.

## Presentation layers

Ned's Take has three progressively detailed surfaces:

1. The live-bar bug opens a compact drawer. It shows Hopedale first, Milford
   second, then the remaining towns, with only a few highlighted incidents.
2. The full daily report expands the network, town, and department sections.
   Each highlighted incident links to its grouped evidence.
3. The incident page shows the complete grouped radio timeline: every source
   transcript, its audio player, lifecycle stage, timestamp, and source-call
   link.

The LLM does not receive the complete day's raw transcript dump. All
transmissions are first classified and grouped in code. A single background LLM
request receives a compact fact brief for the network, towns, and departments,
plus the actual excerpts for selected grouped incidents. The generated section
and incident commentary is persisted. Browser requests only read prepared
results; they never wait for model inference.

## Editions and freshness

Opening Ned's Take always makes a browser request with caching disabled. A
rolling edition is reused only when its source watermark (call count, highest
call ID, and latest call timestamp) still matches the scanner database. When
the watermark changes, the network, town, and department sections are rebuilt
together from the same set of grouped calls.

The rolling fact pack and its commentary are rebuilt by the background worker.
The normal cadence is five minutes. If a browser arrives after newer calls have
landed but before the next commentary run, it receives current verified facts
and deterministic fallback text immediately. It never receives stale facts and
never blocks on the LLM. Incident commentary is stored separately and is used
only while its complete source-call list still matches the current incident.
All JSON responses send `Cache-Control: no-store`.

Final editions are immutable daily snapshots. Once a final edition has been
published, a repeated scheduler run returns that stored edition instead of
overwriting the historical record.

## Highlight timing and outcomes

Highlight cards distinguish three different measurements:

- **Incident span** is the elapsed time from the first grouped transmission to
  the last grouped transmission.
- **Recorded audio** is the sum of the source recording durations.
- **Response time** is shown only when both a dispatch and a later on-scene
  transmission were heard. It measures dispatch to first on scene.

Police outcomes are inferred only from explicit radio language such as warning,
citation, arrest/custody, transport, report taken, gone on arrival, or no
enforcement action. If the outcome was not transmitted, the card says so
instead of guessing.

Town presentation is ordered Hopedale, Milford, then the remaining towns
alphabetically. Hopedale's warning and citation totals are derived from these
same explicit police outcomes.

## Per-call preprocessing boundary

Per-call AI enrichment is useful for suggestions that can be checked against
the transcript: call type, agency, unit candidates, urgency, lifecycle hints,
an explicitly labeled enhanced transcript, and a concise factual summary. These
suggestions are stored in
`scanner_call_enrichments` with the call ID, exact source fingerprint,
model/prompt version, confidence, transcript evidence, processing status,
attempt count, and timestamps. They never overwrite the source
`calls.classification` JSON. A high-confidence call type can be used as an
intelligence-layer fallback when the source classification is empty.

The web presents the source as **Original transcript** and the prepared version
as **AI enhanced call / Enhanced transcript**. The latter may clean
punctuation, spacing, and obvious scanner formatting, but it is validated for
length and vocabulary overlap and cannot replace the source transcript.

The batch can store a per-transmission joke when the recording has enough
substance, but the model is explicitly allowed to leave it blank. Many
recordings are only one fragment of a call, so the incident-level take remains
the primary comedy unit. The processing flow is:

1. deterministically enrich the completed transmission immediately;
2. batch AI classification and transcript-validation work away from the
   transcription path;
3. update the incident whenever a related transmission arrives;
4. generate incident commentary after closure or an idle debounce;
5. rebuild town and daily rollups from those stored incident records.

AI should not be the sole source for response time, incident linkage, address
identity, citations, warnings, arrests, medical facts, or closure. Those fields
need explicit transcript evidence and deterministic validation.

### AI-requested retranscription

The enrichment model cannot hear the audio. It may request a comparison
transcription only from acoustic quality metadata and concrete internal text
problems: repetition, incoherence, a known hallucination pattern, language
mismatch, truncation, an impossible phrase, or a metadata conflict. Short radio
acknowledgments alone are never a retry reason.

Requests require at least 0.8 validation confidence, are stored durably in
`scanner_retranscription_requests`, are unique per source fingerprint, and are
capped at two per call. The web worker dispatches them to
`scanner:stream:retranscribe`, two at a time by default so live transcription
is not starved. The transcriber generates a comparison with no artifact or
database overwrite. It preserves both versions and automatically promotes the
comparison only when the existing deterministic quality scorer had already
marked the original `needs_retry` and the new score improves by at least 0.1
while reaching 0.6. Otherwise the comparison is retained for review.
