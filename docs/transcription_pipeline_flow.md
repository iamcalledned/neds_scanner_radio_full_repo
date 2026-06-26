# Transcription Pipeline: End-to-End Flow

Redis XADD event → Stream Listener → MCP Server → Whisper → DB Insert

---

## Stage 1 — Recording & Event Emission

**`record/multi_scanner_recorder_with_redis.sh`**

- `rtl_sdr` / `aplay` pipelines record raw audio per channel (BFD, MFD, BPD, etc.)
- `inotifywait` watches a staging directory for completed `.wav` files
- On `close_write`: renames the file to `rec_<timestamp>_<tag>.wav`, moves it to the output dir
- Fires 3 parallel Redis writes:

```
XADD scanner:stream:new_call * tag <bfd|mfd|…> file <full_path> time <ISO>
SET scanner:<tag>:transmitting Y EX 10
SET scanner:<tag>:latest_time <ISO>
```

---

## Stage 2 — Stream Listener (the bridge)

**`transcriber/transcribe_stream_listener_mcp.py`**

- Persistent async loop: `XREAD BLOCK 5000ms scanner:stream:new_call` from `scanner:transcriber:last_id`
- For each entry:
  - Skip if file missing → log warning, increment `failed_count`
  - Skip if already in `processed` set
  - Calls `transcribe_file` tool on MCP server via **MCP streamable-HTTP client** (`http://127.0.0.1:8008/mcp`)
  - On success: marks file processed, then for each `SECONDARY_MODELS`:
    - Calls `route_and_transcribe` (no artifacts, no DB write)
    - Appends `{"model": …, "transcript": …}` to `meta["secondary_transcripts"]` in the JSON sidecar
    - Does a direct `UPDATE calls SET extra=? WHERE filename=?` on `scanner_calls.db`
  - **Always** advances `scanner:transcriber:last_id` in Redis (even on failure)

---

## Stage 3 — MCP Server Entry Point

**`transcriber/scanner_transcriber_mcp.py`**

- Long-lived FastMCP process (GPU, port 8008)
- On startup lifespan: builds `ModelRouter` from routing rules, warms default Whisper model into `_RUNTIME["state"]`, imports `scanner_db`
- `transcribe_file` tool → `_transcribe_with_state()` wrapper → delegates to `mcp_routes/transcribe_with_state.py`

---

## Stage 4 — Core Transcribe Tool

**`transcriber/mcp_routes/core_transcribe_tools.py`** → `register_core_transcribe_tools()`

- Registered `transcribe_file` tool: gets `WhisperState` from context, resolves profile, calls `_transcribe_with_state_impl`

---

## Stage 5 — Main Transcription Logic

**`transcriber/mcp_routes/transcribe_with_state.py`** → `transcribe_with_state()`

```
src path
  │
  ├─ is_allowed_fn()          ← path security check
  ├─ file exists?
  ├─ get_duration_fn()         ← reject if < MIN_DURATION
  ├─ is_static_fn()            ← reject if static/noise (RMS check)
  │
  ├─ detect_category_fn()      ← parse tag from filename → feed/town lookup via source_map
  │                               → clean_dir  = ARCHIVE_BASE/clean/<feed>/
  │                               → raw_dir    = ARCHIVE_BASE/raw/<feed>/
  │
  ├─ ModelRouter.resolve()     ← picks model_key + TranscribeSettings by routing rules
  │
  ├─ preprocess_audio_fn()     ← ffmpeg/sox pipeline (noise gate, normalize)
  │                               → writes tmp/<stem>_clean.wav
  │
  ├─ transcribe_wavefile_fn()  ← faster-whisper inference on tmp wav → raw text
  │     (mcp_functions/transcribe_wavefile.py)
  │
  ├─ score_transcript_fn()     ← quality score (length, RMS, word patterns)
  │
  ├─ enrich_meta_in_memory()   ← nlp_zero_shot: call_type, units, address extraction
  │
  ├─ write_artifacts (if True):
  │     ARCHIVE_BASE/clean/<feed>/<stem>.txt   ← raw text
  │     ARCHIVE_BASE/clean/<feed>/<stem>.json  ← full meta dict
  │     ARCHIVE_BASE/clean/<feed>/<stem>.wav   ← clean audio copy
  │
  ├─ scanner_db.insert_call(db_meta)  (if insert_db=True)
  │     → shared/scanner_db.py → scanner_calls.db → table: calls
  │
  └─ cleanup: delete tmp wav, gc.collect(), torch.cuda.empty_cache()
```

Returns `{ok, text, duration, rms, profile, model_key, artifacts: {txt, json, wav}, db, elapsed_s}`

---

## Stage 6 — DB Write

**`shared/scanner_db.py`** → `insert_call()`

- SQLite: `scanner_calls.db`, table `calls`
- Columns include: `town`, `dept`, `filename`, `transcript`, `duration`, `rms`, `timestamp`, `classification` (JSON), `extra` (full meta JSON), `transcription_score`, `needs_retry`, `needs_review`, `hook_request`, derived address fields
- After secondary models run (back in Stage 2), `extra` column is overwritten with the updated meta including `secondary_transcripts`

---

## File Map

| File | Role |
|------|------|
| `record/multi_scanner_recorder_with_redis.sh` | Records audio, emits Redis XADD event |
| `transcriber/transcribe_stream_listener_mcp.py` | Redis consumer loop, MCP HTTP client, secondary model runs |
| `transcriber/scanner_transcriber_mcp.py` | FastMCP server, Whisper lifecycle, tool registration |
| `transcriber/mcp_routes/core_transcribe_tools.py` | `transcribe_file` / `retranscribe_file` tool definitions |
| `transcriber/mcp_routes/transcribe_with_state.py` | All transcription logic: preprocess → whisper → score → enrich → write → DB |
| `transcriber/mcp_functions/transcribe_wavefile.py` | faster-whisper inference wrapper |
| `shared/scanner_db.py` | SQLite `insert_call()` → `scanner_calls.db` |

---

## Notes for Planned Changes

- **Model gatekeeper** — currently lives inside `ModelRouter` (built in `scanner_transcriber_mcp.py` lifespan, used in `transcribe_with_state.py`). A standalone gatekeeper could sit in Stage 2 (listener) or as a pre-check in Stage 5 before Whisper is invoked.
- **Secondary models** — currently driven by `SECONDARY_MODELS` env var in the listener; each runs a separate MCP call with no artifacts/DB write, then the listener patches the DB directly.
- The `insert_db=True` flag passed by the listener is the single switch that controls whether Stage 5 writes to the DB on first-pass transcription.
