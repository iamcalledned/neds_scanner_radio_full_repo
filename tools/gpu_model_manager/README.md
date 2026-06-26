# GPU Model Manager

RTX 5090 control plane — GPU inventory, runtime registry, VRAM policy, and service control.

---

## What Was Built

A two-interface app (web + desktop) that acts as a **capacity control plane** for all GPU compute on this machine. It answers the question "can I safely load another model right now?" and provides visibility into GPU state, running services, and VRAM usage.

### Web UI — FastAPI + Jinja2

Runs at `http://127.0.0.1:8020`. Eight pages:

| Page | URL | What it shows |
|------|-----|---------------|
| Dashboard | `/` | Health aggregate, VRAM bar, scanner status, policy summary, quick links |
| GPU | `/gpu` | nvidia-smi stats, process table with heuristic classification |
| Runtimes | `/runtimes` | Registry table + inline can-start admission tester |
| Services | `/services` | Discovered systemd model services with Start/Stop/Restart buttons |
| Policy | `/policy` | VRAM math table, formula, admission tester form |
| Leases | `/leases` | Active VRAM reservations, create/release controls |
| Logs | `/logs` | Live log tail with level filter and text search |

All pages auto-refresh or have a manual Refresh button. Templates are in
`gpu_model_manager/web/templates/`. CSS/JS in `gpu_model_manager/web/static/`.

### Desktop UI — PySide6

A dark-themed Qt window with the same 7 tabs as the web UI. Each tab uses a
background `QThread` worker so the UI never blocks. Requires PySide6 installed
in the venv.

### Core — shared business logic

All intelligence lives in `gpu_model_manager/core/`. Both the web and desktop
import only from core — there is no logic duplication.

| Module | What it does |
|--------|-------------|
| `gpu_inventory.py` | Reads GPU stats via `nvidia-smi` subprocess — no torch import |
| `process_inspector.py` | Classifies GPU processes by heuristic (ps args inspection) |
| `runtime_registry.py` | Loads runtime definitions from `config/runtimes.json` |
| `service_controller.py` | Discovers and controls systemd user services |
| `endpoint_monitor.py` | HTTP reachability checks for each runtime's endpoint |
| `policy_engine.py` | VRAM admission decisions |
| `lease_manager.py` | TTL-based VRAM reservations stored in `state/leases.json` |
| `health.py` | Aggregates all of the above into a single health status |
| `actions.py` | Start/stop/restart with policy enforcement |
| `command_runner.py` | Safe subprocess wrapper — never uses `shell=True` |

---

## What This Does NOT Do

- Transcribe scanner audio — `scanner-mcp.service` owns that, untouched
- Consume Redis scanner stream events
- Replace `ModelRouter` inside the scanner MCP server
- Implement meeting transcription (placeholder entry in registry, no logic)
- Route AI inference requests
- Kill processes or auto-unload runtimes

---

## File Layout

```
tools/gpu_model_manager/
├── config/
│   └── runtimes.json          ← edit this to add/change/remove runtimes
├── gpu_model_manager/
│   ├── core/                  ← all business logic
│   ├── web/                   ← FastAPI app, routes, templates, static
│   │   ├── templates/         ← Jinja2 HTML (8 pages)
│   │   └── static/            ← dashboard.css, dashboard.js
│   ├── desktop/               ← PySide6 app
│   │   └── widgets/           ← one panel per tab (7 widgets)
│   └── scripts/               ← run_web.py, run_desktop.py, validate_stack.py
├── state/
│   └── leases.json            ← auto-created, runtime VRAM reservations
├── logs/
│   └── gpu_model_manager.log  ← auto-created
├── systemd/
│   └── gpu-model-manager-web.service
├── pyproject.toml
└── requirements.txt
```

---

## Installation

```bash
# Activate venv
pyscan

# Install (first time, from the gpu_model_manager directory)
cd /home/ned/Documents/neds_scanner_radio_full_pipeline_with_git/tools/gpu_model_manager
pip install -e .

# Smoke-test everything
python -m gpu_model_manager.scripts.validate_stack
```

---

## Running

```bash
# Web app — http://127.0.0.1:8020
python -m gpu_model_manager.scripts.run_web

# Desktop app
python -m gpu_model_manager.scripts.run_desktop
```

### Run web as a systemd user service

```bash
cp systemd/gpu-model-manager-web.service ~/.config/systemd/user/
systemctl --user daemon-reload
systemctl --user enable --now gpu-model-manager-web.service
systemctl --user status gpu-model-manager-web.service
```

---

## Runtime Registry — config/runtimes.json

**This is a plain JSON file. Edit it directly. No code changes needed.**

Each entry has these fields:

| Field | Required | Description |
|-------|----------|-------------|
| `key` | yes | Unique identifier used in all API calls |
| `display_name` | yes | Human-readable label shown in UI |
| `kind` | yes | `whisper` or `llm` |
| `owner` | yes | Which subsystem owns this (scanner, meeting, chat, shared) |
| `service_name` | no | systemd user service name — if set, manager can start/stop it |
| `endpoint` | no | URL to check for reachability; LLM endpoints check `/v1/models` |
| `protected` | yes | `true` = never auto-stop, always reserve VRAM |
| `warm` | yes | `true` = always running, `false` = on-demand, `"optional"` = either |
| `estimated_vram_mb` | yes | Expected VRAM usage — used for admission decisions |
| `priority` | yes | `critical`, `interactive`, or `batch` |
| `stop_policy` | yes | `never_auto` or `allowed` |
| `restart_policy` | yes | `manual_confirm` or `allowed` |
| `description` | yes | Free text shown in UI |

**To add a runtime:** append a new JSON object to the array. Restart the app.

**Important endpoint rules:**
- MCP endpoints (e.g. scanner): use the full `/mcp` path, e.g. `http://127.0.0.1:8000/mcp`
- LLM endpoints: use the base URL only, e.g. `http://192.168.86.53:30000` — the manager appends `/v1/models` for health checks
- If `llama-server` or another service binds to a LAN IP instead of `127.0.0.1`, use the LAN IP in the endpoint field

---

## Scanner Protection Policy

`scanner-mcp.service` is a **protected runtime** (set in `config/runtimes.json`).

The manager will:
- Never auto-stop it
- Always deduct `SCANNER_WHISPER_RESERVED_MB` from available VRAM regardless of whether it is detected as running
- Refuse stop/restart unless `confirm_protected=True` is explicitly passed
- Report it as critical priority in all health summaries

---

## VRAM Policy Math

```
effective_available_mb =
    free_vram_mb
    - GPU_SAFETY_MARGIN_MB        (default 2500 MB)
    - SCANNER_WHISPER_RESERVED_MB (default 7000 MB)

allowed = effective_available_mb >= runtime.estimated_vram_mb
```

The safety margin and scanner reserve are environment variables. All other
VRAM numbers come from `config/runtimes.json`.

---

## What Is Fixed vs What Is Configurable

### Fixed in code (changing requires editing Python source)

- The scanner protection guard in `service_controller.py` — `scanner-mcp.service` is in a hardcoded `frozenset` as a last line of defense
- The VRAM formula itself (`free - safety - reserve >= required`)
- Service discovery keywords (`scanner`, `mcp`, `whisper`, `llm`, `vllm`, `llama`, `model`, `transcriber`)
- Process classification heuristics in `process_inspector.py`
- Web app port binding logic (`run_web.py` reads from config)

### Configurable via config/runtimes.json (no restart needed for validate_stack; restart app to take effect)

- Every runtime definition: VRAM estimate, service name, endpoint URL, protection flag, priority, policies
- Add or remove runtimes entirely

### Configurable via environment variables (or .env file in the gpu_model_manager directory)

| Variable | Default | Description |
|----------|---------|-------------|
| `GPU_MGR_HOST` | `127.0.0.1` | Web app bind host |
| `GPU_MGR_PORT` | `8020` | Web app bind port |
| `GPU_SAFETY_MARGIN_MB` | `2500` | Always-free VRAM headroom |
| `SCANNER_WHISPER_RESERVED_MB` | `7000` | VRAM always deducted for scanner |
| `SCANNER_MCP_URL` | `http://127.0.0.1:8000/mcp` | Scanner MCP reachability check URL |
| `LOCAL_LLM_BASE_URL` | `http://192.168.86.53:30000` | llama-server base URL |
| `LOCAL_LLM_MODELS_URL` | `http://192.168.86.53:30000/v1/models` | LLM model list endpoint |
| `LOCAL_LLM_CHAT_URL` | `http://192.168.86.53:30000/v1/chat/completions` | LLM chat endpoint |
| `MEETING_MCP_URL` | `http://127.0.0.1:8011/mcp` | Meeting MCP endpoint |
| `LEASE_FILE` | `state/leases.json` | Where VRAM leases are persisted |
| `GPU_MGR_LOG_DIR` | `logs/` | Log directory |
| `GPU_MGR_LOG_LEVEL` | `INFO` | Log verbosity |
| `GPU_MGR_RUNTIMES_CONFIG` | `config/runtimes.json` | Path to runtime definitions file |
| `SCANNER_PROJECT_ROOT` | `/home/ned/Documents/neds_scanner_radio_full_pipeline_with_git` | Project root override |
| `GPU_MODEL_MANAGER_ROOT` | `tools/gpu_model_manager` under project root | App root override |

---

## API Reference

| Method | Path | Description |
|--------|------|-------------|
| GET | `/api/health` | Manager health aggregate |
| GET | `/api/status` | Dashboard status blob |
| GET | `/api/gpu` | GPU hardware info |
| GET | `/api/gpu/processes` | GPU compute processes with classification |
| GET | `/api/runtimes` | All runtime definitions |
| GET | `/api/runtimes/{key}` | Single runtime with live status |
| GET | `/api/services` | Discovered model services |
| GET | `/api/endpoints` | Endpoint reachability check |
| GET | `/api/policy` | VRAM policy state |
| POST | `/api/policy/can-start` | Admission decision for a runtime |
| GET | `/api/leases` | Active VRAM leases |
| POST | `/api/leases` | Create a lease |
| POST | `/api/leases/{id}/release` | Release a lease |
| POST | `/api/runtime/{key}/start` | Start runtime service |
| POST | `/api/runtime/{key}/stop` | Stop runtime service |
| POST | `/api/runtime/{key}/restart` | Restart runtime service |
| GET | `/api/logs` | Recent log lines |

### Examples

```bash
# Check if meeting_whisper_large can start
curl -X POST http://127.0.0.1:8020/api/policy/can-start \
  -H "Content-Type: application/json" \
  -d '{"runtime_key": "meeting_whisper_large"}'

# Attempt to stop scanner (will be refused)
curl -X POST http://127.0.0.1:8020/api/runtime/scanner_whisper/stop \
  -H "Content-Type: application/json" \
  -d '{"confirm_protected": false}'

# Create a VRAM lease before loading a model
curl -X POST http://127.0.0.1:8020/api/leases \
  -H "Content-Type: application/json" \
  -d '{"runtime_key": "local_llm", "owner": "my_script", "ttl_seconds": 3600}'

# Current GPU state
curl http://127.0.0.1:8020/api/gpu
```

---

## Future Integration Path

- Before launching a secondary Whisper model → call `POST /api/policy/can-start`
- Meeting transcription service → `POST /api/leases` to claim VRAM before loading
- Local LLM launcher → query `GET /api/policy` before loading new model weights
- Any tool that needs to know what's available → `GET /api/runtimes`

The scanner transcription pipeline is **never modified** to integrate with this manager.
All integration goes through the API only.
