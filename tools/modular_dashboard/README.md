# Modular Scanner Dashboard

**Phase 1 — Scanner Control Center**

A clean, modular Python control center for monitoring, starting, stopping, restarting, inspecting, and validating the scanner radio pipeline. Built to replace the old Textual-based `scanner_dashboard.py` with a maintainable foundation that can later grow into a full multi-domain operations center.

---

## What this app is

A web dashboard + PySide6 desktop dashboard that gives you a single place to:

- See the health of all scanner-related systemd user services
- Start / stop / restart individual services
- Restart the entire scanner stack (explicit action only)
- Inspect the Redis scanner queue/backlog
- Validate scanner archive and database paths
- Monitor backlog growth when the transcription listener is down
- View dashboard logs

---

## What it replaces

The old reference dashboard:

```
/home/ned/Documents/neds_scanner_radio_full_pipeline_with_git/tools/scanner_dashboard.py
```

That file is **read-only reference only**. It has not been modified and will not be modified. The new dashboard does not depend on it, import it, or call it.

---

## What Phase 1 includes

- Scanner service monitoring and control
- Redis scanner queue/backlog inspection
- Scanner archive/database path checks
- Basic log viewing
- Web dashboard (FastAPI + Jinja2)
- Desktop dashboard (PySide6)

## What Phase 1 does NOT include

- Meeting transcription controls (future)
- GPU/model management (future)
- Local LLM chat or model controls (future)
- AI traffic-cop routing (future)

The project structure is designed so these can be added as new core modules + web routes + desktop panels without rewriting anything.

---

## Architecture

```
modular_dashboard/
  core/           ← All scanner control logic lives here. No duplication.
  web/            ← FastAPI shell. Only calls core. No business logic.
  desktop/        ← PySide6 shell. Only calls core. No business logic.
  scripts/        ← Entrypoints for web, desktop, and CLI validate.
  systemd/        ← systemd user service for the web app.
```

The **core** owns scanner control behaviour. The web app and desktop app are UI shells only. Nothing is duplicated between them.

### Scanner queue / backlog behaviour

The scanner recorder and listener are modular and queue-backed. When the transcription listener goes down, the Redis work items remain. When the listener comes back up, it picks up the backlog.

This control center:

- Reports backlog size and oldest pending age
- Reports whether the listener is active or inactive
- Does **not** treat listener downtime as data loss
- Does **not** replace the listener
- Does **not** become a mandatory middleman for scanner ingestion

---

## Activate environment

```bash
pyscan
```

This activates `~/venv`. All commands below assume the environment is active.

---

## Install dependencies

```bash
cd /home/ned/Documents/neds_scanner_radio_full_pipeline_with_git/tools/modular_dashboard
pyscan
pip install -r requirements.txt
```

---

## Run web app

```bash
cd /home/ned/Documents/neds_scanner_radio_full_pipeline_with_git/tools/modular_dashboard
pyscan
python -m modular_dashboard.scripts.run_web
```

Open: http://127.0.0.1:8010

---

## Run desktop app

```bash
cd /home/ned/Documents/neds_scanner_radio_full_pipeline_with_git/tools/modular_dashboard
pyscan
python -m modular_dashboard.scripts.run_desktop
```

---

## Validate scanner stack (CLI)

```bash
cd /home/ned/Documents/neds_scanner_radio_full_pipeline_with_git/tools/modular_dashboard
pyscan
python -m modular_dashboard.scripts.validate_stack
```

Exits 0 if all checks pass, 1 if any problems are detected.

---

## Install optional systemd web service

```bash
cp modular_dashboard/systemd/modular-dashboard-web.service ~/.config/systemd/user/
systemctl --user daemon-reload
systemctl --user enable modular-dashboard-web.service
systemctl --user start modular-dashboard-web.service
systemctl --user status modular-dashboard-web.service
```

If your venv path is not `/home/ned/venv`, edit `ExecStart` in the service file before installing.

---

## API endpoints

### Health / status

| Method | Path | Description |
|--------|------|-------------|
| GET | `/api/health` | Basic app liveness |
| GET | `/api/status` | Full dashboard status |
| GET | `/api/scanner/health` | Combined scanner health |
| GET | `/api/scanner/services` | Scanner service statuses |
| GET | `/api/scanner/queue` | Redis queue/backlog status |
| GET | `/api/scanner/redis` | Redis status + scanner keys |
| GET | `/api/scanner/paths` | Path checks |
| GET | `/api/logs` | Recent dashboard log lines |

### Service actions

| Method | Path | Description |
|--------|------|-------------|
| POST | `/api/scanner/service/{name}/start` | Start a service |
| POST | `/api/scanner/service/{name}/stop` | Stop a service |
| POST | `/api/scanner/service/{name}/restart` | Restart a service |

### Stack actions

| Method | Path | Description |
|--------|------|-------------|
| POST | `/api/scanner/stack/restart` | Restart entire stack (explicit only) |
| POST | `/api/scanner/stack/validate` | Validate scanner stack |

---

## Curl test commands

```bash
curl http://127.0.0.1:8010/api/health
curl http://127.0.0.1:8010/api/status
curl http://127.0.0.1:8010/api/scanner/health
curl http://127.0.0.1:8010/api/scanner/services
curl http://127.0.0.1:8010/api/scanner/queue
curl http://127.0.0.1:8010/api/scanner/redis
curl http://127.0.0.1:8010/api/scanner/paths
curl http://127.0.0.1:8010/api/logs

curl -X POST http://127.0.0.1:8010/api/scanner/service/scanner-mcp.service/restart
curl -X POST http://127.0.0.1:8010/api/scanner/stack/validate
```

---

## Configuration

All configuration is in `modular_dashboard/core/config.py`.

Values are read from:
1. Real environment variables (highest priority)
2. `PROJECT_ROOT/.env`
3. Hard defaults in `config.py`

Key settings:

| Setting | Default |
|---------|---------|
| `REDIS_URL` | `redis://127.0.0.1:6379/0` |
| `SCANNER_DB_PATH` | `/home/ned/data/scanner_calls/scanner_calls.db` |
| `ARCHIVE_BASE` | `/home/ned/data/scanner_calls/scanner_archive` |
| `DEFAULT_WEB_HOST` | `127.0.0.1` |
| `DEFAULT_WEB_PORT` | `8010` |
| `LOG_FILE` | `logs/modular_dashboard.log` (inside this directory) |

---

## Future expansion points

These are deliberately not built yet. Each can be added as a new core module + web route + desktop panel:

| Domain | Notes |
|--------|-------|
| Meeting transcription | Add `core/meeting_*.py`, `web/` routes, desktop tab |
| GPU/model management | Add `core/gpu_*.py`, nvidia-smi wrappers |
| Local LLM chat/model controls | Add `core/llm_*.py`, llama-server / vLLM wrappers |
| AI traffic-cop routing | Add `core/router_*.py` |

---

## Important warnings

- Do NOT modify `/home/ned/Documents/neds_scanner_radio_full_pipeline_with_git/tools/scanner_dashboard.py`. It is the old reference only.
- Do NOT modify the scanner transcription pipeline files unless you are intentionally doing scanner pipeline work:
  - `transcriber/transcribe_stream_listener_mcp.py`
  - `transcriber/scanner_transcriber_mcp.py`
- Do NOT make this control center a mandatory middleman for scanner audio ingestion.
- Service stop/restart actions only execute when you explicitly click the button or POST to the API. Nothing is automatic.
