"""
web/routes.py — All FastAPI route definitions (API + pages).

API routes: /api/...
Page routes: /, /scanner, /services, /redis, /logs
"""
from __future__ import annotations

from pathlib import Path
from typing import Any

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates

from modular_dashboard.core.logging_config import get_logger
from modular_dashboard.core.scanner_actions import (
    restart_scanner_service,
    restart_scanner_stack,
    start_scanner_service,
    stop_scanner_service,
    validate_scanner_stack,
    get_service_detail,
)
from modular_dashboard.core.scanner_health import get_dashboard_status, get_scanner_health
from modular_dashboard.core.scanner_paths import check_scanner_paths
from modular_dashboard.core.scanner_queue import check_redis, get_scanner_queue_status, get_redis_dashboard_data
from modular_dashboard.core.scanner_services import get_scanner_services_status
from modular_dashboard.core.utils import utc_now_str
from modular_dashboard.core.config import LOG_FILE

log = get_logger("web.routes")

_TEMPLATES_DIR = Path(__file__).parent / "templates"
templates = Jinja2Templates(directory=str(_TEMPLATES_DIR))

# ---------------------------------------------------------------------------
# Routers
# ---------------------------------------------------------------------------

api_router = APIRouter(prefix="/api")
page_router = APIRouter()


# ---------------------------------------------------------------------------
# API — Health / Status
# ---------------------------------------------------------------------------

@api_router.get("/health")
async def api_health() -> dict:
    return {"status": "ok", "timestamp": utc_now_str(), "app": "modular_dashboard"}


@api_router.get("/status")
async def api_status() -> dict:
    status = get_dashboard_status()
    return status.model_dump()


# ---------------------------------------------------------------------------
# API — Scanner health
# ---------------------------------------------------------------------------

@api_router.get("/scanner/health")
async def api_scanner_health() -> dict:
    health = get_scanner_health()
    return health.model_dump()


@api_router.get("/scanner/services")
async def api_scanner_services() -> dict:
    summary = get_scanner_services_status()
    return summary.model_dump()


@api_router.get("/scanner/queue")
async def api_scanner_queue() -> dict:
    q = get_scanner_queue_status()
    return q.model_dump()


@api_router.get("/scanner/redis")
async def api_scanner_redis() -> dict:
    r = check_redis()
    keys = []
    if r.reachable:
        from modular_dashboard.core.scanner_queue import get_scanner_related_keys
        keys = get_scanner_related_keys(100)
    return {"redis": r.model_dump(), "scanner_keys": keys}


@api_router.get("/scanner/paths")
async def api_scanner_paths() -> dict:
    paths = check_scanner_paths()
    return {"paths": [p.model_dump() for p in paths]}


# ---------------------------------------------------------------------------
# API — Scanner service actions
# ---------------------------------------------------------------------------

@api_router.post("/scanner/service/{service_name}/start")
async def api_service_start(service_name: str) -> dict:
    log.info("API: start %s", service_name)
    resp = start_scanner_service(service_name)
    return resp.model_dump()


@api_router.post("/scanner/service/{service_name}/stop")
async def api_service_stop(service_name: str) -> dict:
    log.info("API: stop %s", service_name)
    resp = stop_scanner_service(service_name)
    return resp.model_dump()


@api_router.post("/scanner/service/{service_name}/restart")
async def api_service_restart(service_name: str) -> dict:
    log.info("API: restart %s", service_name)
    resp = restart_scanner_service(service_name)
    return resp.model_dump()


@api_router.get("/scanner/service/{service_name}/detail")
async def api_service_detail(service_name: str, log_lines: int = 80) -> dict:
    """Return status, properties, and journal logs for a single service."""
    log.info("API: detail %s", service_name)
    return get_service_detail(service_name, log_lines=log_lines)


# ---------------------------------------------------------------------------
# API — Stack actions
# ---------------------------------------------------------------------------

@api_router.post("/scanner/stack/restart")
async def api_stack_restart() -> dict:
    log.warning("API: explicit scanner stack restart requested")
    results = restart_scanner_stack()
    return {
        "action": "restart_stack",
        "results": [r.model_dump() for r in results],
        "timestamp": utc_now_str(),
    }


@api_router.post("/scanner/stack/validate")
async def api_stack_validate() -> dict:
    return validate_scanner_stack()


# ---------------------------------------------------------------------------
# API — Logs
# ---------------------------------------------------------------------------

@api_router.get("/logs")
async def api_logs(lines: int = 200) -> dict:
    log_path = Path(LOG_FILE)
    if not log_path.exists():
        return {"log_file": str(log_path), "lines": [], "error": "Log file does not exist yet."}
    try:
        all_lines = log_path.read_text(encoding="utf-8", errors="replace").splitlines()
        tail = all_lines[-lines:] if len(all_lines) > lines else all_lines
        return {"log_file": str(log_path), "lines": tail, "total_lines_read": len(tail)}
    except Exception as exc:
        return {"log_file": str(log_path), "lines": [], "error": str(exc)}


# ---------------------------------------------------------------------------
# Page routes
# ---------------------------------------------------------------------------

import re as _re

_LOG_RE = _re.compile(
    r"^(?P<ts>\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}[^\s]*)\s+"
    r"(?P<level>DEBUG|INFO|WARNING|ERROR|CRITICAL)\s+"
    r"(?P<logger>\S+)\s+(?P<msg>.*)$"
)


def _parse_log_lines(raw: list[str]) -> list[dict]:
    """Parse raw log lines into structured dicts for the template."""
    result = []
    for line in raw:
        m = _LOG_RE.match(line.rstrip())
        if m:
            result.append({
                "ts": m.group("ts"),
                "level": m.group("level"),
                "logger": m.group("logger"),
                "msg": m.group("msg"),
                "raw": line.rstrip(),
            })
        elif line.strip():
            result.append({
                "ts": "",
                "level": "INFO",
                "logger": "",
                "msg": line.rstrip(),
                "raw": line.rstrip(),
            })
    return result

@page_router.get("/", response_class=HTMLResponse)
async def page_dashboard(request: Request):
    health = get_scanner_health()
    return templates.TemplateResponse(request, "dashboard.html", {"health": health})


@page_router.get("/scanner", response_class=HTMLResponse)
async def page_scanner(request: Request):
    health = get_scanner_health()
    return templates.TemplateResponse(request, "scanner.html", {"health": health})


@page_router.get("/services", response_class=HTMLResponse)
async def page_services(request: Request):
    summary = get_scanner_services_status()
    return templates.TemplateResponse(request, "services.html", {"summary": summary})


@page_router.get("/redis", response_class=HTMLResponse)
async def page_redis(request: Request):
    data = get_redis_dashboard_data()
    return templates.TemplateResponse(request, "redis.html", {"data": data})


@api_router.get("/scanner/redis/dashboard")
async def api_redis_dashboard() -> dict:
    """Return full Redis dashboard data as JSON (for auto-refresh)."""
    return get_redis_dashboard_data()


@page_router.get("/logs", response_class=HTMLResponse)
async def page_logs(request: Request):
    log_path = Path(LOG_FILE)
    log_lines = []
    error = None
    if log_path.exists():
        try:
            all_text = log_path.read_text(encoding="utf-8", errors="replace")
            raw_lines = all_text.splitlines()
            log_lines = _parse_log_lines(raw_lines[-500:])
        except Exception as exc:
            error = str(exc)
    else:
        error = f"Log file not yet created: {log_path}"
    return templates.TemplateResponse(request, "logs.html", {
        "log_lines": log_lines,
        "log_file": str(log_path),
        "error": error,
    })
