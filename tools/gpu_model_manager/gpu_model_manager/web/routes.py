"""
gpu_model_manager/web/routes.py

All API and page routes. One router, two sections.
API routes return JSON. Page routes return Jinja2 templates.
"""
from __future__ import annotations

from pathlib import Path
from typing import Any

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import JSONResponse
from fastapi.templating import Jinja2Templates

from ..core.actions import (
    dry_run_start_runtime,
    restart_runtime,
    start_runtime,
    stop_runtime,
    validate_runtime_stack,
)
from ..core.config import LOG_FILE
from ..core.endpoint_monitor import check_all_runtime_endpoints
from ..core.gpu_inventory import get_gpu_processes, get_gpu_status
from ..core.health import get_dashboard_status, get_manager_health
from ..core.lease_manager import (
    create_lease,
    get_lease_status,
    release_lease,
)
from ..core.policy_engine import can_start_runtime, get_policy_status
from ..core.process_inspector import classify_gpu_process
from ..core.reconciler import get_runtime_reconciliation
from ..core.registry_validator import validate_runtime_registry
from ..core.runtime_registry import (
    get_runtime_definition,
    get_runtime_registry,
    get_runtime_status,
)
from ..core.schemas import ActionRequest, CanStartRequest
from ..core.service_controller import discover_model_services
from ..core.utils import read_log_tail

router = APIRouter()
templates = Jinja2Templates(directory=str(Path(__file__).parent / "templates"))


# ===========================================================================
# API — read
# ===========================================================================

@router.get("/api/health")
def api_health():
    health = get_manager_health()
    return health.model_dump(mode="json")


@router.get("/api/status")
def api_status():
    return get_dashboard_status()


@router.get("/api/gpu")
def api_gpu():
    gpu = get_gpu_status()
    return gpu.model_dump()


@router.get("/api/gpu/processes")
def api_gpu_processes():
    procs = get_gpu_processes()
    enriched = []
    for p in procs:
        d = p.model_dump()
        d["classification"] = classify_gpu_process(p)
        enriched.append(d)
    return {"processes": enriched, "count": len(enriched)}


@router.get("/api/runtimes")
def api_runtimes():
    registry = get_runtime_registry()
    return {
        "runtimes": {k: v.model_dump() for k, v in registry.items()},
        "keys": list(registry.keys()),
    }


@router.get("/api/runtimes/{runtime_key}")
def api_runtime_detail(runtime_key: str):
    status = get_runtime_status(runtime_key)
    if status is None:
        raise HTTPException(status_code=404, detail=f"Runtime '{runtime_key}' not found")
    return status.model_dump(mode="json")


@router.get("/api/services")
def api_services():
    services = discover_model_services()
    return {"services": [s.model_dump() for s in services]}


@router.get("/api/endpoints")
def api_endpoints():
    results = check_all_runtime_endpoints()
    return {k: v.model_dump() for k, v in results.items()}


@router.get("/api/policy")
def api_policy():
    return get_policy_status().model_dump()


@router.get("/api/leases")
def api_leases():
    return get_lease_status().model_dump(mode="json")


@router.get("/api/logs")
def api_logs(n: int = 300):
    lines = read_log_tail(LOG_FILE, n=n)
    return {
        "log_file": str(LOG_FILE),
        "lines": lines,
        "total_lines": len(lines),
    }

    @router.get("/api/runtimes/validate")
    def api_runtimes_validate():
        res = validate_runtime_registry()
        return res.model_dump()

    @router.get("/api/runtimes/reconcile")
    def api_runtimes_reconcile():
        res = get_runtime_reconciliation()
        return res.model_dump()


# ===========================================================================
# API — actions
# ===========================================================================

@router.post("/api/policy/can-start")
def api_can_start(req: CanStartRequest):
    decision = can_start_runtime(req.runtime_key, force=req.force)
    return decision.model_dump()


@router.post("/api/leases")
def api_create_lease(body: dict):
    runtime_key = body.get("runtime_key", "")
    owner = body.get("owner", "web")
    ttl_seconds = body.get("ttl_seconds")
    metadata = body.get("metadata", {})
    if not runtime_key:
        raise HTTPException(status_code=400, detail="runtime_key is required")
    lease = create_lease(runtime_key, owner, ttl_seconds=ttl_seconds, metadata=metadata)
    return lease.model_dump(mode="json")


@router.post("/api/leases/{lease_id}/release")
def api_release_lease(lease_id: str):
    ok = release_lease(lease_id)
    return {"released": ok, "lease_id": lease_id}


@router.post("/api/runtime/{runtime_key}/start")
def api_start_runtime(runtime_key: str, req: ActionRequest = ActionRequest()):
    if req.dry_run:
        result = dry_run_start_runtime(runtime_key)
    else:
        result = start_runtime(runtime_key)
    return result.model_dump()


@router.post("/api/runtime/{runtime_key}/stop")
def api_stop_runtime(runtime_key: str, req: ActionRequest = ActionRequest()):
    result = stop_runtime(runtime_key, confirm_protected=req.confirm_protected)
    return result.model_dump()


@router.post("/api/runtime/{runtime_key}/restart")
def api_restart_runtime(runtime_key: str, req: ActionRequest = ActionRequest()):
    result = restart_runtime(runtime_key, confirm_protected=req.confirm_protected)
    return result.model_dump()


# ===========================================================================
# Pages
# ===========================================================================

@router.get("/")
def page_dashboard(request: Request):
    data = get_dashboard_status()
    return templates.TemplateResponse(
        request=request,
        name="dashboard.html",
        context={"data": data},
    )


@router.get("/gpu")
def page_gpu(request: Request):
    gpu = get_gpu_status()
    procs = get_gpu_processes()
    enriched = []
    for p in procs:
        d = p.model_dump()
        d["classification"] = classify_gpu_process(p)
        enriched.append(d)
    return templates.TemplateResponse(
        request=request,
        name="gpu.html",
        context={"gpu": gpu.model_dump(), "processes": enriched},
    )


@router.get("/runtimes")
def page_runtimes(request: Request):
    registry = get_runtime_registry()
    return templates.TemplateResponse(
        request=request,
        name="runtimes.html",
        context={"runtimes": {k: v.model_dump() for k, v in registry.items()}},
    )


@router.get("/services")
def page_services(request: Request):
    services = discover_model_services()
    return templates.TemplateResponse(
        request=request,
        name="services.html",
        context={"services": [s.model_dump() for s in services]},
    )


@router.get("/policy")
def page_policy(request: Request):
    policy = get_policy_status()
    registry = get_runtime_registry()
    return templates.TemplateResponse(
        request=request,
        name="policy.html",
        context={
            "policy": policy.model_dump(),
            "runtime_keys": list(registry.keys()),
        },
    )


@router.get("/leases")
def page_leases(request: Request):
    lease_status = get_lease_status()
    registry = get_runtime_registry()
    return templates.TemplateResponse(
        request=request,
        name="leases.html",
        context={
            "lease_status": lease_status.model_dump(mode="json"),
            "runtime_keys": list(registry.keys()),
        },
    )


@router.get("/logs")
def page_logs(request: Request):
    lines = read_log_tail(LOG_FILE, n=400)
    return templates.TemplateResponse(
        request=request,
        name="logs.html",
        context={"lines": lines, "log_file": str(LOG_FILE)},
    )
