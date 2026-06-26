"""
gpu_model_manager/core/health.py

Aggregated health across GPU, services, endpoints, policy, and leases.
"""
from __future__ import annotations

from datetime import datetime, timezone

from .endpoint_monitor import check_all_runtime_endpoints
from .gpu_inventory import get_gpu_status
from .lease_manager import get_lease_status
from .logging_config import get_logger
from .policy_engine import get_policy_status
from .runtime_registry import get_protected_runtimes, get_runtime_status
from .schemas import ManagerHealth
from .service_controller import discover_model_services

log = get_logger("health")


def get_manager_health() -> ManagerHealth:
    gpu = get_gpu_status()

    # Protected runtime check
    protected = get_protected_runtimes()
    scanner_ok = True
    for p in protected:
        try:
            status = get_runtime_status(p.key)
            if status and not status.running:
                scanner_ok = False
                log.warning(
                    "Protected runtime %s is not running (service=%s)",
                    p.key,
                    p.service_name,
                )
        except Exception as exc:
            log.debug("Could not check protected runtime %s: %s", p.key, exc)

    # Services
    try:
        services = discover_model_services()
    except Exception:
        services = []
    services_checked = len(services)
    services_active = sum(1 for s in services if s.is_active)

    # Endpoints
    try:
        endpoints = check_all_runtime_endpoints()
    except Exception:
        endpoints = {}

    endpoint_issues = [
        f"{key}: {ep.error or 'unreachable'}"
        for key, ep in endpoints.items()
        if not ep.reachable
    ]

    # Policy
    try:
        policy = get_policy_status()
        policy_summary = (
            f"Free: {policy.free_vram_mb:.0f} MB | "
            f"Effective: {policy.effective_available_mb:.0f} MB | "
            f"Scanner reserve: {policy.scanner_whisper_reserved_mb} MB"
        )
    except Exception:
        policy_summary = "unavailable"

    # Leases
    try:
        leases = get_lease_status()
        active_leases = leases.active_count
    except Exception:
        active_leases = 0

    # Overall status
    if not gpu.available:
        overall = "critical"
    elif not scanner_ok:
        overall = "warning"
    elif endpoint_issues:
        overall = "warning"
    else:
        overall = "healthy"

    return ManagerHealth(
        status=overall,
        gpu_available=gpu.available,
        scanner_protected_ok=scanner_ok,
        services_checked=services_checked,
        services_active=services_active,
        endpoint_issues=endpoint_issues,
        policy_summary=policy_summary,
        active_leases=active_leases,
        timestamp=datetime.now(timezone.utc),
    )


def get_dashboard_status() -> dict:
    """Full status blob for dashboard display."""
    health = get_manager_health()
    gpu = get_gpu_status()
    policy = get_policy_status()
    leases = get_lease_status()

    return {
        "health": health.model_dump(mode="json"),
        "gpu": gpu.model_dump(),
        "policy": policy.model_dump(),
        "leases": leases.model_dump(mode="json"),
    }
