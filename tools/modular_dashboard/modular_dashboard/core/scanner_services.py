"""
core/scanner_services.py — Scanner-specific service aggregation.

Combines known services + dynamic discovery into a single report.
"""
from __future__ import annotations

from modular_dashboard.core.config import (
    KNOWN_SCANNER_SERVICES,
    PRIMARY_SCANNER_SERVICES,
)
from modular_dashboard.core.logging_config import get_logger
from modular_dashboard.core.schemas import (
    ScannerServiceSummary,
    ServiceState,
    ServiceStatus,
)
from modular_dashboard.core.services import (
    discover_scanner_services,
    get_service_status,
)
from modular_dashboard.core.utils import utc_now_str

log = get_logger("scanner_services")


def get_primary_scanner_services() -> list[str]:
    """Return the list of primary/known scanner service names."""
    return list(PRIMARY_SCANNER_SERVICES)


def get_scanner_services_status() -> ScannerServiceSummary:
    """Return status for all known + discovered scanner services."""
    # Start with all known services
    queried = set(KNOWN_SCANNER_SERVICES)
    statuses: list[ServiceStatus] = []

    for svc_name in KNOWN_SCANNER_SERVICES:
        try:
            status = get_service_status(svc_name)
        except Exception as exc:
            log.warning("Error checking %s: %s", svc_name, exc)
            status = ServiceStatus(
                name=svc_name,
                active_state=ServiceState.unknown,
                note=f"Error: {exc}",
            )
        statuses.append(status)

    # Dynamic discovery for extra scanner services
    extra: list[str] = []
    try:
        discovered = discover_scanner_services()
        for svc_name in discovered:
            if svc_name not in queried:
                extra.append(svc_name)
                queried.add(svc_name)
                try:
                    status = get_service_status(svc_name)
                    statuses.append(status)
                except Exception as exc:
                    log.warning("Error checking discovered %s: %s", svc_name, exc)
    except Exception as exc:
        log.warning("Dynamic service discovery failed: %s", exc)

    return ScannerServiceSummary(
        services=statuses,
        discovered_extra=extra,
        timestamp=utc_now_str(),
    )


def validate_required_scanner_services() -> dict:
    """
    Check each primary service and return a mapping of
    service_name -> {"status": ..., "ok": bool, "note": ...}.
    """
    result: dict = {}
    for svc_name in PRIMARY_SCANNER_SERVICES:
        try:
            status = get_service_status(svc_name)
            ok = status.is_active
            note = status.active_state.value
        except Exception as exc:
            ok = False
            note = f"Error: {exc}"
        result[svc_name] = {
            "status": note,
            "ok": ok,
            "active": ok,
        }
    return result
