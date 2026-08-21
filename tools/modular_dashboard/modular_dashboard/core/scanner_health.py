"""
core/scanner_health.py — Combined scanner health report.

Aggregates services, Redis, queue, and paths into a single
ScannerHealthStatus with an overall status and plain-English summary.
"""
from __future__ import annotations

from modular_dashboard.core.config import (
    OPTIONAL_SCANNER_SERVICES,
    REQUIRED_SCANNER_SERVICES,
)
from modular_dashboard.core.logging_config import get_logger
from modular_dashboard.core.schemas import (
    DashboardStatus,
    OverallStatus,
    ScannerHealthStatus,
    ServiceState,
)
from modular_dashboard.core.scanner_paths import check_scanner_paths
from modular_dashboard.core.scanner_queue import check_redis, get_scanner_queue_status
from modular_dashboard.core.scanner_services import get_scanner_services_status
from modular_dashboard.core.utils import utc_now_str

log = get_logger("scanner_health")


def _worst(a: OverallStatus, b: OverallStatus) -> OverallStatus:
    order = [
        OverallStatus.healthy,
        OverallStatus.unknown,
        OverallStatus.warning,
        OverallStatus.critical,
    ]
    return order[max(order.index(a), order.index(b))]


def get_scanner_health() -> ScannerHealthStatus:
    """Build a full scanner health report."""
    problems: list[str] = []
    next_actions: list[str] = []
    overall = OverallStatus.healthy

    # --- Services ---
    service_summary = get_scanner_services_status()

    # Stamp each service with its classification
    for svc in service_summary.services:
        if svc.name in REQUIRED_SCANNER_SERVICES:
            svc.classification = "required"
        elif svc.name in OPTIONAL_SCANNER_SERVICES:
            svc.classification = "optional"
        else:
            svc.classification = "unknown"

    for svc in service_summary.services:
        # Optional/unknown services don't affect overall health
        if svc.classification != "required":
            continue
        if svc.active_state == ServiceState.missing:
            problems.append(f"Required service not found: {svc.name}")
            overall = _worst(overall, OverallStatus.warning)
        elif svc.active_state == ServiceState.failed:
            problems.append(f"Required service failed: {svc.name}")
            overall = _worst(overall, OverallStatus.critical)
            next_actions.append(f"Check logs for {svc.name}: journalctl --user -u {svc.name} -n 50")
        elif svc.active_state not in (ServiceState.active,):
            problems.append(f"Required service inactive: {svc.name} ({svc.active_state.value})")
            overall = _worst(overall, OverallStatus.warning)

    # Specific pipeline checks
    _svc_by_name = {s.name: s for s in service_summary.services}

    recorder = _svc_by_name.get("scanner-recorder.service")
    listener = _svc_by_name.get("scanner-transcriber.service")
    mcp = _svc_by_name.get("scanner-mcp.service")

    recorder_active = recorder and recorder.is_active
    listener_active = listener and listener.is_active
    mcp_active = mcp and mcp.is_active

    # --- Redis ---
    redis_status = check_redis()
    if not redis_status.reachable:
        problems.append(f"Redis is unreachable: {redis_status.error or 'unknown error'}")
        overall = _worst(overall, OverallStatus.critical)
        next_actions.append("Check Redis service: systemctl --user status redis or redis-server status")

    # --- Queue ---
    queue_status = None
    if redis_status.reachable:
        queue_status = get_scanner_queue_status()
        if queue_status.backlog_health not in (OverallStatus.healthy, OverallStatus.unknown):
            problems.append(f"Queue backlog: {queue_status.backlog_note}")
            overall = _worst(overall, queue_status.backlog_health)

    # --- Paths ---
    path_statuses = check_scanner_paths()
    for ps in path_statuses:
        if not ps.exists:
            problems.append(f"Path missing: {ps.label} — {ps.path}")
            overall = _worst(overall, OverallStatus.warning)

    # --- Plain-English summary ---
    summary = _build_summary(
        overall, redis_status.reachable,
        recorder_active, listener_active, mcp_active,
        queue_status, problems,
    )

    if not next_actions and overall != OverallStatus.healthy:
        next_actions.append("Check service logs: journalctl --user -xe")

    return ScannerHealthStatus(
        overall=overall,
        summary=summary,
        problems=problems,
        next_actions=next_actions,
        services=service_summary,
        redis=redis_status,
        queue=queue_status,
        paths=path_statuses,
        timestamp=utc_now_str(),
    )


def _build_summary(
    overall: OverallStatus,
    redis_reachable: bool,
    recorder_active: bool | None,
    listener_active: bool | None,
    mcp_active: bool | None,
    queue_status,
    problems: list[str],
) -> str:
    if not redis_reachable:
        return (
            "Redis is unreachable, so scanner queue state cannot be inspected. "
            "Recording may continue but transcription status is unknown."
        )

    if recorder_active and not listener_active:
        pending = queue_status.pending_count if queue_status else None
        stream_len = queue_status.stream_length if queue_status else None
        count_str = ""
        if pending is not None:
            count_str = f" ({pending} pending items)"
        elif stream_len is not None:
            count_str = f" ({stream_len} items in stream)"
        return (
            f"Recorder appears active and work is accumulating{count_str}, "
            "but the listener/transcriber is not active. "
            "Calls are being queued for later transcription."
        )

    if listener_active and not mcp_active:
        return (
            "Listener may be running, but Scanner MCP is unavailable. "
            "Transcription may fail or the backlog may grow."
        )

    if overall == OverallStatus.healthy:
        return "Scanner pipeline appears healthy. All monitored services are active."

    if overall == OverallStatus.warning:
        short = "; ".join(problems[:2])
        return f"Scanner pipeline has warnings: {short}."

    if overall == OverallStatus.critical:
        short = "; ".join(problems[:2])
        return f"Scanner pipeline has critical issues: {short}."

    return "Scanner pipeline status is unknown."


def get_dashboard_status() -> DashboardStatus:
    """Top-level dashboard status combining all checks."""
    scanner_health = get_scanner_health()
    return DashboardStatus(
        scanner_health=scanner_health,
        timestamp=utc_now_str(),
    )
