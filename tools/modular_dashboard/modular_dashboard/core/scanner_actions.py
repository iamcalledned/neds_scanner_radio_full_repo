"""
core/scanner_actions.py — High-level scanner control actions.

All destructive actions (stop, restart) require explicit invocation.
Nothing here is automatic.
"""
from __future__ import annotations

from modular_dashboard.core.command_runner import run_command, run_systemctl_user
from modular_dashboard.core.config import RESTART_ORDER
from modular_dashboard.core.logging_config import get_logger
from modular_dashboard.core.schemas import ScannerActionResponse, ServiceState
from modular_dashboard.core.services import (
    get_service_status,
    restart_service,
    start_service,
    stop_service,
)

log = get_logger("scanner_actions")


def start_scanner_service(service_name: str) -> ScannerActionResponse:
    """Start a single scanner service."""
    log.info("start_scanner_service: %s", service_name)
    return start_service(service_name)


def stop_scanner_service(service_name: str) -> ScannerActionResponse:
    """Stop a single scanner service."""
    log.info("stop_scanner_service: %s", service_name)
    return stop_service(service_name)


def restart_scanner_service(service_name: str) -> ScannerActionResponse:
    """Restart a single scanner service."""
    log.info("restart_scanner_service: %s", service_name)
    return restart_service(service_name)


def restart_scanner_stack() -> list[ScannerActionResponse]:
    """
    Restart all primary scanner services in conservative order.

    This must only be called when the user has explicitly requested it.
    Do NOT call this automatically.
    """
    log.warning("restart_scanner_stack: explicit stack restart initiated")
    results: list[ScannerActionResponse] = []
    for service_name in RESTART_ORDER:
        status = get_service_status(service_name)
        if status.active_state == ServiceState.missing:
            log.info("restart_scanner_stack: skipping missing service %s", service_name)
            results.append(
                ScannerActionResponse(
                    service_name=service_name,
                    action="restart",
                    success=False,
                    message="Service not found — skipped.",
                    returncode=-1,
                )
            )
            continue
        resp = restart_service(service_name)
        results.append(resp)
        log.info(
            "restart_scanner_stack: %s -> success=%s",
            service_name,
            resp.success,
        )
    return results


def validate_scanner_stack() -> dict:
    """
    Validate each primary scanner service and return a concise report.

    Returns a dict with keys: services, overall_ok, problems.
    """
    from modular_dashboard.core.scanner_services import validate_required_scanner_services
    from modular_dashboard.core.scanner_queue import check_redis
    from modular_dashboard.core.scanner_paths import check_scanner_paths

    svc_results = validate_required_scanner_services()
    redis_status = check_redis()
    paths = check_scanner_paths()

    problems: list[str] = []
    for name, info in svc_results.items():
        if not info["ok"]:
            problems.append(f"Service not active: {name} (status: {info['status']})")

    if not redis_status.reachable:
        problems.append(f"Redis unreachable: {redis_status.error}")

    for ps in paths:
        if not ps.exists:
            problems.append(f"Path missing: {ps.label} ({ps.path})")

    return {
        "services": svc_results,
        "redis": {"reachable": redis_status.reachable, "error": redis_status.error},
        "paths": [ps.model_dump() for ps in paths],
        "problems": problems,
        "overall_ok": len(problems) == 0,
    }


def get_service_detail(service_name: str, log_lines: int = 80) -> dict:
    """
    Return rich detail for a single service: status, properties, and recent logs.

    Safe — read-only. Never modifies service state.
    """
    status = get_service_status(service_name)

    # Full 'systemctl --user status' output
    status_result = run_systemctl_user(["status", "--no-pager", "-l", service_name], timeout=8)

    # Key properties from 'systemctl --user show'
    show_result = run_systemctl_user(
        [
            "show", service_name,
            "--property=MainPID,ExecMainStartTimestamp,ExecMainExitTimestamp,"
            "MemoryCurrent,CPUUsageNSec,RestartCount,UnitFileState,"
            "FragmentPath,Description,ActiveEnterTimestamp",
        ],
        timeout=8,
    )
    props: dict[str, str] = {}
    for line in show_result.stdout.splitlines():
        if "=" in line:
            k, _, v = line.partition("=")
            props[k.strip()] = v.strip()

    # Journalctl logs (--user-unit, no pager, newest last)
    journal_result = run_command(
        [
            "journalctl", "--user-unit", service_name,
            "--no-pager", "-n", str(log_lines), "--output=short-iso",
        ],
        timeout=12,
    )

    return {
        "service_name": service_name,
        "active_state": status.active_state.value,
        "sub_state": status.sub_state,
        "load_state": status.load_state,
        "description": status.description,
        "since": status.since,
        "pid": status.pid,
        "classification": status.classification,
        "systemctl_status": status_result.stdout or status_result.stderr,
        "properties": props,
        "journal_logs": journal_result.stdout or journal_result.stderr or "(no logs available)",
        "journal_error": journal_result.error,
    }

