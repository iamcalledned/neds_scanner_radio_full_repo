"""
core/services.py — Generic systemd user service helpers.

Functions deal with arbitrary service names; scanner-specific
aggregation lives in scanner_services.py.
"""
from __future__ import annotations

import re

from modular_dashboard.core.command_runner import run_systemctl_user
from modular_dashboard.core.logging_config import get_logger
from modular_dashboard.core.schemas import (
    ScannerActionResponse,
    ServiceState,
    ServiceStatus,
)
from modular_dashboard.core.config import SCANNER_SERVICE_KEYWORDS

log = get_logger("services")

# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

_STATE_MAP = {
    "active": ServiceState.active,
    "inactive": ServiceState.inactive,
    "failed": ServiceState.failed,
    "activating": ServiceState.activating,
    "deactivating": ServiceState.deactivating,
}


def _parse_active_state(text: str) -> ServiceState:
    lower = text.strip().lower()
    return _STATE_MAP.get(lower, ServiceState.unknown)


def _parse_status_output(unit: str, stdout: str, returncode: int) -> ServiceStatus:
    """Parse `systemctl --user status <unit>` output into ServiceStatus."""
    # returncode 4 = unit not found; 3 = inactive/failed; 0 = active
    if returncode == 4 or "could not be found" in stdout.lower():
        return ServiceStatus(
            name=unit,
            load_state="not-found",
            active_state=ServiceState.missing,
            raw_output=stdout,
        )

    status = ServiceStatus(name=unit, raw_output=stdout)

    for line in stdout.splitlines():
        line_stripped = line.strip()

        if line_stripped.startswith("Loaded:"):
            # Loaded: loaded (/path/to/unit; enabled; ...)
            m = re.search(r"Loaded:\s+(\S+)", line_stripped)
            if m:
                status.load_state = m.group(1).strip("()")

        elif line_stripped.startswith("Active:"):
            # Active: active (running) since ...
            m = re.search(r"Active:\s+(\w+)\s*\((\w+)\)", line_stripped)
            if m:
                status.active_state = _parse_active_state(m.group(1))
                status.sub_state = m.group(2)
            else:
                m2 = re.search(r"Active:\s+(\w+)", line_stripped)
                if m2:
                    status.active_state = _parse_active_state(m2.group(1))
            # Extract since timestamp
            m_since = re.search(r"since\s+(.+?)(?:\s*;|$)", line_stripped)
            if m_since:
                status.since = m_since.group(1).strip()

        elif "Main PID:" in line_stripped:
            m = re.search(r"Main PID:\s+(\d+)", line_stripped)
            if m:
                try:
                    status.pid = int(m.group(1))
                except ValueError:
                    pass

        elif line_stripped.startswith("●") or re.match(r"^\s*\w[\w\s@.-]+\.service", line_stripped):
            # Description line:  ● scanner-mcp.service - ...
            m = re.search(r"[-–]\s+(.+)$", line_stripped)
            if m:
                status.description = m.group(1).strip()

    return status


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def get_service_status(service_name: str) -> ServiceStatus:
    """Return status for a single systemd user service."""
    result = run_systemctl_user(["status", service_name], timeout=8)
    return _parse_status_output(service_name, result.stdout, result.returncode)


def list_user_services() -> list[ServiceStatus]:
    """List all loaded systemd user services."""
    result = run_systemctl_user(
        ["list-units", "--all", "--no-pager", "--plain", "--no-legend"],
        timeout=15,
    )
    if result.returncode != 0 or result.timed_out:
        log.warning("list_user_services failed: %s", result.error or result.stderr)
        return []

    services: list[ServiceStatus] = []
    for line in result.stdout.splitlines():
        parts = line.split()
        if len(parts) < 4:
            continue
        unit = parts[0]
        if not (unit.endswith(".service") or unit.endswith(".timer")):
            continue
        load = parts[1] if len(parts) > 1 else "unknown"
        active_raw = parts[2] if len(parts) > 2 else "unknown"
        sub = parts[3] if len(parts) > 3 else "unknown"
        desc = " ".join(parts[4:]) if len(parts) > 4 else ""
        services.append(
            ServiceStatus(
                name=unit,
                load_state=load,
                active_state=_parse_active_state(active_raw),
                sub_state=sub,
                description=desc,
            )
        )
    return services


def discover_scanner_services() -> list[str]:
    """Dynamically discover scanner-related user service names."""
    all_services = list_user_services()
    found: list[str] = []
    for svc in all_services:
        name_lower = svc.name.lower()
        if any(kw in name_lower for kw in SCANNER_SERVICE_KEYWORDS):
            found.append(svc.name)
    return found


def start_service(service_name: str) -> ScannerActionResponse:
    """Start a systemd user service."""
    result = run_systemctl_user(["start", service_name], timeout=15)
    return ScannerActionResponse(
        service_name=service_name,
        action="start",
        success=result.success,
        message="Started successfully." if result.success else (result.stderr or result.stdout or "Failed to start."),
        returncode=result.returncode,
        output=result.stdout,
    )


def stop_service(service_name: str) -> ScannerActionResponse:
    """Stop a systemd user service."""
    result = run_systemctl_user(["stop", service_name], timeout=15)
    return ScannerActionResponse(
        service_name=service_name,
        action="stop",
        success=result.success,
        message="Stopped successfully." if result.success else (result.stderr or result.stdout or "Failed to stop."),
        returncode=result.returncode,
        output=result.stdout,
    )


def restart_service(service_name: str) -> ScannerActionResponse:
    """Restart a systemd user service."""
    result = run_systemctl_user(["restart", service_name], timeout=15)
    return ScannerActionResponse(
        service_name=service_name,
        action="restart",
        success=result.success,
        message="Restarted successfully." if result.success else (result.stderr or result.stdout or "Failed to restart."),
        returncode=result.returncode,
        output=result.stdout,
    )
