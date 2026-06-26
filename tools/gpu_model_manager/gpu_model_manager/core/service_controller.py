"""
gpu_model_manager/core/service_controller.py

systemctl --user operations for model-related services.

PROTECTED SERVICES: scanner-mcp.service will be refused stop/restart
unless confirm_protected=True is explicitly passed.
"""
from __future__ import annotations

from typing import Optional

from .command_runner import run_systemctl_user
from .logging_config import get_logger
from .schemas import ServiceStatus

log = get_logger("service_controller")

# Services that may never be stopped without explicit confirmation
_PROTECTED_SERVICES: frozenset[str] = frozenset({"scanner-mcp.service"})

# Keywords used to discover model-related services
_MODEL_KEYWORDS: list[str] = [
    "scanner-mcp",
    "scanner_mcp",
    "whisper",
    "mcp",
    "vllm",
    "llama",
    "llm",
    "model",
    "transcriber",
]


def get_service_status(service_name: str) -> ServiceStatus:
    result = run_systemctl_user([
        "show", service_name,
        "--no-pager",
        "--property=ActiveState,SubState,LoadState,Description,MainPID",
    ])

    if result.returncode != 0:
        stderr_lower = result.stderr.lower()
        if "could not be found" in stderr_lower or "no such" in stderr_lower:
            return ServiceStatus(
                name=service_name,
                active_state="not-found",
                error="service not found",
            )
        # Service exists but systemctl had a minor issue — parse anyway
        if not result.stdout:
            return ServiceStatus(
                name=service_name,
                active_state="unknown",
                error=result.stderr or result.error,
            )

    props: dict[str, str] = {}
    for line in result.stdout.splitlines():
        if "=" in line:
            k, _, v = line.partition("=")
            props[k.strip()] = v.strip()

    main_pid_str = props.get("MainPID", "0")
    pid: Optional[int] = None
    if main_pid_str.isdigit() and main_pid_str != "0":
        pid = int(main_pid_str)

    return ServiceStatus(
        name=service_name,
        active_state=props.get("ActiveState", "unknown"),
        sub_state=props.get("SubState", ""),
        load_state=props.get("LoadState", ""),
        description=props.get("Description", ""),
        pid=pid,
    )


def list_user_services() -> list[ServiceStatus]:
    result = run_systemctl_user([
        "list-units",
        "--type=service",
        "--all",
        "--no-legend",
        "--no-pager",
    ])
    if result.returncode != 0 or not result.stdout:
        return []

    services: list[ServiceStatus] = []
    for line in result.stdout.splitlines():
        parts = line.split()
        if not parts:
            continue
        name = parts[0]
        active = parts[2] if len(parts) > 2 else "unknown"
        sub = parts[3] if len(parts) > 3 else ""
        services.append(ServiceStatus(name=name, active_state=active, sub_state=sub))
    return services


def discover_model_services() -> list[ServiceStatus]:
    """
    Return full ServiceStatus for all user services matching model-related keywords.
    """
    all_services = list_user_services()
    matched: list[ServiceStatus] = []
    for svc in all_services:
        name_lower = svc.name.lower()
        if any(kw in name_lower for kw in _MODEL_KEYWORDS):
            matched.append(get_service_status(svc.name))
    return matched


def _protected_guard(service_name: str, confirm_protected: bool) -> Optional[str]:
    if service_name in _PROTECTED_SERVICES:
        if not confirm_protected:
            msg = (
                f"{service_name} is a protected service and cannot be stopped or "
                "restarted without explicit confirmation. "
                "Pass confirm_protected=True to override. "
                "WARNING: stopping scanner-mcp.service will halt scanner transcription."
            )
            log.warning(
                "PROTECTED SERVICE action blocked for %s (confirm_protected=False)", service_name
            )
            return msg
        else:
            log.warning(
                "PROTECTED SERVICE action CONFIRMED for %s — confirm_protected=True was passed",
                service_name,
            )
    return None


def start_service(service_name: str) -> dict:
    log.info("Starting service: %s", service_name)
    result = run_systemctl_user(["start", service_name])
    return {
        "service": service_name,
        "action": "start",
        "ok": result.returncode == 0,
        "output": result.stderr or result.stdout,
    }


def stop_service(service_name: str, confirm_protected: bool = False) -> dict:
    guard = _protected_guard(service_name, confirm_protected)
    if guard:
        log.warning("Refusing stop of protected service: %s", guard)
        return {
            "service": service_name,
            "action": "stop",
            "ok": False,
            "error": guard,
        }
    log.info("Stopping service: %s", service_name)
    result = run_systemctl_user(["stop", service_name])
    return {
        "service": service_name,
        "action": "stop",
        "ok": result.returncode == 0,
        "output": result.stderr or result.stdout,
    }


def restart_service(service_name: str, confirm_protected: bool = False) -> dict:
    guard = _protected_guard(service_name, confirm_protected)
    if guard:
        log.warning("Refusing restart of protected service: %s", guard)
        return {
            "service": service_name,
            "action": "restart",
            "ok": False,
            "error": guard,
        }
    log.info("Restarting service: %s", service_name)
    result = run_systemctl_user(["restart", service_name])
    return {
        "service": service_name,
        "action": "restart",
        "ok": result.returncode == 0,
        "output": result.stderr or result.stdout,
    }
