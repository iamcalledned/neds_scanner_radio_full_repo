"""
gpu_model_manager/core/actions.py

High-level operations that combine policy checks, service control,
and structured responses.

Rules:
- No process killing
- No auto-unload
- Protected runtimes refused without explicit confirmation
- Dry-run mode only reports decisions without executing
"""
from __future__ import annotations

from .logging_config import get_logger
from .policy_engine import can_start_runtime
from .runtime_registry import get_runtime_definition, list_runtime_keys
from .schemas import ActionResponse
from .service_controller import restart_service, start_service, stop_service

log = get_logger("actions")


def validate_runtime_stack() -> dict:
    """Check every registered runtime: service active? endpoint reachable?"""
    from .runtime_registry import get_runtime_status

    results: dict[str, dict] = {}
    for key in list_runtime_keys():
        try:
            status = get_runtime_status(key)
            if status:
                results[key] = {
                    "display_name": status.definition.display_name,
                    "protected": status.definition.protected,
                    "has_service": status.definition.service_name is not None,
                    "has_endpoint": status.definition.endpoint is not None,
                    "service_active": status.running,
                    "endpoint_reachable": status.endpoint_reachable,
                    "endpoint_error": status.endpoint_error,
                }
        except Exception as exc:
            results[key] = {"error": str(exc)}

    return results


def start_runtime(runtime_key: str) -> ActionResponse:
    defn = get_runtime_definition(runtime_key)
    if defn is None:
        return ActionResponse(
            action="start", runtime_key=runtime_key, success=False, dry_run=False,
            message=f"Runtime '{runtime_key}' not found in registry.",
        )

    decision = can_start_runtime(runtime_key)
    if not decision.allowed:
        log.warning("Policy denied start for %s: %s", runtime_key, decision.blockers)
        return ActionResponse(
            action="start", runtime_key=runtime_key, success=False, dry_run=False,
            message=f"Policy denied: {'; '.join(decision.blockers)}",
            details={"decision": decision.model_dump()},
        )

    if not defn.service_name:
        return ActionResponse(
            action="start", runtime_key=runtime_key, success=False, dry_run=False,
            message="No service_name configured — cannot start via service controller.",
        )

    result = start_service(defn.service_name)
    return ActionResponse(
        action="start", runtime_key=runtime_key,
        success=result["ok"], dry_run=False,
        message=result.get("output") or ("started" if result["ok"] else "failed"),
        details=result,
    )


def stop_runtime(runtime_key: str, confirm_protected: bool = False) -> ActionResponse:
    defn = get_runtime_definition(runtime_key)
    if defn is None:
        return ActionResponse(
            action="stop", runtime_key=runtime_key, success=False, dry_run=False,
            message=f"Runtime '{runtime_key}' not found in registry.",
        )

    if not defn.service_name:
        return ActionResponse(
            action="stop", runtime_key=runtime_key, success=False, dry_run=False,
            message="No service_name configured — cannot stop via service controller.",
        )

    result = stop_service(defn.service_name, confirm_protected=confirm_protected)
    return ActionResponse(
        action="stop", runtime_key=runtime_key,
        success=result["ok"], dry_run=False,
        message=result.get("error") or result.get("output", ""),
        details=result,
    )


def restart_runtime(runtime_key: str, confirm_protected: bool = False) -> ActionResponse:
    defn = get_runtime_definition(runtime_key)
    if defn is None:
        return ActionResponse(
            action="restart", runtime_key=runtime_key, success=False, dry_run=False,
            message=f"Runtime '{runtime_key}' not found in registry.",
        )

    if not defn.service_name:
        return ActionResponse(
            action="restart", runtime_key=runtime_key, success=False, dry_run=False,
            message="No service_name configured — cannot restart via service controller.",
        )

    result = restart_service(defn.service_name, confirm_protected=confirm_protected)
    return ActionResponse(
        action="restart", runtime_key=runtime_key,
        success=result["ok"], dry_run=False,
        message=result.get("error") or result.get("output", ""),
        details=result,
    )


def dry_run_start_runtime(runtime_key: str) -> ActionResponse:
    defn = get_runtime_definition(runtime_key)
    if defn is None:
        return ActionResponse(
            action="start", runtime_key=runtime_key, success=False, dry_run=True,
            message=f"Runtime '{runtime_key}' not found in registry.",
        )

    decision = can_start_runtime(runtime_key)
    return ActionResponse(
        action="start",
        runtime_key=runtime_key,
        success=decision.allowed,
        dry_run=True,
        message=decision.explanation,
        details={"decision": decision.model_dump()},
    )
