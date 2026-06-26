"""
gpu_model_manager/core/runtime_registry.py

Loads runtime definitions from config/runtimes.json.
Edit that file to add, remove, or change runtimes — no code changes needed.

scanner_whisper is PROTECTED. It must never be auto-stopped.
"""
from __future__ import annotations

import json
from typing import Optional

from .config import RUNTIMES_CONFIG_FILE
from .logging_config import get_logger
from .schemas import RuntimeDefinition, RuntimeRegistryResponse, RuntimeStatus

log = get_logger("runtime_registry")


def _load_registry() -> dict[str, RuntimeDefinition]:
    if not RUNTIMES_CONFIG_FILE.exists():
        log.error("Runtimes config not found: %s", RUNTIMES_CONFIG_FILE)
        return {}
    try:
        raw = json.loads(RUNTIMES_CONFIG_FILE.read_text())
        registry: dict[str, RuntimeDefinition] = {}
        for entry in raw:
            r = RuntimeDefinition(**entry)
            registry[r.key] = r
        log.info("Loaded %d runtimes from %s", len(registry), RUNTIMES_CONFIG_FILE)
        return registry
    except Exception as exc:
        log.error("Failed to load runtimes config: %s", exc)
        return {}


_REGISTRY: dict[str, RuntimeDefinition] = _load_registry()


def get_runtime_registry() -> dict[str, RuntimeDefinition]:
    return dict(_REGISTRY)


def get_runtime_definition(key: str) -> Optional[RuntimeDefinition]:
    return _REGISTRY.get(key)


def list_runtime_keys() -> list[str]:
    return list(_REGISTRY.keys())


def get_protected_runtimes() -> list[RuntimeDefinition]:
    return [r for r in _REGISTRY.values() if r.protected]


def get_runtime_status(key: str) -> Optional[RuntimeStatus]:
    defn = get_runtime_definition(key)
    if defn is None:
        return None

    # Lazy imports to avoid circular import at module level
    from .endpoint_monitor import check_http_endpoint
    from .service_controller import get_service_status

    svc_status = None
    endpoint_reachable: Optional[bool] = None
    endpoint_error: Optional[str] = None

    if defn.service_name:
        try:
            svc_status = get_service_status(defn.service_name)
        except Exception as exc:
            log.debug("Could not get service status for %s: %s", defn.service_name, exc)

    if defn.endpoint:
        try:
            ep = check_http_endpoint(defn.endpoint)
            endpoint_reachable = ep.reachable
            endpoint_error = ep.error
        except Exception as exc:
            endpoint_reachable = False
            endpoint_error = str(exc)

    running = bool(svc_status and svc_status.active_state == "active")

    return RuntimeStatus(
        definition=defn,
        service_status=svc_status,
        endpoint_reachable=endpoint_reachable,
        endpoint_error=endpoint_error,
        running=running,
    )


def get_runtime_registry_response() -> RuntimeRegistryResponse:
    return RuntimeRegistryResponse(
        runtimes=get_runtime_registry(),
        keys=list_runtime_keys(),
    )
