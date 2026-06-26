"""
gpu_model_manager/core/reconciler.py

For each runtime, reconcile definition vs live system state:
  - Is it defined in the registry?
  - Does the service exist and is it active?
  - Is the endpoint reachable?
  - Any GPU processes associated with it?
"""
from __future__ import annotations

from .logging_config import get_logger
from .schemas import RuntimeReconcileEntry, RuntimeReconcileResponse

log = get_logger("reconciler")


def get_runtime_reconciliation() -> RuntimeReconcileResponse:
    """
    For every runtime in the registry, produce a full reconcile entry
    showing definition vs. live system state.
    """
    from .endpoint_monitor import check_runtime_endpoint
    from .runtime_registry import get_runtime_registry
    from .service_controller import get_service_status

    registry = get_runtime_registry()
    entries: list[RuntimeReconcileEntry] = []
    active_count = 0
    reachable_count = 0

    for key, defn in registry.items():
        notes: list[str] = []
        service_exists = False
        service_active = False
        endpoint_reachable: bool | None = None
        endpoint_error: str | None = None

        # --- service check ---
        if defn.service_name:
            try:
                svc = get_service_status(defn.service_name)
                service_exists = svc.active_state != "not-found"
                service_active = svc.is_active
                if not service_exists:
                    notes.append(f"service {defn.service_name!r} not found on this system")
                elif not service_active:
                    notes.append(f"service {defn.service_name!r} exists but is not active")
            except Exception as exc:
                notes.append(f"service check error: {exc}")
        else:
            notes.append("no service_name configured")

        # --- endpoint check ---
        if defn.endpoint:
            try:
                ep = check_runtime_endpoint(key)
                endpoint_reachable = ep.reachable
                endpoint_error = ep.error
                if not ep.reachable:
                    notes.append(
                        f"endpoint {defn.endpoint!r} not reachable: {ep.error or 'no response'}"
                    )
            except Exception as exc:
                endpoint_reachable = False
                endpoint_error = str(exc)
                notes.append(f"endpoint check error: {exc}")
        else:
            notes.append("no endpoint configured")

        # --- protected runtime specific ---
        if defn.protected:
            if not service_active:
                notes.append(
                    "WARNING: protected runtime service is not active — "
                    "scanner transcription may be down"
                )
            if endpoint_reachable is False:
                notes.append(
                    "WARNING: protected runtime endpoint is not reachable"
                )

        # --- counts ---
        if service_active:
            active_count += 1
        if endpoint_reachable:
            reachable_count += 1

        entries.append(RuntimeReconcileEntry(
            key=key,
            display_name=defn.display_name,
            protected=defn.protected,
            estimated_vram_mb=defn.estimated_vram_mb,
            service_name=defn.service_name,
            service_exists=service_exists,
            service_active=service_active,
            endpoint_configured=defn.endpoint is not None,
            endpoint_reachable=endpoint_reachable,
            endpoint_url=defn.endpoint,
            endpoint_error=endpoint_error,
            notes=notes,
        ))

    return RuntimeReconcileResponse(
        runtimes=entries,
        total=len(entries),
        active_count=active_count,
        reachable_count=reachable_count,
    )
