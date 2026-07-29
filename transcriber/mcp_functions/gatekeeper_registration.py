"""Idempotent scanner registration with the GPU gatekeeper."""

from __future__ import annotations

from typing import Any, Dict, List

SCANNER_OWNER = "scanner-mcp"
SCANNER_RUNTIME_KEY = "scanner_whisper"
SCANNER_CAPABILITY = "scanner_transcription"
SCANNER_LEASE_TTL_SECONDS = 315_360_000


def matching_scanner_leases(status: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Return reusable scanner leases, newest first."""
    leases = status.get("leases", {}).get("leases", [])
    matches = [
        lease
        for lease in leases
        if lease.get("owner") == SCANNER_OWNER
        and lease.get("runtime_key") == SCANNER_RUNTIME_KEY
        and lease.get("status") in ("active", "pending")
    ]
    return sorted(
        matches,
        key=lambda lease: str(lease.get("created_at", "")),
        reverse=True,
    )


def ensure_scanner_reservation(client: Any, log: Any) -> Dict[str, Any]:
    """Reuse one permanent scanner lease and retire accidental duplicates."""
    try:
        reusable = matching_scanner_leases(client.status())
    except Exception as exc:
        log.warning(
            "Could not inspect existing GPU Gatekeeper leases before registration: %s",
            exc,
        )
        reusable = []

    if reusable:
        lease = reusable[0]
        released = 0
        for duplicate in reusable[1:]:
            lease_id = duplicate.get("lease_id")
            if not lease_id:
                continue
            try:
                response = client.release_runtime(lease_id)
                if response.get("lease_released"):
                    released += 1
            except Exception as exc:
                log.warning(
                    "Could not release duplicate GPU Gatekeeper lease %s: %s",
                    lease_id,
                    exc,
                )

        if released:
            log.warning(
                "Released %d duplicate scanner GPU Gatekeeper lease(s).",
                released,
            )
        return {
            "ok": True,
            "action": "reused_existing_lease",
            "lease": lease,
            "message": "Reused the existing scanner reservation.",
        }

    return client.ensure_runtime(
        capability=SCANNER_CAPABILITY,
        runtime_key=SCANNER_RUNTIME_KEY,
        owner=SCANNER_OWNER,
        allow_protected=True,
        ttl_seconds=SCANNER_LEASE_TTL_SECONDS,
        force=True,
    )
