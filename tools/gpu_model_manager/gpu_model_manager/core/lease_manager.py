"""
gpu_model_manager/core/lease_manager.py

Phase 1: local JSON file lease backend.

A lease is an advisory VRAM reservation — it does NOT lock GPU memory.
The manager uses leases to track intended usage and factor it into admission
decisions, but enforcement is cooperative.

Future phase: replace with Redis-backed lease store.
"""
from __future__ import annotations

import json
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

from .config import LEASE_FILE, STATE_DIR
from .logging_config import get_logger
from .schemas import Lease, LeaseStatus

log = get_logger("lease_manager")


# ---------------------------------------------------------------------------
# Internal I/O
# ---------------------------------------------------------------------------

def _load_raw() -> list[dict]:
    STATE_DIR.mkdir(parents=True, exist_ok=True)
    if not LEASE_FILE.exists():
        return []
    try:
        return json.loads(LEASE_FILE.read_text(encoding="utf-8"))
    except Exception as exc:
        log.error("Failed to load leases from %s: %s", LEASE_FILE, exc)
        return []


def _save_raw(leases: list[dict]) -> None:
    STATE_DIR.mkdir(parents=True, exist_ok=True)
    try:
        LEASE_FILE.write_text(
            json.dumps(leases, indent=2, default=str), encoding="utf-8"
        )
    except Exception as exc:
        log.error("Failed to save leases to %s: %s", LEASE_FILE, exc)


def _dict_to_lease(d: dict) -> Lease:
    now = datetime.now(timezone.utc)
    exp_str = d.get("expires_at")
    exp: Optional[datetime] = None
    if exp_str:
        exp = datetime.fromisoformat(exp_str)
        if exp.tzinfo is None:
            exp = exp.replace(tzinfo=timezone.utc)

    expired = bool(exp and now > exp)

    return Lease(
        lease_id=d["lease_id"],
        runtime_key=d["runtime_key"],
        owner=d["owner"],
        created_at=datetime.fromisoformat(d["created_at"]),
        expires_at=exp,
        ttl_seconds=d.get("ttl_seconds"),
        estimated_vram_mb=d.get("estimated_vram_mb"),
        metadata=d.get("metadata", {}),
        active=d.get("active", True),
        expired=expired,
        force_created=d.get("force_created", False),
    )


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def expire_old_leases() -> int:
    """Mark expired leases inactive. Returns count of newly expired leases."""
    raw = _load_raw()
    now = datetime.now(timezone.utc)
    count = 0
    for d in raw:
        if not d.get("active"):
            continue
        exp_str = d.get("expires_at")
        if not exp_str:
            continue
        exp = datetime.fromisoformat(exp_str)
        if exp.tzinfo is None:
            exp = exp.replace(tzinfo=timezone.utc)
        if now > exp:
            d["active"] = False
            count += 1

    if count:
        _save_raw(raw)
        log.info("Expired %d lease(s)", count)
    return count


def get_active_leases() -> list[Lease]:
    """Return only currently active (non-expired) leases."""
    expire_old_leases()
    raw = _load_raw()
    return [_dict_to_lease(d) for d in raw if d.get("active")]


def get_expired_leases() -> list[Lease]:
    """Return all inactive/expired leases."""
    raw = _load_raw()
    return [_dict_to_lease(d) for d in raw if not d.get("active")]


def list_leases() -> list[Lease]:
    """Backward-compat alias for get_active_leases()."""
    return get_active_leases()


def get_lease_summary() -> dict:
    """Summary counts for dashboard display."""
    active = get_active_leases()
    expired = get_expired_leases()
    total_reserved = sum(
        l.estimated_vram_mb for l in active if l.estimated_vram_mb is not None
    )
    return {
        "active_count": len(active),
        "expired_count": len(expired),
        "total_reserved_mb": total_reserved,
    }


def create_lease(
    runtime_key: str,
    owner: str,
    ttl_seconds: Optional[int] = None,
    metadata: Optional[dict[str, Any]] = None,
    force: bool = False,
) -> Lease:
    """
    Create a VRAM lease for a runtime.

    If force=False (default), the policy engine is consulted first.
    Lease is refused if policy denies and force is not set.
    If force=True, lease is created regardless, but a warning is recorded.

    Raises ValueError if runtime_key is unknown.
    Raises PermissionError if policy denies and force=False.
    """
    from .policy_engine import can_start_runtime
    from .runtime_registry import get_runtime_definition

    defn = get_runtime_definition(runtime_key)
    if defn is None:
        raise ValueError(
            f"Runtime '{runtime_key}' is not in the registry. "
            "Check config/runtimes.json for valid keys."
        )

    force_created = False

    # Protected runtimes: skip policy check, they're always allowed
    if not defn.protected:
        decision = can_start_runtime(runtime_key, force=force)
        if not decision.allowed:
            if not force:
                raise PermissionError(
                    f"Policy denied lease for '{runtime_key}': "
                    f"{'; '.join(decision.blockers)}. "
                    "Pass force=True to override."
                )
            # force=True: record it was forced
            force_created = True
            log.warning(
                "Lease for %s created with force=True despite policy denial: %s",
                runtime_key,
                decision.blockers,
            )

    now = datetime.now(timezone.utc)
    expires_at = now + timedelta(seconds=ttl_seconds) if ttl_seconds else None

    lease = Lease(
        lease_id=str(uuid.uuid4()),
        runtime_key=runtime_key,
        owner=owner,
        created_at=now,
        expires_at=expires_at,
        ttl_seconds=ttl_seconds,
        estimated_vram_mb=defn.estimated_vram_mb,
        metadata=metadata or {},
        active=True,
        expired=False,
        force_created=force_created,
    )

    raw = _load_raw()
    raw.append(lease.model_dump(mode="json"))
    _save_raw(raw)

    log.info(
        "Created lease %s for runtime=%s owner=%s ttl=%s force=%s",
        lease.lease_id, runtime_key, owner, ttl_seconds, force_created,
    )
    return lease


def release_lease(lease_id: str) -> bool:
    raw = _load_raw()
    found = False
    for d in raw:
        if d["lease_id"] == lease_id:
            d["active"] = False
            found = True
    if found:
        _save_raw(raw)
        log.info("Released lease %s", lease_id)
    else:
        log.warning("Lease not found: %s", lease_id)
    return found


def get_lease_status() -> LeaseStatus:
    active = get_active_leases()
    expired = get_expired_leases()
    total_reserved = sum(
        l.estimated_vram_mb for l in active if l.estimated_vram_mb is not None
    )
    return LeaseStatus(
        active_count=len(active),
        expired_count=len(expired),
        total_reserved_mb=total_reserved,
        leases=active,
    )
