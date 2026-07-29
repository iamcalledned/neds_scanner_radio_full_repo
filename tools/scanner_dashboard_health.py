"""Pure formatting helpers for the scanner dashboard health panel."""

from __future__ import annotations

from collections import Counter
from typing import Any, Dict, Iterable, List, Tuple

from rich.markup import escape


def gatekeeper_is_healthy(status: Dict[str, Any]) -> bool:
    """Interpret both legacy and current gatekeeper status responses."""
    explicit_ok = status.get("ok")
    if isinstance(explicit_ok, bool):
        return explicit_ok

    return bool(
        status.get("gpu", {}).get("available")
        and status.get("registry", {}).get("valid")
    )


def format_gatekeeper_leases(leases: Iterable[Dict[str, Any]]) -> str:
    """Group equivalent leases so duplicate records cannot flood the panel."""
    grouped: Counter[Tuple[str, str, int, str]] = Counter()
    for lease in leases:
        key = (
            str(lease.get("owner", "unknown")),
            str(lease.get("runtime_key", "unknown")),
            int(lease.get("estimated_vram_mb", 0) or 0),
            str(lease.get("status", "unknown")),
        )
        grouped[key] += 1

    if not grouped:
        return "  • None"

    lines: List[str] = []
    for (owner, runtime_key, vram, status), count in grouped.items():
        color = "green" if status == "active" else "yellow"
        suffix = f" × {count} leases" if count > 1 else ""
        lines.append(
            f"  • {escape(owner)} ({escape(runtime_key)}): {vram}MB "
            f"[{color}]{escape(status)}[/{color}]{suffix}"
        )
    return "\n".join(lines)


def format_gatekeeper_services(services: Dict[str, Dict[str, Any]]) -> str:
    """Format service state while escaping API-provided markup characters."""
    if not services:
        return "  • None"

    lines: List[str] = []
    for service_name, info in services.items():
        active = bool(info.get("active", False))
        substate = str(info.get("substate", "unknown"))
        color = "green" if active else "red"
        lines.append(
            f"  • {escape(str(service_name))}: "
            f"[{color}]{escape(substate)}[/{color}]"
        )
    return "\n".join(lines)
