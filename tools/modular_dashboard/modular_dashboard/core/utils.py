"""
core/utils.py — General helper utilities.
"""
from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any, Optional

# ---------------------------------------------------------------------------
# Timestamps
# ---------------------------------------------------------------------------

def utc_now_str() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def local_now_str() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def format_age_seconds(seconds: Optional[float]) -> str:
    """Convert seconds into a human-readable age string."""
    if seconds is None:
        return "unknown"
    seconds = int(seconds)
    if seconds < 60:
        return f"{seconds}s"
    minutes, secs = divmod(seconds, 60)
    if minutes < 60:
        return f"{minutes}m {secs}s"
    hours, mins = divmod(minutes, 60)
    if hours < 24:
        return f"{hours}h {mins}m"
    days, hrs = divmod(hours, 24)
    return f"{days}d {hrs}h"


# ---------------------------------------------------------------------------
# JSON
# ---------------------------------------------------------------------------

def safe_json(obj: Any) -> str:
    """Serialise obj to JSON string; return empty dict on error."""
    try:
        return json.dumps(obj, default=str, indent=2)
    except Exception:
        return "{}"


def truncate(text: str, max_len: int = 200) -> str:
    if len(text) <= max_len:
        return text
    return text[:max_len] + "…"


# ---------------------------------------------------------------------------
# Status colours (for web/desktop labels)
# ---------------------------------------------------------------------------

STATUS_COLORS = {
    "healthy": "#22c55e",    # green
    "active": "#22c55e",
    "warning": "#f59e0b",    # amber
    "critical": "#ef4444",   # red
    "failed": "#ef4444",
    "unknown": "#94a3b8",    # slate
    "inactive": "#94a3b8",
    "missing": "#f97316",    # orange
}


def status_color(status: str) -> str:
    return STATUS_COLORS.get(status.lower(), "#94a3b8")
