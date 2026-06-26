"""
gpu_model_manager/core/utils.py

Small utility helpers shared across core modules.
"""
from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def read_log_tail(log_file: Path, n: int = 300) -> list[str]:
    if not log_file.exists():
        return []
    try:
        lines = log_file.read_text(errors="replace").splitlines()
        return lines[-n:] if len(lines) > n else lines
    except Exception:
        return []


def mb_to_gb_str(mb: float) -> str:
    return f"{mb / 1024:.1f} GB"


def format_vram(mb: float) -> str:
    if mb >= 1024:
        return mb_to_gb_str(mb)
    return f"{mb:.0f} MB"


def pct(used: float, total: float) -> float:
    if total <= 0:
        return 0.0
    return round(used / total * 100, 1)
