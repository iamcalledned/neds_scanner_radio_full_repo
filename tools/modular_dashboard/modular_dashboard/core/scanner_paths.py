"""
core/scanner_paths.py — Validate scanner archive / database paths.
"""
from __future__ import annotations

import os
from pathlib import Path

from modular_dashboard.core.config import (
    ARCHIVE_BASE,
    MODULAR_DASHBOARD_ROOT,
    OLD_DASHBOARD_REFERENCE,
    PROJECT_ROOT,
    SCANNER_DB_PATH,
    TRANSCRIBER_DIR,
)
from modular_dashboard.core.logging_config import get_logger
from modular_dashboard.core.schemas import PathStatus

log = get_logger("scanner_paths")


_PATH_DEFS: list[tuple[str, str]] = [
    ("Project Root", PROJECT_ROOT),
    ("Transcriber Dir", TRANSCRIBER_DIR),
    ("Archive Base", ARCHIVE_BASE),
    ("Scanner DB", SCANNER_DB_PATH),
    ("Modular Dashboard Root", MODULAR_DASHBOARD_ROOT),
    ("Old Dashboard Reference", OLD_DASHBOARD_REFERENCE),
]


def check_path(label: str, path: str) -> PathStatus:
    """Check existence, type, and permissions for a path."""
    p = Path(path)
    status = PathStatus(label=label, path=path)
    try:
        status.exists = p.exists()
        if status.exists:
            status.is_file = p.is_file()
            status.is_dir = p.is_dir()
            status.readable = os.access(path, os.R_OK)
            if status.is_dir:
                status.writable = os.access(path, os.W_OK)
            else:
                # For files, writable = parent dir writable
                status.writable = os.access(str(p.parent), os.W_OK)
        else:
            status.note = "Path does not exist"
    except Exception as exc:
        status.note = f"Error checking path: {exc}"
        log.debug("check_path error for %s: %s", path, exc)
    return status


def check_scanner_paths() -> list[PathStatus]:
    """Return path status for all configured scanner-related paths."""
    return [check_path(label, path) for label, path in _PATH_DEFS]
