"""
core/config.py — Centralised configuration for modular_dashboard.

All paths, Redis keys, thresholds, and service names live here.
Override any value with an environment variable of the same name.
"""
from __future__ import annotations

import os
from pathlib import Path

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _env(key: str, default: str) -> str:
    return os.environ.get(key, default)

def _env_int(key: str, default: int) -> int:
    try:
        return int(os.environ.get(key, default))
    except (TypeError, ValueError):
        return default


# ---------------------------------------------------------------------------
# Project roots
# ---------------------------------------------------------------------------

PROJECT_ROOT = _env(
    "PROJECT_ROOT",
    "/home/ned/Documents/neds_scanner_radio_full_pipeline_with_git",
)
MODULAR_DASHBOARD_ROOT = _env(
    "MODULAR_DASHBOARD_ROOT",
    os.path.join(PROJECT_ROOT, "tools", "modular_dashboard"),
)
OLD_DASHBOARD_REFERENCE = _env(
    "OLD_DASHBOARD_REFERENCE",
    os.path.join(PROJECT_ROOT, "tools", "scanner_dashboard.py"),
)
TRANSCRIBER_DIR = _env(
    "TRANSCRIBER_DIR",
    os.path.join(PROJECT_ROOT, "transcriber"),
)

# ---------------------------------------------------------------------------
# Scanner data paths  (read from project .env first, then env vars)
# ---------------------------------------------------------------------------

def _load_project_env() -> dict:
    """Load key=value pairs from PROJECT_ROOT/.env without overwriting real env."""
    env_file = Path(PROJECT_ROOT) / ".env"
    result: dict = {}
    if env_file.exists():
        for line in env_file.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            k, v = line.split("=", 1)
            result[k.strip()] = v.strip()
    return result


_project_env = _load_project_env()


def _env_or_project(key: str, default: str) -> str:
    """Prefer real env var, fall back to project .env, then hard default."""
    if key in os.environ:
        return os.environ[key]
    if key in _project_env:
        return _project_env[key]
    return default


SCANNER_DB_PATH = _env_or_project(
    "SCANNER_DB_PATH",
    "/home/ned/data/scanner_calls/scanner_calls.db",
)
ARCHIVE_BASE = _env_or_project(
    "ARCHIVE_BASE",
    "/home/ned/data/scanner_calls/scanner_archive",
)
REDIS_URL = _env_or_project("REDIS_URL", "redis://127.0.0.1:6379/0")

# ---------------------------------------------------------------------------
# Web server
# ---------------------------------------------------------------------------

DEFAULT_WEB_HOST = _env("DEFAULT_WEB_HOST", "192.168.86.53")
DEFAULT_WEB_PORT = _env_int("DEFAULT_WEB_PORT", 8011)

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

LOG_DIR = _env(
    "LOG_DIR",
    os.path.join(MODULAR_DASHBOARD_ROOT, "logs"),
)
LOG_FILE = _env(
    "LOG_FILE",
    os.path.join(LOG_DIR, "modular_dashboard.log"),
)
LOG_LEVEL = _env("LOG_LEVEL", "INFO")

# ---------------------------------------------------------------------------
# Redis scanner keys/patterns
# ---------------------------------------------------------------------------

SCANNER_REDIS_STREAM_KEY = _env(
    "SCANNER_REDIS_STREAM_KEY",
    "scanner:stream:new_call",
)
SCANNER_REDIS_KEY_PATTERNS: list[str] = [
    "scanner:stream:new_call",
    "scanner:*:latest_time",
    "scanner_api_cache:*",
    "scanner:api:*",
]

# ---------------------------------------------------------------------------
# Backlog / queue thresholds
# ---------------------------------------------------------------------------

SCANNER_PENDING_WARN_COUNT = _env_int("SCANNER_PENDING_WARN_COUNT", 10)
SCANNER_PENDING_CRITICAL_COUNT = _env_int("SCANNER_PENDING_CRITICAL_COUNT", 50)
SCANNER_OLDEST_WARN_SECONDS = _env_int("SCANNER_OLDEST_WARN_SECONDS", 300)    # 5 min
SCANNER_OLDEST_CRITICAL_SECONDS = _env_int("SCANNER_OLDEST_CRITICAL_SECONDS", 1800)  # 30 min

# ---------------------------------------------------------------------------
# Queue backlog interpretation mode
# ---------------------------------------------------------------------------
# auto            — use consumer group pending if available; else warn-only on XLEN
# consumer_pending — only consumer group pending counts drive backlog health
# stream_length   — XLEN drives backlog health (original behaviour)
SCANNER_QUEUE_BACKLOG_MODE = _env("SCANNER_QUEUE_BACKLOG_MODE", "auto")

# ---------------------------------------------------------------------------
# Service classification
# ---------------------------------------------------------------------------

# Services that must be active for the pipeline to be considered healthy.
REQUIRED_SCANNER_SERVICES: list[str] = [
    "scanner-mcp.service",
    "scanner-transcriber.service",
    "scanner-recorder.service",
    "scanner-websocket.service",
    "rtl_tcp@12000.service",
    "rtl_tcp@12001.service",
    "rtl_tcp@12002.service",
    "rtl_tcp@12003.service",
    "rtl_tcp@12004.service",
]

# Services that are shown but do NOT affect overall health if inactive.
OPTIONAL_SCANNER_SERVICES: list[str] = [
    "ned_transcriber.service",
    "scanner-archive-sweep.service",
    "scanner-archive-sweep.timer",
]

# Services that should be hidden from all views entirely.
IGNORED_SCANNER_SERVICES: list[str] = []

# ---------------------------------------------------------------------------
# Known scanner services
# ---------------------------------------------------------------------------

KNOWN_SCANNER_SERVICES: list[str] = [
    "scanner-mcp.service",
    "scanner-transcriber.service",
    "scanner-recorder.service",
    "scanner-websocket.service",
]

# Primary services to check first in pipeline-health order
PRIMARY_SCANNER_SERVICES: list[str] = [
    "scanner-mcp.service",
    "scanner-transcriber.service",
    "scanner-recorder.service",
    "scanner-websocket.service",
]

# Default restart order (conservative)
RESTART_ORDER: list[str] = [
    "scanner-mcp.service",
    "scanner-transcriber.service",
    "scanner-recorder.service",
    "scanner-websocket.service",
]

# Keywords used for dynamic service discovery
SCANNER_SERVICE_KEYWORDS: list[str] = [
    "scanner",
    "rtl",
    "sdr",
    "mcp",
    "transcriber",
    "recorder",
    "websocket",
]

# ---------------------------------------------------------------------------
# Scanner env files (read-only reference)
# ---------------------------------------------------------------------------

SCANNER_ENV_FILE = _env(
    "SCANNER_ENV_FILE",
    os.path.expanduser("~/.config/scanner/env"),
)
RECORDER_ENV_FILE = _env(
    "RECORDER_ENV_FILE",
    os.path.join(PROJECT_ROOT, "config", "scanner_recorder.env"),
)
