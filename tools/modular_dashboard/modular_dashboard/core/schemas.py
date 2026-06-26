"""
core/schemas.py — Pydantic models for all modular_dashboard data structures.
"""
from __future__ import annotations

from enum import Enum
from typing import Any, Optional
from pydantic import BaseModel


# ---------------------------------------------------------------------------
# Enumerations
# ---------------------------------------------------------------------------

class OverallStatus(str, Enum):
    healthy = "healthy"
    warning = "warning"
    critical = "critical"
    unknown = "unknown"


class ServiceState(str, Enum):
    active = "active"
    inactive = "inactive"
    failed = "failed"
    activating = "activating"
    deactivating = "deactivating"
    missing = "missing"
    unknown = "unknown"


# ---------------------------------------------------------------------------
# Command execution
# ---------------------------------------------------------------------------

class CommandResult(BaseModel):
    args: list[str]
    returncode: int
    stdout: str
    stderr: str
    timed_out: bool = False
    error: Optional[str] = None

    @property
    def success(self) -> bool:
        return self.returncode == 0 and not self.timed_out


# ---------------------------------------------------------------------------
# Services
# ---------------------------------------------------------------------------

class ServiceStatus(BaseModel):
    name: str
    load_state: str = "unknown"       # loaded / not-found / masked
    active_state: ServiceState = ServiceState.unknown
    sub_state: str = "unknown"         # running / dead / etc.
    description: str = ""
    pid: Optional[int] = None
    since: str = ""
    raw_output: str = ""
    classification: str = "unknown"    # required / optional / ignored / unknown

    @property
    def is_active(self) -> bool:
        return self.active_state == ServiceState.active

    @property
    def is_missing(self) -> bool:
        return self.active_state == ServiceState.missing


class ScannerServiceSummary(BaseModel):
    services: list[ServiceStatus] = []
    discovered_extra: list[str] = []   # dynamically found scanner services
    timestamp: str = ""

    @property
    def active_count(self) -> int:
        return sum(1 for s in self.services if s.is_active)

    @property
    def failed_count(self) -> int:
        return sum(1 for s in self.services if s.active_state == ServiceState.failed)

    @property
    def missing_count(self) -> int:
        return sum(1 for s in self.services if s.is_missing)


# ---------------------------------------------------------------------------
# Redis
# ---------------------------------------------------------------------------

class RedisStatus(BaseModel):
    reachable: bool = False
    url: str = ""
    error: Optional[str] = None
    server_info: dict[str, Any] = {}


class ScannerQueueStatus(BaseModel):
    stream_key: str = ""
    stream_length: Optional[int] = None
    pending_count: Optional[int] = None
    oldest_pending_age_seconds: Optional[float] = None
    oldest_pending_note: str = ""
    first_entry_id: Optional[str] = None
    last_entry_id: Optional[str] = None
    consumer_groups: list[dict] = []
    recent_items: list[dict] = []
    related_keys: list[dict] = []
    backlog_health: OverallStatus = OverallStatus.unknown
    backlog_note: str = ""
    error: Optional[str] = None


# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

class PathStatus(BaseModel):
    label: str
    path: str
    exists: bool = False
    is_file: bool = False
    is_dir: bool = False
    readable: bool = False
    writable: bool = False
    note: str = ""


# ---------------------------------------------------------------------------
# Scanner health
# ---------------------------------------------------------------------------

class ScannerHealthStatus(BaseModel):
    overall: OverallStatus = OverallStatus.unknown
    summary: str = ""
    problems: list[str] = []
    next_actions: list[str] = []
    services: Optional[ScannerServiceSummary] = None
    redis: Optional[RedisStatus] = None
    queue: Optional[ScannerQueueStatus] = None
    paths: list[PathStatus] = []
    timestamp: str = ""


# ---------------------------------------------------------------------------
# Dashboard top-level
# ---------------------------------------------------------------------------

class DashboardStatus(BaseModel):
    app_name: str = "Modular Scanner Dashboard"
    phase: str = "Phase 1 — Scanner Control Center"
    scanner_health: Optional[ScannerHealthStatus] = None
    timestamp: str = ""


# ---------------------------------------------------------------------------
# Actions
# ---------------------------------------------------------------------------

class ScannerActionRequest(BaseModel):
    service_name: str
    action: str   # start | stop | restart


class ScannerActionResponse(BaseModel):
    service_name: str
    action: str
    success: bool
    message: str
    returncode: int = -1
    output: str = ""


# ---------------------------------------------------------------------------
# Logs
# ---------------------------------------------------------------------------

class LogReadResponse(BaseModel):
    log_file: str
    lines: list[str] = []
    total_lines_read: int = 0
    error: Optional[str] = None
