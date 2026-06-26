"""
gpu_model_manager/core/schemas.py

Pydantic models for all data types used across core, web, and desktop.
"""
from __future__ import annotations

from datetime import datetime
from typing import Any, Optional, Union

from pydantic import BaseModel, Field


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
    def ok(self) -> bool:
        return self.returncode == 0 and not self.timed_out


# ---------------------------------------------------------------------------
# GPU hardware
# ---------------------------------------------------------------------------

class GpuStatus(BaseModel):
    index: int = 0
    name: str
    driver_version: str
    temperature_c: float
    utilization_pct: float
    memory_total_mb: float
    memory_used_mb: float
    memory_free_mb: float
    power_draw_w: Optional[float] = None
    power_limit_w: Optional[float] = None
    available: bool = True
    error: Optional[str] = None


class GpuProcess(BaseModel):
    pid: int
    process_name: str
    used_memory_mb: float
    classification: str = "unknown"
    cmdline: Optional[str] = None


# ---------------------------------------------------------------------------
# Process inspection
# ---------------------------------------------------------------------------

class ProcessDetails(BaseModel):
    pid: int
    ppid: Optional[int] = None
    user: Optional[str] = None
    comm: Optional[str] = None
    args: Optional[str] = None
    classification: str = "unknown"


# ---------------------------------------------------------------------------
# Runtime registry
# ---------------------------------------------------------------------------

class RuntimeDefinition(BaseModel):
    key: str
    display_name: str
    kind: str                        # whisper | llm
    owner: str
    service_name: Optional[str] = None
    endpoint: Optional[str] = None
    protected: bool = False
    warm: Union[bool, str] = False   # True | False | "optional"
    estimated_vram_mb: int
    priority: str                    # critical | interactive | batch
    stop_policy: str                 # never_auto | allowed
    restart_policy: str              # manual_confirm | allowed
    description: str


class ServiceStatus(BaseModel):
    name: str
    active_state: str                # active | inactive | not-found | unknown
    sub_state: str = ""
    load_state: str = ""
    description: str = ""
    pid: Optional[int] = None
    error: Optional[str] = None

    @property
    def is_active(self) -> bool:
        return self.active_state == "active"


class RuntimeStatus(BaseModel):
    definition: RuntimeDefinition
    service_status: Optional[ServiceStatus] = None
    endpoint_reachable: Optional[bool] = None
    endpoint_error: Optional[str] = None
    running: bool = False


class RuntimeRegistryResponse(BaseModel):
    runtimes: dict[str, RuntimeDefinition]
    keys: list[str]


# ---------------------------------------------------------------------------
# Endpoint monitoring
# ---------------------------------------------------------------------------

class EndpointStatus(BaseModel):
    url: str
    reachable: bool
    status_code: Optional[int] = None
    error: Optional[str] = None
    response_ms: Optional[float] = None


# ---------------------------------------------------------------------------
# Policy engine
# ---------------------------------------------------------------------------

class PolicyStatus(BaseModel):
    total_vram_mb: float
    used_vram_mb: float
    free_vram_mb: float
    gpu_safety_margin_mb: int
    scanner_whisper_reserved_mb: int
    effective_available_mb: float
    gpu_available: bool
    scanner_protected: bool


class CanStartRequest(BaseModel):
    runtime_key: str
    force: bool = False


class CanStartDecision(BaseModel):
    allowed: bool
    runtime_key: str
    runtime_display_name: str
    protected: bool = False
    runtime_estimated_vram_mb: int
    gpu_total_vram_mb: float
    gpu_used_vram_mb: float
    gpu_free_vram_mb: float
    gpu_safety_margin_mb: int
    scanner_reserved_vram_mb: int
    effective_available_vram_mb: float
    blockers: list[str]
    warnings: list[str] = Field(default_factory=list)
    suggestions: list[str]
    explanation: str

    # Legacy aliases kept for backward compat with web/desktop that uses old field names
    @property
    def runtime_name(self) -> str:
        return self.runtime_display_name

    @property
    def estimated_vram_mb(self) -> int:
        return self.runtime_estimated_vram_mb

    @property
    def total_vram_mb(self) -> float:
        return self.gpu_total_vram_mb

    @property
    def used_vram_mb(self) -> float:
        return self.gpu_used_vram_mb

    @property
    def free_vram_mb(self) -> float:
        return self.gpu_free_vram_mb

    @property
    def scanner_reserve_mb(self) -> int:
        return self.scanner_reserved_vram_mb

    @property
    def safety_margin_mb(self) -> int:
        return self.gpu_safety_margin_mb

    @property
    def effective_available_mb(self) -> float:
        return self.effective_available_vram_mb


# ---------------------------------------------------------------------------
# Lease manager
# ---------------------------------------------------------------------------

class Lease(BaseModel):
    lease_id: str
    runtime_key: str
    owner: str
    created_at: datetime
    expires_at: Optional[datetime] = None
    ttl_seconds: Optional[int] = None
    estimated_vram_mb: Optional[int] = None
    metadata: dict[str, Any] = Field(default_factory=dict)
    active: bool = True
    expired: bool = False
    force_created: bool = False


class LeaseStatus(BaseModel):
    active_count: int
    expired_count: int = 0
    total_reserved_mb: int = 0
    leases: list[Lease]


# ---------------------------------------------------------------------------
# Runtime registry validation
# ---------------------------------------------------------------------------

class RegistryValidationResult(BaseModel):
    ok: bool
    runtime_count: int
    protected_count: int
    errors: list[str]
    warnings: list[str]


# ---------------------------------------------------------------------------
# Runtime reconciliation
# ---------------------------------------------------------------------------

class RuntimeReconcileEntry(BaseModel):
    key: str
    display_name: str
    protected: bool
    estimated_vram_mb: int
    service_name: Optional[str] = None
    service_exists: bool = False
    service_active: bool = False
    endpoint_configured: bool = False
    endpoint_reachable: Optional[bool] = None
    endpoint_url: Optional[str] = None
    endpoint_error: Optional[str] = None
    notes: list[str] = Field(default_factory=list)


class RuntimeReconcileResponse(BaseModel):
    runtimes: list[RuntimeReconcileEntry]
    total: int
    active_count: int
    reachable_count: int


# ---------------------------------------------------------------------------
# Manager health
# ---------------------------------------------------------------------------

class ManagerHealth(BaseModel):
    status: str                      # healthy | warning | critical | unknown
    gpu_available: bool
    scanner_protected_ok: bool
    services_checked: int
    services_active: int
    endpoint_issues: list[str]
    policy_summary: str
    active_leases: int
    timestamp: datetime


# ---------------------------------------------------------------------------
# Actions
# ---------------------------------------------------------------------------

class ActionRequest(BaseModel):
    confirm_protected: bool = False
    dry_run: bool = False


class ActionResponse(BaseModel):
    action: str
    runtime_key: str
    success: bool
    dry_run: bool
    message: str
    details: dict[str, Any] = Field(default_factory=dict)


# ---------------------------------------------------------------------------
# Log reading
# ---------------------------------------------------------------------------

class LogReadResponse(BaseModel):
    log_file: str
    lines: list[str]
    total_lines: int
    error: Optional[str] = None
