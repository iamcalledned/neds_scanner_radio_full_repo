"""
gpu_model_manager/core/policy_engine.py

The heart of the GPU Model Manager.

VRAM admission policy:
  effective_available_vram_mb =
      gpu_free_vram_mb
      - GPU_SAFETY_MARGIN_MB          (always-free buffer, default 2500 MB)
      - SCANNER_WHISPER_RESERVED_MB   (scanner must always have VRAM, default 7000 MB)

  allowed = effective_available_vram_mb >= runtime.estimated_vram_mb

Phase 1: read-only decisions. No process killing. No auto-unload.
"""
from __future__ import annotations

from .config import GPU_SAFETY_MARGIN_MB, SCANNER_WHISPER_RESERVED_MB
from .gpu_inventory import get_gpu_status
from .logging_config import get_logger
from .runtime_registry import get_runtime_definition, get_protected_runtimes
from .schemas import CanStartDecision, CanStartRequest, PolicyStatus

log = get_logger("policy_engine")


def get_reserved_vram_mb() -> int:
    return SCANNER_WHISPER_RESERVED_MB


def get_effective_available_vram_mb(include_protected_reserve: bool = True) -> float:
    gpu = get_gpu_status()
    if not gpu.available:
        return 0.0
    deductions = float(GPU_SAFETY_MARGIN_MB)
    if include_protected_reserve:
        deductions += float(SCANNER_WHISPER_RESERVED_MB)
    return max(0.0, gpu.memory_free_mb - deductions)


def get_policy_status() -> PolicyStatus:
    gpu = get_gpu_status()
    return PolicyStatus(
        total_vram_mb=gpu.memory_total_mb,
        used_vram_mb=gpu.memory_used_mb,
        free_vram_mb=gpu.memory_free_mb,
        gpu_safety_margin_mb=GPU_SAFETY_MARGIN_MB,
        scanner_whisper_reserved_mb=SCANNER_WHISPER_RESERVED_MB,
        effective_available_mb=get_effective_available_vram_mb(),
        gpu_available=gpu.available,
        scanner_protected=True,
    )


def can_start_runtime(runtime_key: str, force: bool = False) -> CanStartDecision:
    defn = get_runtime_definition(runtime_key)
    gpu = get_gpu_status()
    total = gpu.memory_total_mb
    used = gpu.memory_used_mb
    free = gpu.memory_free_mb
    effective = get_effective_available_vram_mb()

    if defn is None:
        return CanStartDecision(
            allowed=False,
            runtime_key=runtime_key,
            runtime_display_name="unknown",
            protected=False,
            runtime_estimated_vram_mb=0,
            gpu_total_vram_mb=total,
            gpu_used_vram_mb=used,
            gpu_free_vram_mb=free,
            gpu_safety_margin_mb=GPU_SAFETY_MARGIN_MB,
            scanner_reserved_vram_mb=SCANNER_WHISPER_RESERVED_MB,
            effective_available_vram_mb=effective,
            blockers=["runtime_key_not_found"],
            warnings=[],
            suggestions=["Check config/runtimes.json for valid keys."],
            explanation=f"Runtime '{runtime_key}' is not in the registry.",
        )

    # Protected runtimes — capacity is already reserved for them.
    # Do not apply admission logic as if they were optional.
    if defn.protected:
        from .service_controller import get_service_status
        warnings: list[str] = []
        suggestions: list[str] = []

        if defn.service_name:
            try:
                svc = get_service_status(defn.service_name)
                if not svc.is_active:
                    warnings.append(
                        f"Protected scanner runtime service {defn.service_name!r} "
                        "is NOT active. Scanner transcription may be down. "
                        "VRAM reserve is still held by policy."
                    )
                    suggestions.append(
                        f"Run: systemctl --user status {defn.service_name}"
                    )
            except Exception as exc:
                warnings.append(f"Could not check service status: {exc}")

        suggestions.append(
            "This runtime is protected. The manager will not recommend stopping it. "
            f"Manage only via: {defn.service_name or 'its owning service'}."
        )

        explanation = (
            f"[PROTECTED] {defn.display_name} ({runtime_key})\n"
            f"  Capacity is reserved for this runtime by policy.\n"
            f"  Estimated VRAM: {defn.estimated_vram_mb} MB\n"
            f"  GPU total: {total:.0f} MB | Used: {used:.0f} MB | Free: {free:.0f} MB\n"
            f"  Safety margin: {GPU_SAFETY_MARGIN_MB} MB | "
            f"Scanner reserve: {SCANNER_WHISPER_RESERVED_MB} MB\n"
            f"  Effective available (for other runtimes): {effective:.0f} MB\n"
            f"  Service: {defn.service_name or 'none'}\n"
            f"  The manager will not auto-stop or auto-unload this runtime."
        )
        if warnings:
            explanation += "\n  WARNINGS:\n" + "\n".join(f"    - {w}" for w in warnings)

        return CanStartDecision(
            allowed=True,
            runtime_key=runtime_key,
            runtime_display_name=defn.display_name,
            protected=True,
            runtime_estimated_vram_mb=defn.estimated_vram_mb,
            gpu_total_vram_mb=total,
            gpu_used_vram_mb=used,
            gpu_free_vram_mb=free,
            gpu_safety_margin_mb=GPU_SAFETY_MARGIN_MB,
            scanner_reserved_vram_mb=SCANNER_WHISPER_RESERVED_MB,
            effective_available_vram_mb=effective,
            blockers=[],
            warnings=warnings,
            suggestions=suggestions,
            explanation=explanation,
        )

    # Non-protected runtime — apply normal admission logic
    blockers: list[str] = []
    decision_warnings: list[str] = []
    suggestions_out: list[str] = []

    if not gpu.available:
        blockers.append("gpu_unavailable: nvidia-smi could not read GPU state")
        suggestions_out.append("Verify NVIDIA driver: run 'nvidia-smi' manually.")

    if gpu.available and effective < defn.estimated_vram_mb:
        shortage = defn.estimated_vram_mb - effective
        blockers.append(
            f"insufficient_vram: need {defn.estimated_vram_mb} MB, "
            f"effective available {effective:.0f} MB (short by {shortage:.0f} MB)"
        )
        suggestions_out.append(
            f"Free at least {shortage:.0f} MB by stopping another non-protected runtime."
        )
        suggestions_out.append(
            "Check /api/runtimes/reconcile to see what is currently running."
        )

    if gpu.available and effective >= 0 and effective < defn.estimated_vram_mb * 1.1:
        decision_warnings.append(
            f"Effective available VRAM ({effective:.0f} MB) is close to or below "
            f"what {defn.display_name} needs ({defn.estimated_vram_mb} MB). "
            "Actual load may exceed estimate."
        )

    allowed = not blockers
    if force and gpu.available:
        allowed = True
        blockers = []
        decision_warnings.append("force=True was used — policy override applied.")
        suggestions_out = ["Monitor GPU VRAM carefully after loading."]

    verdict = "ALLOWED" if allowed else "DENIED"
    explanation = (
        f"Runtime: {defn.display_name} ({runtime_key})\n"
        f"  Needs:                  {defn.estimated_vram_mb} MB\n"
        f"  GPU total VRAM:         {total:.0f} MB\n"
        f"  GPU used VRAM:          {used:.0f} MB\n"
        f"  GPU free VRAM:          {free:.0f} MB\n"
        f"  - Safety margin:        {GPU_SAFETY_MARGIN_MB} MB\n"
        f"  - Scanner reserve:      {SCANNER_WHISPER_RESERVED_MB} MB\n"
        f"  = Effective available:  {effective:.0f} MB\n"
        f"  Decision:               {verdict}"
    )
    if blockers:
        explanation += "\n  Blockers:\n" + "\n".join(f"    - {b}" for b in blockers)
    if decision_warnings:
        explanation += "\n  Warnings:\n" + "\n".join(f"    - {w}" for w in decision_warnings)

    log.info("can_start(%s) -> %s | effective=%.0f MB", runtime_key, verdict, effective)

    return CanStartDecision(
        allowed=allowed,
        runtime_key=runtime_key,
        runtime_display_name=defn.display_name,
        protected=False,
        runtime_estimated_vram_mb=defn.estimated_vram_mb,
        gpu_total_vram_mb=total,
        gpu_used_vram_mb=used,
        gpu_free_vram_mb=free,
        gpu_safety_margin_mb=GPU_SAFETY_MARGIN_MB,
        scanner_reserved_vram_mb=SCANNER_WHISPER_RESERVED_MB,
        effective_available_vram_mb=effective,
        blockers=blockers,
        warnings=decision_warnings,
        suggestions=suggestions_out,
        explanation=explanation,
    )


def explain_runtime_decision(runtime_key: str) -> str:
    return can_start_runtime(runtime_key).explanation

