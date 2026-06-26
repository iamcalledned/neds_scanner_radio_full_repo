"""
gpu_model_manager/core/gpu_inventory.py

GPU hardware inventory via nvidia-smi.
Does NOT import torch — purely CLI-based.
"""
from __future__ import annotations

from typing import Optional

from .command_runner import run_command
from .logging_config import get_logger
from .schemas import GpuProcess, GpuStatus

log = get_logger("gpu_inventory")

_NVIDIA_SMI_QUERY = ",".join([
    "name",
    "driver_version",
    "temperature.gpu",
    "utilization.gpu",
    "memory.total",
    "memory.used",
    "memory.free",
    "power.draw",
    "power.limit",
])

_PROCESS_QUERY = "pid,process_name,used_memory"


def _parse_float(val: str) -> Optional[float]:
    try:
        return float(val.strip())
    except (ValueError, AttributeError):
        return None


def _unavailable(error: str) -> GpuStatus:
    return GpuStatus(
        name="unavailable",
        driver_version="unknown",
        temperature_c=0.0,
        utilization_pct=0.0,
        memory_total_mb=0.0,
        memory_used_mb=0.0,
        memory_free_mb=0.0,
        available=False,
        error=error,
    )


def get_gpu_status() -> GpuStatus:
    result = run_command([
        "nvidia-smi",
        f"--query-gpu={_NVIDIA_SMI_QUERY}",
        "--format=csv,noheader,nounits",
    ])

    if result.error and "command_not_found" in result.error:
        return _unavailable("nvidia-smi not found")

    if result.returncode != 0 or not result.stdout:
        err = result.error or result.stderr or "nvidia-smi returned no output"
        log.warning("nvidia-smi failed: %s", err)
        return _unavailable(err)

    try:
        parts = [p.strip() for p in result.stdout.split(",")]
        return GpuStatus(
            name=parts[0],
            driver_version=parts[1],
            temperature_c=_parse_float(parts[2]) or 0.0,
            utilization_pct=_parse_float(parts[3]) or 0.0,
            memory_total_mb=_parse_float(parts[4]) or 0.0,
            memory_used_mb=_parse_float(parts[5]) or 0.0,
            memory_free_mb=_parse_float(parts[6]) or 0.0,
            power_draw_w=_parse_float(parts[7]) if len(parts) > 7 else None,
            power_limit_w=_parse_float(parts[8]) if len(parts) > 8 else None,
            available=True,
        )
    except Exception as exc:
        log.error("Failed to parse nvidia-smi output %r: %s", result.stdout, exc)
        return _unavailable(f"parse_error: {exc}")


def get_gpu_processes() -> list[GpuProcess]:
    result = run_command([
        "nvidia-smi",
        f"--query-compute-apps={_PROCESS_QUERY}",
        "--format=csv,noheader,nounits",
    ])

    if result.returncode != 0:
        if result.error and "command_not_found" in result.error:
            return []
        log.debug("nvidia-smi processes query failed: %s", result.stderr)
        return []

    if not result.stdout:
        return []

    processes: list[GpuProcess] = []
    for line in result.stdout.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            parts = [p.strip() for p in line.split(",")]
            if len(parts) >= 3:
                processes.append(GpuProcess(
                    pid=int(parts[0]),
                    process_name=parts[1],
                    used_memory_mb=_parse_float(parts[2]) or 0.0,
                ))
        except Exception as exc:
            log.debug("Failed to parse process line %r: %s", line, exc)

    return processes


def get_gpu_summary() -> dict:
    status = get_gpu_status()
    processes = get_gpu_processes()
    return {
        "status": status.model_dump(),
        "processes": [p.model_dump() for p in processes],
        "process_count": len(processes),
    }


def get_total_vram_mb() -> float:
    return get_gpu_status().memory_total_mb


def get_used_vram_mb() -> float:
    return get_gpu_status().memory_used_mb


def get_free_vram_mb() -> float:
    return get_gpu_status().memory_free_mb
