#!/usr/bin/env python3
"""
gpu_gate.py

Redis-backed cross-process CUDA mutex, with optional GPU health gating via NVML.

Purpose:
- Prevent Whisper + diarization + shadow ASR from running CUDA at the same time.
- Avoid VRAM thrash / CUDA OOM / SDR++ stutters.

Env (optional):
  REDIS_URL=redis://127.0.0.1:6379/0
  GPU_LOCK_KEY=scanner:gpu:lock
  GPU_LOCK_TTL_MS=180000
  GPU_LOCK_RETRY_MS=250
  GPU_MIN_FREE_MB=2500
  GPU_MAX_TEMP_C=80
"""

from __future__ import annotations

import os
import time
import uuid
from dataclasses import dataclass
from typing import Optional, Tuple

import redis

try:
    import pynvml
    _NVML_OK = True
except Exception:
    pynvml = None
    _NVML_OK = False


@dataclass
class GPUGateConfig:
    redis_url: str
    lock_key: str
    ttl_ms: int
    retry_ms: int
    min_free_mb: int
    max_temp_c: int


def cfg_from_env() -> GPUGateConfig:
    return GPUGateConfig(
        redis_url=os.environ.get("REDIS_URL", "redis://127.0.0.1:6379/0"),
        lock_key=os.environ.get("GPU_LOCK_KEY", "scanner:gpu:lock"),
        ttl_ms=int(os.environ.get("GPU_LOCK_TTL_MS", "180000")),
        retry_ms=int(os.environ.get("GPU_LOCK_RETRY_MS", "250")),
        min_free_mb=int(os.environ.get("GPU_MIN_FREE_MB", "2500")),
        max_temp_c=int(os.environ.get("GPU_MAX_TEMP_C", "80")),
    )


def _nvml_init_once() -> None:
    if not _NVML_OK:
        return
    try:
        pynvml.nvmlInit()
    except Exception:
        pass


def gpu_health_ok(min_free_mb: int, max_temp_c: int, gpu_index: int = 0) -> Tuple[bool, dict]:
    """
    Returns (ok, info). If NVML unavailable, returns ok=True (mutex only).
    """
    if not _NVML_OK:
        return True, {"nvml": "unavailable"}

    _nvml_init_once()
    info = {"nvml": "ok", "gpu_index": gpu_index}
    try:
        h = pynvml.nvmlDeviceGetHandleByIndex(gpu_index)
        mem = pynvml.nvmlDeviceGetMemoryInfo(h)
        temp = pynvml.nvmlDeviceGetTemperature(h, pynvml.NVML_TEMPERATURE_GPU)

        free_mb = int(mem.free / (1024 * 1024))
        total_mb = int(mem.total / (1024 * 1024))

        info.update({"free_mb": free_mb, "total_mb": total_mb, "temp_c": int(temp)})

        if free_mb < min_free_mb:
            info["reason"] = "low_vram"
            return False, info
        if int(temp) >= max_temp_c:
            info["reason"] = "high_temp"
            return False, info

        return True, info
    except Exception as e:
        # Don't block pipeline if NVML glitches
        return True, {"nvml": "error", "error": str(e)}


class GPUGate:
    """
    Redis-based lock with optional NVML gating.
    """

    def __init__(self, config: Optional[GPUGateConfig] = None):
        self.cfg = config or cfg_from_env()
        self.r = redis.from_url(self.cfg.redis_url)

    def acquire(self, owner: str, timeout_s: Optional[float] = None, gpu_index: int = 0):
        return _GPUGateContext(self, owner=owner, timeout_s=timeout_s, gpu_index=gpu_index)

    def _try_lock(self, owner: str) -> Optional[str]:
        token = f"{owner}:{uuid.uuid4().hex}"
        ok = self.r.set(self.cfg.lock_key, token, nx=True, px=self.cfg.ttl_ms)
        return token if ok else None

    def _unlock(self, token: str) -> bool:
        # atomic compare+del
        script = """
        if redis.call("GET", KEYS[1]) == ARGV[1] then
          return redis.call("DEL", KEYS[1])
        else
          return 0
        end
        """
        try:
            res = self.r.eval(script, 1, self.cfg.lock_key, token)
            return bool(res)
        except Exception:
            return False


class _GPUGateContext:
    def __init__(self, gate: GPUGate, owner: str, timeout_s: Optional[float], gpu_index: int):
        self.gate = gate
        self.owner = owner
        self.timeout_s = timeout_s
        self.gpu_index = gpu_index
        self.token: Optional[str] = None
        self.health_info = {}

    def __enter__(self):
        start = time.time()
        while True:
            ok, info = gpu_health_ok(
                min_free_mb=self.gate.cfg.min_free_mb,
                max_temp_c=self.gate.cfg.max_temp_c,
                gpu_index=self.gpu_index,
            )
            self.health_info = info

            if ok:
                tok = self.gate._try_lock(self.owner)
                if tok:
                    self.token = tok
                    return self

            if self.timeout_s is not None and (time.time() - start) >= self.timeout_s:
                raise TimeoutError(f"GPU lock timeout owner={self.owner} info={self.health_info}")

            time.sleep(self.gate.cfg.retry_ms / 1000.0)

    def __exit__(self, exc_type, exc, tb):
        if self.token:
            self.gate._unlock(self.token)
        self.token = None
        return False