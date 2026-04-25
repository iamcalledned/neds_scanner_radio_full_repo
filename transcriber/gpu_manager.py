from __future__ import annotations

import json
import os
import socket
import subprocess
import threading
import time
import uuid
from contextlib import AbstractContextManager
from dataclasses import dataclass
from typing import Any, Callable, Optional

from gpu_gate import GPUGate


@dataclass
class GPUReservation:
    reservation_id: str
    owner: str
    purpose: str
    status: str
    resident: bool
    model_key: str = ""
    model_value: str = ""
    vram_mb: Optional[int] = None
    gpu_index: int = 0
    pid: int = 0
    host: str = ""
    created_at: float = 0.0
    updated_at: float = 0.0
    metadata: dict[str, Any] | None = None


class GPUManager:
    def __init__(self, *, gate: Optional[GPUGate] = None, log: Any = None):
        self.gate = gate or GPUGate()
        self.r = self.gate.r
        self.log = log
        self.reservations_key = os.environ.get("GPU_RESERVATIONS_KEY", "scanner:gpu:reservations")
        self.reservation_prefix = os.environ.get("GPU_RESERVATION_PREFIX", "scanner:gpu:reservation:")
        ttl_ms = max(1, int(self.gate.cfg.ttl_ms))
        self.claim_refresh_ms = int(os.environ.get("GPU_CLAIM_REFRESH_MS", str(max(1000, ttl_ms // 3))))
        self._claim_threads: dict[str, threading.Event] = {}
        self._claim_lock = threading.Lock()

    def acquire(
        self,
        owner: str,
        timeout_s: Optional[float] = None,
        gpu_index: int = 0,
        *,
        purpose: str = "",
        model_key: str = "",
        model_value: str = "",
        vram_mb: Optional[int] = None,
        metadata: Optional[dict[str, Any]] = None,
        track_activity: bool = True,
    ) -> "_GPUManagerContext":
        return _GPUManagerContext(
            manager=self,
            owner=owner,
            timeout_s=timeout_s,
            gpu_index=gpu_index,
            purpose=purpose or owner,
            model_key=model_key,
            model_value=model_value,
            vram_mb=vram_mb,
            metadata=metadata,
            track_activity=track_activity,
        )

    def claim(
        self,
        owner: str,
        timeout_s: Optional[float] = None,
        gpu_index: int = 0,
        *,
        purpose: str = "",
        model_key: str = "",
        model_value: str = "",
        vram_mb: Optional[int] = None,
        metadata: Optional[dict[str, Any]] = None,
        track_activity: bool = True,
    ) -> str:
        token = self.gate.claim(owner=owner, timeout_s=timeout_s, gpu_index=gpu_index)
        self._start_claim_refresher(token)
        if track_activity:
            reservation_id = self._create_reservation(
                owner=owner,
                purpose=purpose or owner,
                status="claimed",
                resident=False,
                model_key=model_key,
                model_value=model_value,
                vram_mb=vram_mb,
                gpu_index=gpu_index,
                metadata=metadata,
            )
            self._set_claim_reservation(token, reservation_id)
        return token

    def release(self, token: str) -> bool:
        reservation_id = self._pop_claim_reservation(token)
        self._stop_claim_refresher(token)
        if reservation_id:
            self.release_reservation(reservation_id)
        return self.gate.release(token)

    def reserve(
        self,
        *,
        owner: str,
        purpose: str,
        resident: bool,
        status: str = "reserved",
        model_key: str = "",
        model_value: str = "",
        vram_mb: Optional[int] = None,
        gpu_index: int = 0,
        metadata: Optional[dict[str, Any]] = None,
    ) -> str:
        return self._create_reservation(
            owner=owner,
            purpose=purpose,
            status=status,
            resident=resident,
            model_key=model_key,
            model_value=model_value,
            vram_mb=vram_mb,
            gpu_index=gpu_index,
            metadata=metadata,
        )

    def reserve_resident(
        self,
        *,
        owner: str,
        purpose: str,
        model_key: str = "",
        model_value: str = "",
        vram_mb: Optional[int] = None,
        gpu_index: int = 0,
        metadata: Optional[dict[str, Any]] = None,
    ) -> str:
        return self.reserve(
            owner=owner,
            purpose=purpose,
            resident=True,
            status="resident",
            model_key=model_key,
            model_value=model_value,
            vram_mb=vram_mb,
            gpu_index=gpu_index,
            metadata=metadata,
        )

    def release_reservation(self, reservation_id: str) -> None:
        if not reservation_id:
            return
        try:
            pipe = self.r.pipeline()
            pipe.srem(self.reservations_key, reservation_id)
            pipe.delete(self._reservation_key(reservation_id))
            pipe.execute()
        except Exception as exc:
            self._log_warning(f"[gpu_manager] Failed to release reservation {reservation_id}: {exc}")

    def list_reservations(self) -> list[GPUReservation]:
        try:
            reservation_ids = sorted(self.r.smembers(self.reservations_key))
        except Exception as exc:
            self._log_warning(f"[gpu_manager] Failed to read reservations: {exc}")
            return []

        reservations: list[GPUReservation] = []
        for raw_id in reservation_ids:
            reservation_id = raw_id.decode("utf-8") if isinstance(raw_id, bytes) else str(raw_id)
            data = self._read_reservation(reservation_id)
            if data is not None:
                reservations.append(data)
        reservations.sort(key=lambda item: (item.created_at, item.owner))
        return reservations

    def load_model(
        self,
        *,
        owner: str,
        load_fn: Callable[[], Any],
        timeout_s: float,
        model_key: str = "",
        model_value: str = "",
        vram_mb: Optional[int] = None,
        gpu_index: int = 0,
        metadata: Optional[dict[str, Any]] = None,
        purpose: str = "model-load",
    ) -> tuple[Any, str]:
        with self.acquire(
            owner=f"{owner}:load",
            timeout_s=timeout_s,
            gpu_index=gpu_index,
            purpose=purpose,
            model_key=model_key,
            model_value=model_value,
            vram_mb=vram_mb,
            metadata=metadata,
        ):
            result = load_fn()
            reservation_id = self.reserve_resident(
                owner=owner,
                purpose="resident-model",
                model_key=model_key,
                model_value=model_value,
                vram_mb=vram_mb,
                gpu_index=gpu_index,
                metadata=metadata,
            )
            return result, reservation_id

    def warm_model(self, **kwargs: Any) -> tuple[Any, str]:
        kwargs.setdefault("purpose", "warm-model")
        return self.load_model(**kwargs)

    def start_subprocess(
        self,
        *,
        owner: str,
        command: list[str],
        timeout_s: float,
        popen_factory: Callable[..., subprocess.Popen[Any]] = subprocess.Popen,
        wait_until_ready: Optional[Callable[[subprocess.Popen[Any]], None]] = None,
        gpu_index: int = 0,
        resident: bool = False,
        model_key: str = "",
        model_value: str = "",
        vram_mb: Optional[int] = None,
        metadata: Optional[dict[str, Any]] = None,
        purpose: str = "subprocess-startup",
        **popen_kwargs: Any,
    ) -> tuple[subprocess.Popen[Any], Optional[str]]:
        with self.acquire(
            owner=f"{owner}:startup",
            timeout_s=timeout_s,
            gpu_index=gpu_index,
            purpose=purpose,
            model_key=model_key,
            model_value=model_value,
            vram_mb=vram_mb,
            metadata=metadata,
        ):
            process = popen_factory(command, **popen_kwargs)
            try:
                if wait_until_ready is not None:
                    wait_until_ready(process)
            except Exception:
                if process.poll() is None:
                    process.terminate()
                raise

            reservation_id: Optional[str] = None
            if resident:
                reservation_id = self.reserve_resident(
                    owner=owner,
                    purpose="resident-process",
                    model_key=model_key,
                    model_value=model_value,
                    vram_mb=vram_mb,
                    gpu_index=gpu_index,
                    metadata=metadata,
                )
            return process, reservation_id

    def _create_reservation(
        self,
        *,
        owner: str,
        purpose: str,
        status: str,
        resident: bool,
        model_key: str,
        model_value: str,
        vram_mb: Optional[int],
        gpu_index: int,
        metadata: Optional[dict[str, Any]],
    ) -> str:
        now = time.time()
        reservation_id = f"gpu-resv-{uuid.uuid4().hex}"
        record = GPUReservation(
            reservation_id=reservation_id,
            owner=owner,
            purpose=purpose,
            status=status,
            resident=resident,
            model_key=model_key,
            model_value=model_value,
            vram_mb=vram_mb,
            gpu_index=gpu_index,
            pid=os.getpid(),
            host=socket.gethostname(),
            created_at=now,
            updated_at=now,
            metadata=metadata or {},
        )
        mapping = self._encode_reservation(record)
        try:
            pipe = self.r.pipeline()
            pipe.sadd(self.reservations_key, reservation_id)
            pipe.hset(self._reservation_key(reservation_id), mapping=mapping)
            pipe.execute()
        except Exception as exc:
            self._log_warning(f"[gpu_manager] Failed to create reservation {reservation_id}: {exc}")
        return reservation_id

    def _read_reservation(self, reservation_id: str) -> Optional[GPUReservation]:
        try:
            raw = self.r.hgetall(self._reservation_key(reservation_id))
        except Exception as exc:
            self._log_warning(f"[gpu_manager] Failed to read reservation {reservation_id}: {exc}")
            return None
        if not raw:
            return None

        values = {
            (key.decode("utf-8") if isinstance(key, bytes) else str(key)):
            (value.decode("utf-8") if isinstance(value, bytes) else str(value))
            for key, value in raw.items()
        }
        try:
            metadata = json.loads(values.get("metadata_json", "{}"))
        except Exception:
            metadata = {}
        try:
            vram_text = values.get("vram_mb", "")
            vram_mb = int(vram_text) if vram_text else None
        except ValueError:
            vram_mb = None

        return GPUReservation(
            reservation_id=reservation_id,
            owner=values.get("owner", ""),
            purpose=values.get("purpose", ""),
            status=values.get("status", ""),
            resident=values.get("resident", "0") == "1",
            model_key=values.get("model_key", ""),
            model_value=values.get("model_value", ""),
            vram_mb=vram_mb,
            gpu_index=int(values.get("gpu_index", "0") or 0),
            pid=int(values.get("pid", "0") or 0),
            host=values.get("host", ""),
            created_at=float(values.get("created_at", "0") or 0.0),
            updated_at=float(values.get("updated_at", "0") or 0.0),
            metadata=metadata,
        )

    def _encode_reservation(self, reservation: GPUReservation) -> dict[str, str]:
        return {
            "owner": reservation.owner,
            "purpose": reservation.purpose,
            "status": reservation.status,
            "resident": "1" if reservation.resident else "0",
            "model_key": reservation.model_key,
            "model_value": reservation.model_value,
            "vram_mb": "" if reservation.vram_mb is None else str(int(reservation.vram_mb)),
            "gpu_index": str(reservation.gpu_index),
            "pid": str(reservation.pid),
            "host": reservation.host,
            "created_at": str(reservation.created_at),
            "updated_at": str(reservation.updated_at),
            "metadata_json": json.dumps(reservation.metadata or {}, sort_keys=True),
        }

    def _reservation_key(self, reservation_id: str) -> str:
        return f"{self.reservation_prefix}{reservation_id}"

    def _claim_reservation_key(self, token: str) -> str:
        return f"{self.reservation_prefix}claim:{token}"

    def _set_claim_reservation(self, token: str, reservation_id: str) -> None:
        try:
            self.r.set(self._claim_reservation_key(token), reservation_id)
        except Exception as exc:
            self._log_warning(f"[gpu_manager] Failed to link claim {token} to reservation {reservation_id}: {exc}")

    def _pop_claim_reservation(self, token: str) -> str:
        key = self._claim_reservation_key(token)
        try:
            raw = self.r.get(key)
            self.r.delete(key)
        except Exception as exc:
            self._log_warning(f"[gpu_manager] Failed to unlink claim {token}: {exc}")
            return ""
        if raw is None:
            return ""
        return raw.decode("utf-8") if isinstance(raw, bytes) else str(raw)

    def _start_claim_refresher(self, token: str) -> None:
        stop_event = threading.Event()

        def worker() -> None:
            interval_s = max(0.5, self.claim_refresh_ms / 1000.0)
            while not stop_event.wait(interval_s):
                try:
                    if not self.gate.refresh(token):
                        break
                except Exception as exc:
                    self._log_warning(f"[gpu_manager] Failed to refresh GPU claim {token}: {exc}")
                    break

        thread = threading.Thread(
            target=worker,
            name=f"gpu-claim-refresh-{token[-8:]}",
            daemon=True,
        )
        with self._claim_lock:
            self._claim_threads[token] = stop_event
        thread.start()

    def _stop_claim_refresher(self, token: str) -> None:
        with self._claim_lock:
            stop_event = self._claim_threads.pop(token, None)
        if stop_event is not None:
            stop_event.set()

    def _log_warning(self, message: str) -> None:
        if self.log is not None:
            self.log.warning(message)


class _GPUManagerContext(AbstractContextManager["_GPUManagerContext"]):
    def __init__(
        self,
        *,
        manager: GPUManager,
        owner: str,
        timeout_s: Optional[float],
        gpu_index: int,
        purpose: str,
        model_key: str,
        model_value: str,
        vram_mb: Optional[int],
        metadata: Optional[dict[str, Any]],
        track_activity: bool,
    ):
        self.manager = manager
        self.owner = owner
        self.timeout_s = timeout_s
        self.gpu_index = gpu_index
        self.purpose = purpose
        self.model_key = model_key
        self.model_value = model_value
        self.vram_mb = vram_mb
        self.metadata = metadata
        self.track_activity = track_activity
        self._inner = manager.gate.acquire(owner=owner, timeout_s=timeout_s, gpu_index=gpu_index)
        self.token: Optional[str] = None
        self.health_info: dict[str, Any] = {}
        self.reservation_id: str = ""

    def __enter__(self) -> "_GPUManagerContext":
        inner_ctx = self._inner.__enter__()
        self.token = getattr(inner_ctx, "token", None)
        self.health_info = getattr(inner_ctx, "health_info", {})
        if self.track_activity:
            self.reservation_id = self.manager.reserve(
                owner=self.owner,
                purpose=self.purpose,
                resident=False,
                status="running",
                model_key=self.model_key,
                model_value=self.model_value,
                vram_mb=self.vram_mb,
                gpu_index=self.gpu_index,
                metadata=self.metadata,
            )
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        if self.reservation_id:
            self.manager.release_reservation(self.reservation_id)
            self.reservation_id = ""
        return self._inner.__exit__(exc_type, exc, tb)


_SHARED_MANAGER: Optional[GPUManager] = None


def get_shared_gpu_manager(*, log: Any = None) -> GPUManager:
    global _SHARED_MANAGER
    if _SHARED_MANAGER is None:
        _SHARED_MANAGER = GPUManager(log=log)
    elif log is not None:
        _SHARED_MANAGER.log = log
    return _SHARED_MANAGER
