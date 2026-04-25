from __future__ import annotations

import os
import signal
import subprocess
import threading
import time
import uuid
from dataclasses import dataclass
from typing import Any, Optional

import requests

from gpu_manager import GPUManager


@dataclass
class ManagedVLLMSession:
    session_id: str
    model_key: str
    model_value: str
    api_base: str
    process: subprocess.Popen[Any]
    gate_scope: str
    gpu_token: Optional[str]
    reservation_id: Optional[str]


class ManagedVLLMManager:
    def __init__(self, *, log: Any, gpu_manager: Optional[GPUManager] = None):
        self.log = log
        self.gpu = gpu_manager or GPUManager(log=log)
        self._lock = threading.Lock()
        self._sessions: dict[str, ManagedVLLMSession] = {}
        self._idle_timers: dict[str, threading.Timer] = {}

    def _is_enabled(self, value: Any, default: bool = False) -> bool:
        if value is None:
            return default
        if isinstance(value, bool):
            return value
        return str(value).strip().lower() not in {"0", "false", "no", "off", ""}

    def _append_flag(self, cmd: list[str], chat_cfg: dict[str, Any], cfg_key: str, cli_flag: str) -> None:
        value = chat_cfg.get(cfg_key)
        if value is None or str(value).strip() == "":
            return
        cmd.extend([cli_flag, str(value)])

    def _stream_process_output(self, process: subprocess.Popen[Any], *, model_key: str, session_id: str) -> None:
        if process.stdout is None:
            return

        def worker() -> None:
            try:
                for raw_line in process.stdout:
                    line = raw_line.rstrip()
                    if not line:
                        continue
                    self.log.info("[managed_vllm][%s][%s] %s", model_key, session_id, line)
            except Exception as exc:
                self.log.warning(
                    "[managed_vllm][%s][%s] output stream reader failed: %s",
                    model_key,
                    session_id,
                    exc,
                )

        thread = threading.Thread(
            target=worker,
            name=f"managed-vllm-log-{model_key}-{session_id[:8]}",
            daemon=True,
        )
        thread.start()

    def _gate_scope(self, chat_cfg: dict[str, Any]) -> str:
        scope = str(chat_cfg.get("gpu_gate_scope") or "session").strip().lower()
        if scope in {"request", "session"}:
            return scope
        self.log.warning("[managed_vllm] Invalid gpu_gate_scope=%r; falling back to 'session'", scope)
        return "session"

    def _build_command(self, model_value: str, chat_cfg: dict[str, Any]) -> list[str]:
        vllm_bin = str(chat_cfg.get("vllm_bin") or "/home/ned/vllm_stack/bin/vllm")
        port = int(chat_cfg.get("port", 30000))
        gpu_memory_utilization = str(chat_cfg.get("gpu_memory_utilization", 0.55))
        raw_tool_call_parser = chat_cfg.get("tool_call_parser")
        tool_call_parser = str(raw_tool_call_parser).strip() if raw_tool_call_parser is not None else ""

        cmd = [
            vllm_bin,
            "serve",
            model_value,
            "--port",
            str(port),
            "--gpu-memory-utilization",
            gpu_memory_utilization,
        ]
        self._append_flag(cmd, chat_cfg, "max_model_len", "--max-model-len")
        self._append_flag(cmd, chat_cfg, "max_num_seqs", "--max-num-seqs")
        self._append_flag(cmd, chat_cfg, "tokenizer_mode", "--tokenizer-mode")
        self._append_flag(cmd, chat_cfg, "served_model_name", "--served-model-name")
        if self._is_enabled(chat_cfg.get("trust_remote_code"), default=False):
            cmd.append("--trust-remote-code")
        if self._is_enabled(chat_cfg.get("enable_auto_tool_choice"), default=True):
            cmd.append("--enable-auto-tool-choice")
        if tool_call_parser:
            cmd.extend(["--tool-call-parser", tool_call_parser])
        extra_args = chat_cfg.get("extra_args")
        if isinstance(extra_args, list):
            for item in extra_args:
                text = str(item).strip()
                if text:
                    cmd.append(text)
        return cmd

    def _wait_until_ready(self, api_base: str, timeout: int) -> None:
        health_url = api_base.rstrip("/") + "/models"
        started = time.time()
        last_error: Optional[str] = None
        while time.time() - started < timeout:
            try:
                response = requests.get(health_url, timeout=5)
                if response.ok:
                    return
                last_error = f"http_{response.status_code}"
            except Exception as exc:
                last_error = str(exc)
            time.sleep(1.0)
        raise TimeoutError(f"Timed out waiting for managed vLLM server at {health_url}: {last_error}")

    def _idle_timeout_s(self, chat_cfg: dict[str, Any]) -> Optional[float]:
        raw = chat_cfg.get("idle_timeout_s", chat_cfg.get("idle_timeout", 60))
        try:
            value = float(raw)
        except (TypeError, ValueError):
            self.log.warning("[managed_vllm] Invalid idle_timeout=%r; using 60 seconds", raw)
            value = 60.0
        if value <= 0:
            return None
        return value

    def _cancel_idle_timer_locked(self, session_id: str) -> None:
        timer = self._idle_timers.pop(session_id, None)
        if timer is not None:
            timer.cancel()

    def _schedule_idle_stop(self, session: ManagedVLLMSession, chat_cfg: dict[str, Any]) -> None:
        timeout_s = self._idle_timeout_s(chat_cfg)
        if timeout_s is None:
            return

        timer = threading.Timer(timeout_s, self._stop_idle_session, args=(session.session_id,))
        timer.daemon = True

        with self._lock:
            if session.session_id not in self._sessions:
                return
            self._cancel_idle_timer_locked(session.session_id)
            self._idle_timers[session.session_id] = timer

        self.log.info(
            "[managed_vllm] Session %s model=%s idle cleanup scheduled in %.1fs",
            session.session_id,
            session.model_key,
            timeout_s,
        )
        timer.start()

    def _stop_idle_session(self, session_id: str) -> None:
        self.log.info("[managed_vllm] Session %s reached idle timeout; stopping", session_id)
        self.stop_session(session_id)

    def _terminate_process(self, process: subprocess.Popen[Any], *, terminate_timeout: int = 15) -> None:
        parent_exited = process.poll() is not None

        try:
            os.killpg(process.pid, signal.SIGTERM)
        except ProcessLookupError:
            return
        except Exception as exc:
            self.log.warning("[managed_vllm] Failed to SIGTERM process group for pid=%s: %s", process.pid, exc)
            try:
                process.terminate()
            except Exception:
                pass

        if parent_exited:
            return

        try:
            process.wait(timeout=terminate_timeout)
            return
        except subprocess.TimeoutExpired:
            self.log.warning("[managed_vllm] vLLM pid=%s did not exit after SIGTERM; sending SIGKILL", process.pid)
        except Exception as exc:
            self.log.warning("[managed_vllm] Failed waiting for vLLM pid=%s after SIGTERM: %s", process.pid, exc)

        if process.poll() is not None:
            return

        try:
            os.killpg(process.pid, signal.SIGKILL)
        except ProcessLookupError:
            return
        except Exception as exc:
            self.log.warning("[managed_vllm] Failed to SIGKILL process group for pid=%s: %s", process.pid, exc)
            try:
                process.kill()
            except Exception:
                pass

        try:
            process.wait(timeout=5)
        except Exception as exc:
            self.log.warning("[managed_vllm] Failed waiting for vLLM pid=%s after SIGKILL: %s", process.pid, exc)

    def start_session(
        self,
        *,
        model_key: str,
        model_value: str,
        chat_cfg: dict[str, Any],
        session_id: Optional[str],
        startup_timeout: int,
    ) -> ManagedVLLMSession:
        chosen_session_id = session_id or f"managed-vllm-{uuid.uuid4().hex}"
        gate_scope = self._gate_scope(chat_cfg)

        with self._lock:
            existing = self._sessions.get(chosen_session_id)
            if existing:
                if existing.model_key != model_key:
                    raise RuntimeError(
                        f"session_id '{chosen_session_id}' already belongs to model '{existing.model_key}'"
                    )
                self._cancel_idle_timer_locked(chosen_session_id)
                return existing

        owner = f"managed-vllm:{model_key}:{chosen_session_id}"
        process: Optional[subprocess.Popen[Any]] = None
        gpu_token: Optional[str] = None
        reservation_id: Optional[str] = None
        try:
            def start_process() -> str:
                nonlocal process
                cmd = self._build_command(model_value, chat_cfg)
                api_base = str(chat_cfg.get("api_base") or f"http://127.0.0.1:{int(chat_cfg.get('port', 30000))}/v1")

                self.log.info(
                    "[managed_vllm] Starting session %s model=%s gate_scope=%s",
                    chosen_session_id,
                    model_key,
                    gate_scope,
                )
                self.log.info("[managed_vllm] Command: %s", " ".join(cmd))
                process = subprocess.Popen(
                    cmd,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.STDOUT,
                    stdin=subprocess.DEVNULL,
                    text=True,
                    bufsize=1,
                    start_new_session=True,
                )
                self._stream_process_output(process, model_key=model_key, session_id=chosen_session_id)
                self._wait_until_ready(api_base, startup_timeout)
                return api_base

            if gate_scope == "session":
                gpu_token = self.gpu.claim(
                    owner=owner,
                    timeout_s=startup_timeout,
                    purpose="vllm-session",
                    model_key=model_key,
                    model_value=model_value,
                    metadata={"session_id": chosen_session_id, "phase": "session"},
                )
                api_base = start_process()
            else:
                with self.gpu.acquire(
                    f"{owner}:startup",
                    timeout_s=startup_timeout,
                    purpose="vllm-startup",
                    model_key=model_key,
                    model_value=model_value,
                    metadata={"session_id": chosen_session_id, "phase": "startup"},
                ):
                    api_base = start_process()

            reservation_id = self.gpu.reserve_resident(
                owner=owner,
                purpose="vllm-resident",
                model_key=model_key,
                model_value=model_value,
                metadata={"session_id": chosen_session_id, "api_base": api_base},
            )

            session = ManagedVLLMSession(
                session_id=chosen_session_id,
                model_key=model_key,
                model_value=model_value,
                api_base=api_base,
                process=process,
                gate_scope=gate_scope,
                gpu_token=gpu_token,
                reservation_id=reservation_id,
            )
            with self._lock:
                self._sessions[chosen_session_id] = session
            return session
        except Exception:
            if process is not None:
                self._terminate_process(process, terminate_timeout=10)
            if reservation_id:
                self.gpu.release_reservation(reservation_id)
            if gpu_token:
                self.gpu.release(gpu_token)
            raise

    def stop_session(self, session_id: str) -> None:
        with self._lock:
            self._cancel_idle_timer_locked(session_id)
            session = self._sessions.pop(session_id, None)

        if not session:
            return

        self.log.info("[managed_vllm] Stopping session %s model=%s", session.session_id, session.model_key)
        try:
            self._terminate_process(session.process, terminate_timeout=15)
        finally:
            if session.reservation_id:
                self.gpu.release_reservation(session.reservation_id)
            if session.gpu_token:
                self.gpu.release(session.gpu_token)

    def stop_all(self) -> None:
        with self._lock:
            session_ids = list(self._sessions.keys())
        for session_id in session_ids:
            self.stop_session(session_id)

    def chat_completion(
        self,
        *,
        model_key: str,
        catalog_entry: dict[str, Any],
        messages: list[dict[str, str]],
        temperature: float,
        timeout: int,
        session_id: Optional[str],
        close_session: bool,
    ) -> dict[str, Any]:
        model_value = str(catalog_entry.get("model") or "").strip()
        if not model_value:
            raise RuntimeError(f"Catalog entry for '{model_key}' is missing 'model'")

        chat_cfg = catalog_entry.get("chat")
        if not isinstance(chat_cfg, dict):
            raise RuntimeError(f"Catalog entry for '{model_key}' is missing chat configuration")

        session = self.start_session(
            model_key=model_key,
            model_value=model_value,
            chat_cfg=chat_cfg,
            session_id=session_id,
            startup_timeout=timeout,
        )

        started_at = time.time()
        try:
            def send_request() -> requests.Response:
                return requests.post(
                    session.api_base.rstrip("/") + "/chat/completions",
                    headers={
                        "Authorization": f"Bearer {chat_cfg.get('api_key', 'dummy')}",
                        "Content-Type": "application/json",
                    },
                    json={
                        "model": model_value,
                        "messages": messages,
                        "temperature": temperature,
                    },
                    timeout=timeout,
                )

            if session.gate_scope == "request":
                with self.gpu.acquire(
                    f"managed-vllm:{model_key}:{session.session_id}:request",
                    timeout_s=timeout,
                    purpose="vllm-inference",
                    model_key=model_key,
                    model_value=model_value,
                    metadata={"session_id": session.session_id, "phase": "inference"},
                ):
                    response = send_request()
            else:
                response = send_request()
            response.raise_for_status()
            data = response.json()

            try:
                text = data["choices"][0]["message"]["content"].strip()
            except Exception as exc:
                raise RuntimeError(f"Unexpected chat completion response format: {exc}") from exc

            return {
                "ok": True,
                "text": text,
                "error": None,
                "model_key": model_key,
                "model_value": model_value,
                "session_id": session.session_id,
                "elapsed_s": round(time.time() - started_at, 3),
            }
        finally:
            if close_session:
                self.stop_session(session.session_id)
            else:
                self._schedule_idle_stop(session, chat_cfg)
