from __future__ import annotations

import json
import logging
import re
from collections import OrderedDict
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional

import torch

from faster_whisper import WhisperModel

from gpu_manager import GPUManager


_log = logging.getLogger("scanner-mcp")


@dataclass
class WhisperState:
    model: WhisperModel
    device: torch.device
    gate: GPUManager
    reservation_id: Optional[str] = None


@dataclass
class ModelCacheEntry:
    model: WhisperModel
    reservation_id: Optional[str] = None


@dataclass
class ModelInfo:
    key: str
    model: str
    compute_type: str = "float16"
    device: str = "cuda"
    transcribe: dict[str, Any] | None = None


@dataclass
class RoutingRule:
    model_key: str
    feed_regex: Optional[re.Pattern] = None
    path_regex: Optional[re.Pattern] = None
    min_duration: Optional[float] = None
    max_duration: Optional[float] = None

    def matches(self, *, feed: str, path: Path, duration: float) -> bool:
        if self.feed_regex and not self.feed_regex.search(feed or ""):
            return False
        if self.path_regex and not self.path_regex.search(str(path).lower()):
            return False
        if self.min_duration is not None and duration < self.min_duration:
            return False
        if self.max_duration is not None and duration > self.max_duration:
            return False
        return True


class ModelRouter:
    """
    Lightweight model registry + routing rules.
    Keeps at most `max_cached` models loaded to avoid VRAM thrash.
    """

    def __init__(
        self,
        catalog: dict[str, ModelInfo],
        rules: list[RoutingRule],
        default_key: str,
        max_cached: int = 1,
        gpu_manager: Optional[GPUManager] = None,
    ):
        self.catalog = catalog
        self.rules = rules
        self.default_key = default_key if default_key in catalog else "default"
        self.cache: OrderedDict[str, ModelCacheEntry] = OrderedDict()
        self.max_cached = max(1, max_cached)
        self.gate = gpu_manager or GPUManager(log=_log)

    def _trim_cache(self) -> None:
        while len(self.cache) > self.max_cached:
            evict_key, evict_entry = self.cache.popitem(last=False)
            try:
                if evict_entry.reservation_id:
                    self.gate.release_reservation(evict_entry.reservation_id)
                del evict_entry.model
                torch.cuda.empty_cache()
            except Exception:
                pass
            _log.info(f"[router] Evicted model '{evict_key}' from cache to respect MODEL_CACHE_LIMIT={self.max_cached}")

    def get_model(self, key: str) -> WhisperModel:
        if key not in self.catalog:
            _log.warning(f"[router] Requested model '{key}' not in catalog; falling back to default '{self.default_key}'")
            key = self.default_key

        if key in self.cache:
            self.cache.move_to_end(key)
            return self.cache[key].model

        info = self.catalog[key]
        if info.device != "cuda":
            _log.warning(f"[router] Model '{key}' requested device='{info.device}', but this service is GPU-only. Using cuda.")
        _log.info(f"[router] Loading model '{key}' from {info.model}")
        require_cuda()
        model, reservation_id = self.gate.load_model(
            owner=f"whisper-router:{key}",
            load_fn=lambda: WhisperModel(str(info.model), device="cuda", compute_type=info.compute_type),
            timeout_s=300,
            model_key=key,
            model_value=str(info.model),
            metadata={"compute_type": info.compute_type, "device": "cuda"},
            purpose="whisper-model-load",
        )
        self.cache[key] = ModelCacheEntry(model=model, reservation_id=reservation_id)
        self._trim_cache()
        return model

    def resolve_profile(self, key: Optional[str] = None) -> tuple[str, ModelInfo]:
        resolved = (key or "").strip() or self.default_key
        if resolved not in self.catalog:
            _log.warning(f"[router] Requested profile '{resolved}' not in catalog; falling back to default '{self.default_key}'")
            resolved = self.default_key
        return resolved, self.catalog[resolved]

    def choose_model(self, *, path: Path, feed: str, duration: float) -> str:
        for rule in self.rules:
            if rule.matches(feed=feed, path=path, duration=duration):
                return rule.model_key if rule.model_key in self.catalog else self.default_key
        return self.default_key

    def get_state(self, key: str) -> WhisperState:
        model = self.get_model(key)
        device = torch.device("cuda")
        cache_entry = self.cache.get(key)
        reservation_id = cache_entry.reservation_id if cache_entry else None
        return WhisperState(model=model, device=device, gate=self.gate, reservation_id=reservation_id)

    def close(self) -> None:
        while self.cache:
            _, evict_entry = self.cache.popitem(last=False)
            try:
                if evict_entry.reservation_id:
                    self.gate.release_reservation(evict_entry.reservation_id)
                del evict_entry.model
            except Exception:
                pass
        try:
            torch.cuda.empty_cache()
        except Exception:
            pass


def require_cuda() -> torch.device:
    if not torch.cuda.is_available():
        raise RuntimeError("CUDA is required (GPU-only). torch.cuda.is_available() is False.")
    try:
        torch.backends.cuda.matmul.allow_tf32 = True
        torch.backends.cudnn.allow_tf32 = True
    except Exception:
        pass
    return torch.device("cuda")


def json_from_env(env_value: str, file_path: str) -> Optional[Any]:
    if env_value:
        try:
            return json.loads(env_value)
        except Exception as e:
            _log.warning(f"[router] Failed to parse JSON from env: {e}")
    if file_path:
        p = Path(file_path).expanduser()
        if p.exists():
            try:
                return json.loads(p.read_text())
            except Exception as e:
                _log.warning(f"[router] Failed to parse JSON from {p}: {e}")
    return None


def resolve_model_ref(model_ref: str, *, model_dir: Path, model_base_dir: Path) -> str:
    raw = (model_ref or "").strip()
    if not raw:
        return str(model_dir)

    p = Path(raw).expanduser()
    if p.is_absolute() or raw.startswith(".") or raw.startswith("~"):
        return str(p.resolve())

    if "/" not in raw and "\\" not in raw:
        return str((model_base_dir / p).resolve())

    candidate = (model_base_dir / p).expanduser()
    if candidate.exists():
        return str(candidate.resolve())

    return raw


def normalize_temperature(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, list):
        out: list[float] = []
        for item in value:
            if isinstance(item, (int, float)):
                out.append(float(item))
        if not out:
            return None
        return out
    return None


def merged_transcribe_settings(
    raw: Optional[dict[str, Any]],
    *,
    defaults: dict[str, Any],
    keys: tuple[str, ...],
) -> dict[str, Any]:
    merged: dict[str, Any] = dict(defaults)
    if isinstance(raw, dict):
        for key in keys:
            if key in raw:
                merged[key] = raw.get(key)

    if not merged.get("task"):
        merged["task"] = defaults["task"]
    if not merged.get("language"):
        merged["language"] = defaults["language"]

    try:
        merged["beam_size"] = int(merged.get("beam_size", defaults["beam_size"]))
    except (TypeError, ValueError):
        merged["beam_size"] = defaults["beam_size"]
    if merged["beam_size"] < 1:
        merged["beam_size"] = defaults["beam_size"]

    for bool_key in ("word_timestamps", "condition_on_previous_text", "vad_filter"):
        merged[bool_key] = bool(merged.get(bool_key, defaults[bool_key]))

    prompt = merged.get("initial_prompt")
    if isinstance(prompt, str):
        prompt = prompt.strip()
    merged["initial_prompt"] = prompt or None

    temperature = normalize_temperature(merged.get("temperature"))
    merged["temperature"] = defaults["temperature"] if temperature is None else temperature

    for optional_num_key in (
        "best_of",
        "patience",
        "compression_ratio_threshold",
        "log_prob_threshold",
        "no_speech_threshold",
    ):
        value = merged.get(optional_num_key)
        if value is None:
            continue
        try:
            merged[optional_num_key] = float(value)
        except (TypeError, ValueError):
            merged[optional_num_key] = None

    if merged.get("best_of") is not None:
        merged["best_of"] = int(merged["best_of"])

    return merged


def build_transcribe_kwargs(
    *,
    task: str,
    language: str,
    profile_settings: Optional[dict[str, Any]],
    merged_transcribe_settings_fn: Callable[[Optional[dict[str, Any]]], dict[str, Any]],
    keys: tuple[str, ...],
) -> dict[str, Any]:
    settings = merged_transcribe_settings_fn(profile_settings)
    if task:
        settings["task"] = task
    if language:
        settings["language"] = language

    kwargs: dict[str, Any] = {}
    for key in keys:
        value = settings.get(key)
        if value is None:
            continue
        if key == "initial_prompt" and isinstance(value, str) and not value.strip():
            continue
        kwargs[key] = value
    return kwargs


def resolve_model_profile(router: Optional[ModelRouter], requested_model_key: str = "") -> tuple[str, Optional[ModelInfo]]:
    if not router:
        return "default", None
    return router.resolve_profile(requested_model_key)
