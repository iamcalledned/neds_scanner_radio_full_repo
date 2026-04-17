#!/usr/bin/env python3
"""
scanner_transcriber_mcp.py

Long-lived MCP server that keeps a Whisper model loaded on GPU and exposes tools:
- analyze_audio
- transcribe_file
- retranscribe_file

GPU-only policy:
- If CUDA is not available, this process FAILS FAST (no CPU fallback).

Run (HTTP / Streamable HTTP):
  python3 scanner_transcriber_mcp.py --transport streamable-http --host 127.0.0.1 --port 8008

MCP endpoint (HTTP):
  http://127.0.0.1:8008/mcp
"""

from __future__ import annotations

import os
import sys
from dotenv import load_dotenv
# Load local .env first, then the root .env for shared paths
load_dotenv(os.path.join(os.path.dirname(__file__), ".env"))
_project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
load_dotenv(os.path.join(_project_root, ".env"))
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)

import json
import threading
import logging
import logging.handlers
import argparse
import urllib.request
import urllib.error
from dataclasses import dataclass
from pathlib import Path
from collections import OrderedDict
from typing import Any, Dict, Optional, Tuple, List, Callable
from contextlib import asynccontextmanager

import torch
import numpy as np
import soundfile as sf

from faster_whisper import WhisperModel
from transformers.utils import logging as hf_logging

from mcp.server.fastmcp import FastMCP, Context

from gpu_gate import GPUGate
from mcp_tools.audio_processing import preprocess_audio, get_duration, get_rms, is_static
from mcp_tools.location_inference import call_location_inference_service
from mcp_tools.scoring import score_transcript

from mcp_functions.audio_analysis import process_analyze_audio
from mcp_functions.category_detection import detect_category as _detect_category_impl
from mcp_functions.model_catalog import build_model_catalog as _build_model_catalog_impl
from mcp_functions.router_runtime import (
    build_router as _build_router_impl,
    build_routing_rules as _build_routing_rules_impl,
    ensure_runtime as _ensure_runtime_impl,
)
from mcp_functions.whisper_loader import load_whisper_model as _load_whisper_model_impl
from mcp_config.scanner_transcriber_settings import (
    ALLOWED_ROOTS,
    ARCHIVE_BASE,
    DEFAULT_COMPUTE_TYPE,
    DEFAULT_MODEL_KEY,
    DEFAULT_MODEL_KEY_ENV,
    ENABLE_DB,
    FEED_KEYS,
    INTERACTIVE_ALLOWED_ROOTS,
    LOCATION_INFER_BASE_URL,
    LOCATION_INFER_TIMEOUT_S,
    MIN_DURATION,
    MODEL_BASE_DIR,
    MODEL_CACHE_LIMIT,
    MODEL_CATALOG_FILE,
    MODEL_CATALOG_JSON,
    MODEL_DIR,
    MODEL_ROUTING_FILE,
    MODEL_ROUTING_RULES,
    RMS_THRESHOLD,
    SOURCE_MAP,
    TMP_DIR,
    TRANSCRIBE_DEFAULTS,
    TRANSCRIBE_KEYS,
    WARM_DEFAULT_MODEL,
)
from mcp_routes.interactive_transcribe_segment import register_interactive_transcribe_segment_route
from mcp_routes.location_inference import register_location_inference_tools
from mcp_routes.route_and_transcribe import register_route_and_transcribe_tool
from mcp_routes.transcribe_with_state import transcribe_with_state as _transcribe_with_state_impl


# ==========================
# Logging
# ==========================
LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO").upper()
LOG_DIR = Path(os.environ.get("LOG_DIR", "/home/ned/data/scanner_calls/logs/transcriber_logs"))
LOG_DIR.mkdir(parents=True, exist_ok=True)

_log_fmt = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")

_console_handler = logging.StreamHandler()
_console_handler.setFormatter(_log_fmt)

_file_handler = logging.handlers.RotatingFileHandler(
    LOG_DIR / "scanner_mcp.log", maxBytes=10_000_000, backupCount=5
)
_file_handler.setFormatter(_log_fmt)

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    handlers=[_console_handler, _file_handler],
)
log = logging.getLogger("scanner-mcp")
hf_logging.set_verbosity_error()


# ==========================
# Config
# ==========================
DB_IMPORT_OK = False
scanner_db = None


def _is_under_allowed_roots(p: Path) -> bool:
    return _is_under_roots(p, ALLOWED_ROOTS)


def _is_under_roots(p: Path, roots: List[Path]) -> bool:
    rp = p.expanduser().resolve()
    for root in roots:
        try:
            rp.relative_to(root)
            return True
        except ValueError:
            continue
    return False


def _is_under_interactive_allowed_roots(p: Path) -> bool:
    return _is_under_roots(p, INTERACTIVE_ALLOWED_ROOTS)


def detect_category(file: Path) -> Tuple[str, str]:
    return _detect_category_impl(file, FEED_KEYS)




# ==========================
# Model state (GPU-only)
# ==========================
@dataclass
class WhisperState:
    model: WhisperModel
    device: torch.device
    gate: GPUGate


@dataclass
class ModelInfo:
    key: str
    model: str
    compute_type: str = DEFAULT_COMPUTE_TYPE
    device: str = "cuda"
    transcribe: Dict[str, Any] = None


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
    Keeps at most MODEL_CACHE_LIMIT models loaded to avoid VRAM thrash.
    """

    def __init__(self, catalog: Dict[str, ModelInfo], rules: List[RoutingRule], default_key: str):
        self.catalog = catalog
        self.rules = rules
        self.default_key = default_key if default_key in catalog else "default"
        self.cache: OrderedDict[str, WhisperModel] = OrderedDict()
        self.max_cached = max(1, MODEL_CACHE_LIMIT)
        self.gate = GPUGate()

    def _trim_cache(self) -> None:
        while len(self.cache) > self.max_cached:
            evict_key, evict_model = self.cache.popitem(last=False)
            try:
                del evict_model
                torch.cuda.empty_cache()
            except Exception:
                pass
            log.info(f"[router] Evicted model '{evict_key}' from cache to respect MODEL_CACHE_LIMIT={self.max_cached}")

    def get_model(self, key: str) -> WhisperModel:
        if key not in self.catalog:
            log.warning(f"[router] Requested model '{key}' not in catalog; falling back to default '{self.default_key}'")
            key = self.default_key

        if key in self.cache:
            self.cache.move_to_end(key)
            return self.cache[key]

        info = self.catalog[key]
        if info.device != "cuda":
            log.warning(f"[router] Model '{key}' requested device='{info.device}', but this service is GPU-only. Using cuda.")
        log.info(f"[router] Loading model '{key}' from {info.model}")
        _require_cuda()
        model = WhisperModel(str(info.model), device="cuda", compute_type=info.compute_type)
        self.cache[key] = model
        self._trim_cache()
        return model

    def resolve_profile(self, key: Optional[str] = None) -> Tuple[str, ModelInfo]:
        resolved = (key or "").strip() or self.default_key
        if resolved not in self.catalog:
            log.warning(f"[router] Requested profile '{resolved}' not in catalog; falling back to default '{self.default_key}'")
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
        return WhisperState(model=model, device=device, gate=self.gate)


_RUNTIME_LOCK = threading.Lock()
_RUNTIME: Dict[str, Any] = {}


def _require_cuda() -> torch.device:
    if not torch.cuda.is_available():
        raise RuntimeError("CUDA is required (GPU-only). torch.cuda.is_available() is False.")
    # Optional: TF32 speedups on Ampere+
    try:
        torch.backends.cuda.matmul.allow_tf32 = True
        torch.backends.cudnn.allow_tf32 = True
    except Exception:
        pass
    return torch.device("cuda")


def _json_from_env(env_value: str, file_path: str) -> Optional[Any]:
    """
    Helper: load JSON from an env string or a file path. Returns None on failure.
    """
    if env_value:
        try:
            return json.loads(env_value)
        except Exception as e:
            log.warning(f"[router] Failed to parse JSON from env: {e}")
    if file_path:
        p = Path(file_path).expanduser()
        if p.exists():
            try:
                return json.loads(p.read_text())
            except Exception as e:
                log.warning(f"[router] Failed to parse JSON from {p}: {e}")
    return None


def _resolve_model_ref(model_ref: str) -> str:
    raw = (model_ref or "").strip()
    if not raw:
        return str(MODEL_DIR)

    p = Path(raw).expanduser()
    if p.is_absolute() or raw.startswith(".") or raw.startswith("~"):
        return str(p.resolve())

    # Backward compatible behavior: bare local model folders resolve under MODEL_BASE_DIR.
    if "/" not in raw and "\\" not in raw:
        return str((MODEL_BASE_DIR / p).resolve())

    candidate = (MODEL_BASE_DIR / p).expanduser()
    if candidate.exists():
        return str(candidate.resolve())

    # Keep hub-style IDs (e.g., org/model-name) untouched.
    return raw


def _normalize_temperature(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, list):
        out: List[float] = []
        for item in value:
            if isinstance(item, (int, float)):
                out.append(float(item))
        if not out:
            return None
        return out
    return None


def _merged_transcribe_settings(raw: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    merged: Dict[str, Any] = dict(TRANSCRIBE_DEFAULTS)
    if isinstance(raw, dict):
        for key in TRANSCRIBE_KEYS:
            if key in raw:
                merged[key] = raw.get(key)

    if not merged.get("task"):
        merged["task"] = TRANSCRIBE_DEFAULTS["task"]
    if not merged.get("language"):
        merged["language"] = TRANSCRIBE_DEFAULTS["language"]

    try:
        merged["beam_size"] = int(merged.get("beam_size", TRANSCRIBE_DEFAULTS["beam_size"]))
    except (TypeError, ValueError):
        merged["beam_size"] = TRANSCRIBE_DEFAULTS["beam_size"]
    if merged["beam_size"] < 1:
        merged["beam_size"] = TRANSCRIBE_DEFAULTS["beam_size"]

    for bool_key in ("word_timestamps", "condition_on_previous_text", "vad_filter"):
        merged[bool_key] = bool(merged.get(bool_key, TRANSCRIBE_DEFAULTS[bool_key]))

    prompt = merged.get("initial_prompt")
    if isinstance(prompt, str):
        prompt = prompt.strip()
    merged["initial_prompt"] = prompt or None

    temperature = _normalize_temperature(merged.get("temperature"))
    merged["temperature"] = TRANSCRIBE_DEFAULTS["temperature"] if temperature is None else temperature

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


def _build_transcribe_kwargs(
    *,
    task: str,
    language: str,
    profile_settings: Optional[Dict[str, Any]],
) -> Dict[str, Any]:
    settings = _merged_transcribe_settings(profile_settings)
    if task:
        settings["task"] = task
    if language:
        settings["language"] = language

    kwargs: Dict[str, Any] = {}
    for key in TRANSCRIBE_KEYS:
        value = settings.get(key)
        if value is None:
            continue
        if key == "initial_prompt" and isinstance(value, str) and not value.strip():
            continue
        kwargs[key] = value
    return kwargs


def _resolve_model_profile(router: Optional[ModelRouter], requested_model_key: str = "") -> Tuple[str, Optional[ModelInfo]]:
    if not router:
        return "default", None
    return router.resolve_profile(requested_model_key)


def _resolve_active_model_profile(ctx: Context, requested_model_key: str = "") -> Tuple[str, Optional[ModelInfo]]:
    return _resolve_model_profile(_get_router(ctx), requested_model_key)


def _build_model_catalog() -> Tuple[Dict[str, ModelInfo], str]:
    return _build_model_catalog_impl(
        json_from_env_fn=_json_from_env,
        model_catalog_json=MODEL_CATALOG_JSON,
        model_catalog_file=MODEL_CATALOG_FILE,
        model_dir=MODEL_DIR,
        default_compute_type=DEFAULT_COMPUTE_TYPE,
        merged_transcribe_settings_fn=_merged_transcribe_settings,
        resolve_model_ref_fn=_resolve_model_ref,
        model_info_cls=ModelInfo,
    )


def _build_routing_rules() -> List[RoutingRule]:
    return _build_routing_rules_impl(
        json_from_env_fn=_json_from_env,
        model_routing_rules=MODEL_ROUTING_RULES,
        model_routing_file=MODEL_ROUTING_FILE,
        routing_rule_cls=RoutingRule,
    )


def _build_router() -> ModelRouter:
    return _build_router_impl(
        build_model_catalog_fn=_build_model_catalog,
        build_routing_rules_fn=_build_routing_rules,
        default_model_key_env=DEFAULT_MODEL_KEY_ENV,
        default_model_key=DEFAULT_MODEL_KEY,
        model_router_cls=ModelRouter,
    )


def _ensure_runtime() -> Dict[str, Any]:
    return _ensure_runtime_impl(
        runtime=_RUNTIME,
        runtime_lock=_RUNTIME_LOCK,
        build_router_fn=_build_router,
        warm_default_model=WARM_DEFAULT_MODEL,
        log=log,
    )


def load_whisper_model(model_dir: Path = MODEL_DIR, compute_type: str = DEFAULT_COMPUTE_TYPE) -> WhisperState:
    return _load_whisper_model_impl(
        model_dir=model_dir,
        compute_type=compute_type,
        require_cuda_fn=_require_cuda,
        whisper_model_cls=WhisperModel,
        log=log,
        whisper_state_cls=WhisperState,
        gpu_gate_cls=GPUGate,
    )


def transcribe_wavefile(
    state: WhisperState,
    wav_path: Path,
    *,
    task: str = "transcribe",
    language: str = "en",
    transcribe_settings: Optional[Dict[str, Any]] = None,
) -> str:
    """
    Transcribe a preprocessed WAV using the loaded faster-whisper model.
    Serialized with a GPU lock.
    """
    kwargs = _build_transcribe_kwargs(
        task=task,
        language=language,
        profile_settings=transcribe_settings,
    )
    log.info(
        "[transcribe_wavefile] decode settings: "
        f"beam_size={kwargs.get('beam_size')} "
        f"vad_filter={kwargs.get('vad_filter')} "
        f"language={kwargs.get('language')} "
        f"task={kwargs.get('task')} "
        f"initial_prompt_set={bool(kwargs.get('initial_prompt'))}"
    )

    log.info("[transcribe_wavefile] Acquiring GPU gate…")
    with state.gate.acquire("whisper", timeout_s=120):
        log.info("[transcribe_wavefile] GPU gate acquired, running model.transcribe()…")
        segments, _ = state.model.transcribe(str(wav_path), **kwargs)
        
        text = " ".join([segment.text for segment in segments]).strip()
        snippet = (text[:140] + "…") if len(text) > 140 else text

    log.info(f"[transcribe_wavefile] {snippet} | duration={get_duration(wav_path):.1f}s | rms={get_rms(wav_path):.6f}")
    return text


# ==========================
# Hook request detection
# ==========================
_HOOK_PATTERNS = re.compile(
    r'\b(tow|togi|togis|togie|togies|togy|hook|arts|towing)\b',
    re.IGNORECASE,
)

def detect_hook_request(text: str) -> bool:
    """Return True if the transcript contains a towing/hook keyword."""
    if not text:
        return False
    
    return bool(_HOOK_PATTERNS.search(text))


# ==========================
# Location inference client
# ==========================
def sidecar_json_for_audio(path: Path) -> Path:
    """Return the sidecar .json path for a given audio file path."""
    return path.with_suffix(".json")










# ==========================
# Optional DB integration
# ==========================
def try_import_db() -> None:
    global DB_IMPORT_OK, scanner_db
    if not ENABLE_DB:
        return
    try:
        from shared.scanner_db import (
            insert_call, create_tables, DB_PATH,
            update_call_classification, update_hook_request,
            update_review_status, infer_town_from_filename,
            infer_dept_from_filename,
        )
        # Build a namespace that looks like the old module import
        import types
        _mod = types.SimpleNamespace(
            insert_call=insert_call,
            create_tables=create_tables,
            DB_PATH=DB_PATH,
            update_call_classification=update_call_classification,
            update_hook_request=update_hook_request,
            update_review_status=update_review_status,
            infer_town_from_filename=infer_town_from_filename,
            infer_dept_from_filename=infer_dept_from_filename,
        )
        scanner_db = _mod
        DB_IMPORT_OK = True
        log.info("shared.scanner_db import OK (DB inserts enabled).")
    except Exception as e:
        DB_IMPORT_OK = False
        log.warning(f"shared.scanner_db import failed (DB inserts disabled): {e}")


# ==========================
# MCP server with lifespan
# ==========================
async def _wal_checkpoint_loop():
    """Periodically checkpoint the WAL file to prevent unbounded growth."""
    import asyncio
    while True:
        await asyncio.sleep(300)          # every 5 minutes
        try:
            from shared.scanner_db import wal_checkpoint
            wal_checkpoint()
        except Exception as e:
            log.warning(f"[DB] Periodic WAL checkpoint failed: {e}")


@asynccontextmanager
async def lifespan(_: FastMCP):
    """
    Startup/shutdown hook:
    - build model router and (optionally) warm default model
    - import DB module if available
    - start periodic WAL checkpoint task
    """
    import asyncio
    runtime = _ensure_runtime()
    router = runtime.get("router")
    state: Optional[WhisperState] = runtime.get("state")

    try_import_db()

    # Start background WAL checkpointer
    wal_task = asyncio.create_task(_wal_checkpoint_loop())

    try:
        ctx: Dict[str, Any] = {"router": router}
        if state:
            ctx["state"] = state
        yield ctx
    finally:
        wal_task.cancel()

mcp = FastMCP("scanner-transcriber", json_response=True, lifespan=lifespan)


def _get_state(ctx: Context) -> WhisperState:
    runtime = _ensure_runtime()
    data = ctx.request_context.lifespan_context
    state = data.get("state") or runtime.get("state")
    if state is None:
        router: Optional[ModelRouter] = data.get("router") or runtime.get("router")
        if router:
            log.info("[router] Lazy-loading default model for legacy transcribe_file path")
            state = router.get_state(router.default_key)
            data["state"] = state
            runtime["state"] = state
        else:
            state = load_whisper_model()
            data["state"] = state
            runtime["state"] = state
    elif data.get("state") is None:
        data["state"] = state
    return state


def _get_router(ctx: Context) -> Optional[ModelRouter]:
    return ctx.request_context.lifespan_context.get("router") or _ensure_runtime().get("router")


def _coerce_bool(value: Any, default: bool = False) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().lower() in ("1", "true", "yes", "on")
    return bool(value)


def _interactive_error_status(error_code: str) -> int:
    if error_code in {"invalid_json", "invalid_payload", "missing_path", "path_not_allowed", "missing_file", "too_short", "static"}:
        return 400
    if error_code in {"router_unavailable", "preprocess_failed", "transcribe_failed"}:
        return 500
    return 400


# ==========================
# MCP tools
# ==========================
@mcp.tool()
def analyze_audio(ctx: Context, path: str) -> Dict[str, Any]:
    """
    Analyze a WAV file (duration, RMS, static detection).

    Args:
      path: Absolute or relative path to the WAV file (must be under allowed roots).
    """
    return process_analyze_audio(
        path=path,
        is_allowed_fn=_is_under_allowed_roots,
        min_duration=MIN_DURATION,
        rms_threshold=RMS_THRESHOLD,
    )


def _transcribe_with_state(
    ctx: Optional[Context],
    state: WhisperState,
    *,
    path: str,
    model_key: str,
    profile: str,
    language: str,
    write_artifacts: bool,
    insert_db: bool,
    delete_source_raw: bool,
    custom_output_dir: str,
    skip_wav_copy: bool,
    router: Optional[ModelRouter] = None,
    is_allowed_fn: Callable[[Path], bool] = _is_under_allowed_roots,
) -> Dict[str, Any]:
    return _transcribe_with_state_impl(
        ctx,
        state,
        path=path,
        model_key=model_key,
        profile=profile,
        language=language,
        write_artifacts=write_artifacts,
        insert_db=insert_db,
        delete_source_raw=delete_source_raw,
        custom_output_dir=custom_output_dir,
        skip_wav_copy=skip_wav_copy,
        router=router,
        is_allowed_fn=is_allowed_fn,
        get_duration_fn=get_duration,
        min_duration=MIN_DURATION,
        is_static_fn=is_static,
        get_rms_fn=get_rms,
        rms_threshold=RMS_THRESHOLD,
        detect_category_fn=detect_category,
        archive_base=ARCHIVE_BASE,
        tmp_dir=TMP_DIR,
        get_router_fn=_get_router,
        resolve_model_profile_fn=_resolve_model_profile,
        log=log,
        preprocess_audio_fn=preprocess_audio,
        transcribe_wavefile_fn=transcribe_wavefile,
        score_transcript_fn=score_transcript,
        source_map=SOURCE_MAP,
        model_dir=MODEL_DIR,
        detect_hook_request_fn=detect_hook_request,
        db_state_getter=lambda: (DB_IMPORT_OK, scanner_db),
    )


register_interactive_transcribe_segment_route(
    mcp=mcp,
    ensure_runtime=_ensure_runtime,
    resolve_model_profile=_resolve_model_profile,
    transcribe_with_state=_transcribe_with_state,
    interactive_error_status=_interactive_error_status,
    coerce_bool=_coerce_bool,
    is_under_interactive_allowed_roots=_is_under_interactive_allowed_roots,
)
register_route_and_transcribe_tool(
    mcp=mcp,
    get_router=_get_router,
    is_under_allowed_roots=_is_under_allowed_roots,
    detect_category=detect_category,
    get_duration=get_duration,
    transcribe_with_state=_transcribe_with_state,
)
register_location_inference_tools(
    mcp=mcp,
    call_location_inference_service=call_location_inference_service,
    is_under_allowed_roots=_is_under_allowed_roots,
    sidecar_json_for_audio=sidecar_json_for_audio,
)


@mcp.tool()
def transcribe_file(
    ctx: Context,
    path: str,
    profile: str = "default",
    language: str = "en",
    write_artifacts: bool = True,
    insert_db: bool = True,
    delete_source_raw: bool = False,
    custom_output_dir: str = "",
    skip_wav_copy: bool = False,
) -> Dict[str, Any]:
    """
    Preprocess + transcribe a scanner WAV, then write artifacts under ARCHIVE_BASE/clean/<feed>/.

    Args:
      path: WAV path (must be under allowed roots)
      profile: preprocessing profile (default|radio|static_fix|aggressive)
      language: unused in this implementation (GPU-only fast tokenizer path)
      write_artifacts: if True, writes .json/.txt/.wav to clean folder
      insert_db: if True and scanner_db is available, insert into DB
      delete_source_raw: if True, deletes the source raw WAV after success
    """
    state = _get_state(ctx)

    return _transcribe_with_state(
        ctx,
        state,
        path=path,
        model_key="",
        profile=profile,
        language=language,
        write_artifacts=write_artifacts,
        insert_db=insert_db,
        delete_source_raw=delete_source_raw,
        custom_output_dir=custom_output_dir,
        skip_wav_copy=skip_wav_copy,
    )


@mcp.tool()
def retranscribe_file(
    ctx: Context,
    path: str,
    profile: str = "radio",
    language: str = "en",
) -> Dict[str, Any]:
    """
    Convenience wrapper for transcribe_file with a different preprocessing profile.
    """
    return transcribe_file(
        ctx,
        path=path,
        profile=profile,
        language=language,
        write_artifacts=True,
        insert_db=True,
        delete_source_raw=False,
    )


# ==========================
# Main runner
# ==========================
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--transport", default=os.environ.get("MCP_TRANSPORT", "streamable-http"),
                    choices=["streamable-http", "stdio", "sse"],
                    help="MCP transport (streamable-http recommended for website)")
    ap.add_argument("--host", default=os.environ.get("MCP_HOST", "127.0.0.1"))
    ap.add_argument("--port", type=int, default=int(os.environ.get("MCP_PORT", "8008")))
    args = ap.parse_args()

    log.info(f"Starting MCP server transport={args.transport} host={args.host} port={args.port}")

    # In current MCP SDK, host/port are settings on the FastMCP instance,
    # not kwargs to run().  Apply them before starting.
    mcp.settings.host = args.host
    mcp.settings.port = args.port
    mcp.run(transport=args.transport)


if __name__ == "__main__":
    main()
