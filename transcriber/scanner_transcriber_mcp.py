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

import threading
import logging
import logging.handlers
import argparse
from pathlib import Path
from typing import Any, Dict, Optional, Tuple, Callable
from contextlib import asynccontextmanager

from faster_whisper import WhisperModel
from transformers.utils import logging as hf_logging

from mcp.server.fastmcp import FastMCP, Context

from gpu_gate import GPUGate
from mcp_tools.audio_processing import preprocess_audio, get_duration, get_rms, is_static
from mcp_tools.location_inference import call_location_inference_service
from mcp_tools.scoring import score_transcript

from mcp_functions.audio_analysis import process_analyze_audio
from mcp_functions.category_detection import detect_category as _detect_category_impl
from mcp_functions.model_runtime import (
    ModelInfo,
    ModelRouter,
    RoutingRule,
    WhisperState,
    build_transcribe_kwargs,
    json_from_env,
    merged_transcribe_settings,
    require_cuda,
    resolve_model_profile,
    resolve_model_ref,
)
from mcp_functions.model_catalog import build_model_catalog as _build_model_catalog_impl
from mcp_functions.router_runtime import (
    build_router as _build_router_impl,
    build_routing_rules as _build_routing_rules_impl,
    ensure_runtime as _ensure_runtime_impl,
)
from mcp_functions.server_runtime import (
    coerce_bool as _coerce_bool,
    detect_hook_request,
    get_router as _get_router_impl,
    get_state as _get_state_impl,
    interactive_error_status as _interactive_error_status,
    is_under_roots,
    sidecar_json_for_audio,
    try_import_db as _try_import_db_impl,
    wal_checkpoint_loop as _wal_checkpoint_loop_impl,
)
from mcp_functions.transcribe_wavefile import transcribe_wavefile as _transcribe_wavefile_impl
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
from mcp_routes.core_transcribe_tools import register_core_transcribe_tools
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
    return is_under_roots(p, ALLOWED_ROOTS)


def _is_under_interactive_allowed_roots(p: Path) -> bool:
    return is_under_roots(p, INTERACTIVE_ALLOWED_ROOTS)


def detect_category(file: Path) -> Tuple[str, str]:
    return _detect_category_impl(file, FEED_KEYS)


_RUNTIME_LOCK = threading.Lock()
_RUNTIME: Dict[str, Any] = {}


def _require_cuda():
    return require_cuda()


def _json_from_env(env_value: str, file_path: str) -> Optional[Any]:
    return json_from_env(env_value, file_path)


def _resolve_model_ref(model_ref: str) -> str:
    return resolve_model_ref(model_ref, model_dir=MODEL_DIR, model_base_dir=MODEL_BASE_DIR)


def _merged_transcribe_settings(raw: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    return merged_transcribe_settings(raw, defaults=TRANSCRIBE_DEFAULTS, keys=TRANSCRIBE_KEYS)


def _build_transcribe_kwargs(
    *,
    task: str,
    language: str,
    profile_settings: Optional[Dict[str, Any]],
) -> Dict[str, Any]:
    return build_transcribe_kwargs(
        task=task,
        language=language,
        profile_settings=profile_settings,
        merged_transcribe_settings_fn=_merged_transcribe_settings,
        keys=TRANSCRIBE_KEYS,
    )


def _resolve_model_profile(router: Optional[ModelRouter], requested_model_key: str = "") -> Tuple[str, Optional[ModelInfo]]:
    return resolve_model_profile(router, requested_model_key)


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


def _build_routing_rules() -> list[RoutingRule]:
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
        model_router_factory=lambda catalog, rules, default_key: ModelRouter(
            catalog=catalog,
            rules=rules,
            default_key=default_key,
            max_cached=MODEL_CACHE_LIMIT,
        ),
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
    return _transcribe_wavefile_impl(
        state=state,
        wav_path=wav_path,
        task=task,
        language=language,
        transcribe_settings=transcribe_settings,
        build_transcribe_kwargs_fn=_build_transcribe_kwargs,
        log=log,
        get_duration_fn=get_duration,
        get_rms_fn=get_rms,
    )


# ==========================
# Optional DB integration
# ==========================
def try_import_db() -> None:
    global DB_IMPORT_OK, scanner_db
    DB_IMPORT_OK, scanner_db = _try_import_db_impl(ENABLE_DB, log)


# ==========================
# MCP server with lifespan
# ==========================
async def _wal_checkpoint_loop():
    await _wal_checkpoint_loop_impl(log)


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
    return _get_state_impl(
        ctx,
        ensure_runtime_fn=_ensure_runtime,
        load_whisper_model_fn=load_whisper_model,
        log=log,
    )


def _get_router(ctx: Context) -> Optional[ModelRouter]:
    return _get_router_impl(ctx, ensure_runtime_fn=_ensure_runtime)


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
register_core_transcribe_tools(
    mcp=mcp,
    process_analyze_audio_fn=process_analyze_audio,
    is_under_allowed_fn=_is_under_allowed_roots,
    min_duration=MIN_DURATION,
    rms_threshold=RMS_THRESHOLD,
    get_state_fn=_get_state,
    transcribe_with_state_fn=_transcribe_with_state,
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
