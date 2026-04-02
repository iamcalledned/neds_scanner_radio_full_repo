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

import re
import gc
import json
import time
import shutil
import logging
import logging.handlers
import argparse
import subprocess
import urllib.request
import urllib.error
from dataclasses import dataclass
from pathlib import Path
from collections import OrderedDict
from typing import Any, Dict, Optional, Tuple, List
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
# Config (env override)
# ==========================
ARCHIVE_BASE = Path(os.environ.get("ARCHIVE_BASE", "/home/ned/data/scanner_calls/scanner_archive")).expanduser().resolve()
TMP_DIR = Path(os.environ.get("SCANNER_TMP_DIR", "/tmp/scanner_tmp")).expanduser().resolve()

MODEL_BASE_DIR = Path(os.environ.get("MODEL_BASE_DIR", "/home/ned/models")).expanduser().resolve()
MODEL_DIR = Path(os.environ.get("WHISPER_MODEL_DIR", MODEL_BASE_DIR / "trained_whisper_medium_april_2026_ct2")).expanduser().resolve()
DEFAULT_COMPUTE_TYPE = os.environ.get("WHISPER_COMPUTE_TYPE", "float16")

MIN_DURATION = float(os.environ.get("MIN_DURATION", "2"))
RMS_THRESHOLD = float(os.environ.get("RMS_THRESHOLD", "0.001"))

LOCATION_INFER_BASE_URL = os.environ.get("LOCATION_INFER_BASE_URL", "http://127.0.0.1:8011").rstrip("/")
LOCATION_INFER_TIMEOUT_S = int(os.environ.get("LOCATION_INFER_TIMEOUT_S", "30"))

# Only allow reading audio from under these roots (comma-separated)
ALLOWED_ROOTS = [
    p.strip() for p in os.environ.get("ALLOWED_AUDIO_ROOTS", str(ARCHIVE_BASE)).split(",") if p.strip()
]
ALLOWED_ROOTS = [Path(p).expanduser().resolve() for p in ALLOWED_ROOTS]

# Optional DB insert
ENABLE_DB = os.environ.get("ENABLE_SCANNER_DB", "1").strip() not in ("0", "false", "False")
DB_IMPORT_OK = False
scanner_db = None

# Dynamic model routing config
MODEL_CATALOG_JSON = os.environ.get("MODEL_CATALOG_JSON", "")
MODEL_CATALOG_FILE = os.environ.get("MODEL_CATALOG_FILE", "")
MODEL_ROUTING_RULES = os.environ.get("MODEL_ROUTING_RULES", "")
MODEL_ROUTING_FILE = os.environ.get("MODEL_ROUTING_FILE", "")
DEFAULT_MODEL_KEY = os.environ.get("DEFAULT_MODEL_KEY", "default")
MODEL_CACHE_LIMIT = int(os.environ.get("MODEL_CACHE_LIMIT", "1"))
WARM_DEFAULT_MODEL = os.environ.get("WARM_DEFAULT_MODEL", "1").strip().lower() not in ("0", "false", "no")


# ==========================
# Category detection
# ==========================
FEED_KEYS = [
    "mndfd", "mndpd", "mpd", "mfd", "bpd", "bfd", "pd", "fd",
    "blkfd", "blkpd", "uptfd", "uptpd", "frkpd", "frkfd",
]

SOURCE_MAP = {
    "pd": "Hopedale",
    "fd": "Hopedale",
    "mfd": "Milford",
    "mpd": "Milford",
    "bfd": "Bellingham",
    "bpd": "Bellingham",
    "mndfd": "Mendon",
    "mndpd": "Mendon",
    "uptfd": "Upton",
    "uptpd": "Upton",
    "blkfd": "Blackstone",
    "blkpd": "Blackstone",
    "frkfd": "Franklin",
    "frkpd": "Franklin",
}


def _is_under_allowed_roots(p: Path) -> bool:
    rp = p.expanduser().resolve()
    for root in ALLOWED_ROOTS:
        try:
            rp.relative_to(root)
            return True
        except ValueError:
            continue
    return False


def detect_category(file: Path) -> Tuple[str, str]:
    """Detect feed (e.g., mpd, frkfd) from filename or path."""
    name = file.name.lower()
    parts = " ".join(file.parts).lower()
    for key in FEED_KEYS:
        if re.search(rf"_{key}(?:\.|_|$)", name):
            return key, key
        if f"/{key}/" in parts:
            return key, key
    return "misc", "misc"




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
    path: Path
    compute_type: str = DEFAULT_COMPUTE_TYPE


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
        log.info(f"[router] Loading model '{key}' from {info.path}")
        _require_cuda()
        model = WhisperModel(str(info.path), device="cuda", compute_type=info.compute_type)
        self.cache[key] = model
        self._trim_cache()
        return model

    def choose_model(self, *, path: Path, feed: str, duration: float) -> str:
        for rule in self.rules:
            if rule.matches(feed=feed, path=path, duration=duration):
                return rule.model_key if rule.model_key in self.catalog else self.default_key
        return self.default_key

    def get_state(self, key: str) -> WhisperState:
        model = self.get_model(key)
        device = torch.device("cuda")
        return WhisperState(model=model, device=device, gate=self.gate)


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


def _build_model_catalog() -> Dict[str, ModelInfo]:
    raw = _json_from_env(MODEL_CATALOG_JSON, MODEL_CATALOG_FILE) or {}
    catalog: Dict[str, ModelInfo] = {}

    # Always include default entry (env WHISPER_MODEL_DIR) so existing behavior works.
    catalog["default"] = ModelInfo(
        key="default",
        path=MODEL_DIR,
        compute_type=DEFAULT_COMPUTE_TYPE,
    )

    # Merge user-defined catalog entries
    if isinstance(raw, dict):
        for key, val in raw.items():
            if not isinstance(val, dict):
                continue
            path_str = val.get("path") or val.get("model_path") or val.get("dir")
            if not path_str:
                continue
            
            p = Path(path_str).expanduser()
            if not p.is_absolute():
                p = MODEL_BASE_DIR / p
                
            compute_type = val.get("compute_type", DEFAULT_COMPUTE_TYPE)
            catalog[key] = ModelInfo(key=key, path=p, compute_type=compute_type)

    return catalog


def _build_routing_rules() -> List[RoutingRule]:
    raw = _json_from_env(MODEL_ROUTING_RULES, MODEL_ROUTING_FILE) or []
    rules: List[RoutingRule] = []
    if not isinstance(raw, list):
        return rules

    for item in raw:
        if not isinstance(item, dict):
            continue
        model_key = item.get("model") or item.get("model_key")
        if not model_key:
            continue
        match = item.get("match", item)
        feed_re = match.get("feed_regex") if isinstance(match, dict) else None
        path_re = match.get("path_regex") if isinstance(match, dict) else None
        min_dur = match.get("min_duration") if isinstance(match, dict) else None
        max_dur = match.get("max_duration") if isinstance(match, dict) else None

        try:
            feed_regex = re.compile(feed_re, re.IGNORECASE) if feed_re else None
        except re.error:
            feed_regex = None
        try:
            path_regex = re.compile(path_re, re.IGNORECASE) if path_re else None
        except re.error:
            path_regex = None

        rule = RoutingRule(
            model_key=model_key,
            feed_regex=feed_regex,
            path_regex=path_regex,
            min_duration=float(min_dur) if min_dur is not None else None,
            max_duration=float(max_dur) if max_dur is not None else None,
        )
        rules.append(rule)
    return rules


def _build_router() -> ModelRouter:
    catalog = _build_model_catalog()
    rules = _build_routing_rules()
    default_key = DEFAULT_MODEL_KEY if DEFAULT_MODEL_KEY in catalog else "default"
    return ModelRouter(catalog=catalog, rules=rules, default_key=default_key)


def load_whisper_model(model_dir: Path = MODEL_DIR, compute_type: str = DEFAULT_COMPUTE_TYPE) -> WhisperState:
    # Keep allocator config (your env can override)
    os.environ["PYTORCH_CUDA_ALLOC_CONF"] = os.environ.get(
        "PYTORCH_CUDA_ALLOC_CONF", "expandable_segments:True"
    )

    device = _require_cuda()

    if not model_dir.exists():
        raise FileNotFoundError(f"WHISPER_MODEL_DIR not found: {model_dir}")

    log.info(f"Loading faster-whisper model from: {model_dir}")

    model = WhisperModel(
        str(model_dir),
        device="cuda",
        compute_type=compute_type,
    )

    free, total = torch.cuda.mem_get_info()
    log.info(f"CUDA memory free: {free/(1024**3):.2f} GB / {total/(1024**3):.2f} GB")
    log.info("Model ready on CUDA (fp16)")

    return WhisperState(
        model=model,
        device=device,
        gate=GPUGate(),
    )


def transcribe_wavefile(
    state: WhisperState,
    wav_path: Path,
    *,
    task: str = "transcribe",
    language: str = "en",
) -> str:
    """
    Transcribe a preprocessed WAV using the loaded faster-whisper model.
    Serialized with a GPU lock.
    """
    log.info("[transcribe_wavefile] Acquiring GPU gate…")
    with state.gate.acquire("whisper", timeout_s=120):
        log.info("[transcribe_wavefile] GPU gate acquired, running model.transcribe()…")
        segments, _ = state.model.transcribe(
            str(wav_path),
            task=task,
            language=language,
            beam_size=1,
            word_timestamps=False,
            condition_on_previous_text=False
        )
        
        text = " ".join([segment.text for segment in segments]).strip()

    log.info(f"[transcribe_wavefile] Decoded {len(text)} chars")
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
    router = _build_router()
    state: Optional[WhisperState] = None

    if WARM_DEFAULT_MODEL:
        log.info(f"[lifespan] Warming default model '{router.default_key}'")
        state = router.get_state(router.default_key)
    else:
        log.info("[lifespan] WARM_DEFAULT_MODEL=0; deferring model load until first request")

    try_import_db()

    # Start background WAL checkpointer
    wal_task = asyncio.create_task(_wal_checkpoint_loop())

    try:
        ctx: Dict[str, Any] = {"router": router}
        if state:
            ctx["state"] = state
        yield ctx
    finally:
        # shutdown cleanup
        wal_task.cancel()
        try:
            del state
        except Exception:
            pass
        try:
            router.cache.clear()
        except Exception:
            pass
        gc.collect()
        try:
            torch.cuda.empty_cache()
        except Exception:
            pass


mcp = FastMCP("scanner-transcriber", json_response=True, lifespan=lifespan)


def _get_state(ctx: Context) -> WhisperState:
    data = ctx.request_context.lifespan_context
    state = data.get("state")
    if state is None:
        router: Optional[ModelRouter] = data.get("router")
        if router:
            log.info("[router] Lazy-loading default model for legacy transcribe_file path")
            state = router.get_state(router.default_key)
            data["state"] = state
        else:
            state = load_whisper_model()
            data["state"] = state
    return state


def _get_router(ctx: Context) -> Optional[ModelRouter]:
    return ctx.request_context.lifespan_context.get("router")


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
    ctx: Context,
    state: WhisperState,
    *,
    path: str,
    profile: str,
    language: str,
    write_artifacts: bool,
    insert_db: bool,
    delete_source_raw: bool,
    custom_output_dir: str,
    skip_wav_copy: bool,
) -> Dict[str, Any]:
    src = Path(path).expanduser()
    if not _is_under_allowed_roots(src):
        return {"ok": False, "error": "path_not_allowed", "path": str(src)}

    src = src.resolve()
    if not src.exists():
        return {"ok": False, "error": "missing_file", "path": str(src)}

    dur = get_duration(src)
    if dur < MIN_DURATION:
        return {"ok": False, "error": "too_short", "duration": dur, "min_duration": MIN_DURATION}

    if is_static(src):
        return {"ok": False, "error": "static", "rms": get_rms(src), "rms_threshold": RMS_THRESHOLD}

    clean_subdir, raw_subdir = detect_category(src)
    clean_dir = (ARCHIVE_BASE / "clean" / clean_subdir).resolve()
    raw_dir = (ARCHIVE_BASE / "raw" / raw_subdir).resolve()

    TMP_DIR.mkdir(parents=True, exist_ok=True)
    clean_dir.mkdir(parents=True, exist_ok=True)
    raw_dir.mkdir(parents=True, exist_ok=True)

    tmp = TMP_DIR / f"{src.stem}_clean.wav"

    target_dir = Path(custom_output_dir).expanduser().resolve() if custom_output_dir else clean_dir
    if custom_output_dir:
        target_dir.mkdir(parents=True, exist_ok=True)

    out_txt = target_dir / f"{src.stem}.txt"
    out_json = target_dir / f"{src.stem}.json"
    out_wav = target_dir / f"{src.stem}.wav"

    t0 = time.time()
    try:
        log.info(f"[PRE-PROCESS AUDIO] Preprocessing {src.name} profile={profile}")
        preprocess_audio(src, tmp, profile=profile)
        log.info(f"[PRE-PROCESS AUDIO] Preprocessing done, starting Whisper inference…")
        text = transcribe_wavefile(state, tmp)
        log.info(f"[PRE-PROCESS AUDIO] Whisper inference done in {time.time()-t0:.1f}s")
        # insert cool algo here to determine if they were towed
        
        rms = get_rms(tmp)
        quality = score_transcript(text, dur, rms)
        now_iso = __import__("datetime").datetime.now().isoformat()

        feed = clean_subdir.lower()
        town = SOURCE_MAP.get(feed, "Unknown")
        state_name = "Massachusetts"
        dept = "fire" if "fd" in feed else "police" if "pd" in feed else ""

        transcription_model = MODEL_DIR.name
        hook_requested = detect_hook_request(text)
        log.info(f"hook_requested: {hook_requested}")

        meta: Dict[str, Any] = {
            "filename": out_wav.name,
            "transcript": text,
            "raw_transcript": text,
            "normalized_transcript": text,
            "duration": dur,
            "rms": rms,
            "timestamp": now_iso,
            "source": clean_subdir,
            "town": town,
            "state": state_name,
            "dept": dept,
            "profile": profile,
            "language": language,
            "classification": {
                "zero_shot": {},
                "location": None,
                "address_number": None,
                "address_street": None,
                "units": [],
                "tone_detected": False,
                "agency": None,
                "call_type": None,
                "urgency": None,
            },
            "intent_labeled": False,
            "intent_labeled_at": None,
            "edited_transcript": None,
            "transcription_quality": {
                "score": quality["score"],
                "needs_retry": quality["needs_retry"],
                "needs_review": quality["needs_review"],
                "reasons": quality["reasons"],
            },
            "profile_used": profile,
            "retry_profiles_tried": [profile],
            "transcription_engine": "faster-whisper",
            "transcription_model": transcription_model,
            "hook_request": hook_requested,
        }

        # Regex-based metadata enrichment (address, units, agency, tone, urgency)
        try:
            from nlp_zero_shot import enrich_meta_in_memory
            meta = enrich_meta_in_memory(meta)
        except Exception as e:
            meta.setdefault("warnings", []).append(f"enrichment_failed: {e}")

        if write_artifacts:
            out_txt.write_text(text, encoding="utf-8")
            out_json.write_text(json.dumps(meta, indent=2), encoding="utf-8")
            if not skip_wav_copy:
                shutil.copy(tmp, out_wav)

        # Optional DB insert
        db_result = None
        if insert_db and DB_IMPORT_OK and scanner_db is not None:
            log.info(f"[DB] Inserting record for {out_wav.name} (town={town}, dept={dept})...")
            try:
                if not getattr(scanner_db, "DB_PATH", None) or not scanner_db.DB_PATH.exists():
                    scanner_db.create_tables()

                db_meta = {
                    "town": town,
                    "state": state_name,
                    "dept": dept,
                    "category": clean_subdir,
                    "filename": out_wav.name,
                    "json_path": str(out_json),
                    "wav_path": str(out_wav),
                    "duration": dur,
                    "rms": rms,
                    "transcript": text,
                    "edited_transcript": None,
                    "timestamp": now_iso,
                    "reviewed": 0,
                    "play_count": 0,
                    "classification": meta.get("classification", {}),
                    "intent_labeled": int(meta.get("intent_labeled", 0)),
                    "intent_labeled_at": meta.get("intent_labeled_at"),
                    "extra": meta,
                    "raw_transcript": text,
                    "normalized_transcript": text,
                    "transcription_score": quality["score"],
                    "needs_retry": int(quality["needs_retry"]),
                    "needs_review": int(quality["needs_review"]),
                    "quality_reasons": quality["reasons"],
                    "profile_used": profile,
                    "retry_profiles_tried": [profile],
                    "transcription_engine": "faster-whisper",
                    "transcription_model": meta.get("transcription_model"),
                    "hook_request": hook_requested,
                    # Derived address fields from dictionary-backed extraction
                    "derived_address": meta.get("derived_address"),
                    "derived_street": meta.get("derived_street"),
                    "derived_addr_num": meta.get("derived_addr_num"),
                    "derived_town": meta.get("derived_town"),
                    "derived_lat": meta.get("derived_lat"),
                    "derived_lng": meta.get("derived_lng"),
                    "address_confidence": meta.get("address_confidence", "none"),
                }
                scanner_db.insert_call(db_meta)
                db_result = {"ok": True}
                log.info(f"[DB] Insert successful for {out_wav.name}")
            except Exception as e:
                db_result = {"ok": False, "error": str(e)}
                log.error(f"[DB] Insert failed for {out_wav.name}: {e}")

        # Optional delete source
        deleted = False
        if delete_source_raw:
            try:
                src.unlink()
                deleted = True
            except Exception as e:
                meta.setdefault("warnings", []).append(f"delete_failed: {e}")

        elapsed = time.time() - t0
        return {
            "ok": True,
            "text": text,
            "duration": dur,
            "rms": rms,
            "profile": profile,
            "language": language,
            "source_path": str(src),
            "clean_dir": str(clean_dir),
            "output_dir": str(target_dir),
            "artifacts": {
                "txt": str(out_txt) if write_artifacts else None,
                "json": str(out_json) if write_artifacts else None,
                "wav": str(out_wav) if write_artifacts and not skip_wav_copy else None,
            },
            "db": db_result,
            "deleted_source": deleted,
            "elapsed_s": round(elapsed, 3),
        }

    except subprocess.CalledProcessError as e:
        return {"ok": False, "error": "preprocess_failed", "details": str(e)}
    except Exception as e:
        return {"ok": False, "error": "transcribe_failed", "details": str(e)}
    finally:
        try:
            tmp.unlink(missing_ok=True)
        except Exception:
            pass
        gc.collect()
        try:
            torch.cuda.empty_cache()
        except Exception:
            pass


@mcp.tool()
def transcribe_file(
    ctx: Context,
    path: str,
    profile: str = "default",
    language: str = "en",  # kept for API compatibility; WhisperTokenizerFast decoding is language-agnostic here
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
        profile=profile,
        language=language,
        write_artifacts=write_artifacts,
        insert_db=insert_db,
        delete_source_raw=delete_source_raw,
        custom_output_dir=custom_output_dir,
        skip_wav_copy=skip_wav_copy,
    )


@mcp.tool()
def route_and_transcribe(
    ctx: Context,
    path: str,
    profile: str = "default",
    language: str = "en",
    auto_route: bool = True,
    model_key: str = "",
    write_artifacts: bool = True,
    insert_db: bool = True,
    delete_source_raw: bool = False,
    custom_output_dir: str = "",
    skip_wav_copy: bool = False,
) -> Dict[str, Any]:
    """
    Route a call to the appropriate Whisper model based on routing rules or an explicit model_key.

    Args:
      path: WAV path (must be under allowed roots)
      profile: preprocessing profile
      language: decoding language hint (for API compatibility)
      auto_route: when True, evaluate routing rules; otherwise use model_key or default
      model_key: optional explicit model key from the catalog
    """
    router = _get_router(ctx)
    if not router:
        return {"ok": False, "error": "router_unavailable"}

    src = Path(path).expanduser().resolve()
    if not _is_under_allowed_roots(src):
        return {"ok": False, "error": "path_not_allowed", "path": str(src)}
    if not src.exists():
        return {"ok": False, "error": "missing_file", "path": str(src)}

    feed_hint, _ = detect_category(src)
    duration = get_duration(src)

    chosen_model = model_key.strip() or router.default_key
    if auto_route:
        chosen_model = router.choose_model(path=src, feed=feed_hint, duration=duration)

    state = router.get_state(chosen_model)
    result = _transcribe_with_state(
        ctx,
        state,
        path=str(src),
        profile=profile,
        language=language,
        write_artifacts=write_artifacts,
        insert_db=insert_db,
        delete_source_raw=delete_source_raw,
        custom_output_dir=custom_output_dir,
        skip_wav_copy=skip_wav_copy,
    )

    result["model_key"] = chosen_model
    result["routing"] = {
        "auto_route": auto_route,
        "feed_hint": feed_hint,
        "duration": duration,
    }
    return result


@mcp.tool()
def infer_location(
    ctx: Context,
    transcript: str,
    town: str = "",
    feed: str = "",
    candidate_streets: Optional[list] = None,
    candidate_landmarks: Optional[list] = None,
    candidate_towns: Optional[list] = None,
    notes: str = "",
) -> Dict[str, Any]:
    """
    Run location inference on a transcript by calling the local inference service.

    Args:
      transcript: the call transcript text
      town: optional town hint
      feed: optional feed/category hint (e.g. mpd, frkfd)
      candidate_streets: optional list of street name candidates
      candidate_landmarks: optional list of landmark candidates
      candidate_towns: optional list of town candidates
      notes: optional freeform notes for the inference service
    """
    if not transcript.strip():
        return {"ok": False, "error": "transcript_empty"}

    result = call_location_inference_service(
        transcript=transcript,
        town=town or None,
        feed=feed or None,
        candidate_streets=candidate_streets,
        candidate_landmarks=candidate_landmarks,
        candidate_towns=candidate_towns,
        notes=notes or None,
    )

    out: Dict[str, Any] = {
        "ok": result["ok"],
        "transcript": transcript,
        "town": town or None,
        "feed": feed or None,
        "service": result,
    }
    if result["ok"] and isinstance(result.get("response"), dict):
        out["inferred_location"] = result["response"].get("inference")
    elif not result["ok"]:
        out["error"] = result.get("error")
    return out


@mcp.tool()
def infer_location_for_file(
    ctx: Context,
    path: str,
    candidate_streets: Optional[list] = None,
    candidate_landmarks: Optional[list] = None,
    candidate_towns: Optional[list] = None,
    notes: str = "",
    update_json: bool = False,
) -> Dict[str, Any]:
    """
    Load a call's transcript from its sidecar JSON and run location inference.

    Args:
      path: path to the .wav or .json file (must be under allowed roots)
      candidate_streets: optional street name hints
      candidate_landmarks: optional landmark hints
      candidate_towns: optional town hints
      notes: optional freeform notes for the inference service
      update_json: if True, write inference results back into the sidecar JSON
    """
    src = Path(path).expanduser()
    if not _is_under_allowed_roots(src):
        return {"ok": False, "error": "path_not_allowed", "path": str(src)}

    src = src.resolve()

    if src.suffix.lower() == ".wav":
        json_path = sidecar_json_for_audio(src)
    else:
        json_path = src

    if not json_path.exists():
        return {"ok": False, "error": "json_not_found", "json_path": str(json_path)}

    try:
        meta = json.loads(json_path.read_text(encoding="utf-8"))
    except Exception as e:
        return {"ok": False, "error": f"json_load_failed: {e}", "json_path": str(json_path)}

    # Pick best available transcript field
    transcript = ""
    transcript_field = None
    for field in ("normalized_transcript", "raw_transcript", "transcript"):
        val = meta.get(field, "")
        if val and val.strip():
            transcript = val.strip()
            transcript_field = field
            break

    if not transcript:
        return {"ok": False, "error": "no_transcript_in_json", "json_path": str(json_path)}

    town = meta.get("town") or None
    feed = meta.get("source") or meta.get("category") or None

    result = call_location_inference_service(
        transcript=transcript,
        town=town,
        feed=feed,
        candidate_streets=candidate_streets,
        candidate_landmarks=candidate_landmarks,
        candidate_towns=candidate_towns,
        notes=notes or None,
    )

    out: Dict[str, Any] = {
        "ok": result["ok"],
        "source_path": str(src),
        "json_path": str(json_path),
        "transcript_used": transcript,
        "transcript_field": transcript_field,
        "town": town,
        "feed": feed,
        "service": result,
    }

    if result["ok"] and isinstance(result.get("response"), dict):
        out["inferred_location"] = result["response"].get("inference")
    elif not result["ok"]:
        out["error"] = result.get("error")
        return out

    if update_json and result["ok"]:
        try:
            resp = result["response"]
            meta["location_inference"] = resp.get("inference")
            meta["location_inference_meta"] = {
                "model": resp.get("model"),
                "prompt_version": resp.get("prompt_version"),
                "updated_at": __import__("datetime").datetime.now().isoformat(),
            }
            json_path.write_text(json.dumps(meta, indent=2), encoding="utf-8")
            out["json_updated"] = True
        except Exception as e:
            out["json_updated"] = False
            out.setdefault("warnings", []).append(f"json_update_failed: {e}")

    return out


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
