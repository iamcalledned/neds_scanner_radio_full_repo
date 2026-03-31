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
from typing import Any, Dict, Optional, Tuple
from contextlib import asynccontextmanager

import torch
import torchaudio

from transformers import (
    WhisperForConditionalGeneration,
    WhisperFeatureExtractor,
    WhisperTokenizerFast,
    GenerationConfig,
)
from transformers.utils import logging as hf_logging

from mcp.server.fastmcp import FastMCP, Context

from gpu_gate import GPUGate


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

MODEL_DIR = Path(os.environ.get("WHISPER_MODEL_DIR", "/home/ned/models/trained_whisper_medium_d022526")).expanduser().resolve()

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
# Audio helpers
# ==========================
def get_duration(path: Path) -> float:
    try:
        r = subprocess.run(
            ["ffprobe", "-v", "error", "-show_entries", "format=duration",
             "-of", "default=noprint_wrappers=1:nokey=1", str(path)],
            stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True, check=False
        )
        return float((r.stdout or "").strip() or "0")
    except Exception:
        return 0.0


def get_rms(path: Path) -> float:
    """Compute RMS amplitude using SoX."""
    try:
        r = subprocess.run(
            ["sox", "-t", "wav", str(path), "-n", "stat"],
            stderr=subprocess.PIPE, stdout=subprocess.DEVNULL, text=True, check=False
        )
        for line in r.stderr.splitlines():
            if "RMS" in line and "amplitude" in line:
                return float(line.split(":")[1].strip())
    except Exception:
        pass
    return 0.0


def is_static(path: Path) -> bool:
    """Skip only true dead-air."""
    try:
        return get_rms(path) < RMS_THRESHOLD
    except Exception:
        return False


def preprocess_audio(inp: Path, outp: Path, profile: str = "default") -> None:
    """
    Preprocess using ffmpeg. Profiles let you tune filters without changing code.
    """
    profiles = {
        "default": "highpass=f=100,volume=5dB",
        "radio": "highpass=f=120,lowpass=f=3500,afftdn=nf=-25,volume=6dB",
        "static_fix": "highpass=f=150,lowpass=f=3200,afftdn=nf=-30,volume=7dB",
        "aggressive": "highpass=f=200,lowpass=f=3000,afftdn=nf=-35,acompressor=threshold=-20dB:ratio=4,volume=8dB",
    }
    af = profiles.get(profile, profiles["default"])

    subprocess.run(
        ["ffmpeg", "-y", "-i", str(inp), "-ac", "1", "-ar", "16000", "-af", af, str(outp)],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=True
    )


# ==========================
# Model state (GPU-only)
# ==========================
@dataclass
class WhisperState:
    feature_extractor: WhisperFeatureExtractor
    tokenizer: WhisperTokenizerFast
    model: WhisperForConditionalGeneration
    device: torch.device
    gate: GPUGate


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


def load_whisper_model() -> WhisperState:
    # Keep allocator config (your env can override)
    os.environ["PYTORCH_CUDA_ALLOC_CONF"] = os.environ.get(
        "PYTORCH_CUDA_ALLOC_CONF", "expandable_segments:True"
    )

    device = _require_cuda()

    if not MODEL_DIR.exists():
        raise FileNotFoundError(f"WHISPER_MODEL_DIR not found: {MODEL_DIR}")

    log.info(f"Loading Whisper model from: {MODEL_DIR}")

    # Load components explicitly (Transformers 5.x-friendly)
    feature_extractor = WhisperFeatureExtractor.from_pretrained(str(MODEL_DIR))
    tokenizer = WhisperTokenizerFast.from_pretrained(str(MODEL_DIR))
    model = WhisperForConditionalGeneration.from_pretrained(str(MODEL_DIR))

    # Ensure generation params live in generation_config, not config
    try:
        if hasattr(model, "generation_config") and model.generation_config is not None:
            model.generation_config.forced_decoder_ids = None
            model.generation_config.suppress_tokens = []
        else:
            model.generation_config = GenerationConfig(forced_decoder_ids=None, suppress_tokens=[])
    except Exception:
        pass

    # Move to GPU + fp16
    model.to(device)
    model.eval()
    model = model.half()

    free, total = torch.cuda.mem_get_info()
    log.info(f"CUDA memory free: {free/(1024**3):.2f} GB / {total/(1024**3):.2f} GB")
    log.info("Model ready on CUDA (fp16)")

    return WhisperState(
        feature_extractor=feature_extractor,
        tokenizer=tokenizer,
        model=model,
        device=device,
        gate=GPUGate(),
    )


def transcribe_wavefile(
    state: WhisperState,
    wav_path: Path,
    *,
    num_beams: int = 4,
    max_length: int = 448,
) -> str:
    """
    Transcribe a preprocessed WAV using the loaded model (GPU-only).
    Serialized with a GPU lock.
    """
    try:
        waveform, sr = torchaudio.load(str(wav_path))
    except Exception as e:
        log.warning(f"torchaudio failed to load {wav_path}: {e}")
        return ""

    if waveform.ndim == 2:
        waveform = waveform.mean(dim=0)
    waveform = waveform.squeeze()

    inputs = state.feature_extractor(
        waveform.numpy(),
        sampling_rate=sr,
        return_tensors="pt",
    )

    feats = inputs.input_features.to(state.device, dtype=torch.float16)

    gen_cfg = GenerationConfig.from_model_config(state.model.config)
    gen_cfg.forced_decoder_ids = None
    gen_cfg.suppress_tokens = []

    with state.gate.acquire("whisper", timeout_s=120):
        with torch.inference_mode():
            predicted = state.model.generate(
                feats,
                generation_config=gen_cfg,
                do_sample=False,
                max_length=max_length,
                num_beams=num_beams,
                length_penalty=1.0,
                repetition_penalty=1.2,
                early_stopping=True,
            )

    text = state.tokenizer.batch_decode(predicted, skip_special_tokens=True)[0].strip()
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


def call_location_inference_service(
    transcript: str,
    town: Optional[str] = None,
    feed: Optional[str] = None,
    candidate_streets: Optional[list] = None,
    candidate_landmarks: Optional[list] = None,
    candidate_towns: Optional[list] = None,
    notes: Optional[str] = None,
) -> Dict[str, Any]:
    """POST to the local location inference service. Never raises; failures return ok=False."""
    url = f"{LOCATION_INFER_BASE_URL}/infer/location"
    payload = {
        "transcript": transcript,
        "town": town or None,
        "feed": feed or None,
        "candidate_streets": candidate_streets or [],
        "candidate_landmarks": candidate_landmarks or [],
        "candidate_towns": candidate_towns or [],
        "notes": notes or None,
    }
    body = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(
        url,
        data=body,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=LOCATION_INFER_TIMEOUT_S) as resp:
            raw = resp.read().decode("utf-8")
            try:
                data = json.loads(raw)
            except json.JSONDecodeError as e:
                return {"ok": False, "service_url": url, "request": payload, "response": None,
                        "error": f"bad_json: {e}"}
            return {"ok": True, "service_url": url, "request": payload, "response": data}
    except urllib.error.HTTPError as e:
        body_text = ""
        try:
            body_text = e.read().decode("utf-8")
        except Exception:
            pass
        return {"ok": False, "service_url": url, "request": payload, "response": None,
                "error": f"http_{e.code}: {body_text}"}
    except urllib.error.URLError as e:
        return {"ok": False, "service_url": url, "request": payload, "response": None,
                "error": f"connection_error: {e.reason}"}
    except TimeoutError:
        return {"ok": False, "service_url": url, "request": payload, "response": None,
                "error": "timeout"}
    except Exception as e:
        return {"ok": False, "service_url": url, "request": payload, "response": None,
                "error": str(e)}


# ==========================
# Transcript quality scorer
# ==========================

# Phrases that Whisper commonly hallucinates on weak scanner audio.
_FALLBACK_PHRASES = {
    "radio check",
    "copy that",
    "stand by",
    "go ahead",
    "clear",
    "received",
    "10 4",
    "10-4",
}


def _normalize_phrase(text: str) -> str:
    """Lowercase, strip, collapse internal whitespace."""
    return " ".join(text.lower().split())


def _has_repeated_run(tokens: list[str], n: int = 3) -> bool:
    """Return True if any token appears n times in a row (case-insensitive)."""
    lower = [t.lower() for t in tokens]
    for i in range(len(lower) - (n - 1)):
        if len(set(lower[i:i + n])) == 1:
            return True
    return False


def score_transcript(text: str, duration: float, rms: float) -> dict:
    """
    Rule-based transcript quality scorer. Returns a score (0.0–1.0) and flags.
    Version 1.1: adds RMS-aware and fallback-phrase heuristics.
    """
    if not text or not text.strip():
        return {
            "score": 0.0,
            "needs_review": True,
            "needs_retry": True,
            "reasons": ["empty"],
        }

    score = 1.0
    reasons: list[str] = []
    force_review_reasons = {
        "generic_fallback_phrase",
        "near_static_but_has_transcript",
        "suspiciously_short_for_duration",
    }

    tokens = text.split()
    word_count = len(tokens)
    normalized = _normalize_phrase(text)

    # ── Existing rules ────────────────────────────────────────────────────────

    # Too short for the audio duration (original rule, kept as-is)
    if duration >= 4.0 and word_count <= 2:
        score -= 0.4
        reasons.append("too_short_for_duration")

    # Low word count regardless of duration
    if word_count <= 3:
        score -= 0.2
        reasons.append("low_word_count")

    # Repeated tokens (same word 3+ times in a row)
    if _has_repeated_run(tokens, 3):
        score -= 0.3
        reasons.append("repeated_tokens")

    # Too many single-character tokens (excluding "a" and "i")
    single_char = [t for t in tokens if len(t) == 1 and t.lower() not in ("a", "i")]
    if word_count > 0 and len(single_char) / word_count > 0.4:
        score -= 0.3
        reasons.append("fragmented_text")

    # Junk-looking text: high ratio of non-alphabetic characters
    alpha_chars = sum(1 for c in text if c.isalpha())
    total_chars = len(text.replace(" ", ""))
    if total_chars > 0 and alpha_chars / total_chars < 0.6:
        score -= 0.3
        reasons.append("junk_text")

    # ── New rules ─────────────────────────────────────────────────────────────

    # A. Suspiciously short clean transcript for non-trivial duration
    if duration >= 3.5 and word_count <= 2:
        score -= 0.35
        reasons.append("suspiciously_short_for_duration")

    if duration >= 5.0 and word_count <= 3:
        score -= 0.20
        reasons.append("very_low_density_transcript")

    # B. Very low word density
    if duration > 0:
        words_per_second = word_count / duration
        if duration >= 4.0 and words_per_second < 0.7:
            score -= 0.20
            reasons.append("low_word_density")

    # C. Suspicious generic fallback phrases
    if normalized in _FALLBACK_PHRASES:
        score -= 0.35
        reasons.append("generic_fallback_phrase")
    elif word_count <= 3 and any(phrase in normalized for phrase in _FALLBACK_PHRASES):
        score -= 0.20
        reasons.append("contains_generic_fallback_phrase")

    # D. Low RMS + clean short transcript
    if rms < 0.003 and word_count <= 3 and duration >= 2.5:
        score -= 0.30
        reasons.append("low_rms_short_transcript")

    if rms < 0.002 and word_count <= 4:
        score -= 0.20
        reasons.append("near_static_but_has_transcript")

    # ── Final flags ───────────────────────────────────────────────────────────

    score = max(0.0, min(1.0, score))

    needs_retry = score < 0.5
    needs_review = score < 0.6 or bool(set(reasons) & force_review_reasons)

    return {
        "score": round(score, 3),
        "needs_review": needs_review,
        "needs_retry": needs_retry,
        "reasons": reasons,
    }


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
    - load Whisper model once (GPU-only)
    - import DB module if available
    - start periodic WAL checkpoint task
    """
    import asyncio
    state = load_whisper_model()
    try_import_db()

    # Start background WAL checkpointer
    wal_task = asyncio.create_task(_wal_checkpoint_loop())

    try:
        yield {"state": state}
    finally:
        # shutdown cleanup
        wal_task.cancel()
        try:
            del state
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
    return data["state"]


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
    p = Path(path).expanduser()
    if not _is_under_allowed_roots(p):
        return {"ok": False, "error": "path_not_allowed", "path": str(p)}

    p = p.resolve()
    if not p.exists():
        return {"ok": False, "error": "missing_file", "path": str(p)}

    dur = get_duration(p)
    rms = get_rms(p) if dur > 0 else 0.0
    static = rms < RMS_THRESHOLD

    return {
        "ok": True,
        "path": str(p),
        "duration": dur,
        "rms": rms,
        "static": static,
        "min_duration": MIN_DURATION,
        "rms_threshold": RMS_THRESHOLD,
    }


@mcp.tool()
def transcribe_file(
    ctx: Context,
    path: str,
    profile: str = "default",
    language: str = "en",  # kept for API compatibility; WhisperTokenizerFast decoding is language-agnostic here
    write_artifacts: bool = True,
    insert_db: bool = True,
    delete_source_raw: bool = False,
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

    out_txt = clean_dir / f"{src.stem}.txt"
    out_json = clean_dir / f"{src.stem}.json"
    out_wav = clean_dir / f"{src.stem}.wav"

    t0 = time.time()
    try:
        preprocess_audio(src, tmp, profile=profile)
        text = transcribe_wavefile(state, tmp)
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
            "artifacts": {
                "txt": str(out_txt) if write_artifacts else None,
                "json": str(out_json) if write_artifacts else None,
                "wav": str(out_wav) if write_artifacts else None,
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

    # FastMCP's run signature can vary by SDK version; try host/port kwargs, fall back if not supported.
    try:
        mcp.run(transport=args.transport, host=args.host, port=args.port)
    except TypeError:
        # Older versions ignore host/port in run; in that case, rely on whatever defaults it has.
        # But at least we tried.
        log.warning("FastMCP.run() did not accept host/port args in this SDK version. Using default bind settings.")
        mcp.run(transport=args.transport)


if __name__ == "__main__":
    main()