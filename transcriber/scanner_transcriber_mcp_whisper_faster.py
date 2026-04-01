#!/usr/bin/env python3
"""
scanner_transcriber_mcp_whisper_faster.py

Long-lived MCP server that keeps a faster-whisper (CTranslate2) model loaded on
GPU and exposes tools:
- analyze_audio
- transcribe_file
- retranscribe_file

GPU-only policy:
- If CUDA is not available, this process FAILS FAST (no CPU fallback).

Model must be in CTranslate2 format.  Convert a HuggingFace checkpoint once:
  ct2-transformers-converter \\
      --model /path/to/hf_model \\
      --output_dir /path/to/ct2_model \\
      --copy_files tokenizer.json preprocessor_config.json \\
      --quantization float16

Run (HTTP / Streamable HTTP):
  python3 scanner_transcriber_mcp_whisper_faster.py \\
      --transport streamable-http --host 127.0.0.1 --port 8008

MCP endpoint (HTTP):
  http://127.0.0.1:8008/mcp
"""

from __future__ import annotations

import os
import re
import gc
import json
import time
import shutil
import logging
import argparse
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Optional, Tuple
from contextlib import asynccontextmanager

import torch

from faster_whisper import WhisperModel

from mcp.server.fastmcp import FastMCP, Context

from gpu_gate import GPUGate


# ==========================
# Logging
# ==========================
LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s | %(levelname)s | %(message)s",
)
log = logging.getLogger("scanner-mcp")
logging.getLogger("faster_whisper").setLevel(logging.WARNING)


# ==========================
# Config (env override)
# ==========================
ARCHIVE_BASE = Path(os.environ.get("ARCHIVE_BASE", "/home/ned/data/scanner_calls/scanner_archive")).expanduser().resolve()
TMP_DIR = Path(os.environ.get("SCANNER_TMP_DIR", "/tmp/scanner_tmp")).expanduser().resolve()

#MODEL_DIR = Path(os.environ.get("WHISPER_MODEL_DIR", "/home/ned/models/trained_whisper_medium_d022526")).expanduser().resolve()
MODEL_DIR = Path(os.environ.get("WHISPER_MODEL_DIR", "/home/ned/models/trained_whisper_medium_d022526_ct2_fp16")).expanduser().resolve()


MIN_DURATION = float(os.environ.get("MIN_DURATION", "2"))
RMS_THRESHOLD = float(os.environ.get("RMS_THRESHOLD", "0.001"))

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
    "hpd": "Hopedale",
    "hfd": "Hopedale",
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
    model: WhisperModel
    gate: GPUGate


def _require_cuda() -> None:
    if not torch.cuda.is_available():
        raise RuntimeError("CUDA is required (GPU-only). torch.cuda.is_available() is False.")
    # Optional: TF32 speedups on Ampere+
    try:
        torch.backends.cuda.matmul.allow_tf32 = True
        torch.backends.cudnn.allow_tf32 = True
    except Exception:
        pass


def load_whisper_model() -> WhisperState:
    _require_cuda()

    if not MODEL_DIR.exists():
        raise FileNotFoundError(f"WHISPER_MODEL_DIR not found: {MODEL_DIR}")

    log.info(f"Loading faster-whisper model from: {MODEL_DIR}")

    # faster-whisper handles GPU placement and fp16 internally via CTranslate2
    compute_type = os.environ.get("WHISPER_COMPUTE_TYPE", "float16")
    model = WhisperModel(
        str(MODEL_DIR),
        device="cuda",
        compute_type=compute_type,
    )

    free, total = torch.cuda.mem_get_info()
    log.info(f"CUDA memory free: {free/(1024**3):.2f} GB / {total/(1024**3):.2f} GB")
    log.info(f"faster-whisper model ready on CUDA ({compute_type})")

    return WhisperState(
        model=model,
        gate=GPUGate(),
    )


def transcribe_wavefile(
    state: WhisperState,
    wav_path: Path,
    *,
    num_beams: int = 4,
    language: str = "en",
) -> str:
    """
    Transcribe a preprocessed WAV using faster-whisper (CTranslate2, GPU-only).
    Serialized with a GPU lock.
    """
    with state.gate.acquire("whisper", timeout_s=120):
        segments, _ = state.model.transcribe(
            str(wav_path),
            beam_size=num_beams,
            language=language,
            condition_on_previous_text=False,
            vad_filter=False,  # we do our own RMS gating upstream
        )
        text = " ".join(seg.text.strip() for seg in segments).strip()
    return text


# ==========================
# Optional DB integration
# ==========================
def try_import_db() -> None:
    global DB_IMPORT_OK, scanner_db
    if not ENABLE_DB:
        return
    try:
        import scanner_db as _scanner_db
        scanner_db = _scanner_db
        DB_IMPORT_OK = True
        log.info("scanner_db import OK (DB inserts enabled).")
    except Exception as e:
        DB_IMPORT_OK = False
        log.warning(f"scanner_db import failed (DB inserts disabled): {e}")


# ==========================
# MCP server with lifespan
# ==========================
@asynccontextmanager
async def lifespan(_: FastMCP):
    """
    Startup/shutdown hook:
    - load Whisper model once (GPU-only)
    - import DB module if available
    """
    state = load_whisper_model()
    try_import_db()
    try:
        yield {"state": state}
    finally:
        # shutdown cleanup — CTranslate2 releases GPU memory on model deletion
        try:
            del state
        except Exception:
            pass
        gc.collect()


# Parse host/port early so the FastMCP constructor (which owns them in MCP 1.x)
# gets the right values before the server is bound.
_MCP_HOST = os.environ.get("MCP_HOST", "127.0.0.1")
_MCP_PORT = int(os.environ.get("MCP_PORT", "8000"))

mcp = FastMCP(
    "scanner-transcriber",
    json_response=True,
    lifespan=lifespan,
    host=_MCP_HOST,
    port=_MCP_PORT,
)


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
        text = transcribe_wavefile(state, tmp, language=language)

        rms = get_rms(tmp)
        now_iso = __import__("datetime").datetime.now().isoformat()

        feed = clean_subdir.lower()
        town = SOURCE_MAP.get(feed, "Unknown")
        state_name = "Massachusetts"
        dept = "fire" if "fd" in feed else "police" if "pd" in feed else ""

        meta: Dict[str, Any] = {
            "filename": out_wav.name,
            "transcript": text,
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
        }

        # Optional: in-memory labeler
        try:
            from nlp_zero_shot import classify_meta_in_memory
            meta = classify_meta_in_memory(meta, threshold=0.4)
        except Exception as e:
            meta.setdefault("warnings", []).append(f"labeler_failed: {e}")

        if write_artifacts:
            out_txt.write_text(text, encoding="utf-8")
            out_json.write_text(json.dumps(meta, indent=2), encoding="utf-8")
            shutil.copy(tmp, out_wav)

        # Optional DB insert
        db_result = None
        if insert_db and DB_IMPORT_OK and scanner_db is not None:
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
                }
                scanner_db.insert_call(db_meta)
                db_result = {"ok": True}
            except Exception as e:
                db_result = {"ok": False, "error": str(e)}

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
    # --host / --port override the env vars; update the FastMCP instance's settings directly
    ap.add_argument("--host", default=_MCP_HOST)
    ap.add_argument("--port", type=int, default=_MCP_PORT)
    args = ap.parse_args()

    # Allow CLI flags to override what was baked in at import time
    mcp.settings.host = args.host
    mcp.settings.port = args.port

    log.info(f"Starting MCP server transport={args.transport} host={args.host} port={args.port}")
    mcp.run(transport=args.transport)


if __name__ == "__main__":
    main()