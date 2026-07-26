#!/usr/bin/env python3
"""
compare_models.py — Compare two or three fine-tuned Whisper models against
                     ground-truth validation transcripts.

What it does:
  1. Loads WAV files from a validation folder or the scanner DB.
     Ground truth = edited_transcript from each sample (falls back to transcript).
  2. Runs every WAV through Model A, Model B, and optionally Model C.
  3. Computes WER + CER per-sample and aggregate for each model.
  4. Prints a terminal report.
  5. Writes a JSON results file.
  6. Writes a self-contained HTML report with side-by-side diff colouring.
  7. Writes a plain-text summary report.

Usage:
  python compare_models.py                         # uses compare_config.json
  python compare_models.py --config my_cfg.json
  python compare_models.py --model-a baseline_model_april --model-b new_trained_whisper_v1 --model-c neds_whisper_v3
  python compare_models.py --limit 10              # only first N samples (quick test)
  python compare_models.py --no-html               # skip HTML report
  python compare_models.py --beam 5                # override beam_size for all models
"""

import argparse
import asyncio
import html as html_mod
import json
import os
import sys
import time
import warnings
from typing import Any, Dict, Optional
from datetime import datetime
from pathlib import Path

# Force UTF-8 output on Windows (avoids CP1252 UnicodeEncodeError)
if sys.stdout.encoding and sys.stdout.encoding.lower() != "utf-8":
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")

os.environ.setdefault("PYTORCH_CUDA_ALLOC_CONF", "expandable_segments:True")

import torch

warnings.filterwarnings("ignore", category=UserWarning)
warnings.filterwarnings("ignore", message=".*attention_mask.*")
warnings.filterwarnings("ignore", message=".*SuppressTokens.*")
warnings.filterwarnings("ignore", message=".*weights_only.*")

# ─────────────────────────────────────────────────────────────────────────────
#  Helpers
# ─────────────────────────────────────────────────────────────────────────────

def _require(pkg: str, install: str = None):
    import importlib
    try:
        return importlib.import_module(pkg)
    except ImportError:
        hint = install or pkg
        sys.exit(f"\n  [!] Missing package: {pkg}\n    Install: pip install {hint}\n")


def banner(text: str, char: str = "=", width: int = 66):
    print(f"\n{char * width}\n  {text}\n{char * width}")


def section(text: str):
    print(f"\n{'─' * 60}\n  {text}\n{'─' * 60}".replace("─", "-"))


def ts():
    return datetime.now().strftime("%H:%M:%S")


def info(msg: str):
    print(f"  [{ts()}] {msg}")


def warn(msg: str):
    print(f"  [{ts()}] WARNING: {msg}")


# Keep defaults aligned with transcriber/scanner_transcriber_mcp.py
TRANSCRIBE_DEFAULTS: Dict[str, Any] = {
    "task": "transcribe",
    "language": "en",
    "beam_size": 3,
    "word_timestamps": False,
    "condition_on_previous_text": False,
    "vad_filter": False,
    "initial_prompt": None,
    "temperature": 0.0,
    "best_of": None,
    "patience": None,
    "compression_ratio_threshold": None,
    "log_prob_threshold": None,
    "no_speech_threshold": None,
}

TRANSCRIBE_KEYS = tuple(TRANSCRIBE_DEFAULTS.keys())


def _normalize_temperature(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, list):
        out = []
        for item in value:
            if isinstance(item, (int, float)):
                out.append(float(item))
        return out or None
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
    task: Optional[str],
    language: Optional[str],
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


def _resolve_model_ref(model_ref: str, model_base_dir: Path) -> str:
    raw = (model_ref or "").strip()
    if not raw:
        return ""

    p = Path(raw).expanduser()
    if p.is_absolute() or raw.startswith(".") or raw.startswith("~"):
        return str(p.resolve())

    if "/" not in raw and "\\" not in raw:
        return str((model_base_dir / p).resolve())

    candidate = (model_base_dir / p).expanduser()
    if candidate.exists():
        return str(candidate.resolve())

    # Keep hub-style IDs (org/model-name) untouched.
    return raw


def _load_model_catalog(catalog_path: Optional[Path], model_base_dir: Path) -> tuple[dict, str]:
    catalog = {}
    default_model_key = ""
    if not catalog_path or not catalog_path.exists():
        return catalog, default_model_key

    try:
        raw = json.loads(catalog_path.read_text(encoding="utf-8"))
    except Exception as e:
        warn(f"Could not parse model catalog {catalog_path}: {e}")
        return catalog, default_model_key

    if not isinstance(raw, dict):
        return catalog, default_model_key

    default_model_key = (raw.get("default_model") or "").strip()
    models = raw.get("models", {}) if isinstance(raw.get("models"), dict) else {}
    for key, val in models.items():
        if not isinstance(val, dict):
            continue
        model_ref = val.get("model") or val.get("path") or val.get("model_path") or val.get("dir")
        if not model_ref:
            continue

        catalog[key] = {
            "model": _resolve_model_ref(str(model_ref), model_base_dir),
            "compute_type": val.get("compute_type", "float16"),
            "device": str(val.get("device", "cuda")).strip().lower() or "cuda",
            "transcribe": _merged_transcribe_settings(
                val.get("transcribe") if isinstance(val.get("transcribe"), dict) else val
            ),
        }

    return catalog, default_model_key


def _find_model_catalog_path(base_dir: Path, cfg: dict, explicit: Optional[str]) -> Optional[Path]:
    candidates = []
    if explicit:
        candidates.append(Path(explicit).expanduser())
    if cfg.get("model_catalog"):
        candidates.append(Path(cfg["model_catalog"]).expanduser())
    if os.environ.get("MODEL_CATALOG_FILE"):
        candidates.append(Path(os.environ["MODEL_CATALOG_FILE"]).expanduser())

    # Common sibling layout used in this workspace.
    candidates.append(Path("/home/ned/Documents/neds_scanner_radio_full_pipeline_with_git/transcriber/model_catalog.json"))

    # Optional local copy near compare script.
    candidates.append((base_dir / "model_catalog.json"))

    for candidate in candidates:
        try:
            resolved = candidate.resolve()
        except Exception:
            continue
        if resolved.exists():
            return resolved

    return None


def _resolve_compare_model(
    model_ref: str,
    *,
    catalog: dict,
    fallback_compute_type: str,
    fallback_device: str,
    fallback_settings: Dict[str, Any],
    model_base_dir: Path,
) -> Dict[str, Any]:
    if model_ref in catalog:
        profile = dict(catalog[model_ref])
        profile["key"] = model_ref
        return profile

    return {
        "key": "",
        "model": _resolve_model_ref(model_ref, model_base_dir),
        "compute_type": fallback_compute_type,
        "device": fallback_device,
        "transcribe": _merged_transcribe_settings(fallback_settings),
    }


# ─────────────────────────────────────────────────────────────────────────────
#  Load validation samples  (ground-truth)
# ─────────────────────────────────────────────────────────────────────────────

def load_validation_samples(folder: Path, limit: int = None) -> list[dict]:
    """
    Read every (*.wav, *.json) pair in the validation folder.
    Ground truth = edited_transcript (falls back to transcript).
    Returns a list of dicts: {wav, ref, id, town, dept, duration}
    """
    json_files = sorted(f for f in folder.glob("*.json") if f.name != "manifest.json")

    if limit:
        json_files = json_files[:limit]

    samples = []
    skipped = 0
    for jf in json_files:
        try:
            meta = json.loads(jf.read_text(encoding="utf-8"))
        except Exception as e:
            warn(f"Bad JSON {jf.name}: {e}")
            skipped += 1
            continue

        ref = (meta.get("edited_transcript") or meta.get("transcript") or "").strip()
        if not ref:
            warn(f"No transcript in {jf.name} — skipped")
            skipped += 1
            continue

        wav = jf.with_suffix(".wav")
        if not wav.exists():
            warn(f"No WAV for {jf.name} — skipped")
            skipped += 1
            continue

        samples.append({
            "wav":      wav,
            "ref":      ref,
            "id":       meta.get("id", jf.stem),
            "stem":     jf.stem,
            "town":     meta.get("town", ""),
            "dept":     meta.get("dept", ""),
            "duration": meta.get("duration", 0.0),
        })

    info(f"Loaded {len(samples)} validation samples  (skipped {skipped})")
    return samples


def load_validation_samples_from_db(
    db_path: Path,
    *,
    since: str = "",
    until: str = "",
    limit: int = 5000,
    eval_only: bool = True,
    limit_override: int = None,
    min_duration: float = 2.0,
    use_transcript: bool = False,
) -> list[dict]:
    """
    Pull validation samples directly from scanner_calls.db instead of a folder.

    eval_only=True  (default) — only rows with save_for_eval=1 OR freeze_for_testing=1
                                  (the held-out gold-standard set)
    eval_only=False            — any row with a non-empty edited_transcript
                                  (same pool as step1 training export)

    Returns the same dict shape as load_validation_samples():
        {wav, ref, id, stem, town, dept, duration}
    Only rows whose wav_path exists on disk are included.
    """
    import sqlite3

    if not db_path.exists():
        warn(f"DB not found: {db_path}")
        return []

    uri = f"file:{db_path}?mode=ro&cache=shared"
    conn = sqlite3.connect(uri, uri=True, timeout=30)
    conn.row_factory = sqlite3.Row

    # Patch since/until to cover the full day if only a date is provided
    patched_since = since.strip()
    patched_until = until.strip()
    if use_transcript:
        if patched_since and len(patched_since) == 10 and 'T' not in patched_since:
            patched_since = patched_since + 'T00:00:00'
        if patched_until and len(patched_until) == 10 and 'T' not in patched_until:
            patched_until = patched_until + 'T23:59:59.999999'
    # else: leave as is

    params = []
    extra = ""
    if patched_since:
        extra += " AND timestamp >= ?"
        params.append(patched_since)
    if patched_until:
        extra += " AND timestamp <= ?"
        params.append(patched_until)
    if min_duration and min_duration > 0:
        extra += " AND duration >= ?"
        params.append(float(min_duration))

    if use_transcript:
        where = """
            WHERE transcript IS NOT NULL
            AND length(trim(transcript)) > 0
        """
    elif eval_only:
        where = """
            WHERE edited_transcript IS NOT NULL
            AND length(trim(edited_transcript)) > 0
            AND (save_for_eval = 1 OR freeze_for_testing = 1)
        """
    else:
        where = """
            WHERE edited_transcript IS NOT NULL
            AND length(trim(edited_transcript)) > 0
            AND save_for_eval IS NOT 1
            AND freeze_for_testing IS NOT 1
        """

    row_limit = limit_override if limit_override is not None else limit
    query = f"SELECT * FROM calls {where} {extra} ORDER BY timestamp DESC LIMIT ?"
    params.append(row_limit)

    # Print the final SQL query and parameters for debugging
    print(f"\n  [DEBUG] SQL QUERY: {query}")
    print(f"  [DEBUG] SQL PARAMS: {params}\n")
    rows = conn.execute(query, params).fetchall()
    conn.close()
    info(f"  → {len(rows)} rows returned from DB")

    samples = []
    skipped = 0
    for row in rows:
        if use_transcript:
            ref = (row["transcript"] or "").strip()
        else:
            ref = (row["edited_transcript"] or row["transcript"] or "").strip()
        if not ref:
            skipped += 1
            continue

        wav_path = Path(row["wav_path"]) if row["wav_path"] else None
        if not wav_path or not wav_path.exists():
            warn(f"WAV missing for id={row['id']} {row['filename']} — skipped")
            skipped += 1
            continue

        stem = Path(row["filename"]).stem if row["filename"] else f"call_{row['id']}"
        samples.append({
            "wav":      wav_path,
            "ref":      ref,
            "id":       row["id"],
            "stem":     stem,
            "town":     row["town"] or "",
            "dept":     row["dept"] or "",
            "duration": row["duration"] or 0.0,
        })

    info(f"Loaded {len(samples)} DB samples  (skipped {skipped})")
    return samples


def _is_ct2_model(path: Path) -> bool:
    """True if the folder looks like a CTranslate2 model (has model.bin)."""
    return (path / "model.bin").exists()


def _ensure_ct2(model_path: Path, compute_type: str, label: str) -> Path:
    """
    If model_path is a HuggingFace-format model, convert it to CTranslate2
    in a sibling folder (same path + '_ct2_cmp').  Returns the ct2 path.
    If it's already ct2, return as-is.
    """
    if _is_ct2_model(model_path):
        info(f"[{label}] Already CTranslate2 format — skipping conversion")
        return model_path

    ct2_path = model_path.parent / (model_path.name + "_ct2_cmp")
    if _is_ct2_model(ct2_path):
        info(f"[{label}] Using cached ct2 conversion at: {ct2_path}")
        return ct2_path

    info(f"[{label}] Converting HF model to CTranslate2 (this takes ~1 min) ...")
    info(f"[{label}]   src : {model_path}")
    info(f"[{label}]   dst : {ct2_path}")
    info(f"[{label}]   quantization : {compute_type}")

    # Map compute_type to ct2-transformers-converter --quantization arg
    quant_map = {
        "float16": "float16",
        "int8":    "int8",
        "int8_float16": "int8",
        "float32": "float32",
    }
    quant = quant_map.get(compute_type, "float16")

    try:
        import subprocess
        cmd = [
            "ct2-transformers-converter",
            "--model",       str(model_path),
            "--output_dir",  str(ct2_path),
            "--quantization", quant,
            "--copy_files",
            "tokenizer.json", "preprocessor_config.json", "config.json",
            "tokenizer_config.json", "special_tokens_map.json",
            "vocab.json", "normalizer.json", "generation_config.json",
            "added_tokens.json",
        ]
        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode != 0:
            print(f"  [!] Conversion failed:\n{result.stderr}")
            sys.exit(1)
        info(f"[{label}] Conversion complete")
    except FileNotFoundError:
        print("\n  [!] ct2-transformers-converter not found.")
        print("      Install: pip install ctranslate2")
        sys.exit(1)

    return ct2_path


# ─────────────────────────────────────────────────────────────────────────────
#  Transcribe all samples with one model  (faster-whisper / CTranslate2)
# ─────────────────────────────────────────────────────────────────────────────

def transcribe_all(model_path: str, samples: list[dict],
                   device: str, label: str,
                   compute_type: str = "float16",
                   transcribe_kwargs: Optional[Dict[str, Any]] = None) -> list[str]:
    """
    Load a faster-whisper (CTranslate2) model from model_path and transcribe
    every sample WAV.  If model_path is a HF-format model it will be
    auto-converted first.  Returns transcripts in the same order as samples.
    """
    WhisperModel = _require("faster_whisper", "faster-whisper").WhisperModel

    mp = Path(model_path)

    # Handle HuggingFace Hub IDs (e.g. "openai/whisper-medium") — pass straight
    # through to faster-whisper which can download them natively.
    is_hub_id = not mp.exists() and "/" in model_path and not mp.is_absolute()

    if is_hub_id:
        ct2_path_str = model_path   # faster-whisper downloads from Hub
        info(f"[{label}] Using HuggingFace Hub model: {model_path}")
    else:
        ct2_path_str = str(_ensure_ct2(mp.resolve(), compute_type, label))

    # Map device string to faster-whisper device arg
    fw_device = "cuda" if device == "cuda" else "cpu"
    # On CPU, float16 isn't supported — fall back to int8
    if fw_device == "cpu" and compute_type in ("float16", "int8_float16"):
        info(f"[{label}] CPU detected — switching compute_type to int8")
        compute_type = "int8"

    info(f"[{label}] Loading faster-whisper model ({compute_type}) ...")
    try:
        model = WhisperModel(ct2_path_str, device=fw_device, compute_type=compute_type)
    except Exception as e:
        import traceback
        print(f"\n  [!] Failed to load model {label} from: {ct2_path_str}")
        traceback.print_exc()
        sys.exit(1)

    if torch.cuda.is_available() and fw_device == "cuda":
        free  = torch.cuda.mem_get_info()[0] / 1024**3
        total = torch.cuda.mem_get_info()[1] / 1024**3
        info(f"[{label}] Model on GPU — {free:.1f} GB free / {total:.0f} GB total")
    else:
        info(f"[{label}] Running on CPU")

    decode_kwargs = dict(transcribe_kwargs or {})
    if not decode_kwargs:
        decode_kwargs = _build_transcribe_kwargs(
            task="transcribe",
            language="en",
            profile_settings=None,
        )
    info(
        f"[{label}] decode settings: "
        f"beam_size={decode_kwargs.get('beam_size')} "
        f"vad_filter={decode_kwargs.get('vad_filter')} "
        f"language={decode_kwargs.get('language')} "
        f"task={decode_kwargs.get('task')} "
        f"initial_prompt_set={bool(decode_kwargs.get('initial_prompt'))}"
    )

    results = []
    t0 = time.time()

    for i, s in enumerate(samples):
        try:
            segments, _info = model.transcribe(str(s["wav"]), **decode_kwargs)
            text = " ".join(seg.text.strip() for seg in segments).strip()
            results.append(text)
        except Exception as e:
            warn(f"[{label}] Inference failed {s['stem']}: {e}")
            results.append("")

        # Progress
        done    = i + 1
        pct     = done / len(samples) * 100
        elapsed = time.time() - t0
        eta     = (elapsed / done) * (len(samples) - done) if done else 0
        print(f"\r  [{label}] {done}/{len(samples)}  ({pct:.0f}%)  "
              f"elapsed {elapsed:.0f}s  ETA {eta:.0f}s  ", end="", flush=True)

    print()  # newline after progress bar

    elapsed = time.time() - t0
    info(f"[{label}] Done — {elapsed:.1f}s total  ({elapsed/len(samples):.2f}s/sample)")

    # Release model
    del model
    if torch.cuda.is_available():
        torch.cuda.empty_cache()

    return results


async def _transcribe_all_via_mcp_async(
    *,
    mcp_url: str,
    model_key: str,
    samples: list[dict],
    label: str,
    profile: str,
    language: str,
) -> list[str]:
    ClientSession = _require("mcp.client.session", "mcp").ClientSession
    streamablehttp_client = _require("mcp.client.streamable_http", "mcp").streamablehttp_client

    results = []
    t0 = time.time()
    info(f"[{label}] Using MCP route_and_transcribe at: {mcp_url}")
    info(f"[{label}] MCP model_key={model_key} profile={profile} language={language}")

    async with streamablehttp_client(mcp_url) as streams:
        read, write, *_ = streams
        async with ClientSession(read, write) as session:
            await session.initialize()

            for i, s in enumerate(samples):
                try:
                    resp = await session.call_tool(
                        "route_and_transcribe",
                        {
                            "path": str(s["wav"]),
                            "profile": profile,
                            "language": language,
                            "auto_route": False,
                            "model_key": model_key,
                            # Keep comparison clean: no JSON/TXT/WAV outputs and no DB writes.
                            "write_artifacts": False,
                            "insert_db": False,
                            "delete_source_raw": False,
                            "custom_output_dir": "",
                            "skip_wav_copy": True,
                        },
                    )
                    structured = getattr(resp, "structuredContent", None) or {}
                    payload = structured.get("result") if isinstance(structured, dict) else None

                    # On first sample, dump the raw response shape for debugging
                    if i == 0:
                        info(f"[{label}] DEBUG first MCP response:")
                        info(f"  resp type    : {type(resp).__name__}")
                        info(f"  structuredContent: {str(structured)[:300]}")
                        info(f"  payload      : {str(payload)[:300]}")
                        # Also check .content (text content blocks)
                        content_blocks = getattr(resp, 'content', [])
                        for cb in (content_blocks or [])[:2]:
                            info(f"  content block: type={getattr(cb,'type',None)}  text={str(getattr(cb,'text',''))[:200]}")

                    if payload and payload.get("ok"):
                        text = (payload.get("text") or "").strip()
                        results.append(text)
                    elif payload and "transcript" in payload:
                        # Alternative response shape: {transcript: "...", ...}
                        text = (payload.get("transcript") or "").strip()
                        results.append(text)
                    else:
                        # Try to extract text from .content blocks (tool result format)
                        content_blocks = getattr(resp, 'content', []) or []
                        raw_text = ""
                        for cb in content_blocks:
                            t = getattr(cb, 'text', '') or ''
                            if t.strip():
                                raw_text = t.strip()
                                break
                        if raw_text:
                            # Try to parse as JSON first
                            try:
                                parsed = json.loads(raw_text)
                                text = (parsed.get("text") or parsed.get("transcript") or "").strip()
                                if not text and parsed.get("ok") is False:
                                    warn(f"[{label}] MCP returned ok=false {s['stem']}: {raw_text[:150]}")
                                    results.append("")
                                    continue
                            except (json.JSONDecodeError, AttributeError):
                                text = raw_text
                            results.append(text)
                        else:
                            warn(f"[{label}] MCP failed {s['stem']}: {payload}")
                            results.append("")
                except Exception as e:
                    warn(f"[{label}] MCP exception {s['stem']}: {e}")
                    results.append("")

                done = i + 1
                pct = done / len(samples) * 100
                elapsed = time.time() - t0
                eta = (elapsed / done) * (len(samples) - done) if done else 0
                print(f"\r  [{label}] {done}/{len(samples)}  ({pct:.0f}%)  "
                      f"elapsed {elapsed:.0f}s  ETA {eta:.0f}s  ", end="", flush=True)

    print()
    elapsed = time.time() - t0
    info(f"[{label}] MCP done — {elapsed:.1f}s total  ({elapsed/len(samples):.2f}s/sample)")
    return results


def transcribe_all_via_mcp(
    *,
    mcp_url: str,
    model_key: str,
    samples: list[dict],
    label: str,
    profile: str,
    language: str,
) -> list[str]:
    return asyncio.run(
        _transcribe_all_via_mcp_async(
            mcp_url=mcp_url,
            model_key=model_key,
            samples=samples,
            label=label,
            profile=profile,
            language=language,
        )
    )


# ─────────────────────────────────────────────────────────────────────────────
#  Metrics — WER and CER per-sample and aggregate
# ─────────────────────────────────────────────────────────────────────────────

def compute_wer_cer(predictions: list[str], references: list[str], evaluate_mod) -> dict:
    """Returns aggregate WER, CER, and per-sample list."""
    wer_metric = evaluate_mod.load("wer")
    cer_metric = evaluate_mod.load("cer")

    # Filter out empty predictions (failed transcriptions)
    valid = [(p, r) for p, r in zip(predictions, references) if p and r]
    if not valid:
        return {"wer": None, "cer": None, "per_sample": []}

    preds, refs = zip(*valid)
    agg_wer = wer_metric.compute(predictions=list(preds), references=list(refs))
    agg_cer = cer_metric.compute(predictions=list(preds), references=list(refs))

    # Per-sample
    per_sample = []
    for p, r in zip(predictions, references):
        if not p or not r:
            per_sample.append({"wer": None, "cer": None})
        else:
            try:
                s_wer = wer_metric.compute(predictions=[p], references=[r])
                s_cer = cer_metric.compute(predictions=[p], references=[r])
                per_sample.append({"wer": round(s_wer, 4), "cer": round(s_cer, 4)})
            except Exception:
                per_sample.append({"wer": None, "cer": None})

    return {
        "wer":        round(agg_wer, 4),
        "cer":        round(agg_cer, 4),
        "per_sample": per_sample,
    }


# ─────────────────────────────────────────────────────────────────────────────
#  Reporting helpers
# ─────────────────────────────────────────────────────────────────────────────

_TRUNC = 90   # character width for transcript display


def _trunc(s: str, n: int = _TRUNC) -> str:
    return (s[:n] + "...") if len(s) > n else s


def _wer_bar(wer: float, width: int = 20) -> str:
    """ASCII bar representing WER — full bar = 100% WER."""
    if wer is None:
        return "  [n/a                ]"
    filled = min(width, int(wer * width))
    return f"  [{'#' * filled}{'-' * (width - filled)}]  {wer*100:5.1f}%"


def _fmt_pct(value: Optional[float], digits: int = 2) -> str:
    if value is None:
        return "n/a"
    return f"{value * 100:.{digits}f}%"


def _sample_metric(model_run: dict, index: int, key: str) -> Optional[float]:
    per_sample = model_run.get("metrics", {}).get("per_sample", [])
    if index >= len(per_sample):
        return None
    return per_sample[index].get(key)


def _sort_metric(value: Optional[float]) -> float:
    return float("inf") if value is None else value


def _scoreboard(samples: list[dict], model_runs: list[dict], epsilon: float = 0.01) -> dict:
    win_counts = [0] * len(model_runs)
    sample_winners: list[list[int]] = []
    tie_rows = 0
    error_rows = 0

    for sample_idx in range(len(samples)):
        valid = []
        for model_idx, model_run in enumerate(model_runs):
            wer = _sample_metric(model_run, sample_idx, "wer")
            if wer is not None:
                valid.append((model_idx, wer))

        if not valid:
            sample_winners.append([])
            error_rows += 1
            continue

        best_wer = min(wer for _, wer in valid)
        winners = [idx for idx, wer in valid if abs(wer - best_wer) <= epsilon]
        sample_winners.append(winners)

        if len(winners) == 1:
            win_counts[winners[0]] += 1
        else:
            tie_rows += 1

    ranking = sorted(
        range(len(model_runs)),
        key=lambda idx: (
            _sort_metric(model_runs[idx]["metrics"].get("wer")),
            _sort_metric(model_runs[idx]["metrics"].get("cer")),
            model_runs[idx]["label"].lower(),
        ),
    )

    return {
        "win_counts": win_counts,
        "sample_winners": sample_winners,
        "tie_rows": tie_rows,
        "error_rows": error_rows,
        "ranking": ranking,
    }


def build_text_report(
    samples: list[dict],
    model_runs: list[dict],
    *,
    baseline_label: str,
    output_txt: Path,
    output_json: Optional[Path],
    output_html: Optional[Path],
    published_html: Optional[Path],
) -> str:
    board = _scoreboard(samples, model_runs)
    lines: list[str] = []

    lines.append("=" * 66)
    lines.append(f"  Validation Comparison Report — {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    lines.append("=" * 66)
    lines.append(f"  Samples   : {len(samples)}")
    lines.append(f"  Baseline  : {baseline_label}")
    for model_run in model_runs:
        lines.append(f"  Model {model_run['slot']}   : {model_run['label']}")
    lines.append("")

    lines.append("-" * 60)
    lines.append("  Aggregate Metrics")
    lines.append("-" * 60)
    lines.append(f"  {'Rank':>4}  {'Model':30}  {'WER':>8}  {'CER':>8}  {'Wins':>6}")
    lines.append(f"  {'-'*4}  {'-'*30}  {'-'*8}  {'-'*8}  {'-'*6}")
    for rank, model_idx in enumerate(board["ranking"], start=1):
        model_run = model_runs[model_idx]
        wins = board["win_counts"][model_idx]
        lines.append(
            f"  {rank:>4}  {model_run['label'][:30]:30}  "
            f"{_fmt_pct(model_run['metrics'].get('wer')):>8}  "
            f"{_fmt_pct(model_run['metrics'].get('cer')):>8}  "
            f"{wins:>6}"
        )

    lines.append("")
    for model_idx in board["ranking"]:
        model_run = model_runs[model_idx]
        lines.append(f"  Model {model_run['slot']} WER{_wer_bar(model_run['metrics'].get('wer'))}")

    best_overall_idx = board["ranking"][0] if board["ranking"] else None
    if best_overall_idx is not None:
        best_overall = model_runs[best_overall_idx]
        if best_overall["metrics"].get("wer") is not None:
            lines.append("")
            lines.append(
                f"  Best overall: Model {best_overall['slot']} ({best_overall['label']}) "
                f"at {_fmt_pct(best_overall['metrics'].get('wer'))} WER"
            )

    lines.append("")
    lines.append("-" * 60)
    lines.append("  Per-Sample Results")
    lines.append("-" * 60)
    metric_headers = "  ".join(f"{m['slot']} WER".rjust(8) for m in model_runs)
    lines.append(f"  {'#':>3}  {'Stem':30}  {'Dur':>5}  {metric_headers}  {'Best':>8}")
    lines.append(
        f"  {'-'*3}  {'-'*30}  {'-'*5}  "
        + "  ".join("-" * 8 for _ in model_runs)
        + f"  {'-'*8}"
    )

    for sample_idx, sample in enumerate(samples):
        winner_indices = board["sample_winners"][sample_idx]
        best_label = "ERR" if not winner_indices else "/".join(model_runs[idx]["slot"] for idx in sorted(winner_indices))
        metric_values = []
        for model_run in model_runs:
            metric_values.append(f"{_fmt_pct(_sample_metric(model_run, sample_idx, 'wer'), 1):>8}")
        lines.append(
            f"  {sample_idx + 1:>3}  {sample['stem'][:30]:30}  {sample['duration']:>5.1f}  "
            + "  ".join(metric_values)
            + f"  {best_label:>8}"
        )

    lines.append("")
    lines.append("-" * 60)
    lines.append("  Win Summary")
    lines.append("-" * 60)
    for model_idx, model_run in enumerate(model_runs):
        lines.append(f"  Model {model_run['slot']} wins : {board['win_counts'][model_idx]}")
    lines.append(f"  Tie rows       : {board['tie_rows']}")
    if board["error_rows"]:
        lines.append(f"  Error rows     : {board['error_rows']}")

    hardest = []
    for sample_idx in range(len(samples)):
        valid_wers = [
            _sample_metric(model_run, sample_idx, "wer")
            for model_run in model_runs
            if _sample_metric(model_run, sample_idx, "wer") is not None
        ]
        hardness = min(valid_wers) if valid_wers else 1.0
        hardest.append((hardness, sample_idx))

    hardest.sort(reverse=True)
    lines.append("")
    lines.append("-" * 60)
    lines.append("  5 Hardest Samples (best model still had high WER)")
    lines.append("-" * 60)
    for hardness, sample_idx in hardest[:5]:
        sample = samples[sample_idx]
        lines.append("")
        lines.append(
            f"  [{sample_idx + 1}] {sample['stem']}  "
            f"({sample['dept']}  {sample['duration']:.1f}s  best={_fmt_pct(hardness, 1)})"
        )
        lines.append(f"  BASE: {_trunc(sample['ref'])}")
        for model_run in model_runs:
            wer = _sample_metric(model_run, sample_idx, "wer")
            lines.append(
                f"  {model_run['slot']:<4}{_trunc(model_run['preds'][sample_idx])}  "
                f"[WER {_fmt_pct(wer, 1)}]"
            )

    lines.append("")
    lines.append("  Text report  : " + str(output_txt))
    if output_json:
        lines.append("  JSON results : " + str(output_json))
    if output_html:
        lines.append("  HTML report  : " + str(output_html))
    if published_html:
        lines.append("  Published    : " + str(published_html))
    lines.append("")

    return "\n".join(lines)


def write_text_report(path: Path, report_text: str):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(report_text + "\n", encoding="utf-8")
    info(f"Text report saved: {path}")


# ─────────────────────────────────────────────────────────────────────────────
#  JSON results file
# ─────────────────────────────────────────────────────────────────────────────

def write_json_results(path: Path, samples, model_runs, *, baseline_label: str):
    board = _scoreboard(samples, model_runs)
    per_sample = []

    for sample_idx, sample in enumerate(samples):
        models = []
        for model_run in model_runs:
            models.append({
                "slot": model_run["slot"],
                "label": model_run["label"],
                "model_ref": model_run["ref"],
                "model_key": model_run.get("key") or "",
                "model_path": str(model_run["path"]),
                "prediction": model_run["preds"][sample_idx],
                "wer": _sample_metric(model_run, sample_idx, "wer"),
                "cer": _sample_metric(model_run, sample_idx, "cer"),
            })

        per_sample.append({
            "index": sample_idx,
            "id": sample["id"],
            "stem": sample["stem"],
            "town": sample["town"],
            "dept": sample["dept"],
            "duration": sample["duration"],
            "baseline_label": baseline_label,
            "baseline_text": sample["ref"],
            "winner_slots": [model_runs[idx]["slot"] for idx in board["sample_winners"][sample_idx]],
            "models": models,
        })

    results = {
        "generated_at": datetime.now().isoformat(),
        "num_samples": len(samples),
        "baseline_label": baseline_label,
        "models": [
            {
                "slot": model_run["slot"],
                "label": model_run["label"],
                "model_ref": model_run["ref"],
                "model_key": model_run.get("key") or "",
                "model_path": str(model_run["path"]),
                "device": model_run["device"],
                "compute_type": model_run["compute_type"],
                "decode": model_run["decode"],
                "aggregate": {
                    "wer": model_run["metrics"].get("wer"),
                    "cer": model_run["metrics"].get("cer"),
                    "wins": board["win_counts"][idx],
                },
            }
            for idx, model_run in enumerate(model_runs)
        ],
        "aggregate_ranking": [model_runs[idx]["slot"] for idx in board["ranking"]],
        "tie_rows": board["tie_rows"],
        "error_rows": board["error_rows"],
        "per_sample": per_sample,
    }

    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(results, indent=2, ensure_ascii=False), encoding="utf-8")
    info(f"JSON results saved: {path}")


# ─────────────────────────────────────────────────────────────────────────────
#  HTML report — self-contained, side-by-side diff colouring
# ─────────────────────────────────────────────────────────────────────────────

def _word_diff_html(ref: str, hyp: str) -> str:
    """
    Return an HTML snippet that highlights word-level differences between
    ref and hyp.  Correct words = green, substituted/inserted = red.
    Uses a simple greedy LCS approach — no extra dependencies.
    """
    ref_words = ref.split()
    hyp_words = hyp.split()

    # LCS-based alignment
    m, n = len(ref_words), len(hyp_words)
    # Build dp table
    dp = [[0] * (n + 1) for _ in range(m + 1)]
    for i in range(1, m + 1):
        for j in range(1, n + 1):
            if ref_words[i-1].lower().rstrip(".,!?;:") == hyp_words[j-1].lower().rstrip(".,!?;:"):
                dp[i][j] = dp[i-1][j-1] + 1
            else:
                dp[i][j] = max(dp[i-1][j], dp[i][j-1])

    # Traceback
    ops = []
    i, j = m, n
    while i > 0 or j > 0:
        if i > 0 and j > 0 and ref_words[i-1].lower().rstrip(".,!?;:") == hyp_words[j-1].lower().rstrip(".,!?;:"):
            ops.append(("match", hyp_words[j-1]))
            i -= 1; j -= 1
        elif j > 0 and (i == 0 or dp[i][j-1] >= dp[i-1][j]):
            ops.append(("ins", hyp_words[j-1]))
            j -= 1
        else:
            ops.append(("del", ref_words[i-1]))
            i -= 1
    ops.reverse()

    parts = []
    for op, word in ops:
        ew = html_mod.escape(word)
        if op == "match":
            parts.append(f'<span class="ok">{ew}</span>')
        elif op == "ins":
            parts.append(f'<span class="ins">{ew}</span>')
        else:
            parts.append(f'<span class="del">{ew}</span>')
    return " ".join(parts)


def write_html_report(path: Path, samples, model_runs, *, baseline_label: str):
    board = _scoreboard(samples, model_runs)
    now = datetime.now().strftime("%Y-%m-%d %H:%M")

    summary_rows = []
    for rank, model_idx in enumerate(board["ranking"], start=1):
        model_run = model_runs[model_idx]
        summary_rows.append(
            f"""
      <tr{' class="summary-best"' if rank == 1 else ''}>
        <td>{rank}</td>
        <td>{model_run['slot']}</td>
        <td>{html_mod.escape(model_run['label'])}</td>
        <td>{_fmt_pct(model_run['metrics'].get('wer'))}</td>
        <td>{_fmt_pct(model_run['metrics'].get('cer'))}</td>
        <td>{board['win_counts'][model_idx]}</td>
      </tr>"""
        )

    model_cols = "\n".join('    <col class="c-hyp">' for _ in model_runs)
    header_cells = "\n".join(
        f'      <th>Model {model_run["slot"]} — {html_mod.escape(model_run["label"])}</th>'
        for model_run in model_runs
    )

    rows = []
    for sample_idx, sample in enumerate(samples):
        winner_indices = set(board["sample_winners"][sample_idx])
        winner_label = "ERR" if not winner_indices else "/".join(model_runs[idx]["slot"] for idx in sorted(winner_indices))
        model_cells = []

        for model_idx, model_run in enumerate(model_runs):
            prediction = model_run["preds"][sample_idx] or ""
            diff_html = _word_diff_html(sample["ref"], prediction) if prediction else '<span class="missing">[no prediction]</span>'
            wer = _sample_metric(model_run, sample_idx, "wer")
            cer = _sample_metric(model_run, sample_idx, "cer")
            cell_class = "hyp"
            if not prediction:
                cell_class += " hyp-missing"
            if model_idx in winner_indices:
                cell_class += " hyp-best" if len(winner_indices) == 1 else " hyp-tie"

            model_cells.append(
                f"""
    <td class="{cell_class}">
      <div class="diff">{diff_html}</div>
      <div class="scores">WER {_fmt_pct(wer, 1)} &bull; CER {_fmt_pct(cer, 1)}</div>
    </td>"""
            )

        rows.append(
            f"""
  <tr>
    <td class="num">{sample_idx + 1}</td>
    <td class="meta">
      <div class="stem">{html_mod.escape(sample['stem'])}</div>
      <div class="meta2">{html_mod.escape(sample['dept'])} &bull; {sample['duration']:.1f}s</div>
      <div class="meta3">Best: {winner_label}</div>
    </td>
    <td class="ref">{html_mod.escape(sample['ref'])}</td>
    {''.join(model_cells)}
  </tr>"""
        )

    best_overall_idx = board["ranking"][0] if board["ranking"] else None
    best_overall_html = "<span class='verdict-tie'>n/a</span>"
    if best_overall_idx is not None:
        best = model_runs[best_overall_idx]
        if best["metrics"].get("wer") is not None:
            best_overall_html = (
                f"<span class='verdict-a'>Best overall: Model {best['slot']} "
                f"({html_mod.escape(best['label'])}) at {_fmt_pct(best['metrics'].get('wer'))} WER</span>"
            )

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<title>Model Comparison — {now}</title>
<style>
  :root {{
    --bg: #0d0f13;
    --panel: #171a21;
    --panel-2: #0f1319;
    --line: #2b3240;
    --text: #d9e1ea;
    --muted: #7f8b9d;
    --ok: #7ed957;
    --ins: #ff6b6b;
    --del: #f0a868;
    --best: rgba(46, 204, 113, 0.14);
    --tie: rgba(76, 201, 240, 0.12);
    --missing: rgba(255, 107, 107, 0.08);
  }}
  body {{ font-family: Consolas, 'Courier New', monospace; font-size: 13px;
         background: var(--bg); color: var(--text); margin: 0; padding: 20px; }}
  h1 {{ color: #f3f6fb; font-size: 1.35em; margin: 0 0 16px; }}
  .summary-grid {{
    display: grid;
    grid-template-columns: minmax(320px, 460px) minmax(420px, 1fr);
    gap: 16px;
    margin-bottom: 18px;
  }}
  .summary {{
    background: var(--panel);
    border: 1px solid var(--line);
    padding: 14px 18px;
    border-radius: 10px;
  }}
  .summary table {{ border-collapse: collapse; width: 100%; }}
  .summary th, .summary td {{ padding: 6px 8px; text-align: left; border-bottom: 1px solid #242b37; }}
  .summary th {{ color: #9fb0c8; font-size: 0.85em; }}
  .summary tr:last-child td {{ border-bottom: 0; }}
  .summary .lbl {{ color: var(--muted); }}
  .verdict-a {{ color: #4ec9b0; font-weight: bold; }}
  .verdict-b {{ color: #9cdcfe; font-weight: bold; }}
  .verdict-tie {{ color: #dcdcaa; }}
  .summary-best td {{ background: rgba(78, 201, 176, 0.08); }}
  .summary-note {{ color: var(--muted); line-height: 1.6; }}
  .table-wrap {{ overflow-x: auto; border: 1px solid var(--line); border-radius: 10px; background: var(--panel-2); }}
  table.main {{ border-collapse: collapse; min-width: 1700px; width: 100%; table-layout: fixed; }}
  table.main th {{ background: #1c2330; color: #c8d7ed; text-align: left;
                   padding: 10px 12px; border-bottom: 2px solid #334055; font-size: 0.84em; }}
  table.main td {{ padding: 9px 12px; border-bottom: 1px solid #1e2531;
                   vertical-align: top; word-break: break-word; }}
  tbody tr:nth-child(odd) td {{ background: rgba(255,255,255,0.015); }}
  col.c-num  {{ width: 56px; }}
  col.c-meta {{ width: 220px; }}
  col.c-ref  {{ width: 360px; }}
  col.c-hyp  {{ width: 360px; }}
  span.ok  {{ color: var(--ok); }}
  span.ins {{ color: var(--ins); text-decoration: underline; }}
  span.del {{ color: var(--del); text-decoration: line-through; }}
  .stem {{ color: #9cdcfe; font-size: 0.84em; }}
  .meta2, .meta3 {{ color: var(--muted); font-size: 0.78em; margin-top: 3px; }}
  .scores {{ color: var(--muted); font-size: 0.78em; margin-top: 6px; }}
  .num {{ color: #69788d; font-size: 0.85em; text-align: right; }}
  .ref {{ color: #c9e7b2; }}
  .diff {{ line-height: 1.55em; }}
  .hyp-best {{ background: var(--best) !important; box-shadow: inset 0 0 0 1px rgba(46, 204, 113, 0.3); }}
  .hyp-tie {{ background: var(--tie) !important; box-shadow: inset 0 0 0 1px rgba(76, 201, 240, 0.24); }}
  .hyp-missing {{ background: var(--missing) !important; }}
  .missing {{ color: #ff9c9c; }}
  .legend {{ font-size: 0.82em; color: var(--muted); margin: 0 0 10px; }}
  .legend span {{ margin-right: 16px; }}
</style>
</head>
<body>
<h1>Whisper Model Comparison &mdash; {now}</h1>
<div class="summary-grid">
  <div class="summary">
    <table>
      <tr><td class="lbl">Baseline</td><td>{html_mod.escape(baseline_label)}</td></tr>
      <tr><td class="lbl">Samples</td><td>{len(samples)}</td></tr>
      <tr><td class="lbl">Tie rows</td><td>{board['tie_rows']}</td></tr>
      <tr><td class="lbl">Error rows</td><td>{board['error_rows']}</td></tr>
      <tr><td class="lbl">Verdict</td><td>{best_overall_html}</td></tr>
    </table>
  </div>
  <div class="summary">
    <table>
      <thead>
        <tr>
          <th>Rank</th>
          <th>Slot</th>
          <th>Model</th>
          <th>WER</th>
          <th>CER</th>
          <th>Wins</th>
        </tr>
      </thead>
      <tbody>
        {''.join(summary_rows)}
      </tbody>
    </table>
  </div>
</div>

<div class="legend">
  <span><span class="ok">green</span> = correct word</span>
  <span><span class="ins">red underline</span> = insertion (extra word)</span>
  <span><span class="del">orange strikethrough</span> = deletion (missing word)</span>
  <span style="background:rgba(46, 204, 113, 0.14);padding:1px 4px;">green cell</span> = best model on that clip
  <span style="background:rgba(76, 201, 240, 0.12);padding:1px 4px;">blue cell</span> = tied best
</div>

<div class="table-wrap">
  <table class="main">
    <colgroup>
      <col class="c-num">
      <col class="c-meta">
      <col class="c-ref">
{model_cols}
    </colgroup>
    <thead>
      <tr>
        <th>#</th>
        <th>File</th>
        <th>Baseline — {html_mod.escape(baseline_label)}</th>
{header_cells}
      </tr>
    </thead>
    <tbody>
{''.join(rows)}
    </tbody>
  </table>
</div>
</body>
</html>"""

    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(html, encoding="utf-8")
    info(f"HTML report  saved: {path}")


# ─────────────────────────────────────────────────────────────────────────────
#  Main
# ─────────────────────────────────────────────────────────────────────────────

def _cfg_lookup(cfg: dict, *paths, default=None):
    for path in paths:
        cur = cfg
        found = True
        for key in path:
            if not isinstance(cur, dict) or key not in cur:
                found = False
                break
            cur = cur[key]
        if found:
            return cur
    return default


def _collect_model_specs(args, cfg: dict) -> list[dict]:
    config_specs = []
    raw_models = _cfg_lookup(cfg, ("models",), default=None)
    if isinstance(raw_models, list):
        for idx, item in enumerate(raw_models[:3]):
            if not isinstance(item, dict):
                continue
            ref = str(
                item.get("key")
                or item.get("model")
                or item.get("model_ref")
                or item.get("path")
                or item.get("ref")
                or ""
            ).strip()
            label = str(item.get("label") or item.get("name") or ref).strip()
            if ref:
                config_specs.append({"slot": chr(ord("A") + idx), "ref": ref, "label": label})

    if not config_specs:
        for idx, suffix in enumerate(("a", "b", "c")):
            ref = str(_cfg_lookup(cfg, (f"model_{suffix}",), default="") or "").strip()
            label = str(_cfg_lookup(cfg, (f"label_{suffix}",), default="") or ref).strip()
            if ref:
                config_specs.append({"slot": chr(ord("A") + idx), "ref": ref, "label": label})

    specs = []
    for idx, suffix in enumerate(("a", "b", "c")):
        base = config_specs[idx] if idx < len(config_specs) else {}
        ref = str(getattr(args, f"model_{suffix}") or base.get("ref") or "").strip()
        label = str(getattr(args, f"label_{suffix}") or base.get("label") or ref).strip()
        if ref:
            specs.append({"slot": chr(ord("A") + idx), "ref": ref, "label": label or ref})
    return specs


def main():
    here = Path(__file__).resolve().parent

    ap = argparse.ArgumentParser(description="Compare up to three Whisper models against the edited transcript baseline")
    ap.add_argument("--re-do-calls", action="store_true", help="Use transcript (not edited_transcript) from DB for comparison")
    ap.add_argument("--config",    default=str(here / "compare_config.json"), help="Path to compare_config.json")
    ap.add_argument("--model-a",   default=None, help="Override model A path")
    ap.add_argument("--model-b",   default=None, help="Override model B path")
    ap.add_argument("--model-c",   default=None, help="Override model C path")
    ap.add_argument("--model-catalog", default=None, help="Path to model_catalog.json used by scanner transcriber")
    ap.add_argument("--model-base-dir", default=os.environ.get("MODEL_BASE_DIR", "/home/ned/models/scanner_transcriber_models_active"),
                    help="Base folder for bare model names (same behavior as scanner transcriber)")
    ap.add_argument("--label-a",   default=None, help="Label for model A (display name)")
    ap.add_argument("--label-b",   default=None, help="Label for model B (display name)")
    ap.add_argument("--label-c",   default=None, help="Label for model C (display name)")
    ap.add_argument("--val-dir",   default=None, help="Override validation_data dir")
    ap.add_argument("--out-dir",   default=None, help="Override output dir for results")
    ap.add_argument("--limit",     type=int, default=None, help="Only run first N samples (quick test)")
    ap.add_argument("--beam",      type=int, default=None,
                    help="Override beam_size for all models (default: use profile/catalog like scanner transcriber)")
    ap.add_argument("--no-html",     action="store_true",    help="Skip HTML report generation")
    ap.add_argument("--no-json",     action="store_true",    help="Skip JSON report generation")
    ap.add_argument("--cpu",         action="store_true",    help="Force CPU even if CUDA available")
    ap.add_argument("--compute-type", default=None,
                    choices=["float16", "int8_float16", "int8", "float32"],
                    help="CTranslate2 compute type (default: float16 on GPU, int8 on CPU)")
    ap.add_argument("--use-mcp", action="store_true",
                    help="Transcribe through running MCP server (route_and_transcribe) instead of local model loading")
    ap.add_argument("--mcp-url", default=None,
                    help="MCP endpoint URL")
    ap.add_argument("--mcp-profile", default=None,
                    help="Preprocess profile passed to MCP route_and_transcribe")
    ap.add_argument("--mcp-language", default=None,
                    help="Optional language override for MCP route_and_transcribe (default: model profile language)")
    ap.add_argument("--publish", action="store_true",
                    help="Copy the HTML report into the web templates folder so it appears in the Training Info dropdown")
    ap.add_argument("--templates-dir", default=None,
                    help="Override path to web/templates (default: auto-detected from repo layout)")
    # ── DB source (alternative to validation_dir folder) ──────────────────────
    ap.add_argument("--db", default=None,
                    help="Pull validation samples from scanner_calls.db instead of a WAV folder")
    ap.add_argument("--db-since", default="",
                    help="DB: only rows with timestamp >= this ISO string")
    ap.add_argument("--db-until", default="",
                    help="DB: only rows with timestamp <= this ISO string")
    ap.add_argument("--db-any-reviewed", action="store_true",
                    help="DB: include any reviewed+edited row (default: only save_for_eval / freeze_for_testing rows)")
    ap.add_argument("--db-limit", type=int, default=None,
                    help="DB: max rows to pull (overrides config db_limit, default 500)")
    ap.add_argument("--db-min-duration", type=float, default=None,
                    help="DB: skip rows shorter than this many seconds (default: 2.0)")
    args = ap.parse_args()

    # ── load config ──────────────────────────────────────────────────────────
    cfg_path = Path(args.config).expanduser().resolve()
    cfg = {}
    use_mcp = bool(args.use_mcp)
    mcp_url = os.environ.get("MCP_URL") or "http://127.0.0.1:8000/mcp"
    mcp_profile = os.environ.get("DEFAULT_PROFILE") or "default"
    mcp_language_cfg = ""
    publish_to_web = bool(args.publish)
    db_path_cfg = ""
    db_since_cfg = ""
    db_until_cfg = ""
    db_eval_only_cfg = True
    db_limit_cfg = 500
    db_min_duration_cfg = 2.0

    if cfg_path.exists():
        cfg = json.loads(cfg_path.read_text(encoding="utf-8"))
        info(f"Config: {cfg_path}")
        use_mcp = bool(args.use_mcp or _cfg_lookup(cfg, ("execution", "use_mcp"), ("use_mcp",), default=False))
        mcp_url = args.mcp_url or str(
            _cfg_lookup(cfg, ("execution", "mcp_url"), ("mcp_url",), default=mcp_url) or mcp_url
        )
        mcp_profile = args.mcp_profile or str(
            _cfg_lookup(cfg, ("execution", "mcp_profile"), ("mcp_profile",), default=mcp_profile) or mcp_profile
        )
        mcp_language_cfg = str(
            _cfg_lookup(cfg, ("execution", "mcp_language"), ("mcp_language",), default="") or ""
        ).strip()
        publish_to_web = bool(args.publish or _cfg_lookup(cfg, ("output", "publish_to_web"), ("publish_to_web",), default=False))
        db_path_cfg = str(
            _cfg_lookup(cfg, ("sample_source", "db", "path"), ("db",), default="") or ""
        ).strip()
        db_since_cfg = str(
            _cfg_lookup(cfg, ("sample_source", "db", "since"), ("db_since",), default="") or ""
        ).strip()
        db_until_cfg = str(
            _cfg_lookup(cfg, ("sample_source", "db", "until"), ("db_until",), default="") or ""
        ).strip()
        db_eval_only_cfg = bool(
            _cfg_lookup(cfg, ("sample_source", "db", "eval_only"), ("db_eval_only",), default=True)
        )
        db_limit_cfg = int(
            _cfg_lookup(cfg, ("sample_source", "db", "limit"), ("db_limit",), default=500)
        )
        db_min_duration_cfg = float(
            _cfg_lookup(cfg, ("sample_source", "db", "min_duration"), ("db_min_duration",), default=2.0)
        )
    else:
        if len([m for m in (args.model_a, args.model_b, args.model_c) if m]) < 1:
            print(f"\n  [!] No config found at {cfg_path}")
            print(f"      and no CLI model was provided.")
            print(f"      Create compare_config.json or pass --model-a\n")
            sys.exit(1)

    # Resolve all paths
    base_dir = cfg_path.parent if cfg_path.exists() else here
    model_base_dir = Path(args.model_base_dir).expanduser().resolve()

    model_specs = _collect_model_specs(args, cfg)
    if len(model_specs) < 1:
        print("\n  [!] Provide at least one model in compare_config.json or via CLI.\n")
        sys.exit(1)

    catalog_path = _find_model_catalog_path(base_dir, cfg, args.model_catalog)
    catalog, catalog_default_key = _load_model_catalog(catalog_path, model_base_dir)

    val_dir  = Path(args.val_dir).expanduser().resolve() if args.val_dir \
               else Path(
                    _cfg_lookup(
                        cfg,
                        ("sample_source", "validation_dir"),
                        ("validation_dir",),
                        default=base_dir / "exports_april2026/validation_data",
                    )
               ).expanduser().resolve()

    out_dir  = Path(args.out_dir).expanduser().resolve() if args.out_dir \
               else Path(
                    _cfg_lookup(cfg, ("output", "output_dir"), ("output_dir",), default=base_dir / "comparison_results")
               ).expanduser().resolve()

    out_dir.mkdir(parents=True, exist_ok=True)
    ts_tag   = datetime.now().strftime("%Y%m%d_%H%M%S")

    def _slug(s: str, maxlen: int = 24) -> str:
        import re
        s = re.sub(r"[^a-zA-Z0-9_\-]+", "_", s).strip("_")
        return s[:maxlen].rstrip("_")

    # ── device + profile settings resolution (aligned with scanner transcriber) ──
    auto_device = "cuda" if torch.cuda.is_available() else "cpu"
    fallback_device = "cpu" if args.cpu else auto_device
    fallback_compute_type = args.compute_type or _cfg_lookup(
        cfg, ("defaults", "compute_type"), ("compute_type",), default=None
    ) or ("int8" if fallback_device == "cpu" else "float16")

    transcribe_defaults_cfg = _cfg_lookup(cfg, ("defaults", "transcribe"), ("transcribe",), default=None)
    default_profile_settings = _merged_transcribe_settings(
        transcribe_defaults_cfg if isinstance(transcribe_defaults_cfg, dict) else None
    )

    model_runs = []
    for model_spec in model_specs:
        model_ref = model_spec["ref"]
        if model_ref == "default" and catalog_default_key:
            model_ref = catalog_default_key

        profile = _resolve_compare_model(
            model_ref,
            catalog=catalog,
            fallback_compute_type=fallback_compute_type,
            fallback_device=fallback_device,
            fallback_settings=default_profile_settings,
            model_base_dir=model_base_dir,
        )
        if args.cpu:
            profile["device"] = "cpu"

        decode_kwargs = _build_transcribe_kwargs(
            task=None,
            language=None,
            profile_settings=profile.get("transcribe"),
        )
        if args.beam is not None:
            decode_kwargs["beam_size"] = int(args.beam)

        model_path = profile["model"]
        label = model_spec["label"] or (profile["key"] or Path(model_path).name)
        model_runs.append({
            "slot": model_spec["slot"],
            "ref": model_ref,
            "key": profile["key"],
            "path": model_path,
            "label": label,
            "device": profile["device"],
            "compute_type": profile["compute_type"],
            "decode": decode_kwargs,
            "preds": [],
            "metrics": {},
        })

    compare_slug = "_vs_".join(_slug(model_run["label"]) for model_run in model_runs) or "comparison"
    compare_slug = f"{compare_slug}_{ts_tag}"

    out_txt = out_dir / f"comparison_{compare_slug}.txt"
    out_json = out_dir / f"comparison_{compare_slug}.json"
    out_html = out_dir / f"comparison_{compare_slug}.html"
    baseline_label = "transcript" if getattr(args, "re_do_calls", False) else "edited_transcript"

    # ── banner ────────────────────────────────────────────────────────────────
    banner("Whisper Model Comparison Tool")
    if catalog_path:
        info(f"Model catalog  : {catalog_path}  [OK]")
    else:
        info("Model catalog  : not found (using direct model refs)")
    info(f"Validation dir : {val_dir}  {'[OK]' if val_dir.exists() else '[NOT FOUND]'}")
    info(f"Output dir     : {out_dir}")
    for model_run in model_runs:
        gpu_name = ""
        if model_run["device"] == "cuda" and torch.cuda.is_available():
            gpu_name = f" -- {torch.cuda.get_device_name(0)}"
        info(f"Model {model_run['slot']}        : {model_run['path']}")
        info(f"Label {model_run['slot']}        : {model_run['label']}")
        info(f"Device {model_run['slot']}       : {model_run['device']}{gpu_name}")
        info(f"Compute {model_run['slot']}      : {model_run['compute_type']}  (faster-whisper CTranslate2)")
        info(
            f"Decode {model_run['slot']}       : "
            f"beam={model_run['decode'].get('beam_size')} "
            f"vad={model_run['decode'].get('vad_filter')} "
            f"prompt={'yes' if model_run['decode'].get('initial_prompt') else 'no'}"
        )
    if use_mcp:
        info(f"MCP mode       : ON ({mcp_url})")
        info(f"MCP profile    : {mcp_profile}")
    if args.limit:
        info(f"Limit          : {args.limit} samples (test mode)")

    # Resolve templates dir for web publishing
    _repo_root = Path("/home/ned/Documents/neds_scanner_radio_full_pipeline_with_git")
    _default_templates = _repo_root / "web" / "templates"
    templates_dir = Path(args.templates_dir).expanduser().resolve() if args.templates_dir \
                    else Path(
                        _cfg_lookup(cfg, ("output", "templates_dir"), ("templates_dir",), default=str(_default_templates))
                    ).expanduser().resolve()
    if publish_to_web:
        info(f"Publish        : ON → {templates_dir}")

    # Resolve DB source
    db_source = Path(args.db).expanduser().resolve() if args.db \
                else (Path(db_path_cfg).expanduser().resolve() if db_path_cfg else None)
    db_since  = args.db_since.strip() or db_since_cfg
    db_until  = args.db_until.strip() or db_until_cfg
    db_eval_only = not args.db_any_reviewed and db_eval_only_cfg
    db_limit     = args.db_limit if args.db_limit is not None else db_limit_cfg
    db_min_dur   = args.db_min_duration if args.db_min_duration is not None else db_min_duration_cfg
    use_db       = db_source is not None

    if use_db:
        info(f"Sample source  : DB ({db_source})")
        info(f"  eval_only    : {db_eval_only}  (False = any reviewed+edited row)")
        info(f"  limit        : {db_limit}")
        info(f"  min_duration : {db_min_dur}s")
        if db_since or db_until:
            info(f"  date range   : {db_since or '*'} → {db_until or '*'}")
    else:
        info(f"Sample source  : folder ({val_dir})")
    print()
    print("  *** VALIDATION DATA IS READ-ONLY — never used for training ***")
    print()

    # Check paths
    errors = []
    if not use_db and not val_dir.exists():
        errors.append(f"Validation dir not found: {val_dir}")
    for model_run in model_runs:
        model_path_obj = Path(model_run["path"])
        is_hub_id = not model_path_obj.exists() and "/" in model_run["path"] and not model_path_obj.is_absolute()
        if not (model_path_obj.exists() or is_hub_id):
            errors.append(f"Model {model_run['slot']} not found: {model_run['path']}")
    if errors:
        for e in errors:
            print(f"  [!] {e}")
        sys.exit(1)

    if use_mcp:
        if any(not model_run.get("key") for model_run in model_runs):
            print("\n  [!] --use-mcp requires every compared model to be a model catalog key.")
            print("      Set compare_config models[].key (or model_a/model_b/model_c) to catalog keys.\n")
            sys.exit(1)

    # ── import required packages ─────────────────────────────────────────────
    section("Importing packages")
    evaluate_mod = _require("evaluate",      "evaluate")
    _require("faster_whisper", "faster-whisper")
    _require("jiwer",          "jiwer")
    info("All packages imported")

    # ── load validation samples ──────────────────────────────────────────────
    section("Loading validation samples")
    if use_db:
        samples = load_validation_samples_from_db(
            db_source,
            since=db_since,
            until=db_until,
            eval_only=db_eval_only,
            limit_override=db_limit,
            min_duration=db_min_dur,
            use_transcript=getattr(args, "re_do_calls", False),
        )
    else:
        samples = load_validation_samples(val_dir, limit=args.limit)
    if not samples:
        sys.exit("No valid samples found.")

    refs = [s["ref"] for s in samples]

    # ── transcribe each model ────────────────────────────────────────────────
    for model_run in model_runs:
        section(f"Transcribing with Model {model_run['slot']}: {model_run['label']}")
        if use_mcp:
            model_run["preds"] = transcribe_all_via_mcp(
                mcp_url=mcp_url,
                model_key=model_run["key"],
                samples=samples,
                label=model_run["slot"],
                profile=mcp_profile,
                language=args.mcp_language or mcp_language_cfg or str(model_run["decode"].get("language") or "en"),
            )
        else:
            model_run["preds"] = transcribe_all(
                str(model_run["path"]),
                samples,
                model_run["device"],
                model_run["slot"],
                model_run["compute_type"],
                model_run["decode"],
            )

    # ── compute metrics ───────────────────────────────────────────────────────
    section("Computing WER + CER")
    for model_run in model_runs:
        info(f"Model {model_run['slot']} metrics ...")
        model_run["metrics"] = compute_wer_cer(model_run["preds"], refs, evaluate_mod)
        if model_run["metrics"]["wer"] is None:
            warn(
                f"Model {model_run['slot']}: no valid predictions — WER/CER unavailable "
                f"(all samples may have failed)"
            )
        else:
            info(
                f"  WER={model_run['metrics']['wer']:.4f}  "
                f"CER={model_run['metrics']['cer']:.4f}"
            )

    if not args.no_json:
        write_json_results(
            out_json,
            samples,
            model_runs,
            baseline_label=baseline_label,
        )

    published_html_path = None
    if not args.no_html:
        write_html_report(
            out_html,
            samples,
            model_runs,
            baseline_label=baseline_label,
        )
        if publish_to_web and templates_dir.is_dir():
            import re
            # Filename must match web route pattern: *_training_result_set.html
            safe_slug = re.sub(r"[^a-zA-Z0-9_\-]+", "_", compare_slug).strip("_")
            publish_name = f"{safe_slug}_training_result_set.html"
            published_html_path = templates_dir / publish_name
            import shutil
            shutil.copy2(out_html, published_html_path)
            info(f"Published HTML : {published_html_path}")
            info(f"  → now visible in Training Info dropdown as: {publish_name}")
        elif publish_to_web:
            warn(f"Templates dir not found, skipping publish: {templates_dir}")

    report_text = build_text_report(
        samples,
        model_runs,
        baseline_label=baseline_label,
        output_txt=out_txt,
        output_json=out_json if not args.no_json else None,
        output_html=out_html if not args.no_html else None,
        published_html=published_html_path,
    )
    print(report_text)
    write_text_report(out_txt, report_text)

    banner("Done", char="#")


if __name__ == "__main__":
    main()
#python compare_models.py --db /path/to/scanner_calls.db --re-do-calls
