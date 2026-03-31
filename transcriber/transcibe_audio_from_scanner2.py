#!/usr/bin/env python3
import os
os.environ["PYTORCH_CUDA_ALLOC_CONF"] = "expandable_segments:True"

import gc
import json
import subprocess
import argparse
import datetime
import shutil
import warnings
from pathlib import Path
import torch
import torchaudio
import re

# Local import — assumes scanner_db.py is in the same folder or PYTHONPATH
import scanner_db

from transformers import WhisperProcessor, WhisperForConditionalGeneration, GenerationConfig
from transformers.utils import logging as hf_logging

from nlp_zero_shot import classify_meta_in_memory



# ======================================================
# SETUP
# ======================================================
print("are we in it?")

warnings.filterwarnings("ignore", message="`generation_config` default values")
warnings.filterwarnings("ignore", message="The attention mask is not set")
hf_logging.set_verbosity_error()


DB_PATH = scanner_db.DB_PATH
ARCHIVE_BASE = Path(os.environ.get("ARCHIVE_BASE", "/home/ned/data/scanner_calls/scanner_archive"))

TMP_DIR = Path("/tmp/scanner_tmp")
MIN_DURATION = 2
RMS_THRESHOLD = 0.001

print("[Transcriber] Initializing and loading fine-tuned model...")
#model_dir = Path("/home/ned/models/trained_whisper_medium_baseline_d101325")
model_dir = Path("/home/ned/models/trained_whisper_medium_baseline_d020426/trained_whisper_medium_v1")
print(f"Using model: {model_dir}")

processor = WhisperProcessor.from_pretrained(str(model_dir))
model = WhisperForConditionalGeneration.from_pretrained(str(model_dir))

if hasattr(model, "generation_config"):
    print("has attributes")
    model.generation_config.forced_decoder_ids = None
    model.generation_config.suppress_tokens = []
else:
    print("no attribtes")
    model.generation_config = GenerationConfig(forced_decoder_ids=None, suppress_tokens=[],force_download=True)

if torch.cuda.is_available():
    free, total = torch.cuda.mem_get_info()
    free_gb, total_gb = free / (1024**3), total / (1024**3)
    print(f"[*] CUDA free memory: {free_gb:.2f} GB / {total_gb:.2f} GB")
    device = torch.device("cuda" if free_gb >= 2.0 else "cpu")
else:
    device = torch.device("cpu")
model.to(device)
if device.type == "cuda":
    model = model.half()
print(f"[✓] Model ready on {device}")

# ======================================================
# HELPERS
# ======================================================

def detect_category(file: Path):
    """Detect feed (e.g., mpd, mfd) from filename or path."""
    name = file.name.lower()
    parts = " ".join(file.parts).lower()
    feed_keys = ["mndfd", "mndpd", "mpd", "mfd", "bpd", "bfd", "pd", "fd",
                 "blkfd", "blkpd", "uptfd", "uptpd", "frkpd", "frkfd"]
    for key in feed_keys:
        if re.search(rf"_{key}(?:\.|_|$)", name):
            return key, key
        if f"/{key}/" in parts:
            return key, key
    return "misc", "misc"

def get_rms(path: Path) -> float:
    """Compute RMS amplitude using SoX."""
    try:
        r = subprocess.run(
            ["sox", "-t", "wav", str(path), "-n", "stat"],
            stderr=subprocess.PIPE, stdout=subprocess.DEVNULL, text=True
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
        rms = get_rms(path)
        return rms < RMS_THRESHOLD
    except Exception:
        return False

def get_duration(path: Path) -> float:
    try:
        r = subprocess.run(
            ["ffprobe", "-v", "error", "-show_entries", "format=duration",
             "-of", "default=noprint_wrappers=1:nokey=1", str(path)],
            stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True
        )
        return float(r.stdout.strip())
    except Exception:
        return 0.0

def preprocess_audio(inp: Path, outp: Path):
    subprocess.run(
        ["ffmpeg", "-y", "-i", str(inp), "-ac", "1", "-ar", "16000",
         "-af", "highpass=f=100,volume=5dB", str(outp)],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=True
    )

def transcribe_file(path: Path) -> str:
    """Run Whisper transcription."""
    print("got here")
    try:
        waveform, sr = torchaudio.load(str(path))
    except Exception as e:
        print(f"[warn] torchaudio failed to load {path.name}: {e}")
        return ""

    if waveform.ndim == 2:
        waveform = waveform.mean(dim=0)
    waveform = waveform.squeeze()

    inputs = processor(
        waveform.numpy(),
        sampling_rate=sr,
        return_tensors="pt",
        language="en",
        return_attention_mask=True
    )
    feats = inputs.input_features.to(device, dtype=(torch.float16 if device.type == "cuda" else torch.float32))
    gen_cfg = GenerationConfig.from_model_config(model.config)
    gen_cfg.forced_decoder_ids = None
    gen_cfg.suppress_tokens = []

    with torch.inference_mode():
        print("------- got here --------")
        predicted = model.generate(
            feats,
            generation_config=gen_cfg,
            do_sample=False,
            max_length=448,
            num_beams=4,
            length_penalty=1.0,
            repetition_penalty=1.2,
            early_stopping=True
        )

    text = processor.batch_decode(predicted, skip_special_tokens=True)[0].strip()
    return text

# ======================================================
# CORE
# ======================================================

def process_single_file(filepath: str):
    file = Path(filepath)
    if not file.exists():
        print(f"[!] Missing file {filepath}")
        return

    clean_subdir, raw_subdir = detect_category(file)
    
    CLEAN = ARCHIVE_BASE / "clean" / clean_subdir
    RAW   = ARCHIVE_BASE / "raw" / raw_subdir
    TMP_DIR.mkdir(parents=True, exist_ok=True)
    CLEAN.mkdir(parents=True, exist_ok=True)
    RAW.mkdir(parents=True, exist_ok=True)

    dur = get_duration(file)
    if dur < MIN_DURATION:
        print(f"[SKIP] {file.name} ({dur:.2f}s too short)")
        return
    if is_static(file):
        print(f"[STATIC] {file.name} → skipping pure static")
        return

    tmp = TMP_DIR / f"{file.stem}_clean.wav"
    txt = CLEAN / f"{file.stem}.txt"
    jsn = CLEAN / f"{file.stem}.json"
    wav = CLEAN / f"{file.stem}.wav"

    try:
        preprocess_audio(file, tmp)
        text = transcribe_file(tmp)
        print(f"[TRANSCRIPT] {file.name}: {text[:200]}", flush=True)
        rms = get_rms(tmp)
        now_iso = datetime.datetime.now().isoformat()

        # --- write normal artifacts ---
        txt.write_text(text, encoding="utf-8")
        # --- build initial metadata JSON ---
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

        feed = clean_subdir.lower()
        town = SOURCE_MAP.get(feed, "Unknown")
        state = "Massachusetts"
        dept = "fire" if "fd" in feed else "police" if "pd" in feed else ""

        meta = {
            "filename": wav.name,
            "transcript": text,
            "duration": dur,
            "rms": rms,
            "timestamp": now_iso,
            "source": clean_subdir,
            "town": town,
            "state": state,
            "dept": dept,
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

        # --- run AI labeler entirely in memory ---
        try:
            print(f"[AI] Labeling in-memory for {wav.name} ...")
            meta = classify_meta_in_memory(meta, threshold=0.4)
            print(f"[AI] Intent: {meta['classification']['zero_shot']['intent']} "
                f"({meta['classification']['zero_shot']['confidence']:.2f})")
        except Exception as e:
            print(f"[WARN] Labeler failed in-memory: {e}")

        # --- write final enriched JSON once ---
        jsn.write_text(json.dumps(meta, indent=2), encoding="utf-8")
        shutil.copy(tmp, wav)

        # --- now insert into DB using enriched meta ---
        db_meta = {
            "town": town,
            "state": state,
            "dept": dept,
            "category": clean_subdir,
            "filename": wav.name,
            "json_path": str(jsn),
            "wav_path": str(wav),
            "duration": dur,
            "rms": rms,
            "transcript": text,
            "edited_transcript": None,
            "timestamp": now_iso,
            "reviewed": 0,
            "play_count": 0,
            "classification": meta["classification"],
            "intent_labeled": int(meta.get("intent_labeled", 0)),
            "intent_labeled_at": meta.get("intent_labeled_at"),
            "extra": meta,
        }
        scanner_db.insert_call(db_meta)

        # --- delete raw file ---
        try:
            file.unlink()
            print(f"[DEL] Removed source raw file: {file}")
        except Exception as e:
            print(f"[WARN] Could not delete {file}: {e}")

    except Exception as e:
        print(f"[ERROR] {file.name} processing: {e}")
    finally:
        tmp.unlink(missing_ok=True)
        gc.collect()
        if device.type == "cuda":
            torch.cuda.empty_cache()

# ======================================================
# MAIN
# ======================================================

def main():
    p = argparse.ArgumentParser(description="Transcribe scanner audio file and write to DB")
    p.add_argument("--file", required=True, help="Path to WAV file")
    a = p.parse_args()
    print(f"--- Running standalone for: {a.file} ---")
    if not DB_PATH.exists():
        scanner_db.create_tables()  # ensure DB exists
    process_single_file(a.file)
    print("--- Complete. ---")

if __name__ == "__main__":
    main()
