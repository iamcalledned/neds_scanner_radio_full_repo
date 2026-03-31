#!/usr/bin/env python3
"""
nlp_zero_shot.py — Regex-based metadata enrichment for Ned's Scanner Network

Extracts address, units, agency, tone, urgency, and town/state from
scanner transcripts. No ML model required — pure Python.
"""

import os
import re
import logging
import logging.handlers
from pathlib import Path

from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(__file__), ".env"))

# ======================================================
# Logging
# ======================================================
LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO").upper()
LOG_DIR = Path(os.environ.get("LOG_DIR", "/home/ned/data/scanner_calls/logs/transcriber_logs"))
LOG_DIR.mkdir(parents=True, exist_ok=True)

_log_fmt = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")

_console_handler = logging.StreamHandler()
_console_handler.setFormatter(_log_fmt)

_file_handler = logging.handlers.RotatingFileHandler(
    LOG_DIR / "nlp_zero_shot.log", maxBytes=10_000_000, backupCount=5
)
_file_handler.setFormatter(_log_fmt)

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    handlers=[_console_handler, _file_handler],
)
log = logging.getLogger("nlp-zero-shot")

# ======================================================
# Configuration
# ======================================================

# Known agency names for detection
AGENCIES = [
    "Hopedale PD",
    "Hopedale Fire",
    "Milford PD",
    "Milford Fire",
    "Bellingham PD",
    "Bellingham Fire",
    "Mendon PD",
    "Mendon Fire",
    "Upton PD",
    "Upton Fire",
    "Blackstone PD",
    "Blackstone Fire",
    "Franklin PD",
    "Franklin Fire",
]



# ======================================================
# Helpers
# ======================================================
def enrich_metadata(text: str, meta: dict):
    """Extract address, units, tone, agency, and add town/state."""
    extra = {}

    # === Basic address extraction ===
    addr_match = re.search(r"\b\d+\s+[A-Z][a-z]+(?:\s+[A-Z][a-z]+)*\b", text)
    if addr_match:
        extra["location"] = addr_match.group(0)
        parts = addr_match.group(0).split(" ", 1)
        if len(parts) == 2:
            extra["address_number"], extra["address_street"] = parts

    # === Units (e.g., P-1, E-2, C-3) ===
    units = re.findall(r"\b[A-Z]{1,2}-\d+\b", text)
    if units:
        extra["units"] = units

    # === Tone detection ===
    extra["tone_detected"] = bool(re.search(r"\bTONE\b", text, re.IGNORECASE))

    # === Agency detection ===
    for agency in AGENCIES:
        if agency.lower() in text.lower():
            extra["agency"] = agency
            break

    # === Call type ===
    call_type = re.search(r"([A-Z][a-z]+(?:\s+[A-Z][a-z]+){0,3})(?:,|\s+\d)", text)
    if call_type:
        extra["call_type"] = call_type.group(1).strip()

    # === Urgency / priority markers ===
    urgency_map = {
        "code 3": "high",
        "priority 1": "high",
        "priority 2": "medium",
        "routine": "low",
    }
    for k, v in urgency_map.items():
        if k in text.lower():
            extra["urgency"] = v
            break

    # === Town/State mapping based on feed source ===
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

    feed = meta.get("source") or ""
    town = SOURCE_MAP.get(feed.lower(), "Unknown")
    extra["town"] = town
    extra["state"] = "Massachusetts"

    # Merge into classification dict
    meta["classification"].update(extra)
    return meta



# ======================================================
# Reusable in-memory API
# ======================================================
def enrich_meta_in_memory(meta: dict) -> dict:
    """Run regex-based metadata enrichment directly on a Python dict.

    Extracts address, units, agency, tone, urgency, and town/state
    from the transcript and merges into meta['classification'].
    """
    text = meta.get("edited_transcript") or meta.get("transcript") or ""
    if not text:
        return meta

    meta.setdefault("classification", {})
    meta = enrich_metadata(text, meta)
    log.info(f"[NLP] Enriched metadata for {meta.get('filename', '?')}")
    return meta
