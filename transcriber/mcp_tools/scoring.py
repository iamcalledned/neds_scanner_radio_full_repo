from pathlib import Path



from typing import Any, Dict, Optional, Tuple, List

import os

import numpy as np


from faster_whisper import WhisperModel
from transformers.utils import logging as hf_logging

from mcp.server.fastmcp import FastMCP, Context





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
