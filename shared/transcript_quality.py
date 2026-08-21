"""Small deterministic transcript checks shared by transcription and web AI."""

from __future__ import annotations

import re
from typing import Any, Dict, Sequence


def normalized_words(text: str) -> list[str]:
    """Return lowercase word tokens without punctuation."""
    return re.findall(r"\b[\w']+\b", (text or "").casefold())


def _longest_same_token_run(words: Sequence[str]) -> int:
    longest = 0
    current = 0
    previous = None
    for word in words:
        if word == previous:
            current += 1
        else:
            previous = word
            current = 1
        longest = max(longest, current)
    return longest


def _best_repeated_ngram(words: Sequence[str]) -> tuple[int, int, float]:
    """Return n-gram size, consecutive repeats, and transcript coverage."""
    best = (0, 0, 0.0)
    word_count = len(words)
    for size in range(2, min(8, word_count // 3) + 1):
        for start in range(0, word_count - (size * 2) + 1):
            phrase = tuple(words[start : start + size])
            repeats = 1
            cursor = start + size
            while (
                cursor + size <= word_count
                and tuple(words[cursor : cursor + size]) == phrase
            ):
                repeats += 1
                cursor += size
            coverage = (repeats * size) / word_count
            if (repeats, coverage) > (best[1], best[2]):
                best = (size, repeats, coverage)
    return best


def repetition_hallucination_metrics(text: str) -> Dict[str, Any]:
    """Identify severe Whisper-style token or phrase repetition loops.

    A short legitimate acknowledgment such as ``Received`` is preserved.
    Detection requires either a dominant long single-token run or a repeated
    multi-word phrase covering most of a longer transcript.
    """
    words = normalized_words(text)
    word_count = len(words)
    if not words:
        return {
            "is_repetition_loop": False,
            "word_count": 0,
            "dominant_token": "",
            "dominant_token_count": 0,
            "dominant_token_ratio": 0.0,
            "longest_token_run": 0,
            "repeated_ngram_size": 0,
            "repeated_ngram_count": 0,
            "repeated_ngram_coverage": 0.0,
        }

    counts: Dict[str, int] = {}
    for word in words:
        counts[word] = counts.get(word, 0) + 1
    dominant_token, dominant_count = max(
        counts.items(),
        key=lambda item: item[1],
    )
    dominant_ratio = dominant_count / word_count
    longest_run = _longest_same_token_run(words)
    ngram_size, ngram_count, ngram_coverage = _best_repeated_ngram(words)

    token_loop = (
        word_count >= 8
        and dominant_count >= 8
        and dominant_ratio >= 0.60
        and longest_run >= 6
    )
    phrase_loop = (
        word_count >= 12
        and ngram_size >= 2
        and ngram_count >= 3
        and ngram_coverage >= 0.75
    )
    return {
        "is_repetition_loop": token_loop or phrase_loop,
        "word_count": word_count,
        "dominant_token": dominant_token,
        "dominant_token_count": dominant_count,
        "dominant_token_ratio": round(dominant_ratio, 4),
        "longest_token_run": longest_run,
        "repeated_ngram_size": ngram_size,
        "repeated_ngram_count": ngram_count,
        "repeated_ngram_coverage": round(ngram_coverage, 4),
    }
