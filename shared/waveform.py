"""Compact, presentation-ready waveform envelopes for scanner audio."""

from __future__ import annotations

import base64
import math
import sys
import wave
from array import array
from pathlib import Path
from typing import Any


WAVEFORM_VERSION = 1
DEFAULT_WAVEFORM_POINTS = 512


def _percentile(values: list[float], fraction: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    index = min(
        len(ordered) - 1,
        max(0, round((len(ordered) - 1) * fraction)),
    )
    return ordered[index]


def extract_waveform(
    wav_path: str | Path,
    *,
    points: int = DEFAULT_WAVEFORM_POINTS,
) -> dict[str, Any]:
    """Return a noise-floor-aware RMS envelope encoded as unsigned bytes.

    The result is deliberately small enough to travel with a call API payload.
    It describes the cleaned audio used for transcription, while the WAV stays
    authoritative for playback.
    """
    requested_points = max(32, min(int(points), 2048))
    path = Path(wav_path)
    with wave.open(str(path), "rb") as wav_file:
        channels = wav_file.getnchannels()
        sample_width = wav_file.getsampwidth()
        frame_rate = wav_file.getframerate()
        frame_count = wav_file.getnframes()
        raw = wav_file.readframes(frame_count)

    if sample_width != 2:
        raise ValueError(
            f"waveform extraction requires 16-bit PCM; got {sample_width * 8}-bit"
        )
    if channels < 1 or frame_count < 1:
        raise ValueError("waveform extraction requires non-empty PCM audio")

    samples = array("h")
    samples.frombytes(raw)
    if sys.byteorder != "little":
        samples.byteswap()

    if channels > 1:
        mono = array("h")
        for offset in range(0, len(samples), channels):
            frame = samples[offset : offset + channels]
            if frame:
                mono.append(round(sum(frame) / len(frame)))
        samples = mono

    point_count = min(requested_points, len(samples))
    bucket_size = max(1, math.ceil(len(samples) / point_count))
    levels: list[float] = []
    for start in range(0, len(samples), bucket_size):
        bucket = samples[start : start + bucket_size]
        if not bucket:
            continue
        mean_square = sum(float(sample) ** 2 for sample in bucket) / len(bucket)
        levels.append(math.sqrt(mean_square) / 32768.0)

    # Suppress the stable radio noise floor and normalize against the upper
    # envelope rather than one isolated click or squelch spike.
    noise_floor = _percentile(levels, 0.10)
    ceiling = _percentile(levels, 0.98)
    span = max(ceiling - noise_floor, 1e-9)
    encoded = bytearray()
    for level in levels:
        normalized = max(0.0, min(1.0, (level - noise_floor) / span))
        display_level = normalized**0.72
        encoded.append(round(display_level * 255))

    duration = len(samples) / frame_rate if frame_rate else 0.0
    return {
        "version": WAVEFORM_VERSION,
        "encoding": "uint8-base64",
        "points": len(encoded),
        "sample_rate": frame_rate,
        "duration_seconds": round(duration, 3),
        "peaks": base64.b64encode(bytes(encoded)).decode("ascii"),
    }


def decode_waveform_peaks(waveform: dict[str, Any]) -> list[int]:
    """Decode a stored envelope; primarily useful for validation and tests."""
    if waveform.get("encoding") != "uint8-base64":
        raise ValueError("unsupported waveform encoding")
    peaks = list(base64.b64decode(waveform.get("peaks") or "", validate=True))
    if len(peaks) != int(waveform.get("points") or 0):
        raise ValueError("waveform point count does not match encoded peaks")
    return peaks
