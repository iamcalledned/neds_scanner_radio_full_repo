from __future__ import annotations

import re
from pathlib import Path
from typing import Any, Callable, Optional

from shared.transcript_quality import repetition_hallucination_metrics


def _normalized_words(text: str) -> list[str]:
    return re.findall(r"\b[\w']+\b", text.lower())


def _adaptive_retry_reasons(
    *,
    text: str,
    segments: list[Any],
    duration: float,
    compression_limit: float,
    compression_min_words_per_second: float,
    words_per_second_limit: float,
) -> list[str]:
    reasons: list[str] = []
    compression = max(
        (float(segment.compression_ratio) for segment in segments), default=0.0
    )
    word_rate = len(_normalized_words(text)) / max(duration, 0.1)
    if (
        compression > compression_limit
        and word_rate > compression_min_words_per_second
    ):
        reasons.append("compression")
    if word_rate > words_per_second_limit:
        reasons.append("word_rate")
    if repetition_hallucination_metrics(text)["is_repetition_loop"]:
        reasons.append("repetition_loop")
    return reasons


def _should_apply_squelch_gate(
    *,
    text: str,
    segments: list[Any],
    duration: float,
    enabled: bool,
    max_duration: float,
    no_speech_threshold: float,
    ambiguous_no_speech_threshold: float,
) -> bool:
    """Suppress only calibrated short-clip nuisance hallucinations.

    A no-speech threshold alone is unsafe for scanner traffic: a legitimate
    one-word "Received" call in the independent test set scored about 0.17.
    Require both a noise-like score and a known nuisance/repetition pattern.
    """
    if not enabled or duration > max_duration or not segments:
        return False
    selected_no_speech = min(
        float(segment.no_speech_prob) for segment in segments
    )
    if selected_no_speech < no_speech_threshold:
        return False

    words = _normalized_words(text)
    if not words:
        return False
    if words == ["pop"] or any(word.startswith("fart") for word in words):
        return True
    if words == ["google", "transcript"]:
        return True
    if (
        words == ["roger"]
        and selected_no_speech >= ambiguous_no_speech_threshold
    ):
        return True
    return (
        len(words) >= 8
        and max(words.count(word) for word in set(words)) / len(words) >= 0.5
    )


def transcribe_wavefile(
    *,
    state: Any,
    wav_path: Path,
    task: str,
    language: str,
    transcribe_settings: Optional[dict[str, Any]],
    build_transcribe_kwargs_fn: Callable[..., dict[str, Any]],
    log: Any,
    get_duration_fn: Callable[[Path], float],
    get_rms_fn: Callable[[Path], float],
) -> str:
    """
    Transcribe a preprocessed WAV using the loaded faster-whisper model.
    Serialized with a GPU lock.
    """
    kwargs = build_transcribe_kwargs_fn(
        task=task,
        language=language,
        profile_settings=transcribe_settings,
    )
    adaptive_retry = bool(kwargs.pop("adaptive_retry", False))
    retry_beam_size = int(kwargs.pop("retry_beam_size", 1))
    retry_compression_ratio = float(kwargs.pop("retry_compression_ratio", 4.0))
    retry_compression_min_words_per_second = float(
        kwargs.pop("retry_compression_min_words_per_second", 2.5)
    )
    retry_words_per_second = float(kwargs.pop("retry_words_per_second", 3.0))
    retry_max_metric_growth = float(
        kwargs.pop("retry_max_metric_growth", 1.02)
    )
    squelch_gate = bool(kwargs.pop("squelch_gate", False))
    squelch_max_duration = float(kwargs.pop("squelch_max_duration", 7.0))
    squelch_no_speech_threshold = float(
        kwargs.pop("squelch_no_speech_threshold", 0.16)
    )
    squelch_ambiguous_no_speech_threshold = float(
        kwargs.pop("squelch_ambiguous_no_speech_threshold", 0.30)
    )
    log.info(
        "[transcribe_wavefile] decode settings: "
        f"beam_size={kwargs.get('beam_size')} "
        f"vad_filter={kwargs.get('vad_filter')} "
        f"language={kwargs.get('language')} "
        f"task={kwargs.get('task')} "
        f"initial_prompt_set={bool(kwargs.get('initial_prompt'))}"
    )

    log.info("[transcribe_wavefile] Acquiring GPU gate…")
    with state.gate.acquire("whisper", timeout_s=120):
        log.info("[transcribe_wavefile] GPU gate acquired, running model.transcribe()…")
        segments, _ = state.model.transcribe(str(wav_path), **kwargs)
        segments = list(segments)
        text = " ".join(segment.text for segment in segments).strip()
        duration = get_duration_fn(wav_path)
        retry_reasons = _adaptive_retry_reasons(
            text=text,
            segments=segments,
            duration=duration,
            compression_limit=retry_compression_ratio,
            compression_min_words_per_second=(
                retry_compression_min_words_per_second
            ),
            words_per_second_limit=retry_words_per_second,
        )
        if adaptive_retry and retry_reasons:
            retry_kwargs = dict(kwargs)
            retry_kwargs["beam_size"] = retry_beam_size
            log.warning(
                "[transcribe_wavefile] adaptive retry: "
                f"reasons={','.join(retry_reasons)} "
                f"beam_size={kwargs.get('beam_size')}->{retry_beam_size}"
            )
            retry_segments, _ = state.model.transcribe(
                str(wav_path), **retry_kwargs
            )
            retry_segments = list(retry_segments)
            retry_text = " ".join(
                segment.text for segment in retry_segments
            ).strip()
            initial_compression = max(
                (
                    float(segment.compression_ratio)
                    for segment in segments
                ),
                default=0.0,
            )
            retry_compression = max(
                (
                    float(segment.compression_ratio)
                    for segment in retry_segments
                ),
                default=0.0,
            )
            initial_word_rate = len(_normalized_words(text)) / max(
                duration, 0.1
            )
            retry_word_rate = len(_normalized_words(retry_text)) / max(
                duration, 0.1
            )
            if (
                retry_compression
                <= initial_compression * retry_max_metric_growth
                and retry_word_rate
                <= initial_word_rate * retry_max_metric_growth
            ):
                segments = retry_segments
                text = retry_text
            else:
                log.warning(
                    "[transcribe_wavefile] adaptive retry rejected: "
                    f"compression={initial_compression:.3f}->{retry_compression:.3f} "
                    f"word_rate={initial_word_rate:.3f}->{retry_word_rate:.3f}"
                )
        if _should_apply_squelch_gate(
            text=text,
            segments=segments,
            duration=duration,
            enabled=squelch_gate,
            max_duration=squelch_max_duration,
            no_speech_threshold=squelch_no_speech_threshold,
            ambiguous_no_speech_threshold=(
                squelch_ambiguous_no_speech_threshold
            ),
        ):
            selected_no_speech = min(
                float(segment.no_speech_prob) for segment in segments
            )
            log.info(
                "[transcribe_wavefile] squelch gate suppressed short noise-like "
                f"output: duration={duration:.3f}s "
                f"no_speech={selected_no_speech:.3f} "
                f"threshold={squelch_no_speech_threshold:.3f}"
            )
            text = ""
        snippet = (text[:140] + "…") if len(text) > 140 else text

    log.info(f"[transcribe_wavefile] {snippet} | duration={duration:.1f}s | rms={get_rms_fn(wav_path):.6f}")
    return text
