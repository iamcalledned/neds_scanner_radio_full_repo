from __future__ import annotations

from pathlib import Path
from typing import Any, Callable, Optional


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

        text = " ".join([segment.text for segment in segments]).strip()
        snippet = (text[:140] + "…") if len(text) > 140 else text

    log.info(f"[transcribe_wavefile] {snippet} | duration={get_duration_fn(wav_path):.1f}s | rms={get_rms_fn(wav_path):.6f}")
    return text
