from types import SimpleNamespace

from mcp_functions.transcribe_wavefile import _should_apply_squelch_gate


def segment(no_speech_probability: float) -> SimpleNamespace:
    return SimpleNamespace(no_speech_prob=no_speech_probability)


def should_gate(
    text: str,
    no_speech_probability: float,
    *,
    duration: float = 5.2,
    enabled: bool = True,
) -> bool:
    return _should_apply_squelch_gate(
        text=text,
        segments=[segment(no_speech_probability)],
        duration=duration,
        enabled=enabled,
        max_duration=7.0,
        no_speech_threshold=0.16,
        ambiguous_no_speech_threshold=0.30,
    )


def test_suppresses_calibrated_nuisance_hallucinations() -> None:
    assert should_gate("POP!", 0.31)
    assert should_gate("Not Farting, but It's Farting.", 0.17)
    assert should_gate("Google Transcript", 0.34)


def test_preserves_unrelated_short_speech_above_base_threshold() -> None:
    assert not should_gate("Received.", 0.14)
    assert not should_gate("Received.", 0.18)
    assert not should_gate("Unrecognized short speech.", 0.40)


def test_ambiguous_roger_requires_stronger_no_speech_evidence() -> None:
    assert not should_gate("Roger.", 0.20)
    assert should_gate("Roger.", 0.31)


def test_suppresses_short_noise_repetition() -> None:
    assert should_gate(
        "Tsk, tsk, tsk, tsk, tsk, tsk, tsk, tsk.",
        0.40,
    )


def test_preserves_long_clip_and_mixed_segments() -> None:
    assert not should_gate("POP!", 0.40, duration=7.1)
    assert not _should_apply_squelch_gate(
        text="POP!",
        segments=[segment(0.40), segment(0.10)],
        duration=6.0,
        enabled=True,
        max_duration=7.0,
        no_speech_threshold=0.16,
        ambiguous_no_speech_threshold=0.30,
    )


def test_disabled_or_empty_never_suppresses() -> None:
    assert not should_gate("POP!", 0.40, enabled=False)
    assert not _should_apply_squelch_gate(
        text="POP!",
        segments=[],
        duration=5.0,
        enabled=True,
        max_duration=7.0,
        no_speech_threshold=0.16,
        ambiguous_no_speech_threshold=0.30,
    )
