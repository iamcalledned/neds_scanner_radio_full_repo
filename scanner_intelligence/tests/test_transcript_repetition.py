import sys
import unittest
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[2]
TRANSCRIBER_ROOT = PROJECT_ROOT / "transcriber"
for path in (str(PROJECT_ROOT), str(TRANSCRIBER_ROOT)):
    if path not in sys.path:
        sys.path.insert(0, path)

from mcp_functions.transcribe_wavefile import _adaptive_retry_reasons
from mcp_tools.scoring import score_transcript
from shared.transcript_quality import repetition_hallucination_metrics


class TranscriptRepetitionTest(unittest.TestCase):
    def test_received_loop_is_unusable_and_requires_retry(self) -> None:
        text = " ".join(["Received."] * 55)

        metrics = repetition_hallucination_metrics(text)
        quality = score_transcript(text, duration=16.3, rms=0.065)

        self.assertTrue(metrics["is_repetition_loop"])
        self.assertEqual(metrics["dominant_token"], "received")
        self.assertTrue(quality["needs_retry"])
        self.assertTrue(quality["needs_review"])
        self.assertIn("repetition_loop", quality["reasons"])
        self.assertLess(quality["score"], 0.5)

    def test_short_acknowledgment_and_mixed_traffic_are_preserved(self) -> None:
        self.assertFalse(
            repetition_hallucination_metrics("Received.")[
                "is_repetition_loop"
            ]
        )
        mixed = (
            "Received. Received. Engine four responding to Main Street for "
            "the alarm."
        )
        self.assertFalse(
            repetition_hallucination_metrics(mixed)["is_repetition_loop"]
        )

    def test_repeated_phrase_loop_is_detected(self) -> None:
        text = " ".join(["Thank you for watching."] * 8)
        self.assertTrue(
            repetition_hallucination_metrics(text)["is_repetition_loop"]
        )

    def test_adaptive_decode_retries_repetition_loop(self) -> None:
        class Segment:
            compression_ratio = 1.0

        reasons = _adaptive_retry_reasons(
            text=" ".join(["Received."] * 20),
            segments=[Segment()],
            duration=20.0,
            compression_limit=4.0,
            compression_min_words_per_second=2.5,
            words_per_second_limit=3.0,
        )
        self.assertIn("repetition_loop", reasons)


if __name__ == "__main__":
    unittest.main()
