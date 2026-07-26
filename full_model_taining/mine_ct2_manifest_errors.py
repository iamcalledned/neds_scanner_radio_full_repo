#!/usr/bin/env python3
"""Transcribe a JSONL manifest with a CT2 Whisper model and record per-row errors."""

from __future__ import annotations

import argparse
import json
import re
import time
from pathlib import Path

import jiwer
from faster_whisper import WhisperModel


def normalized_text(text: str) -> str:
    return re.sub(r"\s+", " ", re.sub(r"[^\w\s']", " ", text.lower())).strip()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--model", type=Path, required=True)
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--beam-size", type=int, default=2)
    args = parser.parse_args()

    model_path = args.model.expanduser().resolve()
    manifest_path = args.manifest.expanduser().resolve()
    output_dir = args.output_dir.expanduser().resolve()
    if output_dir.exists() and any(output_dir.iterdir()):
        raise SystemExit(f"Refusing to overwrite non-empty output: {output_dir}")

    rows = [
        json.loads(line)
        for line in manifest_path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    if not rows:
        raise SystemExit(f"No rows in {manifest_path}")

    output_dir.mkdir(parents=True, exist_ok=True)
    predictions_path = output_dir / "predictions.jsonl"
    model = WhisperModel(str(model_path), device="cuda", compute_type="float16")
    results: list[dict] = []
    started = time.time()

    with predictions_path.open("w", encoding="utf-8") as handle:
        for index, row in enumerate(rows, start=1):
            segments, _ = model.transcribe(
                row["audio"],
                task="transcribe",
                language="en",
                beam_size=args.beam_size,
                word_timestamps=False,
                condition_on_previous_text=False,
                vad_filter=False,
                temperature=0.0,
            )
            prediction = " ".join(segment.text.strip() for segment in segments).strip()
            reference_normalized = normalized_text(str(row["text"]))
            prediction_normalized = normalized_text(prediction)
            wer = jiwer.wer(reference_normalized, prediction_normalized)
            words = prediction_normalized.split()
            repeated = (
                len(words) >= 8
                and max(words.count(word) for word in set(words)) / len(words) >= 0.5
            )
            result = {
                "id": str(row["id"]),
                "audio": row["audio"],
                "reference": row["text"],
                "prediction": prediction,
                "reference_normalized": reference_normalized,
                "prediction_normalized": prediction_normalized,
                "wer": wer,
                "repeated": repeated,
            }
            results.append(result)
            handle.write(json.dumps(result, ensure_ascii=False) + "\n")
            if index == 1 or index % 25 == 0 or index == len(rows):
                elapsed = time.time() - started
                rate = elapsed / index
                remaining = rate * (len(rows) - index)
                print(
                    f"{index}/{len(rows)} elapsed={elapsed:.1f}s "
                    f"eta={remaining:.1f}s",
                    flush=True,
                )

    references = [result["reference_normalized"] for result in results]
    hypotheses = [result["prediction_normalized"] for result in results]
    summary = {
        "model": str(model_path),
        "manifest": str(manifest_path),
        "rows": len(results),
        "aggregate_wer": jiwer.wer(references, hypotheses),
        "exact_matches": sum(result["wer"] == 0 for result in results),
        "wer_at_least_0_25": sum(result["wer"] >= 0.25 for result in results),
        "wer_at_least_0_50": sum(result["wer"] >= 0.50 for result in results),
        "wer_at_least_1_00": sum(result["wer"] >= 1.00 for result in results),
        "repeated_predictions": sum(result["repeated"] for result in results),
        "forbidden_phrase_hits": [
            result["id"]
            for result in results
            if "fart" in result["prediction_normalized"]
        ],
        "runtime_seconds": time.time() - started,
    }
    (output_dir / "summary.json").write_text(
        json.dumps(summary, indent=2) + "\n", encoding="utf-8"
    )
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
