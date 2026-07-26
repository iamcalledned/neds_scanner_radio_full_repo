#!/usr/bin/env python3
"""Compare a CT2 model with transcripts already stored in scanner JSON files."""

from __future__ import annotations

import argparse
import json
import re
from pathlib import Path

from faster_whisper import WhisperModel
from jiwer import cer, wer


def normalize(text: str) -> str:
    text = re.sub(r"\bten[- ]four\b", "10 4", text.lower())
    return " ".join(re.sub(r"[^a-z0-9]+", " ", text).split())


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--archive-dir", type=Path, required=True)
    parser.add_argument("--date", required=True)
    parser.add_argument("--model", required=True)
    parser.add_argument("--model-name", default="candidate")
    parser.add_argument("--beam", type=int, default=2)
    parser.add_argument("--recursive", action="store_true")
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()

    rows = []
    pattern = f"rec_{args.date}_*.wav"
    audio_paths = (
        args.archive_dir.rglob(pattern)
        if args.recursive
        else args.archive_dir.glob(pattern)
    )
    for wav in sorted(audio_paths):
        sidecar = wav.with_suffix(".json")
        if not sidecar.is_file():
            continue
        payload = json.loads(sidecar.read_text(encoding="utf-8"))
        reference = str(payload.get("transcript") or "").strip()
        if not reference:
            continue
        rows.append(
            {
                "audio": str(wav),
                "json": str(sidecar),
                "stored_v101_transcript": reference,
                "stored_model": payload.get("transcription_model_key")
                or payload.get("transcription_model"),
            }
        )
    if not rows:
        raise SystemExit(f"No WAV/JSON pairs with stored transcripts for {args.date}")

    model = WhisperModel(args.model, device="cuda", compute_type="float16")
    for index, row in enumerate(rows, 1):
        segments, _ = model.transcribe(
            row["audio"],
            language="en",
            task="transcribe",
            beam_size=args.beam,
            temperature=0.0,
            condition_on_previous_text=False,
            vad_filter=False,
            word_timestamps=False,
        )
        segments = list(segments)
        candidate = " ".join(segment.text.strip() for segment in segments).strip()
        row["candidate_transcript"] = candidate
        row["raw_wer"] = 100.0 * wer(row["stored_v101_transcript"], candidate)
        row["normalized_wer"] = 100.0 * wer(
            normalize(row["stored_v101_transcript"]), normalize(candidate)
        )
        row["raw_cer"] = 100.0 * cer(row["stored_v101_transcript"], candidate)
        print(f"{index}/{len(rows)} {Path(row['audio']).name}", flush=True)

    references = [row["stored_v101_transcript"] for row in rows]
    hypotheses = [row["candidate_transcript"] for row in rows]
    summary = {
        "date": args.date,
        "samples": len(rows),
        "reference": "stored top-level transcript field (V101)",
        "candidate_model": args.model,
        "candidate_name": args.model_name,
        "beam_size": args.beam,
        "aggregate_raw_wer": 100.0 * wer(references, hypotheses),
        "aggregate_normalized_wer": 100.0
        * wer([normalize(x) for x in references], [normalize(x) for x in hypotheses]),
        "aggregate_raw_cer": 100.0 * cer(references, hypotheses),
        "exact_normalized_matches": sum(
            normalize(reference) == normalize(hypothesis)
            for reference, hypothesis in zip(references, hypotheses)
        ),
    }
    report = {"summary": summary, "calls": rows}
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
