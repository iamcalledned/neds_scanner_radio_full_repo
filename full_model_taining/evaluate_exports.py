#!/usr/bin/env python3
"""Evaluate a trained Hugging Face Whisper model on paired export WAV/JSON files."""

from __future__ import annotations

import argparse
import csv
import json
import re
import time
from pathlib import Path

import jiwer
import torch
from transformers import AutoModelForSpeechSeq2Seq, AutoProcessor, pipeline


def normalize(text: str) -> str:
    text = text.lower().replace("-", " ")
    text = re.sub(r"[^\w\s']", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def scores(reference: str, hypothesis: str) -> dict:
    reference = normalize(reference)
    hypothesis = normalize(hypothesis)
    word_result = jiwer.process_words(reference, hypothesis)
    return {
        "wer": 100.0 * word_result.wer,
        "hits": word_result.hits,
        "substitutions": word_result.substitutions,
        "deletions": word_result.deletions,
        "insertions": word_result.insertions,
        "reference_words": len(reference.split()),
        "cer": 100.0 * jiwer.cer(reference, hypothesis),
    }


def corpus_scores(rows: list[dict], hypothesis_key: str) -> dict:
    references = [normalize(row["reference"]) for row in rows]
    hypotheses = [normalize(row[hypothesis_key]) for row in rows]
    result = jiwer.process_words(references, hypotheses)
    return {
        "wer": 100.0 * result.wer,
        "cer": 100.0 * jiwer.cer(references, hypotheses),
        "hits": result.hits,
        "substitutions": result.substitutions,
        "deletions": result.deletions,
        "insertions": result.insertions,
        "reference_words": sum(len(text.split()) for text in references),
    }


def main() -> None:
    here = Path(__file__).resolve().parent
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", type=Path, default=here / "training_config.json")
    parser.add_argument("--exports-dir", type=Path)
    parser.add_argument("--model-dir", type=Path)
    parser.add_argument("--output-dir", type=Path)
    args = parser.parse_args()

    config_file = args.config.expanduser().resolve()
    config = json.loads(config_file.read_text(encoding="utf-8"))

    def configured_path(value: str) -> Path:
        path = Path(value).expanduser()
        return (path if path.is_absolute() else config_file.parent / path).resolve()

    exports_dir = (
        args.exports_dir.expanduser().resolve()
        if args.exports_dir
        else configured_path(config["exports_dir"]) / "eval"
    )
    model_dir = (
        args.model_dir.expanduser().resolve()
        if args.model_dir
        else configured_path(config["output_dir"]) / "best"
    )
    output_dir = (
        args.output_dir.expanduser().resolve()
        if args.output_dir
        else configured_path(config["output_dir"]) / "evaluation"
    )
    output_dir.mkdir(parents=True, exist_ok=True)

    pairs = []
    for json_path in sorted(exports_dir.glob("*.json")):
        wav_path = json_path.with_suffix(".wav")
        if not wav_path.is_file():
            continue
        metadata = json.loads(json_path.read_text(encoding="utf-8"))
        reference = (metadata.get("edited_transcript") or "").strip()
        if not reference:
            continue
        pairs.append((json_path, wav_path, metadata, reference))
    if not pairs:
        raise SystemExit(f"No usable WAV/JSON pairs in {exports_dir}")

    dtype = torch.float16 if torch.cuda.is_available() else torch.float32
    device = 0 if torch.cuda.is_available() else -1
    processor = AutoProcessor.from_pretrained(model_dir, local_files_only=True)
    model = AutoModelForSpeechSeq2Seq.from_pretrained(
        model_dir,
        local_files_only=True,
        dtype=dtype,
        low_cpu_mem_usage=True,
    )
    model.generation_config.forced_decoder_ids = None
    if getattr(model.generation_config, "lang_to_id", None):
        model.generation_config.language = "en"
        model.generation_config.task = "transcribe"
    else:
        model.generation_config.language = None
        model.generation_config.task = None
    transcriber = pipeline(
        "automatic-speech-recognition",
        model=model,
        tokenizer=processor.tokenizer,
        feature_extractor=processor.feature_extractor,
        dtype=dtype,
        device=device,
        chunk_length_s=30,
        stride_length_s=3,
        ignore_warning=True,
    )

    rows = []
    started = time.time()
    for index, (json_path, wav_path, metadata, reference) in enumerate(pairs, start=1):
        print(f"[{index}/{len(pairs)}] {wav_path.name}", flush=True)
        prediction = transcriber(str(wav_path))["text"].strip()
        extra = metadata.get("extra") if isinstance(metadata.get("extra"), dict) else {}
        baseline = (
            extra.get("raw_transcript")
            or extra.get("transcript")
            or metadata.get("transcript")
            or ""
        ).strip()
        model_scores = scores(reference, prediction)
        baseline_scores = scores(reference, baseline)
        rows.append(
            {
                "filename": wav_path.name,
                "duration": float(metadata.get("duration") or 0.0),
                "town": metadata.get("town"),
                "category": metadata.get("category"),
                "reference": reference,
                "model_prediction": prediction,
                "baseline_transcript": baseline,
                "model_wer": model_scores["wer"],
                "model_cer": model_scores["cer"],
                "baseline_wer": baseline_scores["wer"],
                "baseline_cer": baseline_scores["cer"],
                "model_errors": {
                    key: model_scores[key]
                    for key in ("substitutions", "deletions", "insertions")
                },
            }
        )

    model_corpus = corpus_scores(rows, "model_prediction")
    baseline_corpus = corpus_scores(rows, "baseline_transcript")
    summary = {
        "model": str(model_dir),
        "exports_dir": str(exports_dir),
        "files": len(rows),
        "audio_hours": sum(row["duration"] for row in rows) / 3600,
        "runtime_seconds": time.time() - started,
        "fine_tuned_model": model_corpus,
        "exported_transcript_baseline": baseline_corpus,
        "absolute_wer_improvement": baseline_corpus["wer"] - model_corpus["wer"],
        "relative_wer_reduction_percent": (
            100.0 * (baseline_corpus["wer"] - model_corpus["wer"]) / baseline_corpus["wer"]
            if baseline_corpus["wer"]
            else None
        ),
    }
    report = {"summary": summary, "files": rows}
    (output_dir / "eval_report.json").write_text(
        json.dumps(report, indent=2, ensure_ascii=False), encoding="utf-8"
    )
    with (output_dir / "eval_report.csv").open("w", encoding="utf-8", newline="") as handle:
        fieldnames = [
            "filename",
            "duration",
            "town",
            "category",
            "model_wer",
            "model_cer",
            "baseline_wer",
            "baseline_cer",
            "reference",
            "model_prediction",
            "baseline_transcript",
        ]
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({key: row.get(key) for key in fieldnames})

    print(json.dumps(summary, indent=2))
    print(f"Reports saved to {output_dir}")


if __name__ == "__main__":
    main()
