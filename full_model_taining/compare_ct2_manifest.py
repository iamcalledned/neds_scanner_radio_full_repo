#!/usr/bin/env python3
"""Compare CTranslate2 Whisper models on segmented JSONL manifests.

The silence/noise gate is selected using only the evaluation manifest, then
frozen and reported on the test manifest.
"""

from __future__ import annotations

import argparse
import json
import re
from pathlib import Path

import numpy as np
import soundfile as sf
from faster_whisper import WhisperModel
from jiwer import cer, wer
from scipy.signal import resample_poly


def normalized(text: str) -> str:
    text = text.lower()
    text = re.sub(r"\bten[- ]four\b", "10 4", text)
    text = re.sub(r"[^a-z0-9]+", " ", text)
    return " ".join(text.split())


def load_rows(path: Path) -> list[dict]:
    return [json.loads(line) for line in path.read_text().splitlines() if line.strip()]


def load_audio(row: dict) -> np.ndarray:
    audio, rate = sf.read(row["audio"], dtype="float32", always_2d=False)
    if audio.ndim == 2:
        audio = audio.mean(axis=1)
    start = int(round(float(row.get("start", 0.0)) * rate))
    end = int(round(float(row.get("end", len(audio) / rate)) * rate))
    audio = audio[max(0, start):min(len(audio), end)]
    if rate != 16000:
        from math import gcd
        divisor = gcd(rate, 16000)
        audio = resample_poly(audio, 16000 // divisor, rate // divisor).astype(np.float32)
    return audio


def score(rows: list[dict], predictions: list[dict], gate: tuple[float, float] | None) -> dict:
    refs, hyps = [], []
    false_positive = 0
    negatives = 0
    for row, pred in zip(rows, predictions):
        hypothesis = pred["text"]
        if gate and pred["no_speech_prob"] >= gate[0] and pred["avg_logprob"] <= gate[1]:
            hypothesis = ""
        reference = row["text"]
        refs.append(reference)
        hyps.append(hypothesis)
        if not normalized(reference):
            negatives += 1
            false_positive += bool(normalized(hypothesis))
    return {
        "wer": 100 * wer(refs, hyps),
        "normalized_wer": 100 * wer([normalized(x) for x in refs], [normalized(x) for x in hyps]),
        "cer": 100 * cer(refs, hyps),
        "negative_count": negatives,
        "negative_false_positives": false_positive,
        "negative_false_positive_rate": 100 * false_positive / negatives if negatives else 0.0,
    }


def choose_gate(rows: list[dict], predictions: list[dict]) -> tuple[float, float]:
    candidates = []
    for nsp in np.arange(0.1, 1.0, 0.05):
        for logprob in np.arange(-1.5, 0.01, 0.1):
            result = score(rows, predictions, (float(nsp), float(logprob)))
            objective = result["normalized_wer"] + 0.25 * result["negative_false_positive_rate"]
            candidates.append((objective, result["normalized_wer"], -nsp, logprob))
    best = min(candidates)
    return -best[2], best[3]


def transcribe(model_path: str, rows: list[dict], beam_size: int) -> list[dict]:
    model = WhisperModel(model_path, device="cuda", compute_type="float16")
    results = []
    for index, row in enumerate(rows, 1):
        segments, _ = model.transcribe(
            load_audio(row), language="en", task="transcribe", beam_size=beam_size,
            temperature=0.0, condition_on_previous_text=False, vad_filter=False,
            word_timestamps=False,
        )
        segments = list(segments)
        results.append({
            "text": " ".join(segment.text.strip() for segment in segments).strip(),
            "no_speech_prob": max((segment.no_speech_prob for segment in segments), default=1.0),
            "avg_logprob": max((segment.avg_logprob for segment in segments), default=-99.0),
        })
        if index % 25 == 0 or index == len(rows):
            print(f"  {index}/{len(rows)}", flush=True)
    return results


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--eval-manifest", type=Path, required=True)
    parser.add_argument("--test-manifest", type=Path, required=True)
    parser.add_argument("--model", action="append", nargs=2, metavar=("NAME", "PATH"), required=True)
    parser.add_argument("--beam", type=int, default=2)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()

    eval_rows, test_rows = load_rows(args.eval_manifest), load_rows(args.test_manifest)
    report = {"eval_manifest": str(args.eval_manifest), "test_manifest": str(args.test_manifest), "models": {}}
    for name, path in args.model:
        print(f"{name}: evaluation set", flush=True)
        eval_predictions = transcribe(path, eval_rows, args.beam)
        gate = choose_gate(eval_rows, eval_predictions)
        print(f"{name}: test set (gate selected on eval: no_speech>={gate[0]:.2f}, avg_logprob<={gate[1]:.2f})", flush=True)
        test_predictions = transcribe(path, test_rows, args.beam)
        report["models"][name] = {
            "gate": {"no_speech_threshold": gate[0], "avg_logprob_threshold": gate[1]},
            "eval_raw": score(eval_rows, eval_predictions, None),
            "eval_gated": score(eval_rows, eval_predictions, gate),
            "test_raw": score(test_rows, test_predictions, None),
            "test_gated": score(test_rows, test_predictions, gate),
            "eval_predictions": eval_predictions,
            "test_predictions": test_predictions,
        }
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps({k: {m: v[m] for m in ("gate", "eval_raw", "eval_gated", "test_raw", "test_gated")}
                      for k, v in report["models"].items()}, indent=2))


if __name__ == "__main__":
    main()
