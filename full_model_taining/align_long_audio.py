#!/usr/bin/env python3
"""Align manual transcripts to long audio and add <=30s segments to manifests."""

from __future__ import annotations

import argparse
import difflib
import hashlib
import json
import re
from pathlib import Path
from typing import Any

import numpy as np
import torch
from transformers import AutoModelForSpeechSeq2Seq, AutoProcessor, pipeline


def read_json(path: Path) -> dict:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise SystemExit(f"Could not read {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise SystemExit(f"Expected a JSON object in {path}")
    return value


def resolve_path(value: str, config_file: Path) -> Path:
    path = Path(value).expanduser()
    return (path if path.is_absolute() else config_file.parent / path).resolve()


def validate_model_reference(value: str) -> str:
    expanded = Path(value).expanduser()
    if expanded.is_absolute() or value.startswith((".", "~")):
        required = ("config.json", "model.safetensors")
        missing = [name for name in required if not (expanded / name).is_file()]
        if missing:
            raise SystemExit(
                f"Configured local model is missing {', '.join(missing)}: {expanded}\n"
                "Use a complete local Transformers model directory or a Hub ID such as "
                "'openai/whisper-medium.en'."
            )
        return str(expanded.resolve())
    return value


def jsonl_rows(path: Path) -> list[dict]:
    if not path.exists():
        return []
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line]


def cache_key(row: dict, model_id: str, segment_duration: float) -> str:
    payload = {
        "audio": row["audio"],
        "text": row["text"],
        "duration": row["duration"],
        "model": model_id,
        "segment_duration": segment_duration,
        "version": 1,
    }
    return hashlib.sha256(
        json.dumps(payload, sort_keys=True).encode("utf-8")
    ).hexdigest()


def normalized_word(value: str) -> str:
    return re.sub(r"[^a-z0-9']", "", value.lower())


def timestamped_words(result: dict) -> list[dict]:
    words = []
    for chunk in result.get("chunks", []):
        timestamp = chunk.get("timestamp")
        text = str(chunk.get("text", "")).strip()
        if (
            not text
            or not isinstance(timestamp, (list, tuple))
            or len(timestamp) != 2
            or timestamp[0] is None
            or timestamp[1] is None
        ):
            continue
        normalized = normalized_word(text)
        if normalized:
            words.append(
                {
                    "word": normalized,
                    "start": float(timestamp[0]),
                    "end": float(timestamp[1]),
                }
            )
    return words


def align_and_segment(
    row: dict,
    predicted: list[dict],
    segment_duration: float,
    minimum_ratio: float,
    allow_low_confidence: bool,
) -> tuple[list[dict], dict]:
    manual_units = row["text"].split()
    manual_normalized = [normalized_word(word) for word in manual_units]
    predicted_normalized = [word["word"] for word in predicted]
    matcher = difflib.SequenceMatcher(None, manual_normalized, predicted_normalized, autojunk=False)
    ratio = matcher.ratio()
    if ratio < minimum_ratio and not allow_low_confidence:
        raise RuntimeError(
            f"alignment ratio {ratio:.3f} is below {minimum_ratio:.3f} for {row['audio']}"
        )

    anchors: dict[int, float] = {}
    for block in matcher.get_matching_blocks():
        for offset in range(block.size):
            manual_index = block.a + offset
            predicted_word = predicted[block.b + offset]
            anchors[manual_index] = (predicted_word["start"] + predicted_word["end"]) / 2

    count = len(manual_units)
    duration = float(row["duration"])
    if not anchors:
        centers = np.linspace(duration / (2 * count), duration - duration / (2 * count), count)
    else:
        indices = np.array([-0.5, *sorted(anchors), count - 0.5], dtype=float)
        times = np.array([0.0, *(anchors[i] for i in sorted(anchors)), duration], dtype=float)
        centers = np.interp(np.arange(count, dtype=float), indices, times)

    boundaries = np.empty(count + 1, dtype=float)
    boundaries[0] = 0.0
    boundaries[-1] = duration
    if count > 1:
        boundaries[1:-1] = (centers[:-1] + centers[1:]) / 2
    boundaries = np.maximum.accumulate(np.clip(boundaries, 0.0, duration))

    segments = []
    first_word = 0
    segment_number = 0
    while first_word < count:
        start_time = float(boundaries[first_word])
        last_word = first_word
        while (
            last_word + 1 < count
            and float(boundaries[last_word + 2]) - start_time <= segment_duration
        ):
            last_word += 1
        end_time = float(boundaries[last_word + 1])
        if end_time - start_time > segment_duration:
            center = float((centers[first_word] + centers[last_word]) / 2)
            start_time = max(0.0, center - segment_duration / 2)
            end_time = min(duration, start_time + segment_duration)
            start_time = max(0.0, end_time - segment_duration)
        segment_number += 1
        segment = dict(row)
        segment.update(
            {
                "text": " ".join(manual_units[first_word : last_word + 1]),
                "start": round(start_time, 3),
                "end": round(end_time, 3),
                "duration": round(end_time - start_time, 3),
                "source_duration": duration,
                "segment": segment_number,
                "alignment_ratio": round(ratio, 4),
            }
        )
        segments.append(segment)
        first_word = last_word + 1

    return segments, {
        "audio": row["audio"],
        "alignment_ratio": round(ratio, 4),
        "low_confidence": ratio < minimum_ratio,
        "manual_words": count,
        "recognized_words": len(predicted),
        "segments": len(segments),
    }


def main() -> None:
    here = Path(__file__).resolve().parent
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", type=Path, default=here / "training_config.json")
    parser.add_argument(
        "--rebuild-alignment-cache",
        action="store_true",
        help="Ignore prior aligned results and rebuild every long recording.",
    )
    args = parser.parse_args()
    config_file = args.config.expanduser().resolve()
    config = read_json(config_file)
    long_config: dict[str, Any] = config.get("long_audio", {})
    if not long_config.get("enabled", True):
        print("Long-audio alignment is disabled.")
        return

    manifests_dir = resolve_path(config.get("manifests_dir", "manifests"), config_file)
    model_id = validate_model_reference(
        config.get("alignment_model", config.get("model", "openai/whisper-medium.en"))
    )
    segment_duration = float(long_config.get("segment_duration", 29.0))
    if not 0 < segment_duration <= 30:
        raise SystemExit("long_audio.segment_duration must be greater than 0 and at most 30.")

    dtype = torch.float16 if torch.cuda.is_available() else torch.float32
    device = 0 if torch.cuda.is_available() else -1
    processor = AutoProcessor.from_pretrained(model_id)
    model = AutoModelForSpeechSeq2Seq.from_pretrained(
        model_id,
        dtype=dtype,
        low_cpu_mem_usage=True,
        use_safetensors=True,
    )
    model.generation_config.forced_decoder_ids = None
    if getattr(model.generation_config, "lang_to_id", None):
        model.generation_config.language = "en"
        model.generation_config.task = "transcribe"
    else:
        # English-only checkpoints do not define language/task token maps.
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
        stride_length_s=float(long_config.get("stride_length", 3.0)),
        ignore_warning=True,
    )

    report = {}
    cache_dir = manifests_dir / "alignment_cache"
    cache_dir.mkdir(parents=True, exist_ok=True)
    for split in ("train", "eval", "test"):
        manifest_path = manifests_dir / f"{split}.jsonl"
        pending = jsonl_rows(manifests_dir / f"{split}_long.jsonl")
        short_rows = jsonl_rows(manifest_path)
        cache_path = cache_dir / f"{split}.jsonl"
        if args.rebuild_alignment_cache:
            cache_path.write_text("", encoding="utf-8")
        cached_records = [] if args.rebuild_alignment_cache else jsonl_rows(cache_path)
        cached_by_key = {record["cache_key"]: record for record in cached_records}
        aligned_rows = []
        details = []
        for index, row in enumerate(pending, start=1):
            key = cache_key(row, model_id, segment_duration)
            cached = cached_by_key.get(key)
            if cached is not None:
                print(
                    f"[{split}] cached {index}/{len(pending)}: "
                    f"{Path(row['audio']).name}"
                )
                aligned_rows.extend(cached["segments"])
                details.append(cached["detail"])
                continue
            print(f"[{split}] aligning {index}/{len(pending)}: {Path(row['audio']).name}")
            result = transcriber(row["audio"], return_timestamps="word")
            words = timestamped_words(result)
            segments, detail = align_and_segment(
                row,
                words,
                segment_duration,
                float(long_config.get("minimum_alignment_ratio", 0.25)),
                bool(long_config.get("allow_low_confidence", False)),
            )
            aligned_rows.extend(segments)
            details.append(detail)
            record = {"cache_key": key, "segments": segments, "detail": detail}
            with cache_path.open("a", encoding="utf-8") as handle:
                handle.write(json.dumps(record, ensure_ascii=False) + "\n")
                handle.flush()
            if detail["low_confidence"]:
                print(
                    f"[{split}] WARNING: low-confidence alignment "
                    f"{detail['alignment_ratio']:.3f}; retained and flagged"
                )
        with manifest_path.open("w", encoding="utf-8") as handle:
            for row in [*short_rows, *aligned_rows]:
                handle.write(json.dumps(row, ensure_ascii=False) + "\n")
        report[split] = {
            "long_recordings": len(pending),
            "created_segments": len(aligned_rows),
            "details": details,
        }

    report_path = manifests_dir / "alignment_report.json"
    report_path.write_text(json.dumps(report, indent=2), encoding="utf-8")
    print(f"Alignment report written to {report_path}")


if __name__ == "__main__":
    main()
