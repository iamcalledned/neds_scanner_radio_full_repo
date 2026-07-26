#!/usr/bin/env python3
"""Audit Whisper JSONL manifests for duplication, leakage, and suspect alignment."""

from __future__ import annotations

import argparse
import hashlib
import json
import re
from collections import Counter, defaultdict
from pathlib import Path

import numpy as np
import soundfile as sf


NON_SPEECH = {
    "beep",
    "beeps",
    "tone",
    "tones",
    "tone beep",
    "tone beeps",
    "static",
    "silence",
    "unintelligible",
}


def json_default(value: object) -> object:
    if isinstance(value, np.generic):
        return value.item()
    raise TypeError(f"Object of type {type(value).__name__} is not JSON serializable")


def normalized(text: str) -> str:
    return re.sub(r"\s+", " ", re.sub(r"[^\w\s']", " ", text.lower())).strip()


def recording_day(path: str) -> str:
    match = re.search(r"rec_(\d{4}-\d{2}-\d{2})_", Path(path).name)
    return match.group(1) if match else "unknown"


def load_jsonl(path: Path) -> list[dict]:
    if not path.exists():
        return []
    return [
        json.loads(line)
        for line in path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]


def audio_stats(row: dict) -> dict[str, float]:
    start = float(row.get("start", 0.0))
    stop = float(row.get("end", row["duration"]))
    info = sf.info(row["audio"])
    start_frame = max(0, round(start * info.samplerate))
    stop_frame = min(info.frames, round(stop * info.samplerate))
    audio, _ = sf.read(
        row["audio"],
        start=start_frame,
        stop=stop_frame,
        dtype="float32",
        always_2d=False,
    )
    if audio.ndim > 1:
        audio = audio.mean(axis=1)
    if not len(audio):
        return {"rms_db": -120.0, "active_ratio": 0.0, "peak": 0.0}
    rms = float(np.sqrt(np.mean(np.square(audio), dtype=np.float64)))
    frame_size = max(1, round(info.samplerate * 0.02))
    usable = len(audio) - (len(audio) % frame_size)
    if usable:
        frames = audio[:usable].reshape(-1, frame_size)
        frame_rms = np.sqrt(np.mean(np.square(frames), axis=1, dtype=np.float64))
        active_ratio = float(np.mean(frame_rms > 0.003))
    else:
        active_ratio = float(rms > 0.003)
    return {
        "rms_db": round(20 * np.log10(max(rms, 1e-6)), 2),
        "active_ratio": round(active_ratio, 4),
        "peak": round(float(np.max(np.abs(audio))), 5),
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--manifests-dir", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()

    rows_by_split = {
        split: load_jsonl(args.manifests_dir / f"{split}.jsonl")
        for split in ("train", "eval", "test")
    }
    text_locations: dict[str, list[dict]] = defaultdict(list)
    audio_locations: dict[str, set[str]] = defaultdict(set)
    report: dict = {"splits": {}, "cross_split": {}, "flags": []}

    for split, rows in rows_by_split.items():
        texts = Counter()
        days = Counter()
        categories = Counter()
        alignments = []
        non_speech = Counter()
        short_targets = Counter()
        word_rates = []
        for index, row in enumerate(rows):
            text = normalized(str(row.get("text", "")))
            texts[text] += 1
            days[recording_day(row["audio"])] += 1
            categories[str(row.get("category") or "unknown")] += 1
            text_locations[text].append({"split": split, "index": index, "audio": row["audio"]})
            audio_locations[str(Path(row["audio"]).resolve())].add(split)
            if "alignment_ratio" in row:
                alignments.append(float(row["alignment_ratio"]))
            if text in NON_SPEECH:
                non_speech[text] += 1
            words = len(text.split())
            if words <= 3:
                short_targets[text] += 1
            duration = max(float(row.get("duration", 0.0)), 0.001)
            word_rates.append(words / duration)

            flags = []
            if "alignment_ratio" in row and float(row["alignment_ratio"]) < 0.5:
                flags.append("alignment_below_0.50")
            if text and text not in NON_SPEECH and words / duration > 5.0:
                flags.append("word_rate_above_5_per_second")
            if flags:
                report["flags"].append(
                    {
                        "split": split,
                        "index": index,
                        "audio": row["audio"],
                        "text": row.get("text", ""),
                        "flags": flags,
                    }
                )

        report["splits"][split] = {
            "rows": len(rows),
            "hours": round(sum(float(row.get("duration", 0)) for row in rows) / 3600, 3),
            "recording_days": dict(sorted(days.items())),
            "categories": dict(categories.most_common()),
            "unique_normalized_targets": len(texts),
            "duplicate_rows": sum(count - 1 for count in texts.values() if count > 1),
            "most_common_targets": [
                {"text": text, "count": count} for text, count in texts.most_common(25)
            ],
            "non_speech_targets": dict(non_speech.most_common()),
            "short_targets": [
                {"text": text, "count": count}
                for text, count in short_targets.most_common(25)
            ],
            "aligned_rows": len(alignments),
            "alignment_below_0.50": sum(value < 0.5 for value in alignments),
            "alignment_below_0.35": sum(value < 0.35 for value in alignments),
            "alignment_ratio_min": round(min(alignments), 4) if alignments else None,
            "alignment_ratio_median": round(float(np.median(alignments)), 4)
            if alignments
            else None,
            "median_words_per_second": round(float(np.median(word_rates)), 3)
            if word_rates
            else None,
        }

    shared_texts = []
    for text, locations in text_locations.items():
        splits = sorted({location["split"] for location in locations})
        if len(splits) > 1:
            shared_texts.append(
                {
                    "text": text,
                    "count": len(locations),
                    "splits": splits,
                    "locations": locations,
                }
            )
    shared_texts.sort(key=lambda item: (-item["count"], item["text"]))
    shared_audio = [
        {"audio": audio, "splits": sorted(splits)}
        for audio, splits in audio_locations.items()
        if len(splits) > 1
    ]

    days_by_split = {
        split: set(report["splits"][split]["recording_days"])
        for split in rows_by_split
    }
    report["cross_split"] = {
        "exact_shared_audio": shared_audio,
        "exact_shared_targets_count": len(shared_texts),
        "exact_shared_targets": shared_texts[:100],
        "shared_recording_days": {
            "train_eval": sorted(days_by_split["train"] & days_by_split["eval"]),
            "train_test": sorted(days_by_split["train"] & days_by_split["test"]),
            "eval_test": sorted(days_by_split["eval"] & days_by_split["test"]),
        },
    }

    suspect_rows = []
    for split, rows in rows_by_split.items():
        for index, row in enumerate(rows):
            text = normalized(str(row.get("text", "")))
            if (
                text in NON_SPEECH
                or ("alignment_ratio" in row and float(row["alignment_ratio"]) < 0.5)
                or len(text.split()) <= 2
            ):
                stats = audio_stats(row)
                suspect_rows.append(
                    {
                        "split": split,
                        "index": index,
                        "audio": row["audio"],
                        "text": row.get("text", ""),
                        "alignment_ratio": row.get("alignment_ratio"),
                        **stats,
                    }
                )
    report["audio_checks"] = {
        "checked_rows": len(suspect_rows),
        "very_quiet_rows": sum(row["rms_db"] < -45 for row in suspect_rows),
        "low_activity_rows": sum(row["active_ratio"] < 0.1 for row in suspect_rows),
        "rows": suspect_rows,
    }
    report["audit_id"] = hashlib.sha256(
        json.dumps(report, sort_keys=True, default=json_default).encode("utf-8")
    ).hexdigest()

    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(
        json.dumps(report, indent=2, default=json_default), encoding="utf-8"
    )
    print(
        json.dumps(
            {key: value for key, value in report.items() if key != "audio_checks"},
            indent=2,
            default=json_default,
        )
    )
    print(f"Full audit written to {args.output}")


if __name__ == "__main__":
    main()
