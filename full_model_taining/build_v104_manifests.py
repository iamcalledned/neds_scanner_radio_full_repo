#!/usr/bin/env python3
"""Build conservative, day-isolated V104 manifests from reviewed V103 rows."""

from __future__ import annotations

import argparse
import hashlib
import json
import re
from collections import Counter, defaultdict
from pathlib import Path


NON_SPEECH_TARGETS = {
    "beep",
    "beeps",
    "beeeps",
    "tone",
    "tones",
    "dead air",
    "nothing",
    "bluureep",
    "silence",
    "static",
    "unintelligible",
}


def normalized(text: str) -> str:
    return re.sub(r"\s+", " ", re.sub(r"[^\w\s']", " ", text.lower())).strip()


def recording_day(path: str) -> str:
    match = re.search(r"rec_(\d{4}-\d{2}-\d{2})_", Path(path).name)
    return match.group(1) if match else "unknown"


def read_jsonl(path: Path) -> list[dict]:
    if not path.exists():
        return []
    return [
        json.loads(line)
        for line in path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]


def write_jsonl(path: Path, rows: list[dict]) -> None:
    with path.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, ensure_ascii=False) + "\n")


def metadata_for_audio(audio: str, cache: dict[str, dict]) -> dict:
    path = str(Path(audio).with_suffix(".json"))
    if path not in cache:
        try:
            cache[path] = json.loads(Path(path).read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            cache[path] = {}
    return cache[path]


def repetition_fraction(text: str) -> float:
    words = normalized(text).split()
    if not words:
        return 0.0
    return max(Counter(words).values()) / len(words)


def stable_bucket(row: dict, modulus: int = 100) -> int:
    value = f"{row['audio']}:{row.get('start', 0)}:{row.get('end', row.get('duration'))}"
    return int(hashlib.sha256(value.encode("utf-8")).hexdigest()[:8], 16) % modulus


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source-dir", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--eval-day", action="append", default=["2026-04-05"])
    parser.add_argument("--test-day", action="append", default=["2026-04-06"])
    parser.add_argument("--minimum-alignment-ratio", type=float, default=0.5)
    parser.add_argument("--short-target-cap", type=int, default=12)
    parser.add_argument("--negative-holdout-percent", type=int, default=25)
    parser.add_argument(
        "--negative-repeat",
        type=int,
        default=10,
        help="Number of training copies for each reviewed empty non-speech target.",
    )
    args = parser.parse_args()

    eval_days = set(args.eval_day)
    test_days = set(args.test_day)
    if eval_days & test_days:
        raise SystemExit("Evaluation and test days must not overlap.")

    all_rows: list[dict] = []
    for old_split in ("train", "eval", "test"):
        for row in read_jsonl(args.source_dir / f"{old_split}.jsonl"):
            row = dict(row)
            row["original_split"] = old_split
            all_rows.append(row)

    metadata_cache: dict[str, dict] = {}
    rejected: list[dict] = []
    accepted: dict[str, list[dict]] = defaultdict(list)
    hallucination_rows: list[dict] = []
    seen_segments: set[tuple] = set()

    for row in sorted(
        all_rows,
        key=lambda value: (
            value["audio"],
            float(value.get("start", 0)),
            float(value.get("end", value.get("duration", 0))),
        ),
    ):
        segment_key = (
            str(Path(row["audio"]).resolve()),
            float(row.get("start", 0)),
            float(row.get("end", row.get("duration", 0))),
        )
        if segment_key in seen_segments:
            rejected.append({**row, "reason": "duplicate_audio_segment"})
            continue
        seen_segments.add(segment_key)

        metadata = metadata_for_audio(row["audio"], metadata_cache)
        quality = metadata.get("transcription_quality")
        quality = quality if isinstance(quality, dict) else {}
        if not bool(metadata.get("reviewed")):
            rejected.append({**row, "reason": "not_manually_reviewed"})
            continue
        if quality.get("needs_retry") or quality.get("needs_review"):
            rejected.append({**row, "reason": "quality_flagged"})
            continue
        if (
            "alignment_ratio" in row
            and float(row["alignment_ratio"]) < args.minimum_alignment_ratio
        ):
            rejected.append({**row, "reason": "low_alignment"})
            continue
        words = normalized(str(row.get("text", ""))).split()
        if len(words) >= 8 and repetition_fraction(row["text"]) >= 0.5:
            rejected.append({**row, "reason": "repeated_token_outlier"})
            continue

        day = recording_day(row["audio"])
        split = "test" if day in test_days else "eval" if day in eval_days else "train"
        clean = dict(row)
        clean["recording_day"] = day
        clean["manually_reviewed"] = True
        clean["original_text"] = row["text"]
        target = normalized(str(row["text"]))
        is_non_speech = target in NON_SPEECH_TARGETS
        clean["is_non_speech"] = is_non_speech
        if is_non_speech:
            clean["text"] = ""
            clean["target_policy"] = "empty_non_speech"

        if (
            split == "train"
            and is_non_speech
            and stable_bucket(clean) < args.negative_holdout_percent
        ):
            clean["split"] = "hallucination"
            hallucination_rows.append(clean)
            continue

        clean["split"] = split
        accepted[split].append(clean)

    # Limit exact repetition of very short targets in training while retaining
    # diverse audio and every longer dispatch transcript.
    retained_train: list[dict] = []
    short_counts: Counter[str] = Counter()
    for row in accepted["train"]:
        text = normalized(row["text"])
        if len(text.split()) <= 3:
            if short_counts[text] >= args.short_target_cap:
                rejected.append({**row, "reason": "short_target_frequency_cap"})
                continue
            short_counts[text] += 1
        retained_train.append(row)
    accepted["train"] = retained_train

    # Non-speech calls are rare in the archive. Repeat them deliberately so
    # they are frequent enough to affect decoder learning and checkpoint
    # selection. Audio augmentation still varies most repeated presentations.
    oversampled_train: list[dict] = []
    for row in accepted["train"]:
        copies = args.negative_repeat if row["is_non_speech"] else 1
        for copy_index in range(copies):
            copy = dict(row)
            if copies > 1:
                copy["oversample_copy"] = copy_index + 1
            oversampled_train.append(copy)
    accepted["train"] = oversampled_train

    # Keep the dedicated negative holdout separate so the primary splits remain
    # isolated by complete recording day. The chosen evaluation day already
    # contains reviewed non-speech examples with empty targets.
    accepted["eval"].sort(key=lambda row: (row["audio"], row.get("start", 0)))

    args.output_dir.mkdir(parents=True, exist_ok=True)
    for split in ("train", "eval", "test"):
        write_jsonl(args.output_dir / f"{split}.jsonl", accepted[split])
    write_jsonl(args.output_dir / "hallucination.jsonl", hallucination_rows)
    write_jsonl(args.output_dir / "rejected.jsonl", rejected)

    summary = {
        "source_dir": str(args.source_dir.resolve()),
        "eval_days": sorted(eval_days),
        "test_days": sorted(test_days),
        "minimum_alignment_ratio": args.minimum_alignment_ratio,
        "short_target_cap": args.short_target_cap,
        "negative_holdout_percent": args.negative_holdout_percent,
        "negative_repeat": args.negative_repeat,
        "splits": {
            split: {
                "rows": len(accepted[split]),
                "hours": round(
                    sum(float(row.get("duration", 0)) for row in accepted[split]) / 3600,
                    3,
                ),
                "days": dict(
                    Counter(row["recording_day"] for row in accepted[split]).most_common()
                ),
                "non_speech": sum(bool(row["is_non_speech"]) for row in accepted[split]),
            }
            for split in ("train", "eval", "test")
        },
        "hallucination_rows": len(hallucination_rows),
        "rejected": len(rejected),
        "rejection_reasons": dict(Counter(row["reason"] for row in rejected).most_common()),
    }
    (args.output_dir / "summary.json").write_text(
        json.dumps(summary, indent=2), encoding="utf-8"
    )
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
