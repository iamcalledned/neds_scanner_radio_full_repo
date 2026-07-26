#!/usr/bin/env python3
"""Build a preservation-heavy V109 set with reviewed repetition corrections."""

from __future__ import annotations

import argparse
import json
import re
import sqlite3
from collections import Counter
from pathlib import Path


def read_jsonl(path: Path) -> list[dict]:
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]


def write_jsonl(path: Path, rows: list[dict]) -> None:
    with path.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, ensure_ascii=False) + "\n")


def words(text: str) -> list[str]:
    return re.findall(r"[a-z0-9']+", text.lower())


def max_repeated_ngram(text: str) -> int:
    tokens = words(text)
    best = 1
    for size in range(3, 9):
        grams = Counter(tuple(tokens[i : i + size]) for i in range(len(tokens) - size + 1))
        best = max(best, max(grams.values(), default=1))
    return best


def reviewed_repetition_rows(db_path: Path) -> list[dict]:
    connection = sqlite3.connect(db_path)
    connection.row_factory = sqlite3.Row
    records = connection.execute(
        """
        SELECT filename, wav_path, duration, town, dept, timestamp,
               transcript, raw_transcript, edited_transcript, transcription_model
        FROM calls
        WHERE edited_transcript IS NOT NULL
          AND trim(edited_transcript) != ''
        """
    ).fetchall()
    connection.close()

    rows = []
    for record in records:
        raw = str(record["raw_transcript"] or record["transcript"] or "").strip()
        edited = str(record["edited_transcript"] or "").strip()
        wav_path = Path(str(record["wav_path"] or ""))
        if not wav_path.is_file():
            review_path = db_path.parent / "scanner_archive" / "review" / str(record["filename"])
            if review_path.is_file():
                wav_path = review_path
        if not raw or raw == edited or not wav_path.is_file():
            continue
        raw_count = len(words(raw))
        edited_count = len(words(edited))
        repeated = max_repeated_ngram(raw)
        if repeated < 4 and raw_count <= max(45, 2 * edited_count):
            continue
        rows.append(
            {
                "audio": str(wav_path.resolve()),
                "text": edited,
                "duration": float(record["duration"] or 0.0),
                "transcript_source": "reviewed_repetition_correction",
                "category": record["dept"],
                "town": record["town"],
                "id": Path(str(record["filename"])).stem,
                "original_split": "reviewed_db",
                "recording_day": str(record["timestamp"] or "")[:10],
                "manually_reviewed": True,
                "original_text": raw,
                "is_non_speech": False,
                "split": "train",
                "v109_repetition_count": repeated,
                "v109_source_model": record["transcription_model"],
            }
        )
    return rows


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source-dir", type=Path, required=True)
    parser.add_argument("--database", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--repetition-weight", type=int, default=8)
    parser.add_argument("--negative-weight", type=int, default=6)
    parser.add_argument("--short-target-cap", type=int, default=2)
    parser.add_argument("--overwrite", action="store_true")
    args = parser.parse_args()
    if args.output_dir.exists() and any(args.output_dir.iterdir()) and not args.overwrite:
        raise SystemExit(f"Refusing to overwrite non-empty output: {args.output_dir}")

    source = []
    for split in ("train", "eval", "test"):
        source.extend(read_jsonl(args.source_dir / f"{split}.jsonl"))
    eval_rows = read_jsonl(args.source_dir / "eval.jsonl")
    test_rows = read_jsonl(args.source_dir / "test.jsonl")
    held_out_days = {
        str(row.get("recording_day"))
        for row in eval_rows + test_rows
        if row.get("recording_day")
    }
    # The final promotion comparison uses the complete exports/eval directory.
    # Exclude all of its recording days, including 2026-02-25.
    exports_eval = args.source_dir.parent / "exports" / "eval"
    for metadata_path in exports_eval.glob("*.json"):
        metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
        timestamp = str(metadata.get("timestamp") or metadata_path.stem[4:14])
        held_out_days.add(timestamp[:10].replace("_", "-"))

    preservation = []
    negative = []
    short_counts: Counter[str] = Counter()
    seen_audio: set[str] = set()
    for row in source:
        if row.get("original_split") != "train":
            continue
        if str(row.get("recording_day")) in held_out_days:
            continue
        audio = str(Path(str(row["audio"])).resolve())
        if audio in seen_audio:
            continue
        seen_audio.add(audio)
        if row.get("is_non_speech"):
            negative.append(dict(row))
            continue
        clean = " ".join(words(str(row.get("text", ""))))
        if len(clean.split()) <= 3:
            if short_counts[clean] >= args.short_target_cap:
                continue
            short_counts[clean] += 1
        preservation.append(dict(row))

    corrections = [
        row for row in reviewed_repetition_rows(args.database)
        if str(row.get("recording_day")) not in held_out_days
        and str(Path(row["audio"]).resolve()) not in seen_audio
    ]
    train_rows = list(preservation)
    for row in negative:
        for copy_index in range(args.negative_weight):
            copy = dict(row)
            copy["v109_negative_copy"] = copy_index + 1
            train_rows.append(copy)
    for row in corrections:
        for copy_index in range(args.repetition_weight):
            copy = dict(row)
            copy["v109_repetition_copy"] = copy_index + 1
            train_rows.append(copy)

    args.output_dir.mkdir(parents=True, exist_ok=True)
    write_jsonl(args.output_dir / "train.jsonl", train_rows)
    write_jsonl(args.output_dir / "eval.jsonl", eval_rows)
    write_jsonl(args.output_dir / "test.jsonl", test_rows)
    hallucination_path = args.source_dir / "hallucination.jsonl"
    if hallucination_path.is_file():
        write_jsonl(args.output_dir / "hallucination.jsonl", read_jsonl(hallucination_path))

    train_days = {str(row.get("recording_day")) for row in train_rows}
    eval_days = {str(row.get("recording_day")) for row in eval_rows}
    test_days = {str(row.get("recording_day")) for row in test_rows}
    train_audio = {str(Path(str(row["audio"])).resolve()) for row in train_rows}
    eval_audio = {str(Path(str(row["audio"])).resolve()) for row in eval_rows}
    test_audio = {str(Path(str(row["audio"])).resolve()) for row in test_rows}
    audit = {
        "source_dir": str(args.source_dir.resolve()),
        "database": str(args.database.resolve()),
        "train_rows": len(train_rows),
        "preservation_unique_rows": len(preservation),
        "negative_unique_rows": len(negative),
        "reviewed_repetition_unique_rows": len(corrections),
        "v101_repetition_corrections": sum(
            "v101" in str(row.get("v109_source_model", "")).lower() for row in corrections
        ),
        "repetition_weight": args.repetition_weight,
        "negative_weight": args.negative_weight,
        "eval_rows": len(eval_rows),
        "test_rows": len(test_rows),
        "held_out_days": sorted(held_out_days),
        "train_eval_day_overlap": sorted(train_days & eval_days),
        "train_test_day_overlap": sorted(train_days & test_days),
        "train_eval_audio_overlap": sorted(train_audio & eval_audio),
        "train_test_audio_overlap": sorted(train_audio & test_audio),
    }
    flags = []
    for key in (
        "train_eval_day_overlap",
        "train_test_day_overlap",
        "train_eval_audio_overlap",
        "train_test_audio_overlap",
    ):
        if audit[key]:
            flags.append(key)
    audit["flags"] = flags
    (args.output_dir / "audit.json").write_text(json.dumps(audit, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(audit, indent=2))
    if flags:
        raise SystemExit(f"Manifest leakage audit failed: {flags}")


if __name__ == "__main__":
    main()
