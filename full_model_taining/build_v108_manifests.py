#!/usr/bin/env python3
"""Build word-focused and negative-refinement manifests for V108."""

from __future__ import annotations

import argparse
import json
import re
from collections import Counter
from pathlib import Path


ADDRESS_WORDS = {
    "avenue",
    "boulevard",
    "circle",
    "court",
    "drive",
    "highway",
    "lane",
    "parkway",
    "place",
    "road",
    "route",
    "street",
    "terrace",
    "turnpike",
}
PHONETIC_WORDS = {
    "alpha",
    "bravo",
    "charlie",
    "delta",
    "echo",
    "foxtrot",
    "golf",
    "hotel",
    "india",
    "juliet",
    "kilo",
    "lima",
    "mike",
    "november",
    "oscar",
    "papa",
    "quebec",
    "romeo",
    "sierra",
    "tango",
    "uniform",
    "victor",
    "whiskey",
    "xray",
    "yankee",
    "zulu",
}


def normalized(text: str) -> str:
    return re.sub(r"\s+", " ", re.sub(r"[^\w\s']", " ", text.lower())).strip()


def read_jsonl(path: Path) -> list[dict]:
    return [
        json.loads(line)
        for line in path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]


def write_jsonl(path: Path, rows: list[dict]) -> None:
    with path.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, ensure_ascii=False) + "\n")


def word_focus_reasons(text: str) -> list[str]:
    clean = normalized(text)
    words = clean.split()
    word_set = set(words)
    reasons: list[str] = []
    if re.search(r"\d", text):
        reasons.append("number")
    if word_set & ADDRESS_WORDS:
        reasons.append("address")
    if word_set & PHONETIC_WORDS:
        reasons.append("phonetic")
    if len(words) >= 12:
        reasons.append("long_dispatch")
    if re.search(r"\b(?:engine|ladder|rescue|squad|car|unit|control)\s*[- ]?\d+\b", clean):
        reasons.append("unit_identifier")
    return reasons


def ensure_empty(path: Path) -> None:
    if path.exists() and any(path.iterdir()):
        raise SystemExit(f"Output already exists and is not empty: {path}")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source-dir", type=Path, required=True)
    parser.add_argument("--word-output-dir", type=Path, required=True)
    parser.add_argument("--negative-output-dir", type=Path, required=True)
    parser.add_argument("--short-target-cap", type=int, default=4)
    parser.add_argument("--negative-repeat", type=int, default=10)
    args = parser.parse_args()

    if args.short_target_cap < 1 or args.negative_repeat < 1:
        raise SystemExit("Repeat and cap values must be at least 1")
    ensure_empty(args.word_output_dir)
    ensure_empty(args.negative_output_dir)

    source_train = read_jsonl(args.source_dir / "train.jsonl")
    short_counts: Counter[str] = Counter()
    focus_counts: Counter[str] = Counter()
    word_rows: list[dict] = []
    focused_unique = 0
    for row in source_train:
        clean = normalized(str(row.get("text", "")))
        if clean and len(clean.split()) <= 3:
            if short_counts[clean] >= args.short_target_cap:
                continue
            short_counts[clean] += 1

        reasons = [] if row.get("is_non_speech") else word_focus_reasons(row["text"])
        copies = 2 if reasons else 1
        if reasons:
            focused_unique += 1
            focus_counts.update(reasons)
        for copy_index in range(copies):
            copy = dict(row)
            if copies > 1:
                copy["v108_word_focus_copy"] = copy_index + 1
                copy["v108_word_focus_reasons"] = reasons
            word_rows.append(copy)

    negative_rows: list[dict] = []
    unique_negatives = 0
    for row in source_train:
        is_negative = bool(row.get("is_non_speech"))
        unique_negatives += int(is_negative)
        copies = args.negative_repeat if is_negative else 1
        for copy_index in range(copies):
            copy = dict(row)
            if copies > 1:
                copy["v108_negative_copy"] = copy_index + 1
            negative_rows.append(copy)

    for output_dir, train_rows in (
        (args.word_output_dir, word_rows),
        (args.negative_output_dir, negative_rows),
    ):
        output_dir.mkdir(parents=True, exist_ok=True)
        write_jsonl(output_dir / "train.jsonl", train_rows)
        for name in ("eval.jsonl", "test.jsonl", "hallucination.jsonl"):
            source = args.source_dir / name
            if source.is_file():
                write_jsonl(output_dir / name, read_jsonl(source))

    word_summary = {
        "source_dir": str(args.source_dir.resolve()),
        "source_train_rows": len(source_train),
        "word_train_rows": len(word_rows),
        "focused_unique_rows": focused_unique,
        "focus_reason_counts": dict(focus_counts.most_common()),
        "short_target_cap": args.short_target_cap,
        "held_out_splits_unchanged": ["eval", "test", "hallucination"],
    }
    negative_summary = {
        "source_dir": str(args.source_dir.resolve()),
        "source_train_rows": len(source_train),
        "negative_train_rows": len(negative_rows),
        "unique_negative_rows": unique_negatives,
        "negative_repeat": args.negative_repeat,
        "held_out_splits_unchanged": ["eval", "test", "hallucination"],
    }
    (args.word_output_dir / "summary.json").write_text(
        json.dumps(word_summary, indent=2) + "\n", encoding="utf-8"
    )
    (args.negative_output_dir / "summary.json").write_text(
        json.dumps(negative_summary, indent=2) + "\n", encoding="utf-8"
    )
    print(json.dumps({"word": word_summary, "negative": negative_summary}, indent=2))


if __name__ == "__main__":
    main()
