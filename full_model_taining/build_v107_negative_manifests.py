#!/usr/bin/env python3
"""Build V107's negative-refinement manifests without changing held-out splits."""

from __future__ import annotations

import argparse
import json
from pathlib import Path


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


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source-dir", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--negative-repeat", type=int, default=10)
    args = parser.parse_args()

    if args.negative_repeat < 1:
        raise SystemExit("--negative-repeat must be at least 1")
    if args.output_dir.exists() and any(args.output_dir.iterdir()):
        raise SystemExit(f"Output already exists and is not empty: {args.output_dir}")

    source_train = read_jsonl(args.source_dir / "train.jsonl")
    negative_count = sum(bool(row.get("is_non_speech")) for row in source_train)
    if not negative_count:
        raise SystemExit("Source training manifest contains no non-speech rows")

    refined_train: list[dict] = []
    for row in source_train:
        copies = args.negative_repeat if row.get("is_non_speech") else 1
        for copy_index in range(copies):
            copy = dict(row)
            if copies > 1:
                copy["v107_negative_copy"] = copy_index + 1
            refined_train.append(copy)

    args.output_dir.mkdir(parents=True, exist_ok=True)
    write_jsonl(args.output_dir / "train.jsonl", refined_train)
    for name in ("eval.jsonl", "test.jsonl", "hallucination.jsonl"):
        source = args.source_dir / name
        if source.is_file():
            write_jsonl(args.output_dir / name, read_jsonl(source))

    summary = {
        "source_dir": str(args.source_dir.resolve()),
        "negative_repeat": args.negative_repeat,
        "source_train_rows": len(source_train),
        "unique_negative_rows": negative_count,
        "refinement_train_rows": len(refined_train),
        "refinement_negative_presentations": negative_count * args.negative_repeat,
        "held_out_splits_unchanged": ["eval", "test", "hallucination"],
    }
    (args.output_dir / "summary.json").write_text(
        json.dumps(summary, indent=2) + "\n", encoding="utf-8"
    )
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
