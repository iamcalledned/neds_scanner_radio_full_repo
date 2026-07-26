#!/usr/bin/env python3
"""Build V202 manifests by repeating explicit non-speech within the full V201 set."""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path


EXPECTED_SOURCE_TRAIN_ROWS = 1082
EXPECTED_SOURCE_EVAL_ROWS = 35
EXPECTED_NON_SPEECH_ROWS = 25


def load_jsonl(path: Path) -> list[dict]:
    return [
        json.loads(line)
        for line in path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]


def write_jsonl(path: Path, rows: list[dict]) -> None:
    with path.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, ensure_ascii=False) + "\n")


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source-dir", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--non-speech-repeat", type=int, default=8)
    args = parser.parse_args()

    source_dir = args.source_dir.expanduser().resolve()
    output_dir = args.output_dir.expanduser().resolve()
    if args.non_speech_repeat < 1:
        raise SystemExit("--non-speech-repeat must be at least 1")
    if output_dir.exists() and any(output_dir.iterdir()):
        raise SystemExit(f"Refusing to overwrite non-empty output: {output_dir}")

    train_source = load_jsonl(source_dir / "train.jsonl")
    eval_rows = load_jsonl(source_dir / "eval.jsonl")
    if len(train_source) != EXPECTED_SOURCE_TRAIN_ROWS:
        raise SystemExit(
            f"Expected {EXPECTED_SOURCE_TRAIN_ROWS} source training rows, "
            f"found {len(train_source)}"
        )
    if len(eval_rows) != EXPECTED_SOURCE_EVAL_ROWS:
        raise SystemExit(
            f"Expected {EXPECTED_SOURCE_EVAL_ROWS} source eval rows, found {len(eval_rows)}"
        )

    source_ids = [str(row["id"]) for row in train_source]
    eval_ids = [str(row["id"]) for row in eval_rows]
    if len(set(source_ids)) != EXPECTED_SOURCE_TRAIN_ROWS:
        raise SystemExit("V202 source training IDs are not unique")
    if len(set(eval_ids)) != EXPECTED_SOURCE_EVAL_ROWS:
        raise SystemExit("V202 source eval IDs are not unique")
    if set(source_ids) & set(eval_ids):
        raise SystemExit("V202 source train/eval IDs overlap")

    non_speech = [
        row
        for row in train_source
        if row.get("target_policy") == "empty_explicit_non_speech"
    ]
    if len(non_speech) != EXPECTED_NON_SPEECH_ROWS:
        raise SystemExit(
            f"Expected {EXPECTED_NON_SPEECH_ROWS} explicit non-speech rows, "
            f"found {len(non_speech)}"
        )
    if any(row.get("text") for row in non_speech):
        raise SystemExit("An explicit non-speech source row has a non-empty target")

    train_rows = [dict(row, presentation=1) for row in train_source]
    for presentation in range(2, args.non_speech_repeat + 1):
        for source_row in non_speech:
            train_rows.append(dict(source_row, presentation=presentation))

    output_dir.mkdir(parents=True, exist_ok=True)
    train_manifest = output_dir / "train.jsonl"
    eval_manifest = output_dir / "eval.jsonl"
    write_jsonl(train_manifest, train_rows)
    write_jsonl(eval_manifest, eval_rows)

    audit = {
        "version": "V202",
        "source_manifest_dir": str(source_dir),
        "source_train_rows": len(train_source),
        "source_train_unique_ids": len(set(source_ids)),
        "all_source_train_ids_retained": {
            "value": len({str(row["id"]) for row in train_rows}) == len(set(source_ids)),
            "missing": sorted(set(source_ids) - {str(row["id"]) for row in train_rows}),
        },
        "unique_non_speech_rows": len(non_speech),
        "non_speech_repeat": args.non_speech_repeat,
        "non_speech_presentations": len(non_speech) * args.non_speech_repeat,
        "total_train_presentations": len(train_rows),
        "eval_rows": len(eval_rows),
        "eval_unique_ids": len(set(eval_ids)),
        "train_eval_exact_overlap": sorted(set(source_ids) & set(eval_ids)),
        "test_rows_loaded": 0,
        "manifest_sha256": {
            "train": sha256(train_manifest),
            "eval": sha256(eval_manifest),
        },
        "flags": [],
    }
    (output_dir / "audit.json").write_text(
        json.dumps(audit, indent=2) + "\n", encoding="utf-8"
    )
    print(json.dumps(audit, indent=2))


if __name__ == "__main__":
    main()
