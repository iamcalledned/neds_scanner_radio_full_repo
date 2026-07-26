#!/usr/bin/env python3
"""Build V201 manifests from the exact curated train/eval export folders."""

from __future__ import annotations

import argparse
import hashlib
import json
import re
from pathlib import Path


EXPECTED_ROWS = {"train": 1082, "eval": 35}
NON_SPEECH_TARGETS = {
    "beep",
    "beeps",
    "dead air",
    "no audio",
    "no speech",
    "noise",
    "silence",
    "static",
}
FORBIDDEN_SELECTED_TARGET = re.compile(r"\bfart(?:ing|ed|s)?\b", re.IGNORECASE)


def normalized_label(text: str) -> str:
    return re.sub(r"\s+", " ", re.sub(r"[^a-z0-9 ]", " ", text.lower())).strip()


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def build_split(source: Path, split: str) -> tuple[list[dict], list[dict]]:
    rows: list[dict] = []
    normalized_non_speech: list[dict] = []
    errors: list[str] = []

    for metadata_path in sorted(source.glob("*.json")):
        metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
        if "edited_transcript" not in metadata:
            errors.append(f"{metadata_path.name}: missing edited_transcript")
            continue

        reviewed_text = str(metadata["edited_transcript"] or "").strip()
        if FORBIDDEN_SELECTED_TARGET.search(reviewed_text):
            errors.append(
                f"{metadata_path.name}: forbidden selected target {reviewed_text!r}"
            )
            continue

        audio_path = metadata_path.with_suffix(".wav")
        if not audio_path.is_file():
            errors.append(f"{metadata_path.name}: missing sibling WAV")
            continue

        target = reviewed_text
        label = normalized_label(reviewed_text)
        target_policy = "reviewed_edited_transcript"
        if label in NON_SPEECH_TARGETS:
            target = ""
            target_policy = "empty_explicit_non_speech"
            normalized_non_speech.append(
                {
                    "id": metadata_path.stem,
                    "reviewed_text": reviewed_text,
                }
            )

        rows.append(
            {
                "audio": str(audio_path.resolve()),
                "text": target,
                "reviewed_text": reviewed_text,
                "target_policy": target_policy,
                "duration": float(metadata.get("duration") or 0.0),
                "id": metadata_path.stem,
                "split": split,
                "source_json": str(metadata_path.resolve()),
                "curated_export_split": split,
            }
        )

    if errors:
        raise SystemExit("\n".join(errors[:25]))
    if len(rows) != EXPECTED_ROWS[split]:
        raise SystemExit(
            f"{split}: expected {EXPECTED_ROWS[split]} rows, found {len(rows)}"
        )
    return rows, normalized_non_speech


def write_jsonl(path: Path, rows: list[dict]) -> None:
    with path.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, ensure_ascii=False) + "\n")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--exports-dir", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    args = parser.parse_args()

    exports_dir = args.exports_dir.expanduser().resolve()
    output_dir = args.output_dir.expanduser().resolve()
    if output_dir.exists() and any(output_dir.iterdir()):
        raise SystemExit(f"Refusing to overwrite non-empty output: {output_dir}")

    train_rows, train_non_speech = build_split(exports_dir / "train", "train")
    eval_rows, eval_non_speech = build_split(exports_dir / "eval", "eval")

    train_ids = {row["id"] for row in train_rows}
    eval_ids = {row["id"] for row in eval_rows}
    overlap = sorted(train_ids & eval_ids)
    if overlap:
        raise SystemExit(f"Exact train/eval overlap: {overlap[:25]}")

    output_dir.mkdir(parents=True, exist_ok=True)
    train_manifest = output_dir / "train.jsonl"
    eval_manifest = output_dir / "eval.jsonl"
    write_jsonl(train_manifest, train_rows)
    write_jsonl(eval_manifest, eval_rows)

    audit = {
        "version": "V201",
        "base_policy": "fresh official Whisper-medium; no inherited scanner weights",
        "selected_transcript_field": "edited_transcript",
        "train_source": str((exports_dir / "train").resolve()),
        "eval_source": str((exports_dir / "eval").resolve()),
        "train_rows": len(train_rows),
        "train_unique_ids": len(train_ids),
        "eval_rows": len(eval_rows),
        "eval_unique_ids": len(eval_ids),
        "train_eval_exact_overlap": overlap,
        "non_speech_policy": {
            "policy": "retain recording and train with an empty target",
            "labels": sorted(NON_SPEECH_TARGETS),
            "train_normalized": train_non_speech,
            "eval_normalized": eval_non_speech,
        },
        "selected_target_forbidden_term_hits": 0,
        "test_policy": "The test export split is never enumerated or loaded.",
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
