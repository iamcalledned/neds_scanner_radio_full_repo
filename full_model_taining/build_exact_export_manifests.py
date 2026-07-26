#!/usr/bin/env python3
"""Mirror the curated exports train/eval folders without reading exports/test."""

from __future__ import annotations

import argparse
import json
from pathlib import Path


def build_split(source: Path, split: str) -> list[dict]:
    rows: list[dict] = []
    errors: list[str] = []
    for metadata_path in sorted(source.glob("*.json")):
        metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
        text = str(metadata.get("edited_transcript") or "").strip()
        filename = str(metadata.get("filename") or metadata_path.with_suffix(".wav").name)
        sibling_audio = metadata_path.parent / filename
        stem_audio = metadata_path.with_suffix(".wav")
        declared_audio = Path(str(metadata.get("wav_path") or ""))
        audio = next(
            (candidate for candidate in (sibling_audio, stem_audio, declared_audio) if candidate.is_file()),
            None,
        )
        if not text:
            errors.append(f"{metadata_path.name}: empty edited_transcript")
            continue
        if audio is None:
            errors.append(f"{metadata_path.name}: missing audio")
            continue
        rows.append(
            {
                "audio": str(audio.resolve()),
                "text": text,
                "duration": float(metadata.get("duration") or 0.0),
                "id": metadata_path.stem,
                "split": split,
                "source_json": str(metadata_path.resolve()),
                "curated_export_split": split,
            }
        )
    if errors:
        raise SystemExit("\n".join(errors[:25]))
    return rows


def write_jsonl(path: Path, rows: list[dict]) -> None:
    with path.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, ensure_ascii=False) + "\n")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--exports-dir", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    args = parser.parse_args()

    if args.output_dir.exists() and any(args.output_dir.iterdir()):
        raise SystemExit(f"Refusing to overwrite non-empty output: {args.output_dir}")

    train_rows = build_split(args.exports_dir / "train", "train")
    eval_rows = build_split(args.exports_dir / "eval", "eval")
    train_ids = {row["id"] for row in train_rows}
    eval_ids = {row["id"] for row in eval_rows}
    overlap = sorted(train_ids & eval_ids)
    if overlap:
        raise SystemExit(f"Exact train/eval overlap: {overlap[:25]}")

    args.output_dir.mkdir(parents=True, exist_ok=True)
    write_jsonl(args.output_dir / "train.jsonl", train_rows)
    write_jsonl(args.output_dir / "eval.jsonl", eval_rows)
    audit = {
        "exports_dir": str(args.exports_dir.resolve()),
        "train_source": str((args.exports_dir / "train").resolve()),
        "eval_source": str((args.exports_dir / "eval").resolve()),
        "train_rows": len(train_rows),
        "train_unique_ids": len(train_ids),
        "eval_rows": len(eval_rows),
        "eval_unique_ids": len(eval_ids),
        "train_eval_exact_overlap": overlap,
        "test_policy": "exports/test was not read, loaded, copied, or referenced",
        "flags": [],
    }
    (args.output_dir / "audit.json").write_text(
        json.dumps(audit, indent=2) + "\n", encoding="utf-8"
    )
    print(json.dumps(audit, indent=2))


if __name__ == "__main__":
    main()
