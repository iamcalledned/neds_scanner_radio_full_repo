#!/usr/bin/env python3
"""Build a conservative V112 replay manifest from measured V111 errors."""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path


EXPECTED_ROWS = {"train": 1082, "eval": 35}
CONTAMINATION_ID = "rec_2026-03-29_15-28-32_pd"


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
    parser.add_argument("--mining-predictions", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    args = parser.parse_args()

    source_dir = args.source_dir.expanduser().resolve()
    predictions_path = args.mining_predictions.expanduser().resolve()
    output_dir = args.output_dir.expanduser().resolve()
    if output_dir.exists() and any(output_dir.iterdir()):
        raise SystemExit(f"Refusing to overwrite non-empty output: {output_dir}")

    train_source = load_jsonl(source_dir / "train.jsonl")
    eval_rows = load_jsonl(source_dir / "eval.jsonl")
    predictions = load_jsonl(predictions_path)
    if len(train_source) != EXPECTED_ROWS["train"]:
        raise SystemExit(f"Expected 1082 source train rows, found {len(train_source)}")
    if len(eval_rows) != EXPECTED_ROWS["eval"]:
        raise SystemExit(f"Expected 35 source eval rows, found {len(eval_rows)}")

    source_by_id = {str(row["id"]): row for row in train_source}
    predictions_by_id = {str(row["id"]): row for row in predictions}
    if len(source_by_id) != EXPECTED_ROWS["train"]:
        raise SystemExit("Source training IDs are not unique")
    if set(source_by_id) != set(predictions_by_id):
        missing = sorted(set(source_by_id) - set(predictions_by_id))
        extra = sorted(set(predictions_by_id) - set(source_by_id))
        raise SystemExit(f"Mining/source mismatch; missing={missing[:10]} extra={extra[:10]}")

    replay_counts: dict[str, int] = {}
    replay_reasons: dict[str, list[str]] = {}
    for row_id, prediction in predictions_by_id.items():
        count = 1
        reasons = ["full_curated_set"]
        if float(prediction["wer"]) >= 0.50:
            count = max(count, 2)
            reasons.append("v111_wer_at_least_0.50")
        if bool(prediction.get("repeated")):
            count = max(count, 3)
            reasons.append("v111_repetition")
        if row_id == CONTAMINATION_ID or "fart" in str(
            prediction.get("prediction_normalized") or ""
        ):
            count = max(count, 12)
            reasons.append("known_contamination_correction")
        replay_counts[row_id] = count
        replay_reasons[row_id] = reasons

    train_rows: list[dict] = []
    for source_row in train_source:
        row_id = str(source_row["id"])
        for presentation in range(1, replay_counts[row_id] + 1):
            train_rows.append(
                dict(
                    source_row,
                    presentation=presentation,
                    replay_reasons=replay_reasons[row_id],
                    v111_baseline_wer=float(predictions_by_id[row_id]["wer"]),
                )
            )

    output_dir.mkdir(parents=True, exist_ok=True)
    train_manifest = output_dir / "train.jsonl"
    eval_manifest = output_dir / "eval.jsonl"
    write_jsonl(train_manifest, train_rows)
    write_jsonl(eval_manifest, eval_rows)

    retained_ids = {str(row["id"]) for row in train_rows}
    eval_ids = {str(row["id"]) for row in eval_rows}
    audit = {
        "version": "V112",
        "source_manifest_dir": str(source_dir),
        "mining_predictions": str(predictions_path),
        "source_train_rows": len(train_source),
        "source_train_unique_ids": len(source_by_id),
        "all_source_train_ids_retained": retained_ids == set(source_by_id),
        "missing_source_train_ids": sorted(set(source_by_id) - retained_ids),
        "baseline_hard_ids_at_wer_0.50": sum(
            float(row["wer"]) >= 0.50 for row in predictions
        ),
        "baseline_repetition_ids": sum(bool(row.get("repeated")) for row in predictions),
        "contamination_id": CONTAMINATION_ID,
        "contamination_presentations": replay_counts[CONTAMINATION_ID],
        "total_train_presentations": len(train_rows),
        "eval_rows": len(eval_rows),
        "eval_unique_ids": len(eval_ids),
        "train_eval_exact_overlap": sorted(set(source_by_id) & eval_ids),
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
