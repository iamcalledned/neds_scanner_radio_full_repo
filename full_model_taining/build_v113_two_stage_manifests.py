#!/usr/bin/env python3
"""Build V113 targeted-correction and full-set consolidation manifests."""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path


EXPECTED_TRAIN_ROWS = 1082
EXPECTED_EVAL_ROWS = 35
CONTAMINATION_ID = "rec_2026-03-29_15-28-32_pd"
NON_SPEECH_PROBE_IDS = {
    "rec_2026-02-25_00-14-22_pd",
    "rec_2026-02-25_01-13-55_pd",
    "rec_2026-02-25_07-11-12_pd",
    "rec_2026-02-25_08-10-35_pd",
    "rec_2026-02-25_09-10-11_pd",
    "rec_2026-02-25_11-09-17_pd",
    "rec_2026-02-25_14-07-57_pd",
    "rec_2026-03-28_22-37-41_pd",
    "rec_2026-03-29_15-28-32_pd",
    "rec_2026-04-02_14-40-30_bpd",
    "rec_2026-04-05_09-31-10_mfd",
    "rec_2026-04-05_10-31-14_mfd",
    "rec_2026-04-05_14-56-11_mfd",
    "rec_2026-04-05_16-34-04_mfd",
    "rec_2026-04-05_17-34-08_mfd",
    "rec_2026-04-06_08-57-10_mfd",
    "rec_2026-04-06_09-32-41_bpd",
    "rec_2026-04-06_09-57-14_mfd",
    "rec_2026-04-06_10-57-18_mfd",
    "rec_2026-04-07_06-37-23_mfd",
    "rec_2026-04-07_07-37-28_mfd",
    "rec_2026-04-07_08-37-32_mfd",
    "rec_2026-04-07_09-37-36_mfd",
    "rec_2026-04-07_10-37-40_mfd",
    "rec_2026-04-10_11-25-17_mndpd",
}


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


def expanded_rows(
    source_rows: list[dict],
    predictions: dict[str, dict],
    counts: dict[str, int],
    reasons: dict[str, list[str]],
) -> list[dict]:
    rows: list[dict] = []
    for source in source_rows:
        row_id = str(source["id"])
        for presentation in range(1, counts.get(row_id, 0) + 1):
            rows.append(
                dict(
                    source,
                    presentation=presentation,
                    replay_reasons=reasons[row_id],
                    v111_baseline_wer=float(predictions[row_id]["wer"]),
                )
            )
    return rows


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
    prediction_rows = load_jsonl(predictions_path)
    source_by_id = {str(row["id"]): row for row in train_source}
    predictions = {str(row["id"]): row for row in prediction_rows}

    if len(train_source) != EXPECTED_TRAIN_ROWS or len(source_by_id) != EXPECTED_TRAIN_ROWS:
        raise SystemExit("V113 requires exactly 1082 unique source training calls")
    if len(eval_rows) != EXPECTED_EVAL_ROWS:
        raise SystemExit("V113 requires exactly 35 evaluation calls")
    if set(source_by_id) != set(predictions):
        raise SystemExit("V111 mining predictions do not exactly match source training IDs")
    if not NON_SPEECH_PROBE_IDS <= set(source_by_id):
        raise SystemExit("A non-speech probe ID is absent from the curated training set")

    stage1_counts: dict[str, int] = {}
    stage1_reasons: dict[str, list[str]] = {}
    stage2_counts: dict[str, int] = {}
    stage2_reasons: dict[str, list[str]] = {}
    for row_id, prediction in predictions.items():
        hard = float(prediction["wer"]) >= 0.50
        repeated = bool(prediction.get("repeated"))
        non_speech = row_id in NON_SPEECH_PROBE_IDS

        if hard or repeated or non_speech:
            count = 3 if hard else 1
            reasons = ["targeted_corrective_stage"]
            if hard:
                reasons.append("v111_wer_at_least_0.50")
            if repeated:
                count = max(count, 6)
                reasons.append("v111_repetition")
            if non_speech:
                count = max(count, 4)
                reasons.append("non_speech_probe")
            if row_id == CONTAMINATION_ID:
                count = 24
                reasons.append("known_contamination_correction")
            stage1_counts[row_id] = count
            stage1_reasons[row_id] = reasons

        count = 1
        reasons = ["full_curated_set"]
        if hard:
            count = 2
            reasons.append("v111_wer_at_least_0.50")
        if repeated:
            count = max(count, 3)
            reasons.append("v111_repetition")
        if row_id == CONTAMINATION_ID:
            count = 12
            reasons.append("known_contamination_correction")
        stage2_counts[row_id] = count
        stage2_reasons[row_id] = reasons

    stage1_rows = expanded_rows(
        train_source, predictions, stage1_counts, stage1_reasons
    )
    stage2_rows = expanded_rows(
        train_source, predictions, stage2_counts, stage2_reasons
    )

    stage1_dir = output_dir / "stage1_targeted"
    stage2_dir = output_dir / "stage2_consolidation"
    stage1_dir.mkdir(parents=True)
    stage2_dir.mkdir(parents=True)
    for directory, rows in ((stage1_dir, stage1_rows), (stage2_dir, stage2_rows)):
        write_jsonl(directory / "train.jsonl", rows)
        write_jsonl(directory / "eval.jsonl", eval_rows)

    eval_ids = {str(row["id"]) for row in eval_rows}
    stage1_unique = {str(row["id"]) for row in stage1_rows}
    stage2_unique = {str(row["id"]) for row in stage2_rows}
    audit = {
        "version": "V113",
        "source_manifest_dir": str(source_dir),
        "mining_predictions": str(predictions_path),
        "source_train_rows": len(train_source),
        "source_train_unique_ids": len(source_by_id),
        "eval_rows": len(eval_rows),
        "test_rows_loaded": 0,
        "stage1": {
            "strategy": "measured_failures_and_non_speech_only",
            "unique_ids": len(stage1_unique),
            "presentations": len(stage1_rows),
            "hard_ids": sum(float(row["wer"]) >= 0.50 for row in prediction_rows),
            "repetition_ids": sum(bool(row.get("repeated")) for row in prediction_rows),
            "non_speech_probe_ids": len(NON_SPEECH_PROBE_IDS),
            "contamination_presentations": stage1_counts[CONTAMINATION_ID],
            "train_eval_exact_overlap": sorted(stage1_unique & eval_ids),
        },
        "stage2": {
            "strategy": "full_curated_set_consolidation",
            "unique_ids": len(stage2_unique),
            "presentations": len(stage2_rows),
            "all_source_train_ids_retained": stage2_unique == set(source_by_id),
            "missing_source_train_ids": sorted(set(source_by_id) - stage2_unique),
            "contamination_presentations": stage2_counts[CONTAMINATION_ID],
            "train_eval_exact_overlap": sorted(stage2_unique & eval_ids),
        },
        "manifest_sha256": {
            "stage1_train": sha256(stage1_dir / "train.jsonl"),
            "stage1_eval": sha256(stage1_dir / "eval.jsonl"),
            "stage2_train": sha256(stage2_dir / "train.jsonl"),
            "stage2_eval": sha256(stage2_dir / "eval.jsonl"),
        },
        "flags": [],
    }
    (output_dir / "audit.json").write_text(
        json.dumps(audit, indent=2) + "\n", encoding="utf-8"
    )
    print(json.dumps(audit, indent=2))


if __name__ == "__main__":
    main()
