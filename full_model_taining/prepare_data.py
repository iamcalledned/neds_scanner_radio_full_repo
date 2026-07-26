#!/usr/bin/env python3
"""Validate paired scanner exports and write JSONL manifests for Whisper."""

from __future__ import annotations

import argparse
import json
import wave
from collections import Counter
from pathlib import Path


def load_config(path: Path) -> dict:
    if not path.is_file():
        raise SystemExit(f"Config file not found: {path}")
    try:
        config = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise SystemExit(f"Could not read config {path}: {exc}") from exc
    if not isinstance(config, dict):
        raise SystemExit(f"Config root must be a JSON object: {path}")
    return config


def config_path(value: str | Path, config_file: Path) -> Path:
    path = Path(value).expanduser()
    return path if path.is_absolute() else config_file.parent / path


def parse_args() -> argparse.Namespace:
    here = Path(__file__).resolve().parent
    bootstrap = argparse.ArgumentParser(add_help=False)
    bootstrap.add_argument("--config", type=Path, default=here / "training_config.json")
    known, _ = bootstrap.parse_known_args()
    config_file = known.config.expanduser().resolve()
    config = load_config(config_file)

    parser = argparse.ArgumentParser(parents=[bootstrap])
    parser.add_argument(
        "--exports-dir",
        type=Path,
        default=config_path(config.get("exports_dir", "exports"), config_file),
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=config_path(config.get("manifests_dir", "manifests"), config_file),
    )
    parser.add_argument(
        "--max-duration",
        type=float,
        default=float(config.get("max_duration", 30.0)),
        help="Whisper training-window threshold; longer clips are queued for alignment.",
    )
    parser.add_argument(
        "--min-duration", type=float, default=float(config.get("min_duration", 0.25))
    )
    parser.add_argument(
        "--allow-transcript-fallback",
        action=argparse.BooleanOptionalAction,
        default=bool(config.get("allow_transcript_fallback", True)),
        help="Use decoded transcript only when edited_transcript is absent.",
    )
    return parser.parse_args()


def audio_info(path: Path) -> tuple[float, int, int]:
    with wave.open(str(path), "rb") as wav:
        return wav.getnframes() / wav.getframerate(), wav.getframerate(), wav.getnchannels()


def prepare_split(
    split_dir: Path,
    manifest_path: Path,
    long_manifest_path: Path,
    min_duration: float,
    max_duration: float,
    allow_fallback: bool,
    excluded_filenames: set[str],
) -> dict:
    rows: list[dict] = []
    long_rows: list[dict] = []
    rejected: list[dict] = []
    reasons: Counter[str] = Counter()

    json_paths = sorted(split_dir.glob("*.json"))
    wav_stems = {p.stem for p in split_dir.glob("*.wav")}
    json_stems = {p.stem for p in json_paths}

    for stem in sorted(wav_stems - json_stems):
        rejected.append({"file": stem + ".wav", "reason": "missing_json"})
        reasons["missing_json"] += 1

    for json_path in json_paths:
        wav_path = json_path.with_suffix(".wav")
        if wav_path.name in excluded_filenames or json_path.name in excluded_filenames:
            rejected.append({"file": wav_path.name, "reason": "configured_exclusion"})
            reasons["configured_exclusion"] += 1
            continue
        try:
            metadata = json.loads(json_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            rejected.append({"file": json_path.name, "reason": "invalid_json", "detail": str(exc)})
            reasons["invalid_json"] += 1
            continue

        manual = metadata.get("edited_transcript")
        fallback = metadata.get("transcript") if allow_fallback else None
        text = manual if isinstance(manual, str) and manual.strip() else fallback
        source = "edited_transcript" if isinstance(manual, str) and manual.strip() else "transcript"
        if not isinstance(text, str) or not text.strip():
            rejected.append({"file": json_path.name, "reason": "missing_transcript"})
            reasons["missing_transcript"] += 1
            continue
        if not wav_path.is_file():
            rejected.append({"file": json_path.name, "reason": "missing_wav"})
            reasons["missing_wav"] += 1
            continue

        try:
            duration, sample_rate, channels = audio_info(wav_path)
        except (OSError, wave.Error) as exc:
            rejected.append({"file": wav_path.name, "reason": "invalid_wav", "detail": str(exc)})
            reasons["invalid_wav"] += 1
            continue

        if duration < min_duration:
            reason = "too_short"
        elif duration > max_duration:
            reason = "needs_alignment"
        elif sample_rate != 16_000:
            reason = "wrong_sample_rate"
        elif channels != 1:
            reason = "not_mono"
        else:
            reason = ""
        if reason == "needs_alignment":
            long_rows.append(
                {
                    "audio": str(wav_path.resolve()),
                    "text": " ".join(text.split()),
                    "duration": round(duration, 6),
                    "transcript_source": source,
                    "category": metadata.get("category"),
                    "town": metadata.get("town"),
                    "id": metadata.get("id"),
                }
            )
            continue
        if reason:
            rejected.append(
                {
                    "file": wav_path.name,
                    "reason": reason,
                    "duration": round(duration, 6),
                    "sample_rate": sample_rate,
                    "channels": channels,
                }
            )
            reasons[reason] += 1
            continue

        rows.append(
            {
                "audio": str(wav_path.resolve()),
                "text": " ".join(text.split()),
                "duration": round(duration, 6),
                "transcript_source": source,
                "category": metadata.get("category"),
                "town": metadata.get("town"),
                "id": metadata.get("id"),
            }
        )

    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    with manifest_path.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, ensure_ascii=False) + "\n")
    with long_manifest_path.open("w", encoding="utf-8") as handle:
        for row in long_rows:
            handle.write(json.dumps(row, ensure_ascii=False) + "\n")
    rejected_path = manifest_path.with_name(manifest_path.stem + "_rejected.json")
    rejected_path.write_text(json.dumps(rejected, indent=2), encoding="utf-8")
    return {
        "accepted": len(rows),
        "rejected": len(rejected),
        "queued_for_alignment": len(long_rows),
        "rejection_reasons": dict(sorted(reasons.items())),
        "hours": round(sum(row["duration"] for row in rows) / 3600, 3),
        "manual_targets": sum(row["transcript_source"] == "edited_transcript" for row in rows),
        "fallback_targets": sum(row["transcript_source"] == "transcript" for row in rows),
    }


def main() -> None:
    args = parse_args()
    config = load_config(args.config.expanduser().resolve())
    excluded_filenames = {
        value.strip()
        for value in config.get("exclude_filenames", [])
        if isinstance(value, str) and value.strip()
    }
    summary = {}
    for split in ("train", "eval", "test"):
        split_dir = args.exports_dir.resolve() / split
        if not split_dir.is_dir():
            raise SystemExit(f"Missing split directory: {split_dir}")
        summary[split] = prepare_split(
            split_dir,
            args.output_dir.resolve() / f"{split}.jsonl",
            args.output_dir.resolve() / f"{split}_long.jsonl",
            args.min_duration,
            args.max_duration,
            args.allow_transcript_fallback,
            excluded_filenames,
        )
    summary_path = args.output_dir.resolve() / "summary.json"
    summary_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    print(json.dumps(summary, indent=2))
    print(f"Manifests written to {args.output_dir.resolve()}")


if __name__ == "__main__":
    main()
