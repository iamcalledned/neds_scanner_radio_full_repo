#!/usr/bin/env python3
"""Convert the configured best Hugging Face checkpoint for faster-whisper."""

from __future__ import annotations

import argparse
import json
import shutil
import subprocess
import sys
from pathlib import Path


def main() -> None:
    here = Path(__file__).resolve().parent
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", type=Path, default=here / "training_config.json")
    parser.add_argument("--quantization", default="float16")
    args = parser.parse_args()

    config_file = args.config.expanduser().resolve()
    try:
        config = json.loads(config_file.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise SystemExit(f"Could not read config {config_file}: {exc}") from exc

    def configured_path(key: str) -> Path:
        value = config.get(key)
        if not isinstance(value, str) or not value.strip():
            raise SystemExit(f"Missing string setting {key!r} in {config_file}")
        path = Path(value).expanduser()
        return (path if path.is_absolute() else config_file.parent / path).resolve()

    if config.get("merged_output_dir"):
        source = configured_path("merged_output_dir")
    else:
        source = configured_path("output_dir") / "best"
    target = configured_path("ctranslate2_output_dir")
    converter = shutil.which("ct2-transformers-converter")
    if converter is None:
        sibling_converter = Path(sys.executable).with_name("ct2-transformers-converter")
        if sibling_converter.is_file():
            converter = str(sibling_converter)
    if converter is None:
        raise SystemExit(
            "ct2-transformers-converter is not installed. Run "
            "`.venv/bin/pip install ctranslate2` first."
        )
    if not source.is_dir():
        raise SystemExit(f"Best model directory not found: {source}")
    if target.exists() and any(target.iterdir()):
        raise SystemExit(f"Conversion target already exists and is not empty: {target}")

    target.parent.mkdir(parents=True, exist_ok=True)
    copy_files = [
        name
        for name in ("tokenizer.json", "tokenizer_config.json", "generation_config.json")
        if (source / name).is_file()
    ]
    subprocess.run(
        [
            converter,
            "--model",
            str(source),
            "--output_dir",
            str(target),
            "--copy_files",
            *copy_files,
            "--quantization",
            args.quantization,
        ],
        check=True,
    )
    preprocessor_path = source / "preprocessor_config.json"
    if preprocessor_path.is_file():
        shutil.copy2(preprocessor_path, target / "preprocessor_config.json")
    else:
        processor_path = source / "processor_config.json"
        if not processor_path.is_file():
            raise SystemExit(
                f"Neither preprocessor_config.json nor processor_config.json exists in {source}"
            )
        processor_config = json.loads(processor_path.read_text(encoding="utf-8"))
        feature_config = processor_config.get("feature_extractor")
        if not isinstance(feature_config, dict):
            raise SystemExit(f"No feature_extractor settings in {processor_path}")
        (target / "preprocessor_config.json").write_text(
            json.dumps(feature_config, indent=2) + "\n", encoding="utf-8"
        )
    print(f"CTranslate2 model saved to {target}")


if __name__ == "__main__":
    main()
