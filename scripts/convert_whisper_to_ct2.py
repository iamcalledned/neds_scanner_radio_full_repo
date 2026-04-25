#!/usr/bin/env python3
"""
Convert a Hugging Face Whisper checkpoint to CTranslate2/faster-whisper format.

This wrapper handles two local pain points:
- newer Hugging Face Whisper directories may not include ``tokenizer.json``
- some environments hit a ``dtype`` vs ``torch_dtype`` compatibility mismatch

The script builds a temporary normalized model directory, materializes the
missing tokenizer/feature-extractor files there, and then invokes the
CTranslate2 converter through the current Python environment.
"""

from __future__ import annotations

import argparse
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

from transformers import AutoFeatureExtractor, AutoTokenizer


def build_temp_model_dir(source_dir: Path) -> str:
    temp_dir = tempfile.mkdtemp(prefix="ct2-whisper-")
    temp_path = Path(temp_dir)
    asset_dir = Path(tempfile.mkdtemp(prefix="ct2-whisper-assets-"))

    try:
        for child in source_dir.iterdir():
            target = temp_path / child.name
            if child.is_dir():
                shutil.copytree(child, target, symlinks=True)
            else:
                try:
                    target.symlink_to(child)
                except OSError:
                    shutil.copy2(child, target)

        if not (temp_path / "tokenizer.json").exists():
            AutoTokenizer.from_pretrained(str(source_dir)).save_pretrained(str(asset_dir))
            shutil.copy2(asset_dir / "tokenizer.json", temp_path / "tokenizer.json")

        if not (temp_path / "preprocessor_config.json").exists():
            AutoFeatureExtractor.from_pretrained(str(source_dir)).save_pretrained(str(asset_dir))
            shutil.copy2(asset_dir / "preprocessor_config.json", temp_path / "preprocessor_config.json")
    finally:
        shutil.rmtree(asset_dir, ignore_errors=True)

    return temp_dir


def main() -> int:
    parser = argparse.ArgumentParser(description="Convert a Whisper HF model to CT2 with local compatibility fixes.")
    parser.add_argument("--model", required=True, help="Path to the source Hugging Face Whisper model directory.")
    parser.add_argument("--output_dir", required=True, help="Output directory for the converted CT2 model.")
    parser.add_argument("--quantization", default="float16", help="CTranslate2 quantization mode.")
    args, extra_args = parser.parse_known_args()

    source_dir = Path(args.model).expanduser().resolve()
    output_dir = Path(args.output_dir).expanduser().resolve()
    if not source_dir.exists():
        raise FileNotFoundError(f"Source model directory not found: {source_dir}")

    compat_wrapper = Path(__file__).resolve().parent / "ct2_transformers_converter_compat.py"
    if not compat_wrapper.exists():
        raise FileNotFoundError(f"Compatibility wrapper not found: {compat_wrapper}")

    temp_model_dir = build_temp_model_dir(source_dir)
    cmd = [
        sys.executable,
        str(compat_wrapper),
        "--model",
        temp_model_dir,
        "--output_dir",
        str(output_dir),
        "--copy_files",
        "tokenizer.json",
        "preprocessor_config.json",
        "--quantization",
        args.quantization,
        *extra_args,
    ]

    try:
        return subprocess.call(cmd)
    finally:
        shutil.rmtree(temp_model_dir, ignore_errors=True)


if __name__ == "__main__":
    raise SystemExit(main())
