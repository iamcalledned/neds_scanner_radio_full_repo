#!/usr/bin/env python3
"""Merge one saved PEFT LoRA checkpoint into its Whisper base model."""

from __future__ import annotations

import argparse
from pathlib import Path

from peft import PeftModel
from transformers import AutoModelForSpeechSeq2Seq, AutoProcessor


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--base-model", required=True)
    parser.add_argument("--adapter", required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()

    if args.output.exists() and any(args.output.iterdir()):
        raise SystemExit(f"Output already exists and is not empty: {args.output}")

    base = AutoModelForSpeechSeq2Seq.from_pretrained(
        args.base_model,
        low_cpu_mem_usage=True,
    )
    merged = PeftModel.from_pretrained(base, args.adapter).merge_and_unload()
    args.output.mkdir(parents=True, exist_ok=True)
    merged.save_pretrained(args.output)
    AutoProcessor.from_pretrained(args.base_model).save_pretrained(args.output)
    print(f"Merged model saved to {args.output}")


if __name__ == "__main__":
    main()
