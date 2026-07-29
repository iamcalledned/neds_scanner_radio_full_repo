#!/usr/bin/env python3
"""Train a Whisper LoRA adapter on segmented manifests."""

from __future__ import annotations

import argparse
import json
from collections import Counter
from pathlib import Path
from typing import Any

import jiwer
import numpy as np
import torch
from peft import LoraConfig, PeftModel, get_peft_model
from transformers import (
    EarlyStoppingCallback,
    Seq2SeqTrainer,
    Seq2SeqTrainingArguments,
    WhisperForConditionalGeneration,
    WhisperProcessor,
    set_seed,
)

from train_whisper import ManifestDataset, WhisperCollator, normalized_text


def load_config(path: Path) -> dict:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise SystemExit(f"Could not read {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise SystemExit(f"Config must contain a JSON object: {path}")
    return value


def resolve(value: str, relative_to: Path) -> Path:
    path = Path(value).expanduser()
    return (path if path.is_absolute() else relative_to / path).resolve()


def parse_args() -> tuple[argparse.Namespace, dict, Path]:
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--resume-from-checkpoint")
    args = parser.parse_args()
    config_file = args.config.expanduser().resolve()
    return args, load_config(config_file), config_file


def compute_metrics(processor: WhisperProcessor):
    def metrics(prediction: Any) -> dict[str, float]:
        prediction_ids = prediction.predictions
        if isinstance(prediction_ids, tuple):
            prediction_ids = prediction_ids[0]
        label_ids = np.where(
            prediction.label_ids == -100,
            processor.tokenizer.pad_token_id,
            prediction.label_ids,
        )
        hypotheses = [
            normalized_text(text)
            for text in processor.tokenizer.batch_decode(
                prediction_ids, skip_special_tokens=True
            )
        ]
        references = [
            normalized_text(text)
            for text in processor.tokenizer.batch_decode(label_ids, skip_special_tokens=True)
        ]
        negative_indices = [i for i, reference in enumerate(references) if not reference]
        hallucination_rate = (
            100.0
            * sum(bool(hypotheses[i]) for i in negative_indices)
            / len(negative_indices)
            if negative_indices
            else 0.0
        )
        repeated = 0
        for hypothesis in hypotheses:
            words = hypothesis.split()
            if len(words) >= 8 and max(Counter(words).values()) / len(words) >= 0.5:
                repeated += 1
        return {
            "wer": 100.0 * jiwer.wer(references, hypotheses),
            "hallucination_rate": hallucination_rate,
            "repetition_rate": 100.0 * repeated / len(hypotheses),
        }

    return metrics


def main() -> None:
    args, config, config_file = parse_args()
    if not torch.cuda.is_available():
        raise SystemExit("CUDA is required for Whisper Medium LoRA training.")

    base = resolve(config["model"], config_file.parent)
    manifests = resolve(config["manifests_dir"], config_file.parent)
    output = resolve(config["output_dir"], config_file.parent)
    merged = resolve(config["merged_output_dir"], config_file.parent)
    if output.exists() and any(output.iterdir()) and not args.resume_from_checkpoint:
        raise SystemExit(f"Output already exists and is not empty: {output}")
    if merged.exists() and any(merged.iterdir()):
        raise SystemExit(f"Merged output already exists and is not empty: {merged}")

    seed = int(config.get("seed", 42))
    set_seed(seed)
    processor = WhisperProcessor.from_pretrained(str(base), language="en", task="transcribe")
    model = WhisperForConditionalGeneration.from_pretrained(
        str(base),
        use_safetensors=True,
        low_cpu_mem_usage=True,
        dtype=torch.float32,
    )
    model.config.use_cache = False
    model.generation_config.forced_decoder_ids = None
    if getattr(model.generation_config, "lang_to_id", None):
        model.generation_config.language = "en"
        model.generation_config.task = "transcribe"

    target_regex = config.get("lora_target_regex")
    if target_regex is not None:
        if not isinstance(target_regex, str) or not target_regex.strip():
            raise SystemExit("lora_target_regex must be a non-empty string")
        target_modules: str | list[str] = target_regex
    else:
        target_modules = list(config.get("lora_targets", ["q_proj", "v_proj"]))

    lora_config = LoraConfig(
        r=int(config.get("lora_r", 16)),
        lora_alpha=int(config.get("lora_alpha", 32)),
        lora_dropout=float(config.get("lora_dropout", 0.05)),
        target_modules=target_modules,
        bias="none",
    )
    model = get_peft_model(model, lora_config)
    model.print_trainable_parameters()
    trainable = sum(p.numel() for p in model.parameters() if p.requires_grad)
    total = sum(p.numel() for p in model.parameters())

    train_data = ManifestDataset(manifests / "train.jsonl")
    eval_data = ManifestDataset(manifests / "eval.jsonl")
    evaluate_test = bool(config.get("evaluate_test", True))
    test_data = ManifestDataset(manifests / "test.jsonl") if evaluate_test else None
    effective_batch = int(config.get("batch_size", 2)) * int(
        config.get("gradient_accumulation_steps", 8)
    )

    training_args = Seq2SeqTrainingArguments(
        output_dir=str(output),
        num_train_epochs=float(config.get("epochs", 18)),
        learning_rate=float(config.get("learning_rate", 1e-4)),
        per_device_train_batch_size=int(config.get("batch_size", 2)),
        per_device_eval_batch_size=int(config.get("eval_batch_size", 2)),
        gradient_accumulation_steps=int(config.get("gradient_accumulation_steps", 8)),
        warmup_ratio=float(config.get("warmup_ratio", 0.05)),
        lr_scheduler_type=str(config.get("lr_scheduler_type", "cosine")),
        weight_decay=float(config.get("weight_decay", 0.01)),
        fp16=bool(config.get("fp16", True)),
        bf16=bool(config.get("bf16", False)),
        gradient_checkpointing=bool(config.get("gradient_checkpointing", False)),
        eval_strategy="epoch",
        save_strategy="epoch",
        logging_strategy="steps",
        logging_steps=int(config.get("logging_steps", 10)),
        save_total_limit=int(config.get("save_total_limit", 3)),
        predict_with_generate=True,
        generation_max_length=int(config.get("generation_max_length", 448)),
        generation_num_beams=int(config.get("generation_num_beams", 2)),
        load_best_model_at_end=True,
        metric_for_best_model="wer",
        greater_is_better=False,
        remove_unused_columns=False,
        dataloader_num_workers=int(config.get("num_workers", 0)),
        report_to="none",
        optim="adamw_torch",
        max_grad_norm=1.0,
        seed=seed,
        data_seed=seed,
    )
    trainer = Seq2SeqTrainer(
        model=model,
        args=training_args,
        train_dataset=train_data,
        eval_dataset=eval_data,
        data_collator=WhisperCollator(
            processor,
            model.config.decoder_start_token_id,
            model=model,
            augment_probability=float(config.get("augment_probability", 0.0)),
        ),
        compute_metrics=compute_metrics(processor),
        processing_class=processor,
        callbacks=[
            EarlyStoppingCallback(
                early_stopping_patience=int(config.get("early_stopping_patience", 3))
            )
        ],
    )
    train_result = trainer.train(resume_from_checkpoint=args.resume_from_checkpoint)
    best_adapter = output / "best_adapter"
    trainer.save_model(str(best_adapter))
    processor.save_pretrained(str(best_adapter))
    trainer.save_metrics("train", train_result.metrics)
    trainer.save_metrics("eval", trainer.evaluate(metric_key_prefix="eval"))
    if test_data is not None:
        trainer.save_metrics("test", trainer.predict(test_data, metric_key_prefix="test").metrics)
    trainer.save_state()

    # Reload and merge from disk so the deployable model is provably composed
    # from the configured base plus the selected adapter.
    del trainer, model
    torch.cuda.empty_cache()
    base_model = WhisperForConditionalGeneration.from_pretrained(
        str(base), torch_dtype=torch.float16, low_cpu_mem_usage=True
    )
    merged_model = PeftModel.from_pretrained(base_model, str(best_adapter)).merge_and_unload()
    merged.mkdir(parents=True, exist_ok=True)
    merged_model.save_pretrained(str(merged))
    processor.save_pretrained(str(merged))

    run_manifest = {
        "base_model": str(base),
        "manifests_dir": str(manifests),
        "output_dir": str(output),
        "best_adapter": str(best_adapter),
        "merged_output_dir": str(merged),
        "lora": {
            "r": lora_config.r,
            "alpha": lora_config.lora_alpha,
            "dropout": lora_config.lora_dropout,
            "targets": (
                [lora_config.target_modules]
                if isinstance(lora_config.target_modules, str)
                else sorted(lora_config.target_modules)
            ),
            "trainable_parameters": trainable,
            "total_parameters": total,
            "trainable_percent": 100.0 * trainable / total,
        },
        "epochs_requested": training_args.num_train_epochs,
        "learning_rate": training_args.learning_rate,
        "effective_batch_size": effective_batch,
        "generation_num_beams": training_args.generation_num_beams,
        "seed": seed,
        "best_checkpoint": trainer_state_best(output),
    }
    run_manifest_filename = str(config.get("run_manifest_filename", "run_manifest.json"))
    run_manifest_path = Path(run_manifest_filename)
    if run_manifest_path.name != run_manifest_filename:
        raise SystemExit("run_manifest_filename must be a plain filename")
    (output / run_manifest_path).write_text(
        json.dumps(run_manifest, indent=2) + "\n", encoding="utf-8"
    )
    print(json.dumps(run_manifest, indent=2))


def trainer_state_best(output: Path) -> str | None:
    state = output / "trainer_state.json"
    if not state.exists():
        return None
    return json.loads(state.read_text(encoding="utf-8")).get("best_model_checkpoint")


if __name__ == "__main__":
    main()
