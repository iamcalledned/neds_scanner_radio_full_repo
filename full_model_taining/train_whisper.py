#!/usr/bin/env python3
"""Fine-tune Whisper Medium on scanner-radio JSONL manifests."""

from __future__ import annotations

import argparse
import json
import random
import re
from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import jiwer
import numpy as np
import soundfile as sf
import torch
from torch.utils.data import Dataset
from transformers import (
    Seq2SeqTrainer,
    Seq2SeqTrainingArguments,
    WhisperForConditionalGeneration,
    WhisperProcessor,
    set_seed,
)


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


def validate_model_reference(value: str) -> str:
    expanded = Path(value).expanduser()
    if expanded.is_absolute() or value.startswith((".", "~")):
        required = ("config.json", "model.safetensors")
        missing = [name for name in required if not (expanded / name).is_file()]
        if missing:
            raise SystemExit(
                f"Configured local model is missing {', '.join(missing)}: {expanded}\n"
                "Use a complete local Transformers model directory or a Hub ID such as "
                "'openai/whisper-medium.en'."
            )
        return str(expanded.resolve())
    return value


def parse_args() -> argparse.Namespace:
    here = Path(__file__).resolve().parent
    bootstrap = argparse.ArgumentParser(add_help=False)
    bootstrap.add_argument("--config", type=Path, default=here / "training_config.json")
    known, _ = bootstrap.parse_known_args()
    config_file = known.config.expanduser().resolve()
    config = load_config(config_file)

    parser = argparse.ArgumentParser(parents=[bootstrap])
    parser.add_argument(
        "--model",
        type=validate_model_reference,
        default=validate_model_reference(config.get("model", "openai/whisper-medium.en")),
    )
    parser.add_argument(
        "--manifests-dir",
        type=Path,
        default=config_path(config.get("manifests_dir", "manifests"), config_file),
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=config_path(
            config.get("output_dir", "outputs/whisper-medium-scanner"), config_file
        ),
    )
    parser.add_argument("--epochs", type=float, default=float(config.get("epochs", 5.0)))
    parser.add_argument(
        "--learning-rate", type=float, default=float(config.get("learning_rate", 1e-5))
    )
    parser.add_argument("--batch-size", type=int, default=int(config.get("batch_size", 1)))
    parser.add_argument(
        "--eval-batch-size", type=int, default=int(config.get("eval_batch_size", 1))
    )
    parser.add_argument(
        "--gradient-accumulation-steps",
        type=int,
        default=int(config.get("gradient_accumulation_steps", 16)),
    )
    parser.add_argument(
        "--warmup-ratio", type=float, default=float(config.get("warmup_ratio", 0.05))
    )
    parser.add_argument("--save-steps", type=int, default=int(config.get("save_steps", 100)))
    parser.add_argument("--eval-steps", type=int, default=int(config.get("eval_steps", 100)))
    parser.add_argument(
        "--logging-steps", type=int, default=int(config.get("logging_steps", 10))
    )
    parser.add_argument(
        "--metric-for-best-model",
        choices=("wer", "selection_score"),
        default=str(config.get("metric_for_best_model", "wer")),
        help="Checkpoint metric. selection_score adds hallucination and repetition penalties.",
    )
    parser.add_argument("--num-workers", type=int, default=int(config.get("num_workers", 2)))
    parser.add_argument("--seed", type=int, default=int(config.get("seed", 42)))
    parser.add_argument("--resume-from-checkpoint", default=None)
    parser.add_argument(
        "--training-stage",
        choices=("full", "acoustic", "language", "consolidation"),
        default=str(config.get("training_stage", "full")),
        help=(
            "Layer-training policy: acoustic trains the convolution stem and early "
            "encoder blocks; language trains only the decoder; consolidation and "
            "full train all parameters."
        ),
    )
    parser.add_argument(
        "--acoustic-blocks",
        type=int,
        default=int(config.get("acoustic_blocks", 3)),
        help="Number of early encoder transformer blocks opened in acoustic mode.",
    )
    parser.add_argument(
        "--augment-probability",
        type=float,
        default=float(config.get("augment_probability", 0.0)),
        help="Training-only probability of applying conservative gain and static noise.",
    )
    parser.add_argument(
        "--freeze-encoder",
        action=argparse.BooleanOptionalAction,
        default=bool(config.get("freeze_encoder", False)),
    )
    parser.add_argument(
        "--gradient-checkpointing",
        action=argparse.BooleanOptionalAction,
        default=bool(config.get("gradient_checkpointing", True)),
    )
    parser.add_argument(
        "--fp16",
        action=argparse.BooleanOptionalAction,
        default=bool(config.get("fp16", True)),
    )
    parser.add_argument(
        "--bf16",
        action=argparse.BooleanOptionalAction,
        default=bool(config.get("bf16", False)),
    )
    parser.add_argument(
        "--run-test",
        action=argparse.BooleanOptionalAction,
        default=bool(config.get("run_test", True)),
    )
    return parser.parse_args()


class ManifestDataset(Dataset):
    def __init__(self, path: Path):
        self.rows = [
            json.loads(line)
            for line in path.read_text(encoding="utf-8").splitlines()
            if line.strip()
        ]
        if not self.rows:
            raise ValueError(f"No samples in {path}")

    def __len__(self) -> int:
        return len(self.rows)

    def __getitem__(self, index: int) -> dict:
        row = self.rows[index]
        start = int(round(float(row.get("start", 0.0)) * 16_000))
        stop_value = row.get("end")
        stop = int(round(float(stop_value) * 16_000)) if stop_value is not None else None
        audio, sample_rate = sf.read(
            row["audio"],
            start=start,
            stop=stop,
            dtype="float32",
            always_2d=False,
        )
        if sample_rate != 16_000 or audio.ndim != 1:
            raise ValueError(f"Expected mono 16 kHz WAV: {row['audio']}")
        return {"audio": audio, "text": row["text"]}


@dataclass
class WhisperCollator:
    processor: WhisperProcessor
    decoder_start_token_id: int
    model: WhisperForConditionalGeneration | None = None
    augment_probability: float = 0.0

    def augment(self, audio: np.ndarray) -> np.ndarray:
        if self.augment_probability <= 0 or random.random() >= self.augment_probability:
            return audio
        gain_db = random.uniform(-3.0, 3.0)
        augmented = audio * (10.0 ** (gain_db / 20.0))
        signal_power = float(np.mean(np.square(augmented)))
        if signal_power > 0:
            snr_db = random.uniform(18.0, 32.0)
            noise_power = signal_power / (10.0 ** (snr_db / 10.0))
            noise = np.random.normal(0.0, np.sqrt(noise_power), augmented.shape)
            augmented = augmented + noise.astype(np.float32)
        return np.clip(augmented, -1.0, 1.0).astype(np.float32, copy=False)

    def __call__(self, features: list[dict]) -> dict[str, torch.Tensor]:
        training = self.model is not None and self.model.training
        audio = [
            self.augment(item["audio"]) if training else item["audio"]
            for item in features
        ]
        inputs = self.processor.feature_extractor(
            audio,
            sampling_rate=16_000,
            return_tensors="pt",
            return_attention_mask=True,
            padding="max_length",
            max_length=30 * 16_000,
            truncation=True,
        )
        tokenized = self.processor.tokenizer(
            [item["text"] for item in features],
            return_tensors="pt",
            padding=True,
        )
        labels = tokenized.input_ids.masked_fill(tokenized.attention_mask.ne(1), -100)
        if (labels[:, 0] == self.decoder_start_token_id).all().item():
            labels = labels[:, 1:]
        return {
            "input_features": inputs.input_features,
            "attention_mask": inputs.attention_mask,
            "labels": labels,
        }


def normalized_text(text: str) -> str:
    return re.sub(r"\s+", " ", re.sub(r"[^\w\s']", " ", text.lower())).strip()


def configure_trainable_layers(
    model: WhisperForConditionalGeneration,
    stage: str,
    acoustic_blocks: int,
) -> dict[str, Any]:
    for parameter in model.parameters():
        parameter.requires_grad = stage in {"full", "consolidation"}

    trained_groups: list[str] = []
    if stage == "acoustic":
        for parameter in model.model.encoder.conv1.parameters():
            parameter.requires_grad = True
        for parameter in model.model.encoder.conv2.parameters():
            parameter.requires_grad = True
        trained_groups.extend(("encoder.conv1", "encoder.conv2"))

        count = min(max(acoustic_blocks, 0), len(model.model.encoder.layers))
        for index in range(count):
            for parameter in model.model.encoder.layers[index].parameters():
                parameter.requires_grad = True
            trained_groups.append(f"encoder.layers.{index}")
        for parameter in model.model.encoder.layer_norm.parameters():
            parameter.requires_grad = True
        trained_groups.append("encoder.layer_norm")
    elif stage == "language":
        for parameter in model.model.decoder.parameters():
            parameter.requires_grad = True
        if hasattr(model, "proj_out"):
            for parameter in model.proj_out.parameters():
                parameter.requires_grad = True
        trained_groups.extend(("decoder", "proj_out"))
    else:
        trained_groups.append("all")

    total = sum(parameter.numel() for parameter in model.parameters())
    trainable = sum(
        parameter.numel() for parameter in model.parameters() if parameter.requires_grad
    )
    if not trainable:
        raise SystemExit(f"Training stage {stage!r} left zero trainable parameters.")
    return {
        "stage": stage,
        "acoustic_blocks": acoustic_blocks if stage == "acoustic" else None,
        "trained_groups": trained_groups,
        "trainable_parameters": trainable,
        "total_parameters": total,
        "trainable_percent": round(100.0 * trainable / total, 4),
    }


def main() -> None:
    args = parse_args()
    if not torch.cuda.is_available():
        raise SystemExit(
            "CUDA is not available. Whisper Medium full fine-tuning is impractical on CPU. "
            "Install a CUDA-enabled PyTorch build and verify `python -c "
            "\"import torch; print(torch.cuda.is_available())\"` first."
        )
    if args.fp16 and args.bf16:
        raise SystemExit("Choose only one of --fp16 or --bf16.")

    set_seed(args.seed)
    processor = WhisperProcessor.from_pretrained(args.model)
    model = WhisperForConditionalGeneration.from_pretrained(
        args.model,
        use_safetensors=True,
        low_cpu_mem_usage=True,
        dtype=torch.float32,
    )
    model.config.use_cache = not args.gradient_checkpointing
    model.generation_config.forced_decoder_ids = None
    if getattr(model.generation_config, "lang_to_id", None):
        model.generation_config.language = "en"
        model.generation_config.task = "transcribe"
    else:
        # English-only checkpoints do not define language/task token maps.
        model.generation_config.language = None
        model.generation_config.task = None
    if args.freeze_encoder and args.training_stage != "full":
        raise SystemExit("--freeze-encoder cannot be combined with a staged layer policy.")
    if args.freeze_encoder:
        model.freeze_encoder()
        layer_summary = {
            "stage": "full_with_frozen_encoder",
            "trained_groups": ["decoder", "proj_out"],
            "trainable_parameters": sum(
                parameter.numel()
                for parameter in model.parameters()
                if parameter.requires_grad
            ),
            "total_parameters": sum(parameter.numel() for parameter in model.parameters()),
        }
    else:
        layer_summary = configure_trainable_layers(
            model,
            args.training_stage,
            args.acoustic_blocks,
        )
    print(json.dumps({"layer_training": layer_summary}, indent=2))

    train_data = ManifestDataset(args.manifests_dir / "train.jsonl")
    eval_data = ManifestDataset(args.manifests_dir / "eval.jsonl")
    test_path = args.manifests_dir / "test.jsonl"
    test_data = ManifestDataset(test_path) if args.run_test and test_path.exists() else None

    def metrics(prediction: Any) -> dict[str, float]:
        prediction_ids = prediction.predictions
        if isinstance(prediction_ids, tuple):
            prediction_ids = prediction_ids[0]
        label_ids = np.where(
            prediction.label_ids == -100,
            processor.tokenizer.pad_token_id,
            prediction.label_ids,
        )
        hypotheses = processor.tokenizer.batch_decode(prediction_ids, skip_special_tokens=True)
        references = processor.tokenizer.batch_decode(label_ids, skip_special_tokens=True)
        hypotheses = [normalized_text(text) for text in hypotheses]
        references = [normalized_text(text) for text in references]
        wer = 100 * jiwer.wer(references, hypotheses)
        negative_indices = [
            index for index, reference in enumerate(references) if not reference
        ]
        hallucination_rate = (
            100
            * sum(bool(hypotheses[index]) for index in negative_indices)
            / len(negative_indices)
            if negative_indices
            else 0.0
        )
        repeated = 0
        for hypothesis in hypotheses:
            words = hypothesis.split()
            if len(words) >= 8:
                most_common = max(Counter(words).values())
                repeated += most_common / len(words) >= 0.5
        repetition_rate = 100 * repeated / len(hypotheses) if hypotheses else 0.0
        selection_score = wer + 0.25 * hallucination_rate + 0.1 * repetition_rate
        return {
            "wer": wer,
            "hallucination_rate": hallucination_rate,
            "repetition_rate": repetition_rate,
            "selection_score": selection_score,
        }

    output_dir = args.output_dir.resolve()
    training_args = Seq2SeqTrainingArguments(
        output_dir=str(output_dir),
        num_train_epochs=args.epochs,
        learning_rate=args.learning_rate,
        per_device_train_batch_size=args.batch_size,
        per_device_eval_batch_size=args.eval_batch_size,
        gradient_accumulation_steps=args.gradient_accumulation_steps,
        warmup_ratio=args.warmup_ratio,
        gradient_checkpointing=args.gradient_checkpointing,
        fp16=args.fp16,
        bf16=args.bf16,
        eval_strategy="steps",
        save_strategy="steps",
        logging_strategy="steps",
        eval_steps=args.eval_steps,
        save_steps=args.save_steps,
        logging_steps=args.logging_steps,
        predict_with_generate=True,
        generation_max_length=448,
        load_best_model_at_end=True,
        metric_for_best_model=args.metric_for_best_model,
        greater_is_better=False,
        save_total_limit=3,
        remove_unused_columns=False,
        dataloader_num_workers=args.num_workers,
        report_to="none",
        optim="adamw_torch",
        weight_decay=0.01,
        max_grad_norm=1.0,
        seed=args.seed,
        data_seed=args.seed,
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
            augment_probability=args.augment_probability,
        ),
        compute_metrics=metrics,
        processing_class=processor,
    )
    train_result = trainer.train(resume_from_checkpoint=args.resume_from_checkpoint)
    trainer.save_model(str(output_dir / "best"))
    processor.save_pretrained(str(output_dir / "best"))
    trainer.save_metrics("train", train_result.metrics)
    trainer.save_state()
    trainer.save_metrics("eval", trainer.evaluate(metric_key_prefix="eval"))
    if test_data is not None:
        trainer.save_metrics("test", trainer.predict(test_data, metric_key_prefix="test").metrics)
    stage_manifest = {
        "base_model": args.model,
        "output_dir": str(output_dir),
        "manifests_dir": str(args.manifests_dir.resolve()),
        "layer_training": layer_summary,
        "epochs": args.epochs,
        "learning_rate": args.learning_rate,
        "effective_batch_size": args.batch_size * args.gradient_accumulation_steps,
        "augment_probability": args.augment_probability,
        "metric_for_best_model": args.metric_for_best_model,
        "seed": args.seed,
    }
    (output_dir / "stage_manifest.json").write_text(
        json.dumps(stage_manifest, indent=2),
        encoding="utf-8",
    )
    print(f"Best Hugging Face model saved to {output_dir / 'best'}")


if __name__ == "__main__":
    main()
