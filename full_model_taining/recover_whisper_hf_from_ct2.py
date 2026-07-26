#!/usr/bin/env python3
"""Recover a trainable Hugging Face Whisper checkpoint from float CT2 weights."""

from __future__ import annotations

import argparse
import json
import struct
from pathlib import Path

import numpy as np
import torch
from transformers import WhisperForConditionalGeneration, WhisperProcessor


DTYPES = {
    0: np.float32,
    1: np.int8,
    2: np.int16,
    3: np.int32,
    4: np.float16,
}


def read_string(handle) -> str:
    size = struct.unpack("H", handle.read(2))[0]
    return handle.read(size)[:-1].decode("utf-8")


def read_ct2(path: Path) -> dict[str, torch.Tensor]:
    values: dict[str, torch.Tensor] = {}
    with path.open("rb") as handle:
        version = struct.unpack("I", handle.read(4))[0]
        spec = read_string(handle)
        revision = struct.unpack("I", handle.read(4))[0]
        if spec != "WhisperSpec":
            raise ValueError(f"Expected WhisperSpec, found {spec}")
        count = struct.unpack("I", handle.read(4))[0]
        for _ in range(count):
            name = read_string(handle)
            rank = struct.unpack("B", handle.read(1))[0]
            shape = tuple(struct.unpack("I", handle.read(4))[0] for _ in range(rank))
            type_id = struct.unpack("B", handle.read(1))[0]
            size = struct.unpack("I", handle.read(4))[0]
            raw = handle.read(size)
            if type_id not in DTYPES:
                continue
            array = np.frombuffer(raw, dtype=DTYPES[type_id]).reshape(shape).copy()
            values[name] = torch.from_numpy(array)
        alias_count = struct.unpack("I", handle.read(4))[0]
        aliases = [(read_string(handle), read_string(handle)) for _ in range(alias_count)]
    for alias, target in aliases:
        values[alias] = values[target]
    print(json.dumps({"binary_version": version, "spec": spec, "revision": revision,
                      "variables": count, "aliases": alias_count}, indent=2))
    return values


def assign(target: dict[str, torch.Tensor], assigned: set[str], key: str, value) -> None:
    if key not in target:
        raise KeyError(f"Unknown Hugging Face weight: {key}")
    tensor = torch.as_tensor(value)
    if tensor.shape != target[key].shape:
        raise ValueError(f"Shape mismatch for {key}: {tensor.shape} != {target[key].shape}")
    target[key] = tensor.to(dtype=target[key].dtype)
    assigned.add(key)


def map_layer(
    values: dict[str, torch.Tensor],
    state: dict[str, torch.Tensor],
    assigned: set[str],
    side: str,
    index: int,
) -> None:
    ct = f"{side}/layer_{index}"
    hf = f"model.{side}.layers.{index}"

    for ct_group, hf_group in (
        ("self_attention/layer_norm", "self_attn_layer_norm"),
        ("ffn/layer_norm", "final_layer_norm"),
    ):
        assign(state, assigned, f"{hf}.{hf_group}.weight", values[f"{ct}/{ct_group}/gamma"])
        assign(state, assigned, f"{hf}.{hf_group}.bias", values[f"{ct}/{ct_group}/beta"])

    for linear, projection in (("linear_0", "fc1"), ("linear_1", "fc2")):
        assign(state, assigned, f"{hf}.{projection}.weight", values[f"{ct}/ffn/{linear}/weight"])
        assign(state, assigned, f"{hf}.{projection}.bias", values[f"{ct}/ffn/{linear}/bias"])

    self_weight = values[f"{ct}/self_attention/linear_0/weight"].chunk(3, dim=0)
    self_bias = values[f"{ct}/self_attention/linear_0/bias"].chunk(3, dim=0)
    for part, projection in enumerate(("q_proj", "k_proj", "v_proj")):
        assign(state, assigned, f"{hf}.self_attn.{projection}.weight", self_weight[part])
        bias_key = f"{hf}.self_attn.{projection}.bias"
        if bias_key in state:
            assign(state, assigned, bias_key, self_bias[part])
    assign(
        state, assigned, f"{hf}.self_attn.out_proj.weight",
        values[f"{ct}/self_attention/linear_1/weight"],
    )
    assign(
        state, assigned, f"{hf}.self_attn.out_proj.bias",
        values[f"{ct}/self_attention/linear_1/bias"],
    )

    if side == "decoder":
        assign(
            state, assigned, f"{hf}.encoder_attn_layer_norm.weight",
            values[f"{ct}/attention/layer_norm/gamma"],
        )
        assign(
            state, assigned, f"{hf}.encoder_attn_layer_norm.bias",
            values[f"{ct}/attention/layer_norm/beta"],
        )
        assign(
            state, assigned, f"{hf}.encoder_attn.q_proj.weight",
            values[f"{ct}/attention/linear_0/weight"],
        )
        assign(
            state, assigned, f"{hf}.encoder_attn.q_proj.bias",
            values[f"{ct}/attention/linear_0/bias"],
        )
        cross_weight = values[f"{ct}/attention/linear_1/weight"].chunk(2, dim=0)
        cross_bias = values[f"{ct}/attention/linear_1/bias"].chunk(2, dim=0)
        for part, projection in enumerate(("k_proj", "v_proj")):
            assign(state, assigned, f"{hf}.encoder_attn.{projection}.weight", cross_weight[part])
            bias_key = f"{hf}.encoder_attn.{projection}.bias"
            if bias_key in state:
                assign(state, assigned, bias_key, cross_bias[part])
        assign(
            state, assigned, f"{hf}.encoder_attn.out_proj.weight",
            values[f"{ct}/attention/linear_2/weight"],
        )
        assign(
            state, assigned, f"{hf}.encoder_attn.out_proj.bias",
            values[f"{ct}/attention/linear_2/bias"],
        )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--ct2-model", type=Path, required=True)
    parser.add_argument("--template-model", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    args = parser.parse_args()
    if args.output_dir.exists() and any(args.output_dir.iterdir()):
        raise SystemExit(f"Refusing to overwrite non-empty output: {args.output_dir}")

    values = read_ct2(args.ct2_model / "model.bin")
    model = WhisperForConditionalGeneration.from_pretrained(
        args.template_model,
        local_files_only=True,
        dtype=torch.float16,
        low_cpu_mem_usage=True,
    )
    state = model.state_dict()
    assigned: set[str] = set()

    for side in ("encoder", "decoder"):
        assign(state, assigned, f"model.{side}.embed_positions.weight",
               values[f"{side}/position_encodings/encodings"])
        assign(state, assigned, f"model.{side}.layer_norm.weight",
               values[f"{side}/layer_norm/gamma"])
        assign(state, assigned, f"model.{side}.layer_norm.bias",
               values[f"{side}/layer_norm/beta"])
        layers = model.config.encoder_layers if side == "encoder" else model.config.decoder_layers
        for index in range(layers):
            map_layer(values, state, assigned, side, index)

    for conv in ("conv1", "conv2"):
        for parameter in ("weight", "bias"):
            assign(state, assigned, f"model.encoder.{conv}.{parameter}",
                   values[f"encoder/{conv}/{parameter}"])
    assign(state, assigned, "model.decoder.embed_tokens.weight", values["decoder/embeddings/weight"])
    assign(state, assigned, "proj_out.weight", values["decoder/projection/weight"])

    missing = sorted(set(state) - assigned)
    if missing:
        raise RuntimeError(f"Recovery did not assign {len(missing)} weights: {missing[:20]}")
    model.load_state_dict(state, strict=True)

    args.output_dir.mkdir(parents=True, exist_ok=True)
    model.save_pretrained(args.output_dir)
    WhisperProcessor.from_pretrained(
        args.template_model, local_files_only=True
    ).save_pretrained(args.output_dir)
    (args.output_dir / "recovery_manifest.json").write_text(
        json.dumps(
            {
                "source_ct2_model": str(args.ct2_model.resolve()),
                "template_model": str(args.template_model.resolve()),
                "assigned_hf_weights": len(assigned),
                "source_precision": "float16",
            },
            indent=2,
        ) + "\n",
        encoding="utf-8",
    )
    print(f"Recovered {len(assigned)} Hugging Face weights to {args.output_dir}")


if __name__ == "__main__":
    main()
