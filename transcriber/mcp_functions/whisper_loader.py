from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Callable

import torch


def load_whisper_model(
    *,
    model_dir: Path,
    compute_type: str,
    require_cuda_fn: Callable[[], torch.device],
    whisper_model_cls: type,
    log: Any,
    whisper_state_cls: type,
    gpu_gate_cls: type,
) -> Any:
    os.environ["PYTORCH_CUDA_ALLOC_CONF"] = os.environ.get(
        "PYTORCH_CUDA_ALLOC_CONF", "expandable_segments:True"
    )

    device = require_cuda_fn()

    if not model_dir.exists():
        raise FileNotFoundError(f"WHISPER_MODEL_DIR not found: {model_dir}")

    log.info(f"Loading faster-whisper model from: {model_dir}")

    model = whisper_model_cls(
        str(model_dir),
        device="cuda",
        compute_type=compute_type,
    )

    free, total = torch.cuda.mem_get_info()
    log.info(f"CUDA memory free: {free/(1024**3):.2f} GB / {total/(1024**3):.2f} GB")
    log.info("Model ready on CUDA (fp16)")

    return whisper_state_cls(
        model=model,
        device=device,
        gate=gpu_gate_cls(),
    )
