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
    gpu_manager: Any,
) -> Any:
    os.environ["PYTORCH_CUDA_ALLOC_CONF"] = os.environ.get(
        "PYTORCH_CUDA_ALLOC_CONF", "expandable_segments:True"
    )

    device = require_cuda_fn()

    if not model_dir.exists():
        raise FileNotFoundError(f"WHISPER_MODEL_DIR not found: {model_dir}")

    log.info(f"Loading faster-whisper model from: '{model_dir}'")

    def _load_model():
        model = whisper_model_cls(
            str(model_dir),
            device="cuda",
            compute_type=compute_type,
        )

        # large-v3 / other 128-mel models can still end up with 80-mel filters
        # in some runtime paths. Force the feature extractor to match the model.
        try:
            model_n_mels = getattr(model.model, "n_mels", None)
            fx = getattr(model, "feature_extractor", None)

            log.info(
                f"[whisper_loader] model.model.n_mels={model_n_mels} "
                f"feature_extractor.n_mels={getattr(fx, 'n_mels', 'missing')}"
            )

            if fx is not None and model_n_mels and getattr(fx, "n_mels", None) != model_n_mels:
                log.warning(
                    f"[whisper_loader] Fixing mel filter mismatch: "
                    f"feature_extractor.n_mels={fx.n_mels} -> {model_n_mels}"
                )
                fx.n_mels = model_n_mels
                fx.mel_filters = fx.get_mel_filters(
                    fx.sampling_rate,
                    fx.n_fft,
                    n_mels=model_n_mels,
                )

                log.info(
                    f"[whisper_loader] After patch: feature_extractor.n_mels={fx.n_mels}"
                )
        except Exception as exc:
            log.warning(f"[whisper_loader] Could not validate/patch mel filters: {exc}")

        return model

    model, reservation_id = gpu_manager.warm_model(
        owner=f"whisper-loader:{model_dir.name}",
        load_fn=_load_model,
        timeout_s=300,
        model_key=model_dir.name,
        model_value=str(model_dir),
        metadata={"compute_type": compute_type, "device": "cuda"},
    )

    free, total = torch.cuda.mem_get_info()
    log.info(f"CUDA memory free: {free/(1024**3):.2f} GB / {total/(1024**3):.2f} GB")
    log.info("Model ready on CUDA (fp16)")

    return whisper_state_cls(
        model=model,
        device=device,
        gate=gpu_manager,
        reservation_id=reservation_id,
    )