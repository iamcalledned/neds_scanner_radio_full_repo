from __future__ import annotations

from pathlib import Path
from typing import Any, Callable


def build_model_catalog(
    *,
    json_from_env_fn: Callable[[str, str], Any],
    model_catalog_json: str,
    model_catalog_file: str,
    model_dir: Path,
    default_compute_type: str,
    merged_transcribe_settings_fn: Callable[[dict[str, Any] | None], dict[str, Any]],
    resolve_model_ref_fn: Callable[[str], str],
    model_info_cls: type,
) -> tuple[dict[str, Any], str]:
    raw = json_from_env_fn(model_catalog_json, model_catalog_file) or {}
    catalog: dict[str, Any] = {}
    catalog_default_key = "default"

    default_model_ref = str(model_dir)
    default_transcribe = merged_transcribe_settings_fn(None)

    catalog["default"] = model_info_cls(
        key="default",
        model=default_model_ref,
        compute_type=default_compute_type,
        device="cuda",
        transcribe=default_transcribe,
    )

    if isinstance(raw, dict):
        entries = raw.get("models") if isinstance(raw.get("models"), dict) else raw
        if isinstance(raw.get("default_model"), str) and raw.get("default_model"):
            catalog_default_key = raw["default_model"]

        for key, val in entries.items():
            if not isinstance(val, dict):
                continue
            model_ref = val.get("model") or val.get("path") or val.get("model_path") or val.get("dir")
            if not model_ref:
                continue

            resolved_model = resolve_model_ref_fn(str(model_ref))
            compute_type = val.get("compute_type", default_compute_type)
            device = str(val.get("device", "cuda")).strip().lower() or "cuda"
            transcribe_cfg = val.get("transcribe") if isinstance(val.get("transcribe"), dict) else val

            catalog[key] = model_info_cls(
                key=key,
                model=resolved_model,
                compute_type=compute_type,
                device=device,
                transcribe=merged_transcribe_settings_fn(transcribe_cfg),
            )

    if catalog_default_key not in catalog:
        catalog_default_key = "default"

    return catalog, catalog_default_key
