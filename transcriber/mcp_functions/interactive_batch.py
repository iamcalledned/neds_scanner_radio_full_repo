from __future__ import annotations

import gc
import json
import shutil
import subprocess
import time
import uuid
from pathlib import Path
from typing import Any, Callable, Optional

import torch


def transcribe_batch(
    *,
    state: Any,
    segments: list[Any],
    source_audio: str,
    resolved_model_key: str,
    model_value: Optional[str],
    profile: str,
    language: str,
    write_artifacts: bool,
    custom_output_dir: str,
    is_allowed_fn: Callable[[Path], bool],
    get_duration_fn: Callable[[Path], float],
    min_duration: float,
    is_static_fn: Callable[[Path], bool],
    get_rms_fn: Callable[[Path], float],
    rms_threshold: float,
    tmp_dir: Path,
    log: Any,
    preprocess_audio_fn: Callable[..., Any],
    build_transcribe_kwargs_fn: Callable[..., dict[str, Any]],
    transcribe_settings: Optional[dict[str, Any]],
) -> dict[str, Any]:
    batch_id = uuid.uuid4().hex[:12]
    started_at = time.time()
    tmp_dir.mkdir(parents=True, exist_ok=True)

    results: list[dict[str, Any]] = []
    prepared: list[dict[str, Any]] = []

    kwargs = build_transcribe_kwargs_fn(
        task="transcribe",
        language=language,
        profile_settings=transcribe_settings,
    )
    log.info(
        "[interactive_batch] Prepared batch %s with %d segment(s) model=%s",
        batch_id,
        len(segments),
        resolved_model_key,
    )

    def _base_result(index: int, src: Optional[Path], segment_meta: dict[str, Any]) -> dict[str, Any]:
        return {
            "ok": False,
            "text": "",
            "error": None,
            "model_key": resolved_model_key,
            "model_value": model_value,
            "elapsed_s": None,
            "duration": None,
            "rms": None,
            "path": str(src) if src else None,
            "segment_name": src.name if src else None,
            "segment_meta": segment_meta,
            "source_audio": source_audio or None,
            "artifacts": {
                "txt": None,
                "json": None,
                "wav": None,
            },
        }

    for index, raw_item in enumerate(segments):
        if not isinstance(raw_item, dict):
            result = _base_result(index, None, {})
            result["error"] = "invalid_segment"
            results.append(result)
            continue

        raw_path = str(raw_item.get("path") or "").strip()
        segment_meta = raw_item.get("segment_meta")
        if not isinstance(segment_meta, dict):
            segment_meta = {}

        if not raw_path:
            result = _base_result(index, None, segment_meta)
            result["error"] = "missing_path"
            results.append(result)
            continue

        src = Path(raw_path).expanduser()
        result = _base_result(index, src, segment_meta)

        if not is_allowed_fn(src):
            result["error"] = "path_not_allowed"
            results.append(result)
            continue

        src = src.resolve()
        result["path"] = str(src)
        result["segment_name"] = src.name

        if not src.exists():
            result["error"] = "missing_file"
            results.append(result)
            continue

        try:
            duration = get_duration_fn(src)
        except Exception as exc:
            result["error"] = f"duration_failed: {exc}"
            results.append(result)
            continue

        result["duration"] = duration
        if duration < min_duration:
            result["error"] = "too_short"
            results.append(result)
            continue

        if is_static_fn(src):
            result["error"] = "static"
            result["rms"] = get_rms_fn(src)
            results.append(result)
            continue

        tmp_path = tmp_dir / f"{src.stem}_{batch_id}_{index:04d}_clean.wav"
        try:
            preprocess_audio_fn(src, tmp_path, profile=profile)
            result["rms"] = get_rms_fn(tmp_path)
            prepared.append(
                {
                    "index": index,
                    "src": src,
                    "tmp": tmp_path,
                    "result": result,
                }
            )
        except subprocess.CalledProcessError as exc:
            result["error"] = "preprocess_failed"
            result["details"] = str(exc)
        except Exception as exc:
            result["error"] = "preprocess_failed"
            result["details"] = str(exc)

        results.append(result)

    if prepared:
        log.info(
            "[interactive_batch] Acquiring GPU lock once for batch %s (%d ready segment(s))",
            batch_id,
            len(prepared),
        )
        with state.gate.acquire("whisper-batch", timeout_s=120):
            for entry in prepared:
                result = entry["result"]
                segment_started_at = time.time()
                try:
                    decode_segments, _ = state.model.transcribe(str(entry["tmp"]), **kwargs)
                    text = " ".join(segment.text for segment in decode_segments).strip()
                    result["ok"] = True
                    result["text"] = text
                    result["elapsed_s"] = round(time.time() - segment_started_at, 3)
                    result["error"] = None

                    if write_artifacts:
                        target_dir = Path(custom_output_dir).expanduser().resolve() if custom_output_dir else entry["src"].parent
                        target_dir.mkdir(parents=True, exist_ok=True)

                        out_txt = target_dir / f"{entry['src'].stem}.txt"
                        out_json = target_dir / f"{entry['src'].stem}.json"
                        out_wav = target_dir / f"{entry['src'].stem}.wav"

                        out_txt.write_text(text, encoding="utf-8")
                        out_json.write_text(
                            json.dumps(
                                {
                                    "ok": True,
                                    "text": text,
                                    "model_key": resolved_model_key,
                                    "model_value": model_value,
                                    "duration": result["duration"],
                                    "rms": result["rms"],
                                    "segment_meta": result["segment_meta"],
                                    "source_audio": source_audio or None,
                                },
                                indent=2,
                            ),
                            encoding="utf-8",
                        )
                        shutil.copy(entry["tmp"], out_wav)
                        result["artifacts"] = {
                            "txt": str(out_txt),
                            "json": str(out_json),
                            "wav": str(out_wav),
                        }
                except Exception as exc:
                    result["ok"] = False
                    result["error"] = "transcribe_failed"
                    result["details"] = str(exc)
                    result["elapsed_s"] = round(time.time() - segment_started_at, 3)

    for entry in prepared:
        try:
            entry["tmp"].unlink(missing_ok=True)
        except Exception:
            pass

    gc.collect()
    try:
        torch.cuda.empty_cache()
    except Exception:
        pass

    success_count = sum(1 for result in results if result.get("ok"))
    return {
        "ok": success_count == len(results),
        "error": None if success_count == len(results) else "partial_failure",
        "model_key": resolved_model_key,
        "model_value": model_value,
        "count": len(results),
        "success_count": success_count,
        "elapsed_s": round(time.time() - started_at, 3),
        "results": results,
    }
