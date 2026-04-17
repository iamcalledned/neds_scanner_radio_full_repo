from __future__ import annotations

import gc
import json
import shutil
import subprocess
from pathlib import Path
from typing import Any, Callable, Optional

import torch


def transcribe_with_state(
    ctx: Any,
    state: Any,
    *,
    path: str,
    model_key: str,
    profile: str,
    language: str,
    write_artifacts: bool,
    insert_db: bool,
    delete_source_raw: bool,
    custom_output_dir: str,
    skip_wav_copy: bool,
    router: Optional[Any] = None,
    is_allowed_fn: Callable[[Path], bool],
    get_duration_fn: Callable[[Path], float],
    min_duration: float,
    is_static_fn: Callable[[Path], bool],
    get_rms_fn: Callable[[Path], float],
    rms_threshold: float,
    detect_category_fn: Callable[[Path], tuple[str, str]],
    archive_base: Path,
    tmp_dir: Path,
    get_router_fn: Callable[[Any], Any],
    resolve_model_profile_fn: Callable[[Optional[Any], str], tuple[str, Optional[Any]]],
    log: Any,
    preprocess_audio_fn: Callable[..., Any],
    transcribe_wavefile_fn: Callable[..., str],
    score_transcript_fn: Callable[[str, float, float], dict[str, Any]],
    source_map: dict[str, str],
    model_dir: Path,
    detect_hook_request_fn: Callable[[str], bool],
    db_state_getter: Callable[[], tuple[bool, Any]],
) -> dict[str, Any]:
    src = Path(path).expanduser()
    if not is_allowed_fn(src):
        return {"ok": False, "error": "path_not_allowed", "path": str(src)}

    src = src.resolve()
    if not src.exists():
        return {"ok": False, "error": "missing_file", "path": str(src)}

    dur = get_duration_fn(src)
    if dur < min_duration:
        return {"ok": False, "error": "too_short", "duration": dur, "min_duration": min_duration}

    if is_static_fn(src):
        return {"ok": False, "error": "static", "rms": get_rms_fn(src), "rms_threshold": rms_threshold}

    clean_subdir, raw_subdir = detect_category_fn(src)
    clean_dir = (archive_base / "clean" / clean_subdir).resolve()
    raw_dir = (archive_base / "raw" / raw_subdir).resolve()

    tmp_dir.mkdir(parents=True, exist_ok=True)
    clean_dir.mkdir(parents=True, exist_ok=True)
    raw_dir.mkdir(parents=True, exist_ok=True)

    tmp = tmp_dir / f"{src.stem}_clean.wav"

    target_dir = Path(custom_output_dir).expanduser().resolve() if custom_output_dir else clean_dir
    if custom_output_dir:
        target_dir.mkdir(parents=True, exist_ok=True)

    out_txt = target_dir / f"{src.stem}.txt"
    out_json = target_dir / f"{src.stem}.json"
    out_wav = target_dir / f"{src.stem}.wav"

    import time

    t0 = time.time()
    try:
        if router is None and ctx is not None:
            router = get_router_fn(ctx)
        resolved_model_key, model_info = resolve_model_profile_fn(router, model_key)
        log.info(f"[PRE-PROCESS AUDIO] Preprocessing {src.name} profile={profile}")
        preprocess_audio_fn(src, tmp, profile=profile)
        log.info("[PRE-PROCESS AUDIO] Preprocessing done, starting Whisper inference…")
        text = transcribe_wavefile_fn(
            state,
            tmp,
            language=language,
            transcribe_settings=model_info.transcribe if model_info else None,
        )
        log.info(f"[PRE-PROCESS AUDIO] Whisper inference done in {time.time()-t0:.1f}s")

        rms = get_rms_fn(tmp)
        quality = score_transcript_fn(text, dur, rms)
        now_iso = __import__("datetime").datetime.now().isoformat()

        feed = clean_subdir.lower()
        town = source_map.get(feed, "Unknown")
        state_name = "Massachusetts"
        dept = "fire" if "fd" in feed else "police" if "pd" in feed else ""

        transcription_model = model_info.model if model_info else model_dir.name
        hook_requested = detect_hook_request_fn(text)
        log.info(f"hook_requested: {hook_requested}")

        meta: dict[str, Any] = {
            "filename": out_wav.name,
            "transcript": text,
            "raw_transcript": text,
            "normalized_transcript": text,
            "duration": dur,
            "rms": rms,
            "timestamp": now_iso,
            "source": clean_subdir,
            "town": town,
            "state": state_name,
            "dept": dept,
            "profile": profile,
            "language": language,
            "classification": {
                "zero_shot": {},
                "location": None,
                "address_number": None,
                "address_street": None,
                "units": [],
                "tone_detected": False,
                "agency": None,
                "call_type": None,
                "urgency": None,
            },
            "intent_labeled": False,
            "intent_labeled_at": None,
            "edited_transcript": None,
            "transcription_quality": {
                "score": quality["score"],
                "needs_retry": quality["needs_retry"],
                "needs_review": quality["needs_review"],
                "reasons": quality["reasons"],
            },
            "profile_used": profile,
            "retry_profiles_tried": [profile],
            "transcription_engine": "faster-whisper",
            "transcription_model": transcription_model,
            "transcription_model_key": resolved_model_key,
            "hook_request": hook_requested,
        }

        try:
            from nlp_zero_shot import enrich_meta_in_memory

            meta = enrich_meta_in_memory(meta)
        except Exception as e:
            meta.setdefault("warnings", []).append(f"enrichment_failed: {e}")

        if write_artifacts:
            out_txt.write_text(text, encoding="utf-8")
            out_json.write_text(json.dumps(meta, indent=2), encoding="utf-8")
            if not skip_wav_copy:
                shutil.copy(tmp, out_wav)

        db_import_ok, scanner_db = db_state_getter()
        db_result = None
        if insert_db and db_import_ok and scanner_db is not None:
            log.info(f"[DB] Inserting record for {out_wav.name} (town={town}, dept={dept})...")
            try:
                if not getattr(scanner_db, "DB_PATH", None) or not scanner_db.DB_PATH.exists():
                    scanner_db.create_tables()

                db_meta = {
                    "town": town,
                    "state": state_name,
                    "dept": dept,
                    "category": clean_subdir,
                    "filename": out_wav.name,
                    "json_path": str(out_json),
                    "wav_path": str(out_wav),
                    "duration": dur,
                    "rms": rms,
                    "transcript": text,
                    "edited_transcript": None,
                    "timestamp": now_iso,
                    "reviewed": 0,
                    "play_count": 0,
                    "classification": meta.get("classification", {}),
                    "intent_labeled": int(meta.get("intent_labeled", 0)),
                    "intent_labeled_at": meta.get("intent_labeled_at"),
                    "extra": meta,
                    "raw_transcript": text,
                    "normalized_transcript": text,
                    "transcription_score": quality["score"],
                    "needs_retry": int(quality["needs_retry"]),
                    "needs_review": int(quality["needs_review"]),
                    "quality_reasons": quality["reasons"],
                    "profile_used": profile,
                    "retry_profiles_tried": [profile],
                    "transcription_engine": "faster-whisper",
                    "transcription_model": meta.get("transcription_model"),
                    "hook_request": hook_requested,
                    "derived_address": meta.get("derived_address"),
                    "derived_street": meta.get("derived_street"),
                    "derived_addr_num": meta.get("derived_addr_num"),
                    "derived_town": meta.get("derived_town"),
                    "derived_lat": meta.get("derived_lat"),
                    "derived_lng": meta.get("derived_lng"),
                    "address_confidence": meta.get("address_confidence", "none"),
                }
                scanner_db.insert_call(db_meta)
                db_result = {"ok": True}
                log.info(f"[DB] Insert successful for {out_wav.name}")
            except Exception as e:
                db_result = {"ok": False, "error": str(e)}
                log.error(f"[DB] Insert failed for {out_wav.name}: {e}")

        deleted = False
        if delete_source_raw:
            try:
                src.unlink()
                deleted = True
            except Exception as e:
                meta.setdefault("warnings", []).append(f"delete_failed: {e}")

        elapsed = time.time() - t0
        return {
            "ok": True,
            "text": text,
            "duration": dur,
            "rms": rms,
            "profile": profile,
            "language": language,
            "model_key": resolved_model_key,
            "source_path": str(src),
            "clean_dir": str(clean_dir),
            "output_dir": str(target_dir),
            "artifacts": {
                "txt": str(out_txt) if write_artifacts else None,
                "json": str(out_json) if write_artifacts else None,
                "wav": str(out_wav) if write_artifacts and not skip_wav_copy else None,
            },
            "db": db_result,
            "deleted_source": deleted,
            "elapsed_s": round(elapsed, 3),
        }

    except subprocess.CalledProcessError as e:
        return {"ok": False, "error": "preprocess_failed", "details": str(e)}
    except Exception as e:
        return {"ok": False, "error": "transcribe_failed", "details": str(e)}
    finally:
        try:
            tmp.unlink(missing_ok=True)
        except Exception:
            pass
        gc.collect()
        try:
            torch.cuda.empty_cache()
        except Exception:
            pass
