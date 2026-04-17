from __future__ import annotations

import re
from pathlib import Path
from typing import Any, Callable, Optional


_HOOK_PATTERNS = re.compile(
    r"\b(tow|togi|togis|togie|togies|togy|hook|arts|towing)\b",
    re.IGNORECASE,
)


def is_under_roots(p: Path, roots: list[Path]) -> bool:
    rp = p.expanduser().resolve()
    for root in roots:
        try:
            rp.relative_to(root)
            return True
        except ValueError:
            continue
    return False


def detect_hook_request(text: str) -> bool:
    if not text:
        return False
    return bool(_HOOK_PATTERNS.search(text))


def sidecar_json_for_audio(path: Path) -> Path:
    return path.with_suffix(".json")


def try_import_db(enable_db: bool, log: Any) -> tuple[bool, Any]:
    if not enable_db:
        return False, None
    try:
        from shared.scanner_db import (
            insert_call, create_tables, DB_PATH,
            update_call_classification, update_hook_request,
            update_review_status, infer_town_from_filename,
            infer_dept_from_filename,
        )
        import types
        mod = types.SimpleNamespace(
            insert_call=insert_call,
            create_tables=create_tables,
            DB_PATH=DB_PATH,
            update_call_classification=update_call_classification,
            update_hook_request=update_hook_request,
            update_review_status=update_review_status,
            infer_town_from_filename=infer_town_from_filename,
            infer_dept_from_filename=infer_dept_from_filename,
        )
        log.info("shared.scanner_db import OK (DB inserts enabled).")
        return True, mod
    except Exception as e:
        log.warning(f"shared.scanner_db import failed (DB inserts disabled): {e}")
        return False, None


async def wal_checkpoint_loop(log: Any) -> None:
    import asyncio
    while True:
        await asyncio.sleep(300)
        try:
            from shared.scanner_db import wal_checkpoint
            wal_checkpoint()
        except Exception as e:
            log.warning(f"[DB] Periodic WAL checkpoint failed: {e}")


def get_state(
    ctx: Any,
    *,
    ensure_runtime_fn: Callable[[], dict[str, Any]],
    load_whisper_model_fn: Callable[[], Any],
    log: Any,
) -> Any:
    runtime = ensure_runtime_fn()
    data = ctx.request_context.lifespan_context
    state = data.get("state") or runtime.get("state")
    if state is None:
        router = data.get("router") or runtime.get("router")
        if router:
            log.info("[router] Lazy-loading default model for legacy transcribe_file path")
            state = router.get_state(router.default_key)
            data["state"] = state
            runtime["state"] = state
        else:
            state = load_whisper_model_fn()
            data["state"] = state
            runtime["state"] = state
    elif data.get("state") is None:
        data["state"] = state
    return state


def get_router(ctx: Any, *, ensure_runtime_fn: Callable[[], dict[str, Any]]) -> Any:
    return ctx.request_context.lifespan_context.get("router") or ensure_runtime_fn().get("router")


def coerce_bool(value: Any, default: bool = False) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().lower() in ("1", "true", "yes", "on")
    return bool(value)


def interactive_error_status(error_code: str) -> int:
    if error_code in {"invalid_json", "invalid_payload", "missing_path", "path_not_allowed", "missing_file", "too_short", "static"}:
        return 400
    if error_code in {"router_unavailable", "preprocess_failed", "transcribe_failed"}:
        return 500
    return 400
