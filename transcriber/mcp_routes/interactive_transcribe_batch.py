from __future__ import annotations

from typing import Any, Callable, Optional

from starlette.requests import Request
from starlette.responses import JSONResponse, Response


def register_interactive_transcribe_batch_route(
    *,
    mcp: Any,
    ensure_runtime: Callable[[], dict[str, Any]],
    resolve_model_profile: Callable[[Optional[Any], str], tuple[str, Optional[Any]]],
    transcribe_batch: Callable[..., dict[str, Any]],
    interactive_error_status: Callable[[str], int],
    coerce_bool: Callable[[Any, bool], bool],
    is_under_interactive_allowed_roots: Callable[[Any], bool],
) -> None:
    @mcp.custom_route("/interactive/transcribe-batch", methods=["POST"])
    async def interactive_transcribe_batch(request: Request) -> Response:
        try:
            payload = await request.json()
        except Exception as exc:
            return JSONResponse(
                {
                    "ok": False,
                    "error": f"invalid_json: {exc}",
                    "model_key": "",
                    "model_value": None,
                    "elapsed_s": None,
                    "count": 0,
                    "success_count": 0,
                    "results": [],
                },
                status_code=interactive_error_status("invalid_json"),
            )

        if not isinstance(payload, dict):
            return JSONResponse(
                {
                    "ok": False,
                    "error": "invalid_payload",
                    "model_key": "",
                    "model_value": None,
                    "elapsed_s": None,
                    "count": 0,
                    "success_count": 0,
                    "results": [],
                },
                status_code=interactive_error_status("invalid_payload"),
            )

        segments = payload.get("segments")
        if not isinstance(segments, list) or not segments:
            return JSONResponse(
                {
                    "ok": False,
                    "error": "missing_segments",
                    "model_key": "",
                    "model_value": None,
                    "elapsed_s": None,
                    "count": 0,
                    "success_count": 0,
                    "results": [],
                },
                status_code=interactive_error_status("invalid_payload"),
            )

        profile = str(payload.get("profile") or "default").strip() or "default"
        language = str(payload.get("language") or "en").strip() or "en"
        model_key = str(payload.get("model_key") or "").strip()
        write_artifacts = coerce_bool(payload.get("write_artifacts"), default=False)
        custom_output_dir = str(payload.get("custom_output_dir") or "").strip()
        source_audio = str(payload.get("source_audio") or "").strip()

        try:
            runtime = ensure_runtime()
            router = runtime.get("router")
            if not router:
                raise RuntimeError("router_unavailable")

            resolved_model_key, model_info = resolve_model_profile(router, model_key)
            state = router.get_state(resolved_model_key)

            result = transcribe_batch(
                state=state,
                segments=segments,
                source_audio=source_audio,
                resolved_model_key=resolved_model_key,
                model_value=model_info.model if model_info else None,
                profile=profile,
                language=language,
                write_artifacts=write_artifacts,
                custom_output_dir=custom_output_dir,
                is_allowed_fn=is_under_interactive_allowed_roots,
                transcribe_settings=model_info.transcribe if model_info else None,
            )
        except Exception as exc:
            return JSONResponse(
                {
                    "ok": False,
                    "error": f"transcribe_failed: {exc}",
                    "model_key": model_key,
                    "model_value": None,
                    "elapsed_s": None,
                    "count": len(segments),
                    "success_count": 0,
                    "results": [],
                },
                status_code=interactive_error_status("transcribe_failed"),
            )

        return JSONResponse(result, status_code=200)
