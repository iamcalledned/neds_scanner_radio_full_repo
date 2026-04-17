from __future__ import annotations

from typing import Any, Callable, Optional

from starlette.requests import Request
from starlette.responses import JSONResponse, Response


def register_interactive_transcribe_segment_route(
    *,
    mcp: Any,
    ensure_runtime: Callable[[], dict[str, Any]],
    resolve_model_profile: Callable[[Optional[Any], str], tuple[str, Optional[Any]]],
    transcribe_with_state: Callable[..., dict[str, Any]],
    interactive_error_status: Callable[[str], int],
    coerce_bool: Callable[[Any, bool], bool],
    is_under_interactive_allowed_roots: Callable[[Any], bool],
) -> None:
    @mcp.custom_route("/interactive/transcribe-segment", methods=["POST"])
    async def interactive_transcribe_segment(request: Request) -> Response:
        try:
            payload = await request.json()
        except Exception as e:
            return JSONResponse(
                {
                    "ok": False,
                    "text": "",
                    "error": f"invalid_json: {e}",
                    "model_key": "",
                    "model_value": None,
                    "elapsed_s": None,
                    "duration": None,
                    "rms": None,
                },
                status_code=interactive_error_status("invalid_json"),
            )

        if not isinstance(payload, dict):
            return JSONResponse(
                {
                    "ok": False,
                    "text": "",
                    "error": "invalid_payload",
                    "model_key": "",
                    "model_value": None,
                    "elapsed_s": None,
                    "duration": None,
                    "rms": None,
                },
                status_code=interactive_error_status("invalid_payload"),
            )

        path = str(payload.get("path") or "").strip()
        if not path:
            return JSONResponse(
                {
                    "ok": False,
                    "text": "",
                    "error": "missing_path",
                    "model_key": "",
                    "model_value": None,
                    "elapsed_s": None,
                    "duration": None,
                    "rms": None,
                },
                status_code=interactive_error_status("missing_path"),
            )

        profile = str(payload.get("profile") or "default").strip() or "default"
        language = str(payload.get("language") or "en").strip() or "en"
        model_key = str(payload.get("model_key") or "").strip()
        write_artifacts = coerce_bool(payload.get("write_artifacts"), default=False)
        custom_output_dir = str(payload.get("custom_output_dir") or "").strip()

        try:
            runtime = ensure_runtime()
            router = runtime.get("router")
            if not router:
                raise RuntimeError("router_unavailable")

            resolved_model_key, model_info = resolve_model_profile(router, model_key)
            state = router.get_state(resolved_model_key)

            result = transcribe_with_state(
                None,
                state,
                path=path,
                model_key=resolved_model_key,
                profile=profile,
                language=language,
                write_artifacts=write_artifacts,
                insert_db=False,
                delete_source_raw=False,
                custom_output_dir=custom_output_dir,
                skip_wav_copy=False,
                router=router,
                is_allowed_fn=is_under_interactive_allowed_roots,
            )
        except Exception as e:
            result = {"ok": False, "error": "transcribe_failed", "details": str(e)}
            model_info = None
            resolved_model_key = model_key

        error_code = result.get("error")
        error_text = None
        if not result.get("ok"):
            error_text = error_code or "transcribe_failed"
            if result.get("details"):
                error_text = f"{error_text}: {result['details']}"

        return JSONResponse(
            {
                "ok": bool(result.get("ok")),
                "text": result.get("text", "") if result.get("ok") else "",
                "error": error_text,
                "model_key": result.get("model_key") or resolved_model_key,
                "model_value": model_info.model if model_info else None,
                "elapsed_s": result.get("elapsed_s"),
                "duration": result.get("duration"),
                "rms": result.get("rms"),
            },
            status_code=200 if result.get("ok") else interactive_error_status(error_code or ""),
        )
