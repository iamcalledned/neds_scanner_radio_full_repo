from __future__ import annotations

from typing import Any, Callable

from starlette.requests import Request
from starlette.responses import JSONResponse, Response


def _coerce_bool(value: Any, *, default: bool = False) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() not in {"0", "false", "no", "off", ""}


def register_interactive_chat_completion_route(
    *,
    mcp: Any,
    load_raw_model_catalog: Callable[[], dict[str, Any]],
    managed_chat_completion: Callable[..., dict[str, Any]],
) -> None:
    @mcp.custom_route("/interactive/chat-completion", methods=["POST"])
    async def interactive_chat_completion(request: Request) -> Response:
        try:
            payload = await request.json()
        except Exception as exc:
            return JSONResponse({"ok": False, "error": f"invalid_json: {exc}"}, status_code=400)

        if not isinstance(payload, dict):
            return JSONResponse({"ok": False, "error": "invalid_payload"}, status_code=400)

        model_key = str(payload.get("model_key") or "").strip()
        messages = payload.get("messages")
        temperature = float(payload.get("temperature", 0.1))
        timeout = int(payload.get("timeout", 300))
        session_id = str(payload.get("session_id") or "").strip() or None
        close_session = _coerce_bool(payload.get("close_session"), default=True)

        if not model_key:
            return JSONResponse({"ok": False, "error": "missing_model_key"}, status_code=400)
        if not isinstance(messages, list) or not messages:
            return JSONResponse({"ok": False, "error": "missing_messages"}, status_code=400)

        catalog = load_raw_model_catalog()
        models = catalog.get("models") if isinstance(catalog, dict) else None
        if not isinstance(models, dict):
            return JSONResponse({"ok": False, "error": "invalid_model_catalog"}, status_code=500)

        entry = models.get(model_key)
        if not isinstance(entry, dict):
            return JSONResponse({"ok": False, "error": "unknown_model_key"}, status_code=400)

        if str(entry.get("kind") or "").strip().lower() != "chat":
            return JSONResponse({"ok": False, "error": "model_not_chat_capable"}, status_code=400)

        try:
            result = managed_chat_completion(
                model_key=model_key,
                catalog_entry=entry,
                messages=messages,
                temperature=temperature,
                timeout=timeout,
                session_id=session_id,
                close_session=close_session,
            )
        except Exception as exc:
            return JSONResponse(
                {
                    "ok": False,
                    "error": f"chat_completion_failed: {exc}",
                    "model_key": model_key,
                    "session_id": session_id,
                },
                status_code=500,
            )

        return JSONResponse(result, status_code=200)
