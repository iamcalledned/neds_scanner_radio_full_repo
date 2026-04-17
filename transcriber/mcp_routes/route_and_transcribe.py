from __future__ import annotations

from pathlib import Path
from typing import Any, Callable

from mcp.server.fastmcp import Context


def register_route_and_transcribe_tool(
    *,
    mcp: Any,
    get_router: Callable[[Any], Any],
    is_under_allowed_roots: Callable[[Path], bool],
    detect_category: Callable[[Path], tuple[str, str]],
    get_duration: Callable[[Path], float],
    transcribe_with_state: Callable[..., dict[str, Any]],
) -> None:
    @mcp.tool()
    def route_and_transcribe(
        ctx: Context,
        path: str,
        profile: str = "default",
        language: str = "en",
        auto_route: bool = True,
        model_key: str = "",
        write_artifacts: bool = True,
        insert_db: bool = True,
        delete_source_raw: bool = False,
        custom_output_dir: str = "",
        skip_wav_copy: bool = False,
    ) -> dict[str, Any]:
        """
        Route a call to the appropriate Whisper model based on routing rules or an explicit model_key.

        Args:
          path: WAV path (must be under allowed roots)
          profile: preprocessing profile
          language: decoding language hint (for API compatibility)
          auto_route: when True, evaluate routing rules; otherwise use model_key or default
          model_key: optional explicit model key from the catalog
        """
        router = get_router(ctx)
        if not router:
            return {"ok": False, "error": "router_unavailable"}

        src = Path(path).expanduser().resolve()
        if not is_under_allowed_roots(src):
            return {"ok": False, "error": "path_not_allowed", "path": str(src)}
        if not src.exists():
            return {"ok": False, "error": "missing_file", "path": str(src)}

        feed_hint, _ = detect_category(src)
        duration = get_duration(src)

        chosen_model = model_key.strip() or router.default_key
        if auto_route:
            chosen_model = router.choose_model(path=src, feed=feed_hint, duration=duration)

        state = router.get_state(chosen_model)
        result = transcribe_with_state(
            ctx,
            state,
            path=str(src),
            model_key=chosen_model,
            profile=profile,
            language=language,
            write_artifacts=write_artifacts,
            insert_db=insert_db,
            delete_source_raw=delete_source_raw,
            custom_output_dir=custom_output_dir,
            skip_wav_copy=skip_wav_copy,
        )

        result["model_key"] = chosen_model
        result["routing"] = {
            "auto_route": auto_route,
            "feed_hint": feed_hint,
            "duration": duration,
        }
        return result
