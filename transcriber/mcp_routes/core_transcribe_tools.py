from __future__ import annotations

from pathlib import Path
from typing import Any, Callable

from mcp.server.fastmcp import Context


def register_core_transcribe_tools(
    *,
    mcp: Any,
    process_analyze_audio_fn: Callable[..., dict[str, Any]],
    is_under_allowed_fn: Callable[[Path], bool],
    min_duration: float,
    rms_threshold: float,
    get_state_fn: Callable[[Any], Any],
    transcribe_with_state_fn: Callable[..., dict[str, Any]],
) -> None:
    @mcp.tool()
    def analyze_audio(ctx: Context, path: str) -> dict[str, Any]:
        """
        Analyze a WAV file (duration, RMS, static detection).

        Args:
          path: Absolute or relative path to the WAV file (must be under allowed roots).
        """
        return process_analyze_audio_fn(
            path=path,
            is_allowed_fn=is_under_allowed_fn,
            min_duration=min_duration,
            rms_threshold=rms_threshold,
        )

    @mcp.tool()
    def transcribe_file(
        ctx: Context,
        path: str,
        profile: str = "default",
        language: str = "en",
        write_artifacts: bool = True,
        insert_db: bool = True,
        delete_source_raw: bool = False,
        custom_output_dir: str = "",
        skip_wav_copy: bool = False,
    ) -> dict[str, Any]:
        """
        Preprocess + transcribe a scanner WAV, then write artifacts under ARCHIVE_BASE/clean/<feed>/.

        Args:
          path: WAV path (must be under allowed roots)
          profile: preprocessing profile (default|radio|static_fix|aggressive)
          language: unused in this implementation (GPU-only fast tokenizer path)
          write_artifacts: if True, writes .json/.txt/.wav to clean folder
          insert_db: if True and scanner_db is available, insert into DB
          delete_source_raw: if True, deletes the source raw WAV after success
        """
        state = get_state_fn(ctx)
        return transcribe_with_state_fn(
            ctx,
            state,
            path=path,
            model_key="",
            profile=profile,
            language=language,
            write_artifacts=write_artifacts,
            insert_db=insert_db,
            delete_source_raw=delete_source_raw,
            custom_output_dir=custom_output_dir,
            skip_wav_copy=skip_wav_copy,
        )

    @mcp.tool()
    def retranscribe_file(
        ctx: Context,
        path: str,
        profile: str = "radio",
        language: str = "en",
    ) -> dict[str, Any]:
        """
        Convenience wrapper for transcribe_file with a different preprocessing profile.
        """
        return transcribe_file(
            ctx,
            path=path,
            profile=profile,
            language=language,
            write_artifacts=True,
            insert_db=True,
            delete_source_raw=False,
        )
