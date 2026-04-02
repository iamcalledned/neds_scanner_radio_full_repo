from typing import Dict, Any, Callable
from pathlib import Path
from mcp_tools.audio_processing import get_duration, get_rms

def process_analyze_audio(
    path: str,
    is_allowed_fn: Callable[[Path], bool],
    min_duration: float,
    rms_threshold: float,
) -> Dict[str, Any]:
    """
    Analyze a WAV file (duration, RMS, static detection).
    """
    p = Path(path).expanduser()
    if not is_allowed_fn(p):
        return {"ok": False, "error": "path_not_allowed", "path": str(p)}

    p = p.resolve()
    if not p.exists():
        return {"ok": False, "error": "missing_file", "path": str(p)}

    dur = get_duration(p)
    rms = get_rms(p) if dur > 0 else 0.0
    static = rms < rms_threshold

    return {
        "ok": True,
        "path": str(p),
        "duration": dur,
        "rms": rms,
        "static": static,
        "min_duration": min_duration,
        "rms_threshold": rms_threshold,
    }
