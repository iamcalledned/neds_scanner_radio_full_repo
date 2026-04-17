from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Callable, Optional

from mcp.server.fastmcp import Context


def register_location_inference_tools(
    *,
    mcp: Any,
    call_location_inference_service: Callable[..., dict[str, Any]],
    is_under_allowed_roots: Callable[[Path], bool],
    sidecar_json_for_audio: Callable[[Path], Path],
) -> None:
    @mcp.tool()
    def infer_location(
        ctx: Context,
        transcript: str,
        town: str = "",
        feed: str = "",
        candidate_streets: Optional[list] = None,
        candidate_landmarks: Optional[list] = None,
        candidate_towns: Optional[list] = None,
        notes: str = "",
    ) -> dict[str, Any]:
        """
        Run location inference on a transcript by calling the local inference service.

        Args:
          transcript: the call transcript text
          town: optional town hint
          feed: optional feed/category hint (e.g. mpd, frkfd)
          candidate_streets: optional list of street name candidates
          candidate_landmarks: optional list of landmark candidates
          candidate_towns: optional list of town candidates
          notes: optional freeform notes for the inference service
        """
        if not transcript.strip():
            return {"ok": False, "error": "transcript_empty"}

        result = call_location_inference_service(
            transcript=transcript,
            town=town or None,
            feed=feed or None,
            candidate_streets=candidate_streets,
            candidate_landmarks=candidate_landmarks,
            candidate_towns=candidate_towns,
            notes=notes or None,
        )

        out: dict[str, Any] = {
            "ok": result["ok"],
            "transcript": transcript,
            "town": town or None,
            "feed": feed or None,
            "service": result,
        }
        if result["ok"] and isinstance(result.get("response"), dict):
            out["inferred_location"] = result["response"].get("inference")
        elif not result["ok"]:
            out["error"] = result.get("error")
        return out

    @mcp.tool()
    def infer_location_for_file(
        ctx: Context,
        path: str,
        candidate_streets: Optional[list] = None,
        candidate_landmarks: Optional[list] = None,
        candidate_towns: Optional[list] = None,
        notes: str = "",
        update_json: bool = False,
    ) -> dict[str, Any]:
        """
        Load a call's transcript from its sidecar JSON and run location inference.

        Args:
          path: path to the .wav or .json file (must be under allowed roots)
          candidate_streets: optional street name hints
          candidate_landmarks: optional landmark hints
          candidate_towns: optional town hints
          notes: optional freeform notes for the inference service
          update_json: if True, write inference results back into the sidecar JSON
        """
        src = Path(path).expanduser()
        if not is_under_allowed_roots(src):
            return {"ok": False, "error": "path_not_allowed", "path": str(src)}

        src = src.resolve()

        if src.suffix.lower() == ".wav":
            json_path = sidecar_json_for_audio(src)
        else:
            json_path = src

        if not json_path.exists():
            return {"ok": False, "error": "json_not_found", "json_path": str(json_path)}

        try:
            meta = json.loads(json_path.read_text(encoding="utf-8"))
        except Exception as e:
            return {"ok": False, "error": f"json_load_failed: {e}", "json_path": str(json_path)}

        transcript = ""
        transcript_field = None
        for field in ("normalized_transcript", "raw_transcript", "transcript"):
            val = meta.get(field, "")
            if val and val.strip():
                transcript = val.strip()
                transcript_field = field
                break

        if not transcript:
            return {"ok": False, "error": "no_transcript_in_json", "json_path": str(json_path)}

        town = meta.get("town") or None
        feed = meta.get("source") or meta.get("category") or None

        result = call_location_inference_service(
            transcript=transcript,
            town=town,
            feed=feed,
            candidate_streets=candidate_streets,
            candidate_landmarks=candidate_landmarks,
            candidate_towns=candidate_towns,
            notes=notes or None,
        )

        out: dict[str, Any] = {
            "ok": result["ok"],
            "source_path": str(src),
            "json_path": str(json_path),
            "transcript_used": transcript,
            "transcript_field": transcript_field,
            "town": town,
            "feed": feed,
            "service": result,
        }

        if result["ok"] and isinstance(result.get("response"), dict):
            out["inferred_location"] = result["response"].get("inference")
        elif not result["ok"]:
            out["error"] = result.get("error")
            return out

        if update_json and result["ok"]:
            try:
                resp = result["response"]
                meta["location_inference"] = resp.get("inference")
                meta["location_inference_meta"] = {
                    "model": resp.get("model"),
                    "prompt_version": resp.get("prompt_version"),
                    "updated_at": __import__("datetime").datetime.now().isoformat(),
                }
                json_path.write_text(json.dumps(meta, indent=2), encoding="utf-8")
                out["json_updated"] = True
            except Exception as e:
                out["json_updated"] = False
                out.setdefault("warnings", []).append(f"json_update_failed: {e}")

        return out
