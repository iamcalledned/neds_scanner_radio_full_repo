from pathlib import Path
import json

import urllib.request
import urllib.error

from typing import Any, Dict, Optional, Tuple, List
from contextlib import asynccontextmanager
import os

import numpy as np


from faster_whisper import WhisperModel
from transformers.utils import logging as hf_logging

from mcp.server.fastmcp import FastMCP, Context

from gpu_gate import GPUGate
from mcp_tools.audio_processing import preprocess_audio

LOCATION_INFER_BASE_URL = os.environ.get("LOCATION_INFER_BASE_URL", "http://127.0.0.1:8011").rstrip("/")
LOCATION_INFER_TIMEOUT_S = int(os.environ.get("LOCATION_INFER_TIMEOUT_S", "30"))

def call_location_inference_service(
    transcript: str,
    town: Optional[str] = None,
    feed: Optional[str] = None,
    candidate_streets: Optional[list] = None,
    candidate_landmarks: Optional[list] = None,
    candidate_towns: Optional[list] = None,
    notes: Optional[str] = None,
) -> Dict[str, Any]:
    """POST to the local location inference service. Never raises; failures return ok=False."""
    url = f"{LOCATION_INFER_BASE_URL}/infer/location"
    payload = {
        "transcript": transcript,
        "town": town or None,
        "feed": feed or None,
        "candidate_streets": candidate_streets or [],
        "candidate_landmarks": candidate_landmarks or [],
        "candidate_towns": candidate_towns or [],
        "notes": notes or None,
    }
    body = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(
        url,
        data=body,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=LOCATION_INFER_TIMEOUT_S) as resp:
            raw = resp.read().decode("utf-8")
            try:
                data = json.loads(raw)
            except json.JSONDecodeError as e:
                return {"ok": False, "service_url": url, "request": payload, "response": None,
                        "error": f"bad_json: {e}"}
            return {"ok": True, "service_url": url, "request": payload, "response": data}
    except urllib.error.HTTPError as e:
        body_text = ""
        try:
            body_text = e.read().decode("utf-8")
        except Exception:
            pass
        return {"ok": False, "service_url": url, "request": payload, "response": None,
                "error": f"http_{e.code}: {body_text}"}
    except urllib.error.URLError as e:
        return {"ok": False, "service_url": url, "request": payload, "response": None,
                "error": f"connection_error: {e.reason}"}
    except TimeoutError:
        return {"ok": False, "service_url": url, "request": payload, "response": None,
                "error": "timeout"}
    except Exception as e:
        return {"ok": False, "service_url": url, "request": payload, "response": None,
                "error": str(e)}
