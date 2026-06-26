"""
gpu_model_manager/core/config.py

Central configuration. All values have sensible defaults.
Override any value via environment variables.
"""
from __future__ import annotations

import os
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
PROJECT_ROOT = Path(
    os.environ.get(
        "SCANNER_PROJECT_ROOT",
        "/home/ned/Documents/neds_scanner_radio_full_pipeline_with_git",
    )
)

GPU_MODEL_MANAGER_ROOT = Path(
    os.environ.get(
        "GPU_MODEL_MANAGER_ROOT",
        str(PROJECT_ROOT / "tools" / "gpu_model_manager"),
    )
)

TRANSCRIBER_DIR = Path(
    os.environ.get("TRANSCRIBER_DIR", str(PROJECT_ROOT / "transcriber"))
)

# ---------------------------------------------------------------------------
# Web server
# ---------------------------------------------------------------------------
DEFAULT_WEB_HOST: str = os.environ.get("GPU_MGR_HOST", "127.0.0.1")
DEFAULT_WEB_PORT: int = int(os.environ.get("GPU_MGR_PORT", "8020"))

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
LOG_DIR = Path(
    os.environ.get("GPU_MGR_LOG_DIR", str(GPU_MODEL_MANAGER_ROOT / "logs"))
)
LOG_FILE = LOG_DIR / "gpu_model_manager.log"
LOG_LEVEL: str = os.environ.get("GPU_MGR_LOG_LEVEL", "INFO").upper()

# ---------------------------------------------------------------------------
# VRAM policy
# ---------------------------------------------------------------------------
GPU_SAFETY_MARGIN_MB: int = int(os.environ.get("GPU_SAFETY_MARGIN_MB", "2500"))
SCANNER_WHISPER_RESERVED_MB: int = int(
    os.environ.get("SCANNER_WHISPER_RESERVED_MB", "7000")
)

# ---------------------------------------------------------------------------
# Service endpoints
# ---------------------------------------------------------------------------
SCANNER_MCP_URL: str = os.environ.get(
    "SCANNER_MCP_URL", "http://127.0.0.1:8008/mcp"
)
LOCAL_LLM_BASE_URL: str = os.environ.get(
    "LOCAL_LLM_BASE_URL", "http://192.168.86.53:30000"
)
LOCAL_LLM_MODELS_URL: str = os.environ.get(
    "LOCAL_LLM_MODELS_URL", "http://192.168.86.53:30000/v1/models"
)
LOCAL_LLM_CHAT_URL: str = os.environ.get(
    "LOCAL_LLM_CHAT_URL", "http://192.168.86.53:30000/v1/chat/completions"
)
MEETING_MCP_URL: str = os.environ.get(
    "MEETING_MCP_URL", "http://127.0.0.1:8011/mcp"
)

# ---------------------------------------------------------------------------
# Runtime registry config file
# ---------------------------------------------------------------------------
RUNTIMES_CONFIG_FILE = Path(
    os.environ.get(
        "GPU_MGR_RUNTIMES_CONFIG",
        str(GPU_MODEL_MANAGER_ROOT / "config" / "runtimes.json"),
    )
)

# ---------------------------------------------------------------------------
# Lease store (phase 1: local JSON file)
# ---------------------------------------------------------------------------
LEASE_BACKEND: str = os.environ.get("LEASE_BACKEND", "local_file")
STATE_DIR = GPU_MODEL_MANAGER_ROOT / "state"
LEASE_FILE = Path(
    os.environ.get("LEASE_FILE", str(STATE_DIR / "leases.json"))
)
