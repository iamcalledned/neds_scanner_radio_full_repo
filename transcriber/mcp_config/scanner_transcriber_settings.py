from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Dict, List


ARCHIVE_BASE = Path(os.environ.get("ARCHIVE_BASE", "/home/ned/data/scanner_calls/scanner_archive")).expanduser().resolve()
TMP_DIR = Path(os.environ.get("SCANNER_TMP_DIR", "/tmp/scanner_tmp")).expanduser().resolve()

MODEL_BASE_DIR = Path(os.environ.get("MODEL_BASE_DIR", "/home/ned/models")).expanduser().resolve()
MODEL_DIR = Path(os.environ.get("WHISPER_MODEL_DIR", MODEL_BASE_DIR / "trained_whisper_medium_april_2026_ct2")).expanduser().resolve()
DEFAULT_COMPUTE_TYPE = os.environ.get("WHISPER_COMPUTE_TYPE", "float16")

MIN_DURATION = float(os.environ.get("MIN_DURATION", "2"))
RMS_THRESHOLD = float(os.environ.get("RMS_THRESHOLD", "0.001"))

LOCATION_INFER_BASE_URL = os.environ.get("LOCATION_INFER_BASE_URL", "http://127.0.0.1:8011").rstrip("/")
LOCATION_INFER_TIMEOUT_S = int(os.environ.get("LOCATION_INFER_TIMEOUT_S", "30"))

ALLOWED_ROOTS = [
    p.strip() for p in os.environ.get("ALLOWED_AUDIO_ROOTS", str(ARCHIVE_BASE)).split(",") if p.strip()
]
ALLOWED_ROOTS = [Path(p).expanduser().resolve() for p in ALLOWED_ROOTS]
INTERACTIVE_EXTRA_ALLOWED_ROOTS = [
    Path("/home/ned/data/town_hall_streams").expanduser().resolve(),
]

_seen_roots = set()
INTERACTIVE_ALLOWED_ROOTS: List[Path] = []
for _root in [*ALLOWED_ROOTS, *INTERACTIVE_EXTRA_ALLOWED_ROOTS]:
    _key = str(_root)
    if _key not in _seen_roots:
        _seen_roots.add(_key)
        INTERACTIVE_ALLOWED_ROOTS.append(_root)
del _seen_roots

ENABLE_DB = os.environ.get("ENABLE_SCANNER_DB", "1").strip() not in ("0", "false", "False")

MODEL_CATALOG_JSON = os.environ.get("MODEL_CATALOG_JSON", "")
MODEL_CATALOG_FILE = os.environ.get("MODEL_CATALOG_FILE", "")
MODEL_ROUTING_RULES = os.environ.get("MODEL_ROUTING_RULES", "")
MODEL_ROUTING_FILE = os.environ.get("MODEL_ROUTING_FILE", "")
DEFAULT_MODEL_KEY = os.environ.get("DEFAULT_MODEL_KEY", "default")
DEFAULT_MODEL_KEY_ENV = os.environ.get("DEFAULT_MODEL_KEY")
MODEL_CACHE_LIMIT = int(os.environ.get("MODEL_CACHE_LIMIT", "1"))
WARM_DEFAULT_MODEL = os.environ.get("WARM_DEFAULT_MODEL", "1").strip().lower() not in ("0", "false", "no")

TRANSCRIBE_DEFAULTS: Dict[str, Any] = {
    "task": "transcribe",
    "language": "en",
    "beam_size": 3,
    "word_timestamps": False,
    "condition_on_previous_text": False,
    "vad_filter": False,
    "initial_prompt": None,
    "temperature": 0.0,
    "best_of": None,
    "patience": None,
    "compression_ratio_threshold": None,
    "log_prob_threshold": None,
    "no_speech_threshold": None,
}

TRANSCRIBE_KEYS = tuple(TRANSCRIBE_DEFAULTS.keys())

FEED_KEYS = [
    "mndfd", "mndpd", "mpd", "mfd", "bpd", "bfd", "pd", "fd",
    "blkfd", "blkpd", "uptfd", "uptpd", "frkpd", "frkfd",
]

SOURCE_MAP = {
    "pd": "Hopedale",
    "fd": "Hopedale",
    "mfd": "Milford",
    "mpd": "Milford",
    "bfd": "Bellingham",
    "bpd": "Bellingham",
    "mndfd": "Mendon",
    "mndpd": "Mendon",
    "uptfd": "Upton",
    "uptpd": "Upton",
    "blkfd": "Blackstone",
    "blkpd": "Blackstone",
    "frkfd": "Franklin",
    "frkpd": "Franklin",
}
