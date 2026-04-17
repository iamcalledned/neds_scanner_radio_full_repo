from __future__ import annotations

import re
from pathlib import Path
from typing import Sequence


def detect_category(file: Path, feed_keys: Sequence[str]) -> tuple[str, str]:
    """Detect feed (e.g., mpd, frkfd) from filename or path."""
    name = file.name.lower()
    parts = " ".join(file.parts).lower()
    for key in feed_keys:
        if re.search(rf"_{key}(?:\.|_|$)", name):
            return key, key
        if f"/{key}/" in parts:
            return key, key
    return "misc", "misc"
