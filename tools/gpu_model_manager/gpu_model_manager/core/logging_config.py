"""
gpu_model_manager/core/logging_config.py

Sets up console + rotating file logging for the gpu_mgr logger hierarchy.
"""
from __future__ import annotations

import logging
import logging.handlers

from .config import LOG_DIR, LOG_FILE, LOG_LEVEL

_configured = False


def setup_logging(level: str = LOG_LEVEL) -> None:
    global _configured
    if _configured:
        return
    _configured = True

    LOG_DIR.mkdir(parents=True, exist_ok=True)

    fmt = logging.Formatter(
        "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s"
    )

    root = logging.getLogger("gpu_mgr")
    root.setLevel(getattr(logging, level, logging.INFO))
    root.propagate = False

    if not root.handlers:
        ch = logging.StreamHandler()
        ch.setFormatter(fmt)
        root.addHandler(ch)

        fh = logging.handlers.RotatingFileHandler(
            LOG_FILE, maxBytes=10_000_000, backupCount=5, encoding="utf-8"
        )
        fh.setFormatter(fmt)
        root.addHandler(fh)


def get_logger(name: str) -> logging.Logger:
    setup_logging()
    return logging.getLogger(f"gpu_mgr.{name}")
