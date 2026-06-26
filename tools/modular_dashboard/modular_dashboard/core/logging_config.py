"""
core/logging_config.py — Configure rotating-file + console logging.
"""
from __future__ import annotations

import logging
import logging.handlers
import os
from pathlib import Path

from modular_dashboard.core.config import LOG_DIR, LOG_FILE, LOG_LEVEL


def setup_logging(name: str = "modular_dashboard") -> logging.Logger:
    """Configure and return a root logger for the application.

    Call once at application start-up (web or desktop).
    Safe to call multiple times — handlers are added only once.
    """
    logger = logging.getLogger(name)

    if logger.handlers:
        return logger  # already configured

    level = getattr(logging, LOG_LEVEL.upper(), logging.INFO)
    logger.setLevel(level)

    formatter = logging.Formatter(
        "%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # Console handler
    ch = logging.StreamHandler()
    ch.setLevel(level)
    ch.setFormatter(formatter)
    logger.addHandler(ch)

    # Rotating file handler — create log directory if missing
    log_dir = Path(LOG_DIR)
    try:
        log_dir.mkdir(parents=True, exist_ok=True)
        fh = logging.handlers.RotatingFileHandler(
            LOG_FILE,
            maxBytes=5 * 1024 * 1024,   # 5 MB
            backupCount=5,
            encoding="utf-8",
        )
        fh.setLevel(level)
        fh.setFormatter(formatter)
        logger.addHandler(fh)
    except OSError as exc:
        logger.warning("Could not create log file %s: %s", LOG_FILE, exc)

    return logger


def get_logger(module_name: str) -> logging.Logger:
    """Return a child logger for the given module name."""
    return logging.getLogger(f"modular_dashboard.{module_name}")
