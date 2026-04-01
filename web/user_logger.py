#!/usr/bin/env python3
"""
user_logger.py — User activity and audit logging

Records page views and user actions to SQLite alongside the main scanner DB.
Must be called from within a Flask request context.

Table: user_activity_log
  - session_hash: one-way hash of the scanner_session cookie (safe to store)
  - client_id: JS-side fingerprint cookie, if present
  - ip: client IP (respects CF-Connecting-IP and X-Forwarded-For)
  - action: e.g. "page_view", "play_audio", "transcript_edit", "submit_intent", "segment_label"
  - detail: JSON blob with action-specific fields (filename, feed, etc.)
"""

import hashlib
import datetime
import json
import os
import sqlite3
import logging
from pathlib import Path

from dotenv import load_dotenv
from flask import request

# Load root .env for SCANNER_DB_PATH
_project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
load_dotenv(os.path.join(_project_root, ".env"))

DB_PATH = Path(os.environ.get("SCANNER_DB_PATH", "/home/ned/data/scanner_calls/scanner_calls.db"))
logger = logging.getLogger("scanner_web.user_activity")

_table_ready = False


def init_user_activity_table():
    """Create the user_activity_log table and indexes if they don't exist."""
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS user_activity_log (
                id           INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp    TEXT    NOT NULL,
                session_hash TEXT,
                client_id    TEXT,
                ip           TEXT,
                action       TEXT    NOT NULL,
                detail       TEXT
            );
        """)
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_ual_timestamp ON user_activity_log(timestamp);"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_ual_action ON user_activity_log(action);"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_ual_session ON user_activity_log(session_hash);"
        )
        conn.commit()


def _hash_session(session_id: str) -> str:
    """One-way hash of a session ID — safe to store, not reversible."""
    return hashlib.sha256(session_id.encode()).hexdigest()[:16]


def log_activity(action: str, detail: dict = None) -> None:
    """
    Log a user action from within a Flask request context.

    Silently swallows all exceptions so logging never crashes the app.

    Args:
        action: Short label for the event, e.g. "play_audio", "transcript_edit",
                "page_view", "submit_intent", "segment_label".
        detail: Optional dict with action-specific fields (filename, feed, page, etc.).
    """
    global _table_ready
    if not _table_ready:
        init_user_activity_table()
        _table_ready = True

    try:
        session_id = request.cookies.get("scanner_session")
        session_hash = _hash_session(session_id) if session_id else None

        client_id = request.cookies.get("client_id")

        ip = (
            request.headers.get("CF-Connecting-IP")
            or request.headers.get("X-Forwarded-For", request.remote_addr)
        )

        now = datetime.datetime.now().isoformat()
        detail_json = json.dumps(detail) if detail else None

        with sqlite3.connect(DB_PATH) as conn:
            conn.execute(
                """
                INSERT INTO user_activity_log
                    (timestamp, session_hash, client_id, ip, action, detail)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (now, session_hash, client_id, ip, action, detail_json),
            )
            conn.commit()
    except Exception as e:
        logger.warning("user_activity.persist_failed action=%s error=%s", action, e)
