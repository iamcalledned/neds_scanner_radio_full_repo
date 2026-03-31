#!/usr/bin/env python3
"""
client_tracker.py — WebSocket client tracking and audit module

Logs unique browser sessions, IPs, and fingerprints for Socket.IO clients.
Data stored in SQLite: /home/ned/data/scanner_calls/scanner_calls.db
"""

import sqlite3
import hashlib
import datetime
import json
from pathlib import Path

DB_PATH = Path("/home/ned/data/scanner_calls/scanner_calls.db")

# ======================================================
#  DB SETUP
# ======================================================
def init_client_table():
    with sqlite3.connect(DB_PATH) as conn:
        c = conn.cursor()
        c.execute("""
            CREATE TABLE IF NOT EXISTS client_sessions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                client_id TEXT,
                ip TEXT,
                user_agent TEXT,
                origin TEXT,
                referrer TEXT,
                language TEXT,
                fingerprint TEXT,
                first_seen TEXT,
                last_seen TEXT,
                geo_json TEXT,
                connection_count INTEGER DEFAULT 1
            );
        """)
        conn.commit()

def log_client_connection(client_id, ip, user_agent, origin, referrer, language, geo_json=None):
    fingerprint_src = f"{client_id or ''}|{ip}|{user_agent or ''}|{origin or ''}"
    fingerprint = hashlib.sha256(fingerprint_src.encode()).hexdigest()[:16]
    now = datetime.datetime.now().isoformat()

    with sqlite3.connect(DB_PATH) as conn:
        c = conn.cursor()
        c.execute("SELECT id, connection_count FROM client_sessions WHERE fingerprint = ?", (fingerprint,))
        row = c.fetchone()

        if row:
            # Existing client
            c.execute("""
                UPDATE client_sessions
                SET last_seen = ?, connection_count = connection_count + 1
                WHERE id = ?
            """, (now, row[0]))
        else:
            # New client
            c.execute("""
                INSERT INTO client_sessions
                    (client_id, ip, user_agent, origin, referrer, language, fingerprint,
                     first_seen, last_seen, geo_json)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                client_id, ip, user_agent, origin, referrer, language,
                fingerprint, now, now, json.dumps(geo_json or {})
            ))
        conn.commit()

def fetch_client_geo(ip):
    """Optional lightweight IP geolocation."""
    try:
        import requests
        resp = requests.get(f"https://ipapi.co/{ip}/json/", timeout=2)
        if resp.status_code == 200:
            return resp.json()
    except Exception:
        pass
    return None
