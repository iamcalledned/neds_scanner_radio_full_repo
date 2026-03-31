#!/usr/bin/env python3
"""
shared/scanner_db.py — Unified metadata manager for Ned's Scanner Network

Single source of truth for every component (transcriber, web, tools).

Schema:
  - calls           – scanner audio metadata, transcripts, quality fields
  - user_activity_log – web analytics / audit trail

Enhancements over the old per-component copies:
  - Loads DB_PATH from environment (no hardcoded /home/ned)
  - Superset schema covering both transcriber and web needs
  - WAL + shared-cache for concurrent readers
  - Structured RotatingFileHandler logging
"""

import os
import sqlite3
import json
import subprocess
import logging
import logging.handlers
import shutil
import redis as _redis_lib
from pathlib import Path
from datetime import datetime, date
from contextlib import closing

from dotenv import load_dotenv

# Load the *root* .env first (shared paths), then allow local .env overrides
_project_root = Path(__file__).resolve().parent.parent
load_dotenv(_project_root / ".env")

# ======================================================
#  Logging
# ======================================================
LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO").upper()
LOG_DIR = Path(os.environ.get("LOG_DIR", "/home/ned/data/scanner_calls/logs/transcriber_logs"))
LOG_DIR.mkdir(parents=True, exist_ok=True)

_log_fmt = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")

_console_handler = logging.StreamHandler()
_console_handler.setFormatter(_log_fmt)

_file_handler = logging.handlers.RotatingFileHandler(
    LOG_DIR / "scanner_db.log", maxBytes=10_000_000, backupCount=5
)
_file_handler.setFormatter(_log_fmt)

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    handlers=[_console_handler, _file_handler],
)
log = logging.getLogger("scanner-db")

# ======================================================
#  Paths from environment
# ======================================================
DB_PATH = Path(os.environ.get(
    "SCANNER_DB_PATH",
    "/home/ned/data/scanner_calls/scanner_calls.db",
))

ARCHIVE_BASE = Path(os.environ.get(
    "ARCHIVE_BASE",
    "/home/ned/data/scanner_calls/scanner_archive",
))

REVIEW_DIR = Path(os.environ.get(
    "REVIEW_DIR",
    str(ARCHIVE_BASE / "review"),
))

# ======================================================
#  TOWN → FEED MAPPING
# ======================================================
TOWN_MAP = {
    "hopedale": ["pd", "fd"],
    "milford": ["mpd", "mfd"],
    "bellingham": ["bpd", "bfd"],
    "mendon": ["mndpd", "mndfd"],
    "blackstone": ["blkpd", "blkfd"],
    "upton": ["uptpd", "uptfd"],
    "franklin": ["frkpd", "frkfd"],
}

# ======================================================
#  INFERENCE HELPERS
# ======================================================
def infer_town_from_filename(filename: str) -> str:
    name = filename.lower()
    for town, feeds in TOWN_MAP.items():
        for feed in feeds:
            if f"_{feed}" in name or f"/{feed}/" in name:
                return town
    return "unknown"


def infer_dept_from_filename(filename: str) -> str:
    """Infer department (police/fire) from feed code in filename or path."""
    name = filename.lower()
    for town, feeds in TOWN_MAP.items():
        for feed in feeds:
            if f"_{feed}" in name or f"/{feed}/" in name:
                if "pd" in feed:
                    return "police"
                elif "fd" in feed:
                    return "fire"
    return "unknown"


# ======================================================
#  CONNECTION HELPERS
# ======================================================
def get_conn(readonly: bool = False):
    """Return a SQLite connection with WAL + shared cache + timeout."""
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)

    uri = f"file:{DB_PATH}?{'mode=ro&' if readonly else ''}cache=shared"
    conn = sqlite3.connect(uri, uri=True, timeout=30, check_same_thread=False)
    conn.row_factory = sqlite3.Row

    if not readonly:
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA synchronous=NORMAL;")
        conn.execute("PRAGMA busy_timeout=5000;")
    return conn


# ======================================================
#  SCHEMA  (superset — transcriber + web)
# ======================================================
def create_tables():
    with get_conn() as conn:
        conn.execute("""
        CREATE TABLE IF NOT EXISTS calls (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            town TEXT,
            state TEXT,
            dept TEXT,
            category TEXT,
            filename TEXT UNIQUE,
            json_path TEXT,
            wav_path TEXT,
            duration REAL,
            rms REAL DEFAULT 0.0,
            transcript TEXT,
            edited_transcript TEXT,
            timestamp TEXT,
            reviewed INTEGER DEFAULT 0,
            play_count INTEGER DEFAULT 0,
            classification JSON,
            intent_labeled INTEGER DEFAULT 0,
            intent_labeled_at TEXT,
            embedding BLOB,
            extra JSON,
            raw_transcript TEXT,
            normalized_transcript TEXT,
            transcription_score REAL,
            needs_retry INTEGER DEFAULT 0,
            needs_review INTEGER DEFAULT 0,
            quality_reasons TEXT,
            profile_used TEXT,
            retry_profiles_tried TEXT,
            transcription_engine TEXT,
            transcription_model TEXT,
            hook_request BOOLEAN DEFAULT FALSE,
            derived_address TEXT,
            derived_street TEXT,
            derived_addr_num TEXT,
            derived_town TEXT,
            derived_lat REAL,
            derived_lng REAL,
            address_confidence TEXT DEFAULT 'none'
        );
        """)
        conn.execute("CREATE INDEX IF NOT EXISTS idx_calls_town_dept ON calls(town, dept);")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_calls_timestamp ON calls(timestamp);")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_calls_derived_addr ON calls(derived_street, derived_town);")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_calls_latlon ON calls(derived_lat, derived_lng);")

        # Web analytics table
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
        conn.execute("CREATE INDEX IF NOT EXISTS idx_ual_timestamp ON user_activity_log(timestamp);")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_ual_action    ON user_activity_log(action);")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_ual_session   ON user_activity_log(session_hash);")

        # ── MassGIS address table (street dictionary + geocoding cache) ──
        conn.execute("""
        CREATE TABLE IF NOT EXISTS addresses (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            master_addr_id  INTEGER,
            street_name     TEXT NOT NULL,
            street_name_id  INTEGER,
            str_name_base   TEXT,
            pre_dir         TEXT,
            pre_type        TEXT,
            pre_mod         TEXT,
            post_type       TEXT,
            post_dir        TEXT,
            post_mod        TEXT,
            addr_num        TEXT,
            addr_num_int    INTEGER,
            unit            TEXT,
            floor           TEXT,
            building        TEXT,
            town            TEXT NOT NULL,
            community       TEXT,
            zipcode         TEXT,
            county          TEXT,
            state           TEXT DEFAULT 'MA',
            latitude        REAL,
            longitude       REAL,
            point_type      TEXT,
            source          TEXT DEFAULT 'massgis',
            imported_at     TEXT,
            UNIQUE(master_addr_id)
        );
        """)
        conn.execute("CREATE INDEX IF NOT EXISTS idx_addr_town        ON addresses(town);")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_addr_street       ON addresses(street_name);")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_addr_base         ON addresses(str_name_base);")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_addr_town_street  ON addresses(town, street_name);")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_addr_num_street   ON addresses(addr_num_int, street_name);")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_addr_latlon       ON addresses(latitude, longitude);")

        # ── Distinct streets view for fast transcript matching ──
        conn.execute("""
        CREATE TABLE IF NOT EXISTS streets (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            street_name_id  INTEGER UNIQUE,
            street_name     TEXT NOT NULL,
            str_name_base   TEXT,
            pre_dir         TEXT,
            pre_type        TEXT,
            post_type       TEXT,
            post_dir        TEXT,
            post_mod        TEXT,
            town            TEXT NOT NULL,
            min_addr_num    INTEGER,
            max_addr_num    INTEGER,
            addr_count      INTEGER DEFAULT 0,
            UNIQUE(street_name, town)
        );
        """)
        conn.execute("CREATE INDEX IF NOT EXISTS idx_streets_town  ON streets(town);")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_streets_base  ON streets(str_name_base);")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_streets_name  ON streets(street_name);")

        # ── Geocoding cache (for addresses resolved via Nominatim/OSM) ──
        conn.execute("""
        CREATE TABLE IF NOT EXISTS geocode_cache (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            query       TEXT NOT NULL UNIQUE,
            latitude    REAL,
            longitude   REAL,
            display     TEXT,
            source      TEXT DEFAULT 'nominatim',
            cached_at   TEXT
        );
        """)
        conn.execute("CREATE INDEX IF NOT EXISTS idx_geocache_query ON geocode_cache(query);")

        log.info(f"[DB] Created or verified tables at {DB_PATH}")


def drop_tables():
    with get_conn() as conn:
        conn.execute("DROP TABLE IF EXISTS calls;")
        conn.execute("DROP TABLE IF EXISTS user_activity_log;")
        log.info("[DB] Dropped all tables.")


# ======================================================
#  UTILITIES
# ======================================================
def get_rms(wav_path: Path) -> float:
    try:
        r = subprocess.run(
            ["sox", "-t", "wav", str(wav_path), "-n", "stat"],
            stderr=subprocess.PIPE, stdout=subprocess.DEVNULL, text=True
        )
        for line in r.stderr.splitlines():
            if "RMS" in line and "amplitude" in line:
                return float(line.split(":")[1].strip())
    except Exception as e:
        log.warning(f"[WARN] RMS calc failed for {wav_path}: {e}")
    return 0.0


# ======================================================
#  INSERT / UPDATE
# ======================================================
def _to_json_str(value) -> str | None:
    """Serialize a list or dict to a JSON string; pass through strings and None unchanged."""
    if value is None:
        return None
    if isinstance(value, (list, dict)):
        return json.dumps(value)
    return value


def insert_call(meta: dict):
    """Insert or replace a single enriched call record (full superset schema)."""
    if not meta.get("town"):
        meta["town"] = infer_town_from_filename(meta.get("filename", ""))
    if not meta.get("dept"):
        meta["dept"] = infer_dept_from_filename(meta.get("filename", ""))
    if not meta.get("state"):
        meta["state"] = "Massachusetts"

    with get_conn() as conn:
        conn.execute("""
        INSERT OR REPLACE INTO calls
        (town, state, dept, category, filename,
         json_path, wav_path, duration, rms,
         transcript, edited_transcript, timestamp,
         reviewed, play_count, classification,
         intent_labeled, intent_labeled_at, extra,
         raw_transcript, normalized_transcript, transcription_score,
         needs_retry, needs_review, quality_reasons,
         profile_used, retry_profiles_tried,
         transcription_engine, transcription_model,
         hook_request,
         derived_address, derived_street, derived_addr_num,
         derived_town, derived_lat, derived_lng, address_confidence)
        VALUES
        (:town, :state, :dept, :category, :filename,
         :json_path, :wav_path, :duration, :rms,
         :transcript, :edited_transcript, :timestamp,
         :reviewed, :play_count, :classification,
         :intent_labeled, :intent_labeled_at, :extra,
         :raw_transcript, :normalized_transcript, :transcription_score,
         :needs_retry, :needs_review, :quality_reasons,
         :profile_used, :retry_profiles_tried,
         :transcription_engine, :transcription_model,
         :hook_request,
         :derived_address, :derived_street, :derived_addr_num,
         :derived_town, :derived_lat, :derived_lng, :address_confidence)
        """, {
            **meta,
            "classification": json.dumps(meta.get("classification", {})),
            "extra": json.dumps(meta.get("extra", {})),
            "quality_reasons": _to_json_str(meta.get("quality_reasons")),
            "retry_profiles_tried": _to_json_str(meta.get("retry_profiles_tried")),
            "raw_transcript": meta.get("raw_transcript"),
            "normalized_transcript": meta.get("normalized_transcript"),
            "transcription_score": meta.get("transcription_score"),
            "needs_retry": meta.get("needs_retry", 0),
            "needs_review": meta.get("needs_review", 0),
            "profile_used": meta.get("profile_used"),
            "transcription_engine": meta.get("transcription_engine"),
            "transcription_model": meta.get("transcription_model"),
            "hook_request": bool(meta.get("hook_request", False)),
            "derived_address": meta.get("derived_address"),
            "derived_street": meta.get("derived_street"),
            "derived_addr_num": meta.get("derived_addr_num"),
            "derived_town": meta.get("derived_town"),
            "derived_lat": meta.get("derived_lat"),
            "derived_lng": meta.get("derived_lng"),
            "address_confidence": meta.get("address_confidence", "none"),
        })
        log.info(f"[DB] Inserted/updated record: {meta.get('filename')} "
                 f"(town={meta.get('town')}, dept={meta.get('dept')}, "
                 f"addr={meta.get('derived_address', 'none')})")


def update_call_classification(meta: dict):
    """Update classification fields after AI labeling."""
    with get_conn() as conn:
        conn.execute("""
            UPDATE calls
               SET classification = :classification,
                   intent_labeled = :intent_labeled,
                   intent_labeled_at = :intent_labeled_at
             WHERE json_path = :json_path;
        """, {
            "classification": json.dumps(meta.get("classification", {})),
            "intent_labeled": int(meta.get("intent_labeled", False)),
            "intent_labeled_at": meta.get("intent_labeled_at"),
            "json_path": meta.get("json_path"),
        })
        log.info(f"[DB] Updated classification for {meta.get('filename')}")


def update_intent(filename: str, classification: dict):
    now = datetime.now().isoformat()
    with get_conn() as conn:
        conn.execute("""
            UPDATE calls
            SET classification = ?, intent_labeled = 1, intent_labeled_at = ?
            WHERE filename = ?
        """, (json.dumps(classification), now, filename))
        log.info(f"[DB] Updated intent for {filename}")


def increment_play_count(filename: str):
    with get_conn() as conn:
        conn.execute("UPDATE calls SET play_count = play_count + 1 WHERE filename = ?", (filename,))


def update_hook_request(filename: str, value: bool):
    with get_conn() as conn:
        conn.execute("UPDATE calls SET hook_request = ? WHERE filename = ?", (value, filename))


def update_review_status(filename: str, reviewed: bool):
    with get_conn() as conn:
        conn.execute("UPDATE calls SET reviewed = ? WHERE filename = ?", (int(reviewed), filename))


def submit_edit_to_sqlite(filename: str, feed: str, new_transcript: str,
                          archive_base: str = None, review_dir: str = None) -> dict:
    """
    Submit an edited transcript to SQLite and copy files for review.

    Args:
        filename:      The .wav filename
        feed:          Department feed ID (pd, fd, etc)
        new_transcript: The edited transcript text
        archive_base:  Base path to scanner archive (default from env)
        review_dir:    Path to review directory (default from env)

    Returns:
        dict: {'success': bool, 'error': str or None}
    """
    archive_base = archive_base or str(ARCHIVE_BASE / "clean")
    review_dir = review_dir or str(REVIEW_DIR)

    try:
        src_wav = Path(archive_base) / feed / filename
        review_path = Path(review_dir)

        if not src_wav.exists():
            return {"success": False, "error": f"Source file not found: {src_wav}"}

        # 1. Update SQLite database
        with get_conn() as conn:
            cur = conn.cursor()
            cur.execute("""
                UPDATE calls
                SET edited_transcript = ?
                WHERE wav_path = ?
            """, (new_transcript, str(src_wav)))

            if cur.rowcount == 0:
                cur.execute("""
                    INSERT INTO calls
                    (wav_path, edited_transcript)
                    VALUES (?, ?)
                """, (str(src_wav), new_transcript))

            conn.commit()

        # 2. Copy file to review directory
        review_path.mkdir(parents=True, exist_ok=True)
        dst_wav = review_path / filename
        shutil.copy2(src_wav, dst_wav)

        return {"success": True}

    except Exception as e:
        log.error(f"Failed to submit edit for {filename}: {e}")
        return {"success": False, "error": str(e)}


# ======================================================
#  QUERIES (read-only)
# ======================================================
def fetch_latest(limit=10):
    with closing(get_conn(readonly=True)) as conn:
        rows = conn.execute("SELECT * FROM calls ORDER BY timestamp DESC LIMIT ?", (limit,)).fetchall()
        return [dict(r) for r in rows]


def search_transcripts(keyword: str, limit=50000):
    like = f"%{keyword.lower()}%"
    with closing(get_conn(readonly=True)) as conn:
        rows = conn.execute("""
            SELECT * FROM calls
            WHERE lower(transcript) LIKE ? OR lower(edited_transcript) LIKE ?
            ORDER BY timestamp DESC LIMIT ?
        """, (like, like, limit)).fetchall()
        return [dict(r) for r in rows]


def get_by_town_dept(town: str, dept: str, limit=50):
    with closing(get_conn(readonly=True)) as conn:
        rows = conn.execute("""
            SELECT * FROM calls
            WHERE town = ? AND dept = ?
            ORDER BY timestamp DESC LIMIT ?
        """, (town, dept, limit)).fetchall()
        return [dict(r) for r in rows]


def avg_rms_by_feed():
    with closing(get_conn(readonly=True)) as conn:
        rows = conn.execute("""
            SELECT town, category, COUNT(*) AS count, AVG(rms) AS avg_rms
            FROM calls
            GROUP BY town, category
            ORDER BY town, avg_rms DESC;
        """).fetchall()
        return [dict(r) for r in rows]


def fetch_edited_calls(limit=5000, include_empty=False):
    """Return calls that have an edited transcript."""
    with closing(get_conn(readonly=True)) as conn:
        if include_empty:
            rows = conn.execute("""
                SELECT * FROM calls
                WHERE edited_transcript IS NOT NULL
                ORDER BY timestamp DESC LIMIT ?
            """, (limit,)).fetchall()
        else:
            rows = conn.execute("""
                SELECT * FROM calls
                WHERE edited_transcript IS NOT NULL
                  AND length(trim(edited_transcript)) > 0
                ORDER BY timestamp DESC LIMIT ?
            """, (limit,)).fetchall()
        return [dict(r) for r in rows]


def read_metadata_from_sqlite(wav_filepath, r):
    """
    Read metadata from SQLite and overlay live play-count from Redis.

    Args:
        wav_filepath: Full path to the .wav file to look up.
        r:            An active Redis connection (decode_responses=True).
    Returns:
        dict of metadata, or empty dict if not found.
    """
    metadata = {}
    base_filename = os.path.basename(wav_filepath)
    log.debug(f"Reading metadata for {base_filename} from SQLite DB...")

    sql = "SELECT * FROM calls WHERE wav_path = ?"
    try:
        with closing(get_conn(readonly=True)) as conn:
            db_data_row = conn.execute(sql, (wav_filepath,)).fetchone()
            if db_data_row:
                metadata = dict(db_data_row)
                for field in ("classification", "extra"):
                    try:
                        if field in metadata and isinstance(metadata[field], str):
                            metadata[field] = json.loads(metadata[field])
                        else:
                            metadata[field] = metadata.get(field, {})
                    except json.JSONDecodeError:
                        log.warning(f"Could not decode '{field}' JSON from DB for {base_filename}")
                        metadata[field] = {}
            else:
                log.debug(f"No metadata found in DB for {base_filename}")
    except Exception as e:
        log.warning(f"Error reading metadata from SQLite for {base_filename}: {e}")
        metadata = {}

    # Overlay live play-count from Redis
    play_count_key = f"scanner:play_count:{base_filename}"
    try:
        play_count_str = r.get(play_count_key)
        metadata["play_count"] = int(play_count_str) if play_count_str else 0
    except (_redis_lib.RedisError, ValueError) as e:
        log.warning(f"Could not read play count from Redis for {base_filename}: {e}")
        if "play_count" not in metadata:
            metadata["play_count"] = 0

    return metadata


def get_todays_stats() -> dict:
    """
    Get total calls, total minutes, active feeds, and hook count for today.
    """
    today = datetime.now().strftime("%Y-%m-%d")
    with closing(get_conn(readonly=True)) as conn:
        row = conn.execute("""
            SELECT
                COUNT(*) as call_count,
                COALESCE(SUM(duration), 0) as total_duration,
                COUNT(DISTINCT dept) as active_feeds
            FROM calls
            WHERE date(timestamp) = ?
        """, (today,)).fetchone()

        hooks_row = conn.execute("""
            SELECT COUNT(*) as hook_count
            FROM calls
            WHERE hook_request = '1' AND date(timestamp) = ?
        """, (today,)).fetchone()

        return {
            "total_calls": row["call_count"],
            "total_minutes": round(row["total_duration"] / 60, 1),
            "active_feeds": row["active_feeds"],
            "total_hooks_today": hooks_row["hook_count"] if hooks_row else 0,
        }


def get_todays_hook_counts_by_feed() -> dict:
    """Return hook_request=1 counts for today keyed by feed/category code."""
    today = datetime.now().strftime("%Y-%m-%d")
    with closing(get_conn(readonly=True)) as conn:
        rows = conn.execute("""
            SELECT category, COUNT(*) as hook_count
            FROM calls
            WHERE hook_request = '1' AND date(timestamp) = ?
            GROUP BY category
        """, (today,)).fetchall()
        return {row["category"]: row["hook_count"] for row in rows}


# ======================================================
#  BULK IMPORT
# ======================================================
def _bulk_insert_calls(cur, records):
    query = """
        INSERT INTO calls (
            town, state, dept, category, filename, json_path, wav_path,
            duration, rms, transcript, edited_transcript,
            timestamp, reviewed, play_count, classification,
            intent_labeled, intent_labeled_at, extra
        )
        VALUES (
            :town, :state, :dept, :category, :filename, :json_path, :wav_path,
            :duration, :rms, :transcript, :edited_transcript,
            :timestamp, :reviewed, :play_count, :classification,
            :intent_labeled, :intent_labeled_at, :extra
        )
    """
    cur.executemany(query, records)
    log.info(f"[DB] Inserted {len(records):,} new records...")


def import_existing_jsons(base_dir: str = None):
    base_dir = base_dir or str(ARCHIVE_BASE / "clean")
    base = Path(base_dir)
    if not base.exists():
        log.warning(f"[WARN] Base dir not found: {base_dir}")
        return

    start_time = datetime.now()
    processed = 0
    skipped = 0
    inserted = 0

    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute("SELECT filename FROM calls")
        existing_filenames = {row[0] for row in cur.fetchall()}
        log.info(f"[DB] Found {len(existing_filenames):,} existing records")

        new_records = []

        for js in base.rglob("*.json"):
            processed += 1
            try:
                data = json.loads(js.read_text())
                filename = data.get("filename") or js.stem + ".wav"
                if filename in existing_filenames:
                    skipped += 1
                    continue

                wav_path = js.with_suffix(".wav")
                category = data.get("source", "")
                town = infer_town_from_filename(filename)
                dept = infer_dept_from_filename(filename)
                state = data.get("state", "Massachusetts")

                meta = {
                    "town": town,
                    "state": state,
                    "dept": dept,
                    "category": category,
                    "filename": filename,
                    "json_path": str(js),
                    "wav_path": str(wav_path),
                    "duration": data.get("duration", 0.0),
                    "rms": data.get("rms", 0.0),
                    "transcript": data.get("transcript", ""),
                    "edited_transcript": data.get("edited_transcript", ""),
                    "timestamp": data.get("timestamp", datetime.now().isoformat()),
                    "reviewed": 0,
                    "play_count": data.get("play_count", 0),
                    "classification": json.dumps(data.get("classification", {})),
                    "intent_labeled": int(data.get("intent_labeled", False)),
                    "intent_labeled_at": data.get("intent_labeled_at"),
                    "extra": json.dumps(data),
                }
                new_records.append(meta)

                if len(new_records) >= 1000:
                    _bulk_insert_calls(cur, new_records)
                    conn.commit()
                    existing_filenames.update(m["filename"] for m in new_records)
                    inserted += len(new_records)
                    new_records.clear()
                    log.info(f"[DB] Progress: processed={processed:,} inserted={inserted:,} skipped={skipped:,}")

            except Exception as e:
                log.error(f"[ERROR] Failed to import {js}: {e}")

        if new_records:
            _bulk_insert_calls(cur, new_records)
            conn.commit()
            inserted += len(new_records)

    elapsed = (datetime.now() - start_time).total_seconds()
    log.info("─────────────────────────────────────────────")
    log.info(f"[SUMMARY] Import complete:")
    log.info(f"  Base dir:    {base_dir}")
    log.info(f"  Processed:   {processed:,}")
    log.info(f"  Inserted:    {inserted:,}")
    log.info(f"  Skipped:     {skipped:,} (duplicates)")
    log.info(f"  Elapsed:     {elapsed:.1f} sec ({processed / elapsed:.1f} files/sec)")
    log.info("─────────────────────────────────────────────")


# ======================================================
#  ADDRESS / STREET LOOKUPS
# ======================================================
def get_streets_for_town(town: str) -> list[dict]:
    """Return all distinct streets for a town (from the streets table)."""
    with get_conn(readonly=True) as conn:
        rows = conn.execute(
            "SELECT * FROM streets WHERE UPPER(town) = UPPER(?) ORDER BY street_name",
            (town,),
        ).fetchall()
        return [dict(r) for r in rows]


def lookup_street(street_fragment: str, town: str = None) -> list[dict]:
    """Fuzzy-ish street lookup: matches if the base name appears in the query.
    Returns matching streets sorted by address count (most popular first)."""
    fragment = street_fragment.strip().upper()
    with get_conn(readonly=True) as conn:
        if town:
            rows = conn.execute("""
                SELECT * FROM streets
                WHERE UPPER(town) = UPPER(?)
                  AND (UPPER(street_name) LIKE ? OR UPPER(str_name_base) LIKE ?)
                ORDER BY addr_count DESC
            """, (town, f"%{fragment}%", f"%{fragment}%")).fetchall()
        else:
            rows = conn.execute("""
                SELECT * FROM streets
                WHERE UPPER(street_name) LIKE ? OR UPPER(str_name_base) LIKE ?
                ORDER BY addr_count DESC
            """, (f"%{fragment}%", f"%{fragment}%")).fetchall()
        return [dict(r) for r in rows]


def validate_address(number: int, street_name: str, town: str = None) -> list[dict]:
    """Check if a specific address (number + street) exists in the addresses table."""
    with get_conn(readonly=True) as conn:
        if town:
            rows = conn.execute("""
                SELECT * FROM addresses
                WHERE addr_num_int = ?
                  AND UPPER(street_name) = UPPER(?)
                  AND UPPER(town) = UPPER(?)
                LIMIT 5
            """, (number, street_name, town)).fetchall()
        else:
            rows = conn.execute("""
                SELECT * FROM addresses
                WHERE addr_num_int = ?
                  AND UPPER(street_name) = UPPER(?)
                LIMIT 10
            """, (number, street_name)).fetchall()
        return [dict(r) for r in rows]


def get_address_coords(number: int, street_name: str, town: str = None) -> dict | None:
    """Return lat/lng for a specific address, or None."""
    results = validate_address(number, street_name, town)
    for r in results:
        if r.get("latitude") and r.get("longitude"):
            return {"latitude": r["latitude"], "longitude": r["longitude"],
                    "town": r["town"], "street_name": r["street_name"],
                    "addr_num": r["addr_num"]}
    return None


def get_geocode_cache(query: str) -> dict | None:
    """Look up a cached geocode result."""
    with get_conn(readonly=True) as conn:
        row = conn.execute(
            "SELECT * FROM geocode_cache WHERE query = ?", (query,)
        ).fetchone()
        return dict(row) if row else None


def set_geocode_cache(query: str, lat: float, lon: float, display: str = "",
                      source: str = "nominatim"):
    """Cache a geocode result."""
    with get_conn() as conn:
        conn.execute("""
            INSERT OR REPLACE INTO geocode_cache (query, latitude, longitude, display, source, cached_at)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (query, lat, lon, display, source, datetime.now().isoformat()))


def address_stats() -> dict:
    """Return summary stats about the address tables."""
    with get_conn(readonly=True) as conn:
        addr_count = conn.execute("SELECT COUNT(*) FROM addresses").fetchone()[0]
        street_count = conn.execute("SELECT COUNT(*) FROM streets").fetchone()[0]
        geo_count = conn.execute("SELECT COUNT(*) FROM addresses WHERE latitude IS NOT NULL").fetchone()[0]
        town_counts = conn.execute("""
            SELECT town, COUNT(*) as cnt FROM addresses
            GROUP BY town ORDER BY cnt DESC
        """).fetchall()
        return {
            "total_addresses": addr_count,
            "total_streets": street_count,
            "geocoded_addresses": geo_count,
            "by_town": {r["town"]: r["cnt"] for r in town_counts},
        }


# ======================================================
#  CLI
# ======================================================
if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Scanner DB Manager (WAL + Read-only support)")
    parser.add_argument("action", choices=[
        "create", "drop", "import", "latest", "search", "rmsavg", "edited", "addrstats"
    ])
    parser.add_argument("--keyword")
    parser.add_argument("--limit", type=int, default=10)
    args = parser.parse_args()

    if args.action == "create":
        create_tables()
    elif args.action == "drop":
        drop_tables()
    elif args.action == "import":
        create_tables()
        import_existing_jsons()
    elif args.action == "latest":
        for r in fetch_latest(limit=args.limit):
            log.info(f"{r['timestamp']}  {r.get('town','?'):10} {r.get('dept','?'):7} "
                     f"{r.get('category','?'):8} RMS={r.get('rms',0):.4f}  {r['filename']}")
    elif args.action == "search":
        if not args.keyword:
            log.warning("Need --keyword")
        else:
            for r in search_transcripts(args.keyword, limit=args.limit):
                log.info(f"{r['timestamp']}  {r.get('town','?'):10} {r.get('dept','?'):7} "
                         f"{r.get('category','?'):8} RMS={r.get('rms',0):.4f}  {r['filename']}")
    elif args.action == "rmsavg":
        for r in avg_rms_by_feed():
            log.info(f"{r['town']:10} {r['category']:8} Count={r['count']:4} AvgRMS={r['avg_rms']:.4f}")
    elif args.action == "edited":
        for r in fetch_edited_calls(limit=args.limit, include_empty=False):
            log.info(f"{r['timestamp']}  {r.get('town','?'):10} {r.get('dept','?'):7} "
                     f"{r.get('category','?'):8}  {r['filename']}")
    elif args.action == "addrstats":
        stats = address_stats()
        log.info(f"[ADDR] Total addresses: {stats['total_addresses']:,}")
        log.info(f"[ADDR] Total streets:   {stats['total_streets']:,}")
        log.info(f"[ADDR] Geocoded:        {stats['geocoded_addresses']:,}")
        for town, cnt in stats.get("by_town", {}).items():
            log.info(f"  {town:15} {cnt:,} addresses")
