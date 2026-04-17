from flask import Blueprint, render_template, send_from_directory, request, jsonify, redirect, abort
from pathlib import Path
from collections import defaultdict
from werkzeug.utils import secure_filename
import json
import shutil
import os
import time
import threading
import uuid
from datetime import datetime, date, timedelta
import redis
import logging

from shared.scanner_db import (
    get_conn,
    increment_play_count,
    submit_edit_to_sqlite,
    set_save_for_eval,
    set_freeze_for_testing,
    ensure_columns,
    fetch_reviewed_edited_calls,
)
from user_logger import log_activity

# Run column migrations on import (safe to call multiple times)
ensure_columns()



scanner_bp = Blueprint("scanner", __name__)
logger = logging.getLogger("scanner_web.routes_scanner")
GOOGLE_MAPS_API_KEY = os.environ.get('GOOGLE_MAPS_API_KEY', '')
LOGIN_PROCESS_URL = os.environ.get('LOGIN_PROCESS_URL', 'http://127.0.0.1:8010/api/login')
LOGIN_API_URL = os.environ.get('LOGIN_API_URL', 'http://127.0.0.1:8010')
ARCHIVE_DIR = os.environ.get("ARCHIVE_DIR", os.path.join(os.environ.get("ARCHIVE_BASE", "/home/ned/data/scanner_calls/scanner_archive"), "clean"))
REVIEW_DIR = Path(os.environ.get("REVIEW_DIR", os.path.join(os.environ.get("ARCHIVE_BASE", "/home/ned/data/scanner_calls/scanner_archive"), "review")))
SEGMENT_DIR = Path(os.environ.get("SEGMENT_DIR", os.path.join(os.environ.get("ARCHIVE_BASE", "/home/ned/data/scanner_calls/scanner_archive"), "segmentation/processed")))
CALLS_PER_PAGE = 10
REDIS_URL = os.environ.get('REDIS_URL', 'redis://127.0.0.1:6379/0')


VALID_FEEDS = {
    "pd", "fd", "mpd", "mfd", "sfd", "bpd", "bfd",
    "mndfd", "mndpd", "blkfd", "blkpd", "uptfd", "uptpd",
    "frkpd", "frkfd", "milpd", "milfd", "medpd", "medfd", "foxpd",
}


def _discover_training_reports():
    """Return available training result templates in web/templates."""
    templates_dir = Path(__file__).resolve().parent.parent / "templates"
    reports = []
    for p in sorted(templates_dir.glob("*_training_result_set.html"), reverse=True):
        label = p.stem.replace("_", " ").title()
        reports.append(
            {
                "template": p.name,
                "label": label,
                "updated_ts": p.stat().st_mtime,
            }
        )
    return reports



# Simple in-memory active user registry. Key: client_id -> {last_seen, ip, ua, page}
ACTIVE_USERS = {}
ACTIVE_LOCK = threading.Lock()
ACTIVE_TIMEOUT = 120  # seconds considered "active"

API_CACHE = {}
API_CACHE_LOCK = threading.Lock()
API_CACHE_TTL = {
    "latest": 10,
    "home_live_calls": 10,
    "stats": 30,
    "today_counts": 30,
    "archive_calls": 15
}
API_CACHE_PREFIX = "scanner_api_cache:"


def _get_redis_client():
    try:
        return redis.from_url(REDIS_URL, decode_responses=True)
    except Exception:
        return None


def _safe_fromisoformat(value):
    if not value:
        return None
    try:
        if isinstance(value, str) and value.endswith("Z"):
            value = value[:-1] + "+00:00"
        dt = datetime.fromisoformat(value)
        if dt.tzinfo is not None:
            return dt.astimezone().replace(tzinfo=None)
        return dt
    except Exception:
        return None


def _timestamp_from_filename(filename):
    stem = Path(filename).stem
    parts = stem.split("_")
    if len(parts) < 3:
        return None
    try:
        return datetime.strptime(f"{parts[1]}_{parts[2]}", "%Y-%m-%d_%H-%M-%S")
    except Exception:
        return None


def _row_to_metadata(row):
    metadata = dict(row)
    for field in ("classification", "extra"):
        raw = metadata.get(field)
        if isinstance(raw, str):
            try:
                metadata[field] = json.loads(raw)
            except json.JSONDecodeError:
                metadata[field] = {}
        elif raw is None:
            metadata[field] = {}

    extra = metadata.get("extra") or {}
    if isinstance(extra, dict):
        if extra.get("enhanced_transcript") and not metadata.get("enhanced_transcript"):
            metadata["enhanced_transcript"] = extra["enhanced_transcript"]
        if extra.get("derived_full_address") and not metadata.get("derived_full_address"):
            metadata["derived_full_address"] = extra["derived_full_address"]

    if metadata.get("derived_address") and not metadata.get("derived_full_address"):
        metadata["derived_full_address"] = metadata["derived_address"]

    return metadata


def _row_to_call_payload(row, feed_override=None, timestamp_format="%b %d, %I:%M %p"):
    metadata = _row_to_metadata(row)
    edited_transcript = row["edited_transcript"] or ""
    transcript = row["transcript"] or "(no transcript)"
    edit_pending = False  # no longer used; kept for schema compatibility

    ts = _safe_fromisoformat(row["timestamp"]) or _timestamp_from_filename(row["filename"])
    timestamp_human = ts.strftime(timestamp_format) if ts else row["filename"]

    return {
        "file": row["filename"],
        "path": f"/scanner/audio/{row['filename']}",
        "transcript": transcript,
        "edited_transcript": edited_transcript,
        "enhanced_transcript": metadata.get("enhanced_transcript", ""),
        "edit_pending": edit_pending,
        "save_for_eval": bool(row["save_for_eval"]) if "save_for_eval" in row.keys() else False,
        "freeze_for_testing": bool(row["freeze_for_testing"]) if "freeze_for_testing" in row.keys() else False,
        "timestamp": row["timestamp"] or "",
        "timestamp_human": timestamp_human,
        "feed": feed_override or row["category"] or "",
        "duration": row["duration"] or 0,
        "metadata": metadata,
    }


def _archive_cache_key(feed, offset, limit):
    return f"archive_calls:{feed}:{offset}:{limit}"


def _get_cached_response(cache_key):
    ttl = API_CACHE_TTL.get(cache_key)
    if not ttl:
        return None
    now = time.time()
    with API_CACHE_LOCK:
        cached = API_CACHE.get(cache_key)
        if not cached:
            return None
        if now - cached["timestamp"] > ttl:
            API_CACHE.pop(cache_key, None)
            return None
        return cached["data"]

    return None


def _get_cached_response_redis(cache_key):
    redis_client = _get_redis_client()
    if not redis_client:
        return None
    key = f"{API_CACHE_PREFIX}{cache_key}"
    try:
        cached = redis_client.get(key)
        if not cached:
            return None
        return json.loads(cached)
    except Exception:
        return None


def _set_cached_response(cache_key, data):
    with API_CACHE_LOCK:
        API_CACHE[cache_key] = {"timestamp": time.time(), "data": data}


def _set_cached_response_redis(cache_key, data):
    redis_client = _get_redis_client()
    if not redis_client:
        return
    ttl = API_CACHE_TTL.get(cache_key)
    if not ttl:
        return
    key = f"{API_CACHE_PREFIX}{cache_key}"
    try:
        redis_client.setex(key, ttl, json.dumps(data))
    except Exception:
        return


def _compute_latest():
    latest = {}
    with get_conn(readonly=True) as conn:
        for key in sorted(VALID_FEEDS):
            try:
                row = conn.execute("""
                    SELECT filename, duration, transcript, edited_transcript, extra
                    FROM calls
                    WHERE category = ?
                    ORDER BY timestamp DESC
                    LIMIT 1
                """, (key,)).fetchone()

                if not row:
                    latest[key] = None
                    continue

                extra = {}
                if row["extra"]:
                    try:
                        extra = json.loads(row["extra"])
                    except json.JSONDecodeError:
                        extra = {}

                transcript = (
                    extra.get("enhanced_transcript")
                    or row["edited_transcript"]
                    or row["transcript"]
                )
                latest[key] = {
                    "file": row["filename"],
                    "transcript": transcript.strip()[:300] if transcript else None,
                    "duration": row["duration"] or 0,
                }
            except Exception as e:
                logger.warning("scanner_latest failed for %s: %s", key, e)
                latest[key] = None

    return latest


def _compute_home_live_calls(limit=6):
    with get_conn(readonly=True) as conn:
        rows = conn.execute("""
            SELECT *
            FROM calls
            WHERE category IN ({placeholders})
            ORDER BY timestamp DESC
            LIMIT ?
        """.format(placeholders=",".join("?" for _ in VALID_FEEDS)), [*sorted(VALID_FEEDS), limit]).fetchall()

    calls = []
    for row in rows:
        try:
            calls.append(_row_to_call_payload(row))
        except Exception as e:
            logger.warning("home_live_calls failed for %s: %s", row["filename"], e)
    return {"calls": calls}


def _compute_stats():
    stats = {
        "total_calls_today": 0,
        "total_calls": 0,
        "total_calls_all_time": 0,
        "total_disk_usage_bytes": 0,
        "total_disk_usage_readable": "",
        "total_minutes": 0,
        "active_feeds": 0
    }

    with get_conn(readonly=True) as conn:
        totals = conn.execute("""
            SELECT
                COUNT(*) AS total_calls_all_time,
                COALESCE(SUM(CASE WHEN date(timestamp) = ? THEN 1 ELSE 0 END), 0) AS total_calls_today
            FROM calls
        """, (date.today().isoformat(),)).fetchone()
        stats["total_calls_today"] = totals["total_calls_today"] if totals else 0
        stats["total_calls_all_time"] = totals["total_calls_all_time"] if totals else 0

    for feed in VALID_FEEDS:
        try:
            feed_dir = Path(ARCHIVE_DIR) / feed
            for file_path in list(feed_dir.glob("rec_*.wav")) + list(feed_dir.glob("rec_*.mp3")):
                try:
                    stats["total_disk_usage_bytes"] += file_path.stat().st_size
                except Exception as e:
                    logger.warning("Could not process file %s: %s", file_path.name, e)
        except Exception as e:
            logger.warning("Failed to access directory for %s: %s", feed, e)

    from shared.scanner_db import get_todays_stats
    stats2 = get_todays_stats()
    stats["total_minutes"] = stats2['total_minutes']
    stats["active_feeds"] = stats2['active_feeds']
    stats["total_calls"] = stats2['total_calls']
    stats["total_hooks_today"] = stats2['total_hooks_today']

    size_bytes = stats["total_disk_usage_bytes"]
    if size_bytes < 1024:
        stats["total_disk_usage_readable"] = f"{size_bytes} Bytes"
    elif size_bytes < 1024**2:
        stats["total_disk_usage_readable"] = f"{size_bytes / 1024:.2f} KB"
    elif size_bytes < 1024**3:
        stats["total_disk_usage_readable"] = f"{size_bytes / 1024**2:.2f} MB"
    else:
        stats["total_disk_usage_readable"] = f"{size_bytes / 1024**3:.2f} GB"

    return stats


def _compute_today_counts():
    results = {
        feed_id: {"count": 0, "latest_time": None, "hooks_count": 0}
        for feed_id in VALID_FEEDS
    }

    with get_conn(readonly=True) as conn:
        rows = conn.execute("""
            SELECT
                category,
                COUNT(*) AS count,
                MAX(timestamp) AS latest_time,
                SUM(CASE WHEN hook_request IN (1, '1', TRUE) THEN 1 ELSE 0 END) AS hooks_count
            FROM calls
            WHERE date(timestamp) = ?
            GROUP BY category
        """, (date.today().isoformat(),)).fetchall()

    for row in rows:
        if row["category"] in results:
            results[row["category"]] = {
                "count": row["count"] or 0,
                "latest_time": row["latest_time"],
                "hooks_count": row["hooks_count"] or 0,
            }

    return results


def warm_api_cache():
    latest = _compute_latest()
    home_live_calls = _compute_home_live_calls()
    stats = _compute_stats()
    today_counts = _compute_today_counts()

    _set_cached_response("latest", latest)
    _set_cached_response_redis("latest", latest)
    _set_cached_response("home_live_calls", home_live_calls)
    _set_cached_response_redis("home_live_calls", home_live_calls)
    _set_cached_response("stats", stats)
    _set_cached_response_redis("stats", stats)
    _set_cached_response("today_counts", today_counts)
    _set_cached_response_redis("today_counts", today_counts)

    for feed_id in VALID_FEEDS:
        payload = _compute_archive_calls(feed_id, 0, 10)
        cache_key = _archive_cache_key(feed_id, 0, 10)
        _set_cached_response(cache_key, payload)
        _set_cached_response_redis(cache_key, payload)


def _compute_archive_calls(feed, offset, limit):
    calls = []
    today_str = date.today().isoformat()

    with get_conn(readonly=True) as conn:
        total_row = conn.execute("""
            SELECT COUNT(*) AS total_count
            FROM calls
            WHERE category = ? AND date(timestamp) = ?
        """, (feed, today_str)).fetchone()
        rows = conn.execute("""
            SELECT *
            FROM calls
            WHERE category = ? AND date(timestamp) = ?
            ORDER BY timestamp DESC
            LIMIT ? OFFSET ?
        """, (feed, today_str, limit, offset)).fetchall()

    for row in rows:
        try:
            calls.append(_row_to_call_payload(row, feed_override=feed))
        except Exception as e:
            logger.warning("API failed to load metadata for %s: %s", row["filename"], e)

    return {"calls": calls, "total_count": total_row["total_count"] if total_row else 0}



def load_calls(directory, feed="pd", filter_today=False, limit=None):
    """
    Load calls from a directory, using SQLite for metadata instead of JSON files.
    """
    params = [feed]
    clauses = ["category = ?"]
    sql = "SELECT * FROM calls WHERE {where} ORDER BY timestamp DESC"

    if filter_today:
        clauses.append("date(timestamp) = ?")
        params.append(date.today().isoformat())

    if limit:
        sql += " LIMIT ?"
        params.append(limit)

    with get_conn(readonly=True) as conn:
        rows = conn.execute(sql.format(where=" AND ".join(clauses)), params).fetchall()

    calls = []
    for row in rows:
        try:
            calls.append(_row_to_call_payload(row, feed_override=feed))
        except Exception as e:
            logger.warning("Failed to load metadata for %s: %s", row["filename"], e)

    return calls

def load_archive(directory):
    archive = {}
    for wav in sorted(Path(directory).glob("*.wav"), reverse=True):
        base = wav.stem
        txt = wav.with_suffix(".txt")
        try:
            date_str = base.split("_")[1]
            call_date = datetime.strptime(date_str, "%Y-%m-%d").date()
            day_key = call_date.strftime("%Y-%m-%d")
        except Exception:
            day_key = "unknown"

        timestamp = base.replace("rec_", "").replace("_", " ")
        try:
            parts = base.split("_")
            timestamp_str = f"{parts[1]}_{parts[2]}"
            dt = datetime.strptime(timestamp_str, "%Y-%m-%d_%H-%M-%S")
            timestamp_human = dt.strftime("%b %d, %I:%M %p")
        except Exception:
            timestamp_human = timestamp

        data = {
            "file": wav.name,
            "path": f"/scanner/audio/{wav.name}",
            "transcript": txt.read_text() if txt.exists() else "(no transcript)",
            "timestamp": timestamp,
            "timestamp_human": timestamp_human
        }
        archive.setdefault(day_key, []).append(data)
    return dict(sorted(archive.items(), reverse=True))


@scanner_bp.route("/scanner/segments")
def scanner_segments():
    calls = []
    for wav in sorted(SEGMENT_DIR.glob("*.wav"), reverse=True):
        base = wav.stem
        json_path = wav.with_suffix(".json")
        transcript = "(no transcript)"
        speaker = ""
        timestamp_human = wav.stem.replace("_", " ")

        if json_path.exists():
            try:
                with open(json_path) as f:
                    data = json.load(f)
                transcript = data.get("transcript", transcript)
                speaker = data.get("speaker", "")
                timestamp_human = datetime.fromisoformat(data.get("timestamp")).strftime("%b %d, %I:%M %p")
            except Exception:
                pass

        calls.append({
            "file": wav.name,
            "path": f"/scanner/audio/{wav.name}",
            "transcript": transcript,
            "timestamp_human": timestamp_human,
            "speaker": speaker
        })

    log_activity("page_view", {"page": "segments"})
    return render_template("scanner_segments.html", calls=calls)


@scanner_bp.route("/scanner/view")
def scanner_view():
    """
    Renders the unified scanner page.
    This is the main entry point from your scanner landing page.
    It determines which feed to show based on the '?feed=' URL parameter.
    """
    feed = request.args.get("feed")

    # Validate the feed name to prevent errors
    if not feed or feed not in VALID_FEEDS:
        # Return a 404 Not Found error if the feed is missing or invalid
        return abort(404)

    # Load the initial batch of calls for the requested feed
    calls = load_calls(
        f"{ARCHIVE_DIR}/{feed}", 
        feed=feed, 
        filter_today=True, 
        limit=CALLS_PER_PAGE   # added calls per page limit here
        )    
    # Render the single, unified template with the first page of calls
    log_activity("page_view", {"page": "feed_view", "feed": feed})
    return render_template("scanner_view.html",
                           calls=calls[:CALLS_PER_PAGE],
                           initial_call_count=len(calls) # limit to 10
                           )

@scanner_bp.route("/scanner/api/archive_calls")
def archive_calls_api():
    """
    API endpoint for lazy-loading calls on the scanner_view page.
    This is the SUPER-EFFICIENT version that won't abend.
    """
    feed = request.args.get("feed")
    offset = int(request.args.get("offset", 0))
    limit = int(request.args.get("limit", 10))

    if not feed or feed not in VALID_FEEDS:
        return jsonify({"error": "Invalid or missing feed parameter"}), 400

    try:
        cache_key = _archive_cache_key(feed, offset, limit)
        cached = _get_cached_response_redis(cache_key) or _get_cached_response(cache_key)
        if cached is not None:
            return jsonify(cached)

        payload = _compute_archive_calls(feed, offset, limit)
        if offset == 0 and limit == 10:
            _set_cached_response(cache_key, payload)
            _set_cached_response_redis(cache_key, payload)
        return jsonify(payload)

    except Exception as e:
        logger.exception("archive_calls_api crashed: %s", e)
        return jsonify({"error": str(e)}), 500


@scanner_bp.route("/scanner_pd")
def scanner_pd_api():
    return archive_calls_api("pd")

@scanner_bp.route("/scanner_fire")
def scanner_fire_api():
    return archive_calls_api("fd")

@scanner_bp.route("/scanner_mpd")
def scanner_mpd_api():
    return archive_calls_api("mpd")

@scanner_bp.route("/scanner_sfd")
def scanner_sfd_api():
    return archive_calls_api("sfd")

@scanner_bp.route("/scanner_bpd")
def scanner_bpd_api():
    return archive_calls_api("bpd")

@scanner_bp.route("/scanner_bfd")
def scanner_bfd_api():
    return archive_calls_api("bfd")

@scanner_bp.route("/scanner_blkfd")
def scanner_blkfd_api():
    return archive_calls_api("blkfd")

@scanner_bp.route("/scanner_blkpd")
def scanner_blkpd_api():
    return archive_calls_api("blkpd")


@scanner_bp.route("/scanner_mndfd")
def scanner_mndfd_api():
    return archive_calls_api("mndfd")

@scanner_bp.route("/scanner_mndpd")
def scanner_mndpd_api():
    return archive_calls_api("mndpd")

@scanner_bp.route("/scanner_uptfd")
def scanner_uptfd_api():
    return archive_calls_api("uptfd")
@scanner_bp.route("/scanner_uptpd")
def scanner_uptpd_api():
    return archive_calls_api("uptpd")




@scanner_bp.route("/scanner_milpd")
def scanner_milpd_api():
    return archive_calls_api("milpd")

@scanner_bp.route("/scanner_milfd")
def scanner_milfd_api():
    return archive_calls_api("milfd")

@scanner_bp.route("/scanner_medpd")
def scanner_medpd_api():
    return archive_calls_api("medpd")

@scanner_bp.route("/scanner_medfd")
def scanner_medfd_api():
    return archive_calls_api("medfd")


@scanner_bp.route("/scanner_foxpd")
def scanner_foxpd_api():
    return archive_calls_api("foxpd")

@scanner_bp.route("/scanner_frkpd")
def scanner_frkpd_api():
    return archive_calls_api("frkpd")

@scanner_bp.route("/scanner_frkfd")
def scanner_frkfd_api():
    return archive_calls_api("frkfd")




@scanner_bp.route("/scanner_mfd")
def scanner_mfd_api():
    return archive_calls_api("mfd")

# Backwards-compatible alias in case any old links point here
@scanner_bp.route("/scanner_fd")
def scanner_fd_alias():
    return archive_calls_api("fd")




@scanner_bp.route("/scanner")
def scanner_list():
    log_activity("page_view", {"page": "home"})
    return render_template("scanner.html")


# Accept trailing slash as well so `/scanner/` doesn't 404.
@scanner_bp.route("/scanner/")
def scanner_list_slash():
    return scanner_list()


@scanner_bp.route("/scanner/training")
def scanner_training_info():
    reports = _discover_training_reports()
    selected = request.args.get("report", "").strip()

    allowed = {r["template"] for r in reports}
    if not selected or selected not in allowed:
        selected = reports[0]["template"] if reports else ""

    log_activity("page_view", {"page": "training_info", "report": selected})
    return render_template(
        "scanner_training_info.html",
        reports=reports,
        selected_report=selected,
    )


@scanner_bp.route("/scanner/training/result/<template_name>")
def scanner_training_result(template_name):
    reports = _discover_training_reports()
    allowed = {r["template"] for r in reports}
    if template_name not in allowed:
        return abort(404)
    return render_template(template_name)




@scanner_bp.route("/scanner/archive")
def scanner_archive():
    feed = request.args.get("feed")  # 'pd', 'fd', 'mpd'
    day = request.args.get("day")
    page = int(request.args.get("page", 1))
    json_mode = request.args.get("json") == "1"
    days_back = int(request.args.get("days_back", 30))  # default 30, no hard cap

    cutoff = datetime.now() - timedelta(days=days_back)
    calls_per_page = 10

    # ============================================
    # 1️⃣ QUICK SUMMARY MODE: list days + counts
    # ============================================
    if json_mode and not day:
        params = [cutoff.isoformat(timespec="seconds")]
        clauses = ["timestamp >= ?"]
        if feed:
            clauses.append("category = ?")
            params.append(feed)

        with get_conn(readonly=True) as conn:
            rows = conn.execute(f"""
                SELECT date(timestamp) AS day_key, COUNT(*) AS call_count
                FROM calls
                WHERE {' AND '.join(clauses)}
                GROUP BY date(timestamp)
                ORDER BY day_key DESC
            """, params).fetchall()

        day_counts = {row["day_key"]: row["call_count"] for row in rows if row["day_key"]}

        return jsonify({
            "days": list(day_counts.keys()),
            "call_totals": day_counts
        })

    # ============================================
    # 2️⃣ DETAILED MODE: load calls for a specific day
    # ============================================
    if json_mode and day:
        start = (page - 1) * calls_per_page
        params = [day]
        clauses = ["date(timestamp) = ?"]
        if feed:
            clauses.append("category = ?")
            params.append(feed)

        with get_conn(readonly=True) as conn:
            rows = conn.execute(f"""
                SELECT *
                FROM calls
                WHERE {' AND '.join(clauses)}
                ORDER BY timestamp DESC
                LIMIT ? OFFSET ?
            """, [*params, calls_per_page, start]).fetchall()

        calls = [_row_to_call_payload(row, feed_override=feed or row["category"], timestamp_format="%Y-%m-%d %H-%M-%S") for row in rows]

        return jsonify({"calls": calls, "total": len(calls)})

    # ============================================
    # 3️⃣ Full page render (first load)
    # ============================================
    log_activity("page_view", {"page": "archive", "feed": feed})
    return render_template(
        "scanner_archive.html",
        archive={},  # empty on first render; JS will populate
        call_totals={},
        calls_per_page=calls_per_page,
        feed=feed
    )



@scanner_bp.route("/scanner/audio/<filename>")
def scanner_audio(filename):
    _clean = Path(ARCHIVE_DIR)
    search_paths = [_clean / d for d in [
        "pd", "fd", "mpd", "mfd", "sfd", "bpd", "bfd",
        "mndfd", "mndpd", "uptfd", "uptpd", "blkpd", "blkfd",
        "milpd", "milfd", "medpd", "medfd", "foxpd", "frkpd", "frkfd",
    ]] + [SEGMENT_DIR]

    for path in search_paths:
        file_path = path / filename
        if file_path.exists():
            return send_from_directory(path, filename)

    return "File not found", 404


@scanner_bp.route("/scanner/submit_edit", methods=["POST"])
def submit_edit():
    logger.debug("submit_edit.request")
    data = request.get_json()
    if not data:
        return jsonify({"success": False, "error": "Invalid JSON"}), 400

    raw_filename = data.get("filename")
    if not raw_filename:
        return jsonify({"success": False, "error": "Filename required"}), 400

    filename = secure_filename(raw_filename)
    if not filename.endswith(".wav"):
        return jsonify({"success": False, "error": "Invalid file type"}), 400

    new_transcript = data.get("transcript", "").strip()
    feed = data.get("feed", "pd")

    # Use the new SQLite function
    result = submit_edit_to_sqlite(
        filename=filename,
        feed=feed, 
        new_transcript=new_transcript,
        archive_base=ARCHIVE_DIR,
        review_dir=str(REVIEW_DIR)
    )

    if result['success']:
        logger.info("submit_edit.saved filename=%s feed=%s", filename, feed)
        log_activity("transcript_edit", {"filename": filename, "feed": feed})
        return jsonify({"success": True})
    else:
        return jsonify({"success": False, "error": result['error']}), 500


@scanner_bp.route("/scanner/approve_transcript", methods=["POST"])
def approve_transcript():
    """Mark a transcript as 'looks good' by copying transcript → edited_transcript.
    Passing approve=false clears the edited_transcript (un-approve).
    This is the primary way to flag calls as good training data.
    """
    data = request.get_json()
    if not data:
        return jsonify({"success": False, "error": "Invalid JSON"}), 400

    raw_filename = data.get("filename")
    if not raw_filename:
        return jsonify({"success": False, "error": "Filename required"}), 400

    filename = secure_filename(raw_filename)
    if not filename.endswith(".wav"):
        return jsonify({"success": False, "error": "Invalid file type"}), 400

    approve = data.get("approve", True)
    feed = data.get("feed", "pd")

    if approve:
        # Read the current transcript from DB and copy it to edited_transcript
        with get_conn(readonly=True) as conn:
            row = conn.execute(
                "SELECT transcript FROM calls WHERE filename = ? LIMIT 1", (filename,)
            ).fetchone()
        if not row:
            return jsonify({"success": False, "error": "Call not found"}), 404

        transcript = (row["transcript"] or "").strip()
        if not transcript:
            return jsonify({"success": False, "error": "No transcript to approve"}), 400

        result = submit_edit_to_sqlite(
            filename=filename,
            feed=feed,
            new_transcript=transcript,
            archive_base=ARCHIVE_DIR,
            review_dir=str(REVIEW_DIR),
        )
        if result["success"]:
            logger.info("approve_transcript.approved filename=%s feed=%s", filename, feed)
            log_activity("transcript_approved", {"filename": filename, "feed": feed})
            return jsonify({"success": True, "approved": True})
        else:
            return jsonify({"success": False, "error": result["error"]}), 500
    else:
        # Un-approve: clear edited_transcript
        try:
            with get_conn() as conn:
                conn.execute(
                    "UPDATE calls SET edited_transcript = NULL WHERE filename = ?",
                    (filename,)
                )
            logger.info("approve_transcript.unapproved filename=%s feed=%s", filename, feed)
            log_activity("transcript_unapproved", {"filename": filename, "feed": feed})
            return jsonify({"success": True, "approved": False})
        except Exception as e:
            logger.error("approve_transcript.unapprove_error filename=%s error=%s", filename, e)
            return jsonify({"success": False, "error": str(e)}), 500


@scanner_bp.route("/scanner/save_for_eval", methods=["POST"])
def save_for_eval_route():
    """Toggle save_for_eval flag on a call. Body: {filename, feed, save: bool}"""
    data = request.get_json()
    if not data:
        return jsonify({"success": False, "error": "Invalid JSON"}), 400

    raw_filename = data.get("filename")
    if not raw_filename:
        return jsonify({"success": False, "error": "Filename required"}), 400

    filename = secure_filename(raw_filename)
    if not filename.endswith(".wav"):
        return jsonify({"success": False, "error": "Invalid file type"}), 400

    save = bool(data.get("save", True))
    feed = data.get("feed", "pd")

    result = set_save_for_eval(filename, save)
    if result["success"]:
        logger.info("save_for_eval filename=%s feed=%s value=%s", filename, feed, save)
        log_activity("save_for_eval", {"filename": filename, "feed": feed, "save": save})
    return jsonify(result), 200 if result["success"] else 500


@scanner_bp.route("/scanner/freeze_for_testing", methods=["POST"])
def freeze_for_testing_route():
    """Toggle freeze_for_testing flag on a call. Body: {filename, feed, freeze: bool}"""
    data = request.get_json()
    if not data:
        return jsonify({"success": False, "error": "Invalid JSON"}), 400

    raw_filename = data.get("filename")
    if not raw_filename:
        return jsonify({"success": False, "error": "Filename required"}), 400

    filename = secure_filename(raw_filename)
    if not filename.endswith(".wav"):
        return jsonify({"success": False, "error": "Invalid file type"}), 400

    freeze = bool(data.get("freeze", True))
    feed = data.get("feed", "pd")

    result = set_freeze_for_testing(filename, freeze)
    if result["success"]:
        logger.info("freeze_for_testing filename=%s feed=%s value=%s", filename, feed, freeze)
        log_activity("freeze_for_testing", {"filename": filename, "feed": feed, "freeze": freeze})
    return jsonify(result), 200 if result["success"] else 500


# ── Review Edited Calls page ──────────────────────────────────────────────────

@scanner_bp.route("/scanner/review")
def review_edited_calls():
    return render_template("scanner_review.html")


@scanner_bp.route("/scanner/api/reviewed_calls")
def reviewed_calls_api():
    try:
        offset = int(request.args.get("offset", 0))
        limit = int(request.args.get("limit", 20))
        since = request.args.get("since", "2026-01-01")
        limit = min(limit, 50)  # cap at 50
    except ValueError:
        return jsonify({"error": "Invalid parameters"}), 400

    rows = fetch_reviewed_edited_calls(offset=offset, limit=limit, since=since)

    calls = []
    for row in rows:
        extra = {}
        if row.get("extra"):
            try:
                extra = json.loads(row["extra"])
            except Exception:
                extra = {}

        feed = row.get("category") or ""
        ts_raw = row.get("timestamp") or ""
        ts = _safe_fromisoformat(ts_raw)
        timestamp_human = ts.strftime("%b %d, %I:%M %p") if ts else ts_raw

        calls.append({
            "file": row["filename"],
            "path": f"/scanner/audio/{row['filename']}",
            "feed": feed,
            "transcript": row.get("transcript") or "",
            "edited_transcript": row.get("edited_transcript") or "",
            "save_for_eval": bool(row.get("save_for_eval")),
            "freeze_for_testing": bool(row.get("freeze_for_testing")),
            "duration": row.get("duration") or 0,
            "timestamp": ts_raw,
            "timestamp_human": timestamp_human,
            "derived_address": row.get("derived_address") or "",
            "address_confidence": row.get("address_confidence") or "none",
            "transcription_model": row.get("transcription_model") or "",
            "enhanced_transcript": extra.get("enhanced_transcript", ""),
        })

    return jsonify({
        "calls": calls,
        "offset": offset,
        "limit": limit,
        "returned": len(calls),
        "has_more": len(calls) == limit,
    })


@scanner_bp.route("/scanner/submit_vote", methods=["POST"])
def submit_vote():
    data = request.get_json()
    if not data:
        return jsonify({"success": False, "error": "Invalid JSON"}), 400

    filename = data.get("filename")
    model_name = data.get("model")
    
    if not filename or not model_name:
        return jsonify({"success": False, "error": "Filename and model required"}), 400

    try:
        with get_conn() as conn:
            row = conn.execute("SELECT extra FROM calls WHERE filename = ?", (filename,)).fetchone()
            if not row:
                return jsonify({"success": False, "error": "Call not found"}), 404
                
            extra = row["extra"]
            if isinstance(extra, str):
                try:
                    extra_data = json.loads(extra)
                except json.JSONDecodeError:
                    extra_data = {}
            else:
                extra_data = extra or {}
                
            extra_data["best_transcript_vote"] = model_name
            
            conn.execute("UPDATE calls SET extra = ? WHERE filename = ?", (json.dumps(extra_data), filename))
            conn.commit()
            
        log_activity("transcript_vote", {"filename": filename, "model": model_name})
        return jsonify({"success": True})
    except Exception as e:
        logger.error(f"Failed to submit vote for {filename}: {e}")
        return jsonify({"success": False, "error": str(e)}), 500

@scanner_bp.route('/scanner/_heartbeat', methods=['POST'])
def scanner_heartbeat():
    """Receive periodic heartbeats from clients to mark them active."""
    data = request.get_json(silent=True) or {}
    client_id = data.get('client_id') or str(uuid.uuid4())
    page = data.get('page', '')
    ua = request.headers.get('User-Agent', '')
    now = time.time()
    with ACTIVE_LOCK:
        ACTIVE_USERS[client_id] = {
            'last_seen': now,
            'ip': request.remote_addr,
            'ua': ua,
            'page': page,
        }
    return jsonify({'success': True, 'client_id': client_id})



@scanner_bp.route('/scanner/login')
def scanner_login():
    """Redirect to the external login process (FastAPI Cognito flow).

    The external login process should handle the auth code flow and redirect
    back to your app's redirect URI. The default `LOGIN_PROCESS_URL` points to
    the FastAPI login endpoint in your other project. Configure via
    environment variable `LOGIN_PROCESS_URL`.
    """
    return redirect(LOGIN_PROCESS_URL)


@scanner_bp.route('/scanner/admin/active')
def scanner_active():
    """Return currently active clients seen within ACTIVE_TIMEOUT seconds."""
    cutoff = time.time() - ACTIVE_TIMEOUT
    with ACTIVE_LOCK:
        # remove stale entries to keep memory small
        stale = [k for k, v in ACTIVE_USERS.items() if v['last_seen'] < cutoff]
        for k in stale:
            del ACTIVE_USERS[k]
        active = [
            {
                'client_id': k,
                'ip': v['ip'],
                'ua': v['ua'],
                'page': v.get('page', ''),
                'last_seen': v['last_seen']
            }
            for k, v in ACTIVE_USERS.items()
        ]
    return jsonify({'active_count': len(active), 'active': active})

@scanner_bp.route('/scanner/api/logged_in_users')
def logged_in_users_api():
    """Alias for /scanner/admin/active for header consistency."""
    return scanner_active()


@scanner_bp.route("/scanner/api/new_counts")
def new_counts():
    """
    Efficiently count new calls for each feed since a given timestamp.
    Expects query params like ?pd_since=ISO_TIMESTAMP&fd_since=...
    """
    counts = {}

    with get_conn(readonly=True) as conn:
        for feed_id in VALID_FEEDS:
            since_str = request.args.get(f"{feed_id}_since")
            try:
                since_dt = _safe_fromisoformat(since_str)
                if not since_dt:
                    counts[feed_id] = 0
                    continue

                row = conn.execute("""
                    SELECT COUNT(*) AS new_call_count
                    FROM calls
                    WHERE category = ? AND timestamp > ?
                """, (feed_id, since_dt.isoformat(timespec="seconds"))).fetchone()
                counts[feed_id] = row["new_call_count"] if row else 0
            except Exception:
                counts[feed_id] = 0

    return jsonify(counts)

@scanner_bp.route("/api/pd_heatmap")
def pd_heatmap():
    now = datetime.now()
    start = now - timedelta(days=6)
    heatmap = defaultdict(lambda: [0] * 24)

    with get_conn(readonly=True) as conn:
        rows = conn.execute("""
            SELECT timestamp
            FROM calls
            WHERE category = 'pd' AND timestamp >= ?
            ORDER BY timestamp ASC
        """, (start.isoformat(timespec="seconds"),)).fetchall()

    for row in rows:
        dt = _safe_fromisoformat(row["timestamp"])
        if not dt:
            continue
        date_key = dt.strftime("%Y-%m-%d")
        heatmap[date_key][dt.hour] += 1

    sorted_days = sorted(heatmap.keys())
    matrix = [heatmap[day] for day in sorted_days]

    return jsonify({"days": sorted_days, "data": matrix})

@scanner_bp.route("/scanner/submit_segment_label", methods=["POST"])
def submit_segment_label():
    data = request.get_json()
    if not data:
        return jsonify({"success": False, "error": "Invalid JSON"}), 400

    filename = data.get("filename")
    speaker = data.get("speaker")
    label = data.get("label", "").strip()

    if not filename or not speaker:
        return jsonify({"success": False, "error": "Missing required fields"}), 400

    json_path = SEGMENT_DIR / filename
    if json_path.suffix != ".wav":
        return jsonify({"success": False, "error": "Invalid file type"}), 400

    json_file = json_path.with_suffix(".json")
    if not json_file.exists():
        return jsonify({"success": False, "error": "Metadata JSON not found"}), 404

    try:
        with open(json_file) as f:
            meta = json.load(f)

        meta["speaker_role"] = speaker  # e.g., "dispatcher" or "officer"
        if label:
            meta["speaker_label"] = label  # e.g., "303", "Control", etc.

        with open(json_file, "w") as f:
            json.dump(meta, f, indent=2)

        log_activity("segment_label", {"filename": filename, "speaker": speaker, "label": label})
        return jsonify({"success": True})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500



# In your Flask Python file

    


@scanner_bp.route("/scanner/api/latest")
def scanner_latest():
    """Return the latest transcript for each feed (pd, fd, mpd, etc.)."""
    from flask import jsonify

    cached = _get_cached_response_redis("latest") or _get_cached_response("latest")
    if cached is not None:
        return jsonify(cached)

    latest = _compute_latest()
    _set_cached_response("latest", latest)
    _set_cached_response_redis("latest", latest)
    return jsonify(latest)


@scanner_bp.route("/scanner/api/home_live_calls")
def scanner_home_live_calls():
    cached = _get_cached_response_redis("home_live_calls") or _get_cached_response("home_live_calls")
    if cached is not None:
        return jsonify(cached)

    payload = _compute_home_live_calls()
    _set_cached_response("home_live_calls", payload)
    _set_cached_response_redis("home_live_calls", payload)
    return jsonify(payload)

@scanner_bp.route('/scanner/api/user_count')
def get_user_count():
    """
    Returns the current number of connected Socket.IO clients.
    """
    try:
        # This dictionary holds all active Engine.IO clients
        client_count = len(socketio.server.eio.clients)
        
        return jsonify({
            "connected_users": client_count
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500



@scanner_bp.route("/scanner/api/stats")
def scanner_stats():
    """
    Provides statistics about the scanner audio archive, including
    the number of calls recorded today, the total number of calls,
    and the total size of all recorded files on disk.
    """
    cached = _get_cached_response_redis("stats") or _get_cached_response("stats")
    if cached is not None:
        return jsonify(cached)
    stats = _compute_stats()
    logger.debug("scanner_stats.computed")
    _set_cached_response("stats", stats)
    _set_cached_response_redis("stats", stats)
    return jsonify(stats)


@scanner_bp.route("/scanner/increment_play", methods=["POST"])
def increment_play():
    logger.debug("increment_play.request")
    data = request.get_json()
    filename = data.get("filename")
    feed = data.get("feed")

    if not filename or not feed:
        return jsonify({"error": "Missing filename or feed"}), 400

    try:
        increment_play_count(filename)
        with get_conn(readonly=True) as conn:
            row = conn.execute("SELECT play_count FROM calls WHERE filename = ?", (filename,)).fetchone()
            play_count = row["play_count"] if row else 0

        redis_client = _get_redis_client()
        if redis_client:
            redis_client.set(f"scanner:play_count:{filename}", play_count)
    except Exception as e:
        return jsonify({"error": f"Failed to update: {e}"}), 500

    logger.info("increment_play.updated filename=%s feed=%s play_count=%s", filename, feed, play_count)
    log_activity("play_audio", {"filename": filename, "feed": feed})
    return jsonify({"play_count": play_count})



@scanner_bp.route("/scanner/api/today_counts")

def today_counts():
    """
    Get the total number of calls for the current day for each feed.
    The day is determined by the server's local time.
    """
    cached = _get_cached_response_redis("today_counts") or _get_cached_response("today_counts")
    if cached is not None:
        return jsonify(cached)

    results = _compute_today_counts()
    _set_cached_response("today_counts", results)
    _set_cached_response_redis("today_counts", results)
    return jsonify(results)


@scanner_bp.route("/scanner/api/call_activity")
def scanner_call_activity():
    """
    Returns aggregated call activity stats (hourly, per-minute, per-channel)
    based on the stats.log file in the archive logs directory.
    """
    import csv
    from collections import Counter
    from datetime import datetime

    _archive_base = os.environ.get("ARCHIVE_BASE", "/home/ned/data/scanner_calls/scanner_archive")
    
    STATS_LOG = Path(_archive_base) / "logs" / "stats.log"
    data = {
        "calls_by_hour": {},
        "calls_by_minute": {},
        "calls_by_channel": {},
        "recent_calls": []
    }

    if not STATS_LOG.exists():
        return jsonify({"error": "stats.log not found"}), 404

    try:
        with STATS_LOG.open("r", encoding="utf-8") as f:
            reader = csv.reader(f)
            rows = [row for row in reader if len(row) >= 4]
    except Exception as e:
        return jsonify({"error": f"Failed to read stats.log: {e}"}), 500

    if not rows:
        return jsonify({"message": "No stats available"}), 200

    # Each row: channel,hour,minute,timestamp_with_ms
    channels = [r[0] for r in rows]
    hours = [r[1] for r in rows]
    minutes = [r[2] for r in rows]

    # Count frequencies
    data["calls_by_channel"] = dict(Counter(channels))
    data["calls_by_hour"] = dict(Counter(hours))
    data["calls_by_minute"] = dict(Counter(minutes))

    # Recent calls (last 20)
    recent = rows[-20:]
    data["recent_calls"] = [
        {"channel": r[0], "timestamp": r[3]} for r in recent
    ]

    # Optional: compute overall totals
    data["total_calls_logged"] = len(rows)
    data["last_updated"] = datetime.now().isoformat(timespec="seconds")

    return jsonify(data)


@scanner_bp.route("/scanner/town")
def scanner_town():
    """
    Renders the town-specific landing page.
    The JavaScript in 'town.html' will read the
    '?town=...' parameter and build the page dynamically.
    """
    return render_template("town.html")


@scanner_bp.route("/scanner/submit_intent", methods=["POST"])
def submit_intent():
    """
    Allows a human user to classify a call with contextual metadata for training.
    Captures intent, officer tag, road/street, and optional notes.
    Saves updated metadata JSON for ML training and updates the clean source file
    to indicate this call has been labeled.
    """
    logger.debug("submit_intent.request")
    data = request.get_json()
    if not data:
        return jsonify({"success": False, "error": "Invalid JSON"}), 400

    raw_filename = data.get("filename")
    if not raw_filename:
        return jsonify({"success": False, "error": "Filename required"}), 400

    filename = secure_filename(raw_filename)
    if not filename.endswith(".wav"):
        return jsonify({"success": False, "error": "Invalid file type"}), 400

    # ────────────────────────────────
    # Extract classification details
    # ────────────────────────────────
    feed = data.get("feed", "pd")
    intents = data.get("intents", [])
    dispositions = data.get("dispositions", [])
    officer_tag = data.get("officer", "").strip() or None
    road = data.get("road", "").strip() or None
    notes = data.get("notes", "").strip() or None

    if not isinstance(intents, list):
        return jsonify({"success": False, "error": "Intents must be a list"}), 400

    if not isinstance(dispositions, list):
        return jsonify({"success": False, "error": "Dispositions must be a list"}), 400

    src_dir = Path(ARCHIVE_DIR) / feed
    src_wav = src_dir / filename
    src_json = src_wav.with_suffix(".json")

    if not src_wav.exists() or not src_json.exists():
        return jsonify({"success": False, "error": "Source file missing"}), 404

    try:
        with open(src_json) as f:
            meta = json.load(f)

        # ────────────────────────────────
        # Add or update manual classification block
        # ────────────────────────────────
        now = datetime.now().isoformat()
        meta.setdefault("classification", {})
        meta["classification"].update({
            "intents": intents,
            "dispositions": dispositions,
            "officer": officer_tag,
            "road": road,
            "notes": notes,
            "labeled_by": "manual",
            "labeled_at": now
        })

        # ────────────────────────────────
        # Mark this clean file as labeled for reference
        # ────────────────────────────────
        meta["intent_labeled"] = True
        meta["intent_labeled_at"] = now

        # Save back to CLEAN
        with open(src_json, "w") as f:
            json.dump(meta, f, indent=2)

        # ────────────────────────────────
        # Create labeled copy for model training
        # ────────────────────────────────
        _archive_base = os.environ.get("ARCHIVE_BASE", "/home/ned/data/scanner_calls/scanner_archive")
        INTENT_DIR = Path(_archive_base) / "review_intent"
        INTENT_DIR.mkdir(parents=True, exist_ok=True)

        dst_wav = INTENT_DIR / src_wav.name
        dst_json = INTENT_DIR / src_json.name

        shutil.copy2(src_wav, dst_wav)
        with open(dst_json, "w") as f:
            json.dump(meta, f, indent=2)

        logger.info("submit_intent.saved filename=%s feed=%s intents=%s dispositions=%s", filename, feed, len(intents), len(dispositions))
        log_activity("submit_intent", {"filename": filename, "feed": feed, "intents": intents, "dispositions": dispositions})
        return jsonify({
            "success": True,
            "message": f"Intent metadata updated for {filename}",
            "saved_to": str(dst_json)
        })

    except Exception as e:
        logger.exception("submit_intent error: %s", e)
        return jsonify({"success": False, "error": str(e)}), 500


@scanner_bp.route("/scanner/api/call_coords")
def call_coords():
    """
    Returns lat/lng points for calls that have derived coordinates.
    Query params:
      range = day | week | month | all  (default: week)
      town  = town name, or omit / 'all' for every town
    """
    from shared.scanner_db import get_conn

    range_param = request.args.get("range", "week").lower()
    town_param  = request.args.get("town", "").strip().lower()

    cutoffs = {
        "day":   timedelta(days=1),
        "week":  timedelta(days=7),
        "month": timedelta(days=30),
    }

    try:
        with get_conn(readonly=True) as conn:
            params = []
            clauses = [
                "derived_lat  IS NOT NULL",
                "derived_lng  IS NOT NULL",
                "derived_lat  != 0",
                "derived_lng  != 0",
            ]

            if range_param in cutoffs:
                cutoff_str = (datetime.now() - cutoffs[range_param]).isoformat(timespec="seconds")
                clauses.append("timestamp >= ?")
                params.append(cutoff_str)

            if town_param and town_param != "all":
                clauses.append("UPPER(derived_town) = UPPER(?)")
                params.append(town_param)

            sql = f"""
                SELECT derived_lat AS lat, derived_lng AS lng, derived_town AS town
                FROM calls
                WHERE {' AND '.join(clauses)}
                ORDER BY timestamp DESC
                LIMIT 50000
            """
            rows = conn.execute(sql, params).fetchall()

        points = [{"lat": r["lat"], "lng": r["lng"]} for r in rows]
        return jsonify({"points": points, "count": len(points)})
    except Exception as e:
        logger.exception("call_coords error: %s", e)
        return jsonify({"points": [], "count": 0, "error": str(e)}), 500


@scanner_bp.route("/scanner/heatmap")
def scanner_heatmap():
    return render_template("scanner_heatmap.html", google_maps_api_key=GOOGLE_MAPS_API_KEY)


@scanner_bp.route("/scanner/api/geo_towns")
def geo_towns():
    """
    Returns per-town geographic data derived from the addresses table.
    Each town gets a centroid lat/lng, street count, and recent call count.
    """
    from shared.scanner_db import get_conn
    try:
        with get_conn(readonly=True) as conn:
            rows = conn.execute("""
                SELECT
                    town,
                    COUNT(DISTINCT street_name) AS street_count,
                    AVG(latitude)               AS lat,
                    AVG(longitude)              AS lng
                FROM addresses
                WHERE latitude  IS NOT NULL
                  AND longitude IS NOT NULL
                  AND town IS NOT NULL
                  AND town != ''
                GROUP BY town
                ORDER BY town
            """).fetchall()

            call_rows = conn.execute("""
                SELECT derived_town, COUNT(*) AS call_count
                FROM calls
                WHERE derived_town IS NOT NULL AND derived_town != ''
                GROUP BY derived_town
            """).fetchall()

        call_counts = {r["derived_town"].upper(): r["call_count"] for r in call_rows}

        towns = []
        for r in rows:
            towns.append({
                "name": r["town"],
                "lat": round(r["lat"], 6),
                "lng": round(r["lng"], 6),
                "street_count": r["street_count"],
                "call_count": call_counts.get(r["town"].upper(), 0),
            })

        return jsonify({"towns": towns})
    except Exception as e:
        logger.exception("geo_towns error: %s", e)
        return jsonify({"towns": [], "error": str(e)}), 500


