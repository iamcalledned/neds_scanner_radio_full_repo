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
import requests, datetime
from datetime import datetime, date, timedelta
import fcntl
import redis
import logging

from shared.scanner_db import read_metadata_from_sqlite, submit_edit_to_sqlite
from user_logger import log_activity



scanner_bp = Blueprint("scanner", __name__)
logger = logging.getLogger("scanner_web.routes_scanner")
LOGIN_PROCESS_URL = os.environ.get('LOGIN_PROCESS_URL', 'http://127.0.0.1:8010/api/login')
LOGIN_API_URL = os.environ.get('LOGIN_API_URL', 'http://127.0.0.1:8010')
ARCHIVE_DIR = os.environ.get("ARCHIVE_DIR", os.path.join(os.environ.get("ARCHIVE_BASE", "/home/ned/data/scanner_calls/scanner_archive"), "clean"))
PD_DIR = Path(os.path.join(ARCHIVE_DIR, "pd"))
REVIEW_DIR = Path(os.environ.get("REVIEW_DIR", os.path.join(os.environ.get("ARCHIVE_BASE", "/home/ned/data/scanner_calls/scanner_archive"), "review")))
SEGMENT_DIR = Path(os.environ.get("SEGMENT_DIR", os.path.join(os.environ.get("ARCHIVE_BASE", "/home/ned/data/scanner_calls/scanner_archive"), "segmentation/processed")))
CALLS_PER_PAGE = 10
REDIS_URL = os.environ.get('REDIS_URL', 'redis://127.0.0.1:6379/0')


VALID_FEEDS = {"pd", "fd", "mpd", "mfd", "sfd", "bpd", "bfd", "mndfd", "mndpd", "blkfd", "blkpd", "uptfd", "uptpd", "frkpd", "frkfd"}



# Simple in-memory active user registry. Key: client_id -> {last_seen, ip, ua, page}
ACTIVE_USERS = {}
ACTIVE_LOCK = threading.Lock()
ACTIVE_TIMEOUT = 120  # seconds considered "active"

API_CACHE = {}
API_CACHE_LOCK = threading.Lock()
API_CACHE_TTL = {
    "latest": 10,
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
    from pathlib import Path
    import json as json_lib

    feeds = {
        "pd": Path(f"{ARCHIVE_DIR}/pd"),
        "fd": Path(f"{ARCHIVE_DIR}/fd"),
        "mpd": Path(f"{ARCHIVE_DIR}/mpd"),
        "mfd": Path(f"{ARCHIVE_DIR}/mfd"),
        "sfd": Path(f"{ARCHIVE_DIR}/sfd"),
        "bpd": Path(f"{ARCHIVE_DIR}/bpd"),
        "bfd": Path(f"{ARCHIVE_DIR}/bfd"),
        "mndfd": Path(f"{ARCHIVE_DIR}/mndfd"),
        "mndpd": Path(f"{ARCHIVE_DIR}/mndpd"),
        "uptfd": Path(f"{ARCHIVE_DIR}/uptfd"),
        "uptpd": Path(f"{ARCHIVE_DIR}/uptpd"),
        "blkpd": Path(f"{ARCHIVE_DIR}/blkpd"),
        "blkfd": Path(f"{ARCHIVE_DIR}/blkfd"),
        "milpd": Path(f"{ARCHIVE_DIR}/milpd"),
        "milfd": Path(f"{ARCHIVE_DIR}/milfd"),
        "medpd": Path(f"{ARCHIVE_DIR}/medpd"),
        "medfd": Path(f"{ARCHIVE_DIR}/medfd"),
        "foxpd": Path(f"{ARCHIVE_DIR}/foxpd"),
        "frkpd": Path(f"{ARCHIVE_DIR}/frkpd"),
        "frkfd": Path(f"{ARCHIVE_DIR}/frkfd"),
    }

    latest = {}
    for key, path in feeds.items():
        try:
            files = sorted(path.glob("rec_*.json"), reverse=True)
            if not files:
                latest[key] = None
                continue
            latest_file = files[0]
            with open(latest_file, "r") as f:
                data = json_lib.load(f)
            transcript = (
                data.get("enhanced_transcript")
                or data.get("edited_transcript")
                or data.get("transcript")
            )
            latest[key] = {
                "file": latest_file.name,
                "transcript": transcript.strip()[:300] if transcript else None,
            }
        except Exception as e:
            logger.warning("scanner_latest failed for %s: %s", key, e)
            latest[key] = None

    return latest


def _compute_stats():
    feeds = {
        "pd": Path(f"{ARCHIVE_DIR}/pd"),
        "fd": Path(f"{ARCHIVE_DIR}/fd"),
        "mpd": Path(f"{ARCHIVE_DIR}/mpd"),
        "mfd": Path(f"{ARCHIVE_DIR}/mfd"),
        "sfd": Path(f"{ARCHIVE_DIR}/sfd"),
        "bpd": Path(f"{ARCHIVE_DIR}/bpd"),
        "bfd": Path(f"{ARCHIVE_DIR}/bfd"),
        "mndfd": Path(f"{ARCHIVE_DIR}/mndfd"),
        "mndpd": Path(f"{ARCHIVE_DIR}/mndpd"),
        "uptfd": Path(f"{ARCHIVE_DIR}/uptfd"),
        "uptpd": Path(f"{ARCHIVE_DIR}/uptpd"),
        "blkpd": Path(f"{ARCHIVE_DIR}/blkpd"),
        "blkfd": Path(f"{ARCHIVE_DIR}/blkfd"),
        "milpd": Path(f"{ARCHIVE_DIR}/milpd"),
        "milfd": Path(f"{ARCHIVE_DIR}/milfd"),
        "medpd": Path(f"{ARCHIVE_DIR}/medpd"),
        "medfd": Path(f"{ARCHIVE_DIR}/medfd"),
        "foxpd": Path(f"{ARCHIVE_DIR}/foxpd"),
        "frkpd": Path(f"{ARCHIVE_DIR}/frkpd"),
        "frkfd": Path(f"{ARCHIVE_DIR}/frkfd"),
    }

    stats = {
        "total_calls_today": 0,
        "total_calls": 0,
        "total_calls_all_time": 0,
        "total_disk_usage_bytes": 0,
        "total_disk_usage_readable": "",
        "total_minutes": 0,
        "active_feeds": 0
    }

    today_date = date.today()
    all_files = []

    for key, path in feeds.items():
        try:
            files_in_feed = list(path.glob("rec_*.wav")) + list(path.glob("rec_*.mp3"))
            all_files.extend(files_in_feed)
        except Exception as e:
            logger.warning("Failed to access directory for %s: %s", key, e)
            continue

    for file_path in all_files:
        try:
            file_stat = file_path.stat()
            file_mtime = date.fromtimestamp(file_stat.st_mtime)

            if file_mtime == today_date:
                stats["total_calls_today"] += 1

            stats["total_disk_usage_bytes"] += file_stat.st_size
        except Exception as e:
            logger.warning("Could not process file %s: %s", file_path.name, e)

    stats["total_calls_all_time"] = len(all_files)

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
    feeds = {
        "pd": PD_DIR,
        "fd": Path(f"{ARCHIVE_DIR}/fd"),
        "mpd": Path(f"{ARCHIVE_DIR}/mpd"),
        "mfd": Path(f"{ARCHIVE_DIR}/mfd"),
        "sfd": Path(f"{ARCHIVE_DIR}/sfd"),
        "bpd": Path(f"{ARCHIVE_DIR}/bpd"),
        "bfd": Path(f"{ARCHIVE_DIR}/bfd"),
        "mndfd": Path(f"{ARCHIVE_DIR}/mndfd"),
        "mndpd": Path(f"{ARCHIVE_DIR}/mndpd"),
        "uptfd": Path(f"{ARCHIVE_DIR}/uptfd"),
        "uptpd": Path(f"{ARCHIVE_DIR}/uptpd"),
        "blkpd": Path(f"{ARCHIVE_DIR}/blkpd"),
        "blkfd": Path(f"{ARCHIVE_DIR}/blkfd"),
        "milpd": Path(f"{ARCHIVE_DIR}/milpd"),
        "milfd": Path(f"{ARCHIVE_DIR}/milfd"),
        "medpd": Path(f"{ARCHIVE_DIR}/medpd"),
        "medfd": Path(f"{ARCHIVE_DIR}/medfd"),
        "foxpd": Path(f"{ARCHIVE_DIR}/foxpd"),
        "frkpd": Path(f"{ARCHIVE_DIR}/frkpd"),
        "frkfd": Path(f"{ARCHIVE_DIR}/frkfd"),
    }
    results = {}
    today_start = datetime.combine(date.today(), datetime.min.time())

    for feed_id, feed_path in feeds.items():
        daily_call_count = 0
        latest_time = None

        if not feed_path.exists():
            results[feed_id] = {"count": 0, "latest_time": None}
            continue

        wav_files = list(feed_path.glob("*.wav"))
        for wav_file in wav_files:
            try:
                filename_parts = wav_file.stem.split('_')
                timestamp_str_to_parse = f"{filename_parts[1]}_{filename_parts[2]}"
                file_dt = datetime.strptime(timestamp_str_to_parse, "%Y-%m-%d_%H-%M-%S")

                if file_dt >= today_start:
                    daily_call_count += 1
                    if latest_time is None or file_dt > latest_time:
                        latest_time = file_dt
            except (ValueError, IndexError):
                continue

        results[feed_id] = {
            "count": daily_call_count,
            "latest_time": latest_time.isoformat() if latest_time else None,
            "hooks_count": 0
        }

    # Overlay per-feed hook counts from DB (category col = feed code: pd, mpd, bpd, …)
    try:
        from shared.scanner_db import get_todays_hook_counts_by_feed
        hook_counts = get_todays_hook_counts_by_feed()
        for feed_id, count in hook_counts.items():
            if feed_id in results:
                results[feed_id]["hooks_count"] = count
    except Exception as e:
        logger.warning("Failed to load hook counts: %s", e)

    return results


def warm_api_cache():
    latest = _compute_latest()
    stats = _compute_stats()
    today_counts = _compute_today_counts()

    _set_cached_response("latest", latest)
    _set_cached_response_redis("latest", latest)
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
    today = date.today()
    directory = Path(f"{ARCHIVE_DIR}/{feed}")
    calls = []

    today_str = today.strftime("%Y-%m-%d")
    pattern = f"rec_{today_str}_*.wav"
    todays_wav_paths = sorted(directory.glob(pattern), reverse=True)
    total_count = len(todays_wav_paths)
    paths_to_load = todays_wav_paths[offset : offset + limit]

    r = redis.from_url(REDIS_URL, decode_responses=True)

    for wav in paths_to_load:
        base = wav.stem
        wav_path_str = str(wav.absolute())

        try:
            metadata = read_metadata_from_sqlite(wav_path_str, r)
            parts = base.split("_")
            timestamp_str = f"{parts[1]}_{parts[2]}"
            dt = datetime.strptime(timestamp_str, "%Y-%m-%d_%H-%M-%S")
            timestamp_human = dt.strftime("%b %d, %I:%M %p")

            transcript = metadata.get("transcript", "(no transcript)")
            edited_transcript = metadata.get("edited_transcript", "")
            enhanced_transcript = metadata.get("enhanced_transcript", "")
            edit_pending = metadata.get("edit_pending", False)

            if metadata.get("edited") and edited_transcript:
                transcript = edited_transcript
                edit_pending = False
            elif edited_transcript:
                transcript = edited_transcript
                edit_pending = True

            calls.append({
                "file": wav.name,
                "path": f"/scanner/audio/{wav.name}",
                "transcript": transcript,
                "edited_transcript": edited_transcript,
                "enhanced_transcript": enhanced_transcript,
                "edit_pending": edit_pending,
                "timestamp": base.replace("rec_", "").replace("_", " "),
                "timestamp_human": timestamp_human,
                "feed": feed,
                "metadata": metadata
            })
        except Exception as e:
            logger.warning("API failed to load metadata for %s: %s", base, e)

    return {"calls": calls, "total_count": total_count}



def load_calls(directory, feed="pd", filter_today=False, limit=None):
    """
    Load calls from a directory, using SQLite for metadata instead of JSON files.
    """
    calls = []
    today = date.today()

    # Get Redis connection from the app context
    r = redis.from_url(REDIS_URL, decode_responses=True)

    for wav in sorted(Path(directory).glob("*.wav"), reverse=True):
        base = wav.stem
        wav_path = str(wav.absolute())  # Full path for SQLite lookup

        try:
            date_str = base.split("_")[1]
            call_date = datetime.strptime(date_str, "%Y-%m-%d").date()
        except Exception:
            call_date = None

        if filter_today and call_date != today:
            continue

        timestamp = base.replace("rec_", "").replace("_", " ")
        try:
            dt = datetime.strptime(base.replace("rec_", ""), "%Y-%m-%d_%H-%M-%S")
            timestamp_human = dt.strftime("%b %d, %I:%M %p")
        except Exception:
            timestamp_human = timestamp

        try:
            # Get metadata from SQLite instead of JSON file
            metadata = read_metadata_from_sqlite(wav_path, r)
            parts = base.split("_")
            timestamp_str = f"{parts[1]}_{parts[2]}"
            dt = datetime.strptime(timestamp_str, "%Y-%m-%d_%H-%M-%S")
            timestamp_human = dt.strftime("%b %d, %I:%M %p")

            # Determine transcript and edit status from metadata
            transcript = metadata.get("transcript", "(no transcript)")
            edited_transcript = metadata.get("edited_transcript", "")
            enhanced_transcript = metadata.get("enhanced_transcript", "")
            edit_pending = metadata.get("edit_pending", False)

            # If there's an edited version and it's approved, use that
            if metadata.get("edited") and edited_transcript:
                transcript = edited_transcript
                edit_pending = False
            # If there's an edited version but not approved, mark as pending
            elif edited_transcript:
                transcript = edited_transcript
                edit_pending = True

            calls.append({
                "file": wav.name,
                "path": f"/scanner/audio/{wav.name}",
                "transcript": transcript,
                "edited_transcript": edited_transcript,
                "enhanced_transcript": enhanced_transcript,
                "edit_pending": edit_pending,
                "timestamp": timestamp,
                "timestamp_human": timestamp_human,
                "feed": feed,
                "metadata": metadata
            })

            if limit and len(calls) >= limit:
                break

        except Exception as e:
            logger.warning("Failed to load metadata for %s: %s", base, e)

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




@scanner_bp.route("/scanner/archive")
def scanner_archive():
    feed = request.args.get("feed")  # 'pd', 'fd', 'mpd'
    day = request.args.get("day")
    page = int(request.args.get("page", 1))
    json_mode = request.args.get("json") == "1"

    base_dir = Path(ARCHIVE_DIR)
    if feed:
        base_dir = base_dir / feed

    seven_days_ago = datetime.now() - timedelta(days=7)
    calls_per_page = 10

    # ============================================
    # 1️⃣ QUICK SUMMARY MODE: list days + counts
    # ============================================
    if json_mode and not day:
        day_counts = defaultdict(int)

        for wav in sorted(base_dir.glob("rec_*.wav"), reverse=True):
            base = wav.stem
            try:
                date_str = base.split("_")[1]
                date_obj = datetime.strptime(date_str, "%Y-%m-%d")
            except Exception:
                continue

            if date_obj < seven_days_ago:
                break  # ✅ stop scanning once too old

            day_key = date_obj.strftime("%Y-%m-%d")
            day_counts[day_key] += 1

        # ✅ Return summary only (no heavy transcript load)
        return jsonify({
            "days": sorted(day_counts.keys(), reverse=True),
            "call_totals": dict(day_counts)
        })

    # ============================================
    # 2️⃣ DETAILED MODE: load calls for a specific day
    # ============================================
    if json_mode and day:
        start = (page - 1) * calls_per_page
        end = start + calls_per_page
        calls = []

        for wav in sorted(base_dir.glob(f"rec_{day}_*.wav"), reverse=True)[start:end]:
            base = wav.stem
            json_path = wav.with_suffix(".json")

            try:
                parts = base.split("_")
                timestamp_str = f"{parts[1]}_{parts[2]}"

                dt = datetime.strptime(timestamp_str, "%Y-%m-%d_%H-%M-%S")
                timestamp_human = dt.strftime("%b %d, %I:%M %p")
            except Exception:
                timestamp_human = base

            # 🟢 Only include metadata, not full transcript or audio
            transcript = ""
            if json_path.exists():
                try:
                    with open(json_path) as f:
                        meta = json.load(f)
                    transcript = meta.get("edited_transcript") or meta.get("transcript", "")
                except Exception:
                    pass

            calls.append({
                "file": wav.name,
                "path": f"/scanner/audio/{wav.name}",
                "timestamp_human": timestamp_human,
                "transcript": transcript or "(no transcript)"
            })

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
    logger.info("submit_edit request received")
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
        log_activity("transcript_edit", {"filename": filename, "feed": feed})
        return jsonify({"success": True})
    else:
        return jsonify({"success": False, "error": result['error']}), 500

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
    feeds = {
        "pd": PD_DIR,
        "fd": Path(f"{ARCHIVE_DIR}/fd"),
        "mpd": Path(f"{ARCHIVE_DIR}/mpd"),
        "mfd": Path(f"{ARCHIVE_DIR}/mfd"),
        "sfd": Path(f"{ARCHIVE_DIR}/sfd"),
        "bpd": Path(f"{ARCHIVE_DIR}/bpd"),
        "bfd": Path(f"{ARCHIVE_DIR}/bfd"),
        "mndfd": Path(f"{ARCHIVE_DIR}/mndfd"),
        "mndpd": Path(f"{ARCHIVE_DIR}/mndpd"),
        "uptfd": Path(f"{ARCHIVE_DIR}/uptfd"),
        "uptpd": Path(f"{ARCHIVE_DIR}/uptpd"),
        "blkpd": Path(f"{ARCHIVE_DIR}/blkpd"),
        "blkfd": Path(f"{ARCHIVE_DIR}/blkfd"),
        "milpd": Path(f"{ARCHIVE_DIR}/milpd"),
        "milfd": Path(f"{ARCHIVE_DIR}/milfd"),
        "medpd": Path(f"{ARCHIVE_DIR}/medpd"),
        "medfd": Path(f"{ARCHIVE_DIR}/medfd"),
        "foxpd": Path(f"{ARCHIVE_DIR}/foxpd"),
        "frkpd": Path(f"{ARCHIVE_DIR}/frkpd"),
        "frkfd": Path(f"{ARCHIVE_DIR}/frkfd"),


    }
    counts = {}

    for feed_id, feed_path in feeds.items():
        since_str = request.args.get(f"{feed_id}_since")
        if not since_str:
            counts[feed_id] = 0
            continue

        try:
            # Python's fromisoformat before 3.11 doesn't like 'Z' suffix.
            if since_str.endswith('Z'):
                since_str = since_str[:-1] + '+00:00'
            since_dt = datetime.fromisoformat(since_str)
        except (ValueError, TypeError):
            counts[feed_id] = 0
            continue

        new_call_count = 0
        for wav_file in feed_path.glob("*.wav"):
            try:
                # Filename format: rec_YYYY-MM-DD_HH-MM-SS.wav
                filename_ts_str = wav_file.stem.replace("rec_", "")
                file_dt = datetime.strptime(filename_ts_str, "%Y-%m-%d_%H-%M-%S")
                if file_dt > since_dt.replace(tzinfo=None): # Compare naive datetimes
                    new_call_count += 1
            except (ValueError, IndexError):
                continue # Ignore malformed filenames
        counts[feed_id] = new_call_count

    return jsonify(counts)

@scanner_bp.route("/api/pd_heatmap")
def pd_heatmap():
    now = datetime.now()
    start = now - timedelta(days=6)
    heatmap = defaultdict(lambda: [0] * 24)

    for file in PD_DIR.glob("*.json"):
        try:
            with open(file) as f:
                meta = json.load(f)
            ts = meta.get("timestamp")
            if not ts:
                continue
            dt = datetime.fromisoformat(ts)
            if dt < start:
                continue
            date_key = dt.strftime("%Y-%m-%d")
            heatmap[date_key][dt.hour] += 1
        except Exception:
            continue

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
    logger.info("Scanner stats computed")
    _set_cached_response("stats", stats)
    _set_cached_response_redis("stats", stats)
    return jsonify(stats)


@scanner_bp.route("/scanner/increment_play", methods=["POST"])
def increment_play():
    logger.info("increment_play request received")
    data = request.get_json()
    filename = data.get("filename")
    feed = data.get("feed")

    if not filename or not feed:
        return jsonify({"error": "Missing filename or feed"}), 400

    json_path = Path(f"{ARCHIVE_DIR}/{feed}") / (Path(filename).stem + ".json")
    if not json_path.exists():
        return jsonify({"error": "JSON not found"}), 404

    # --- Lock + read/write safely ---
    try:
        with open(json_path, "r+", encoding="utf-8") as f:
            # Lock the file so only one process updates it at a time
            fcntl.flock(f, fcntl.LOCK_EX)
            try:
                meta = json.load(f)
            except Exception:
                meta = {}

            meta["play_count"] = meta.get("play_count", 0) + 1

            # Rewind and rewritc atomically
            f.seek(0)
            json.dump(meta, f, indent=2)
            f.truncate()
            f.flush()
            os.fsync(f.fileno())

            fcntl.flock(f, fcntl.LOCK_UN)
    except Exception as e:
        return jsonify({"error": f"Failed to update: {e}"}), 500

    log_activity("play_audio", {"filename": filename, "feed": feed})
    return jsonify({"play_count": meta["play_count"]})



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
    logger.info("submit_intent request received")
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

        logger.info("Intent saved for %s", filename)
        log_activity("submit_intent", {"filename": filename, "feed": feed, "intents": intents, "dispositions": dispositions})
        return jsonify({
            "success": True,
            "message": f"Intent metadata updated for {filename}",
            "saved_to": str(dst_json)
        })

    except Exception as e:
        logger.exception("submit_intent error: %s", e)
        return jsonify({"success": False, "error": str(e)}), 500


