from flask import Blueprint, jsonify, send_from_directory, abort
from pathlib import Path
import os
import json
import logging

from shared.scanner_db import get_conn

api_scanner_bp = Blueprint("api_scanner", __name__)
logger = logging.getLogger("scanner_web.routes_api_scanner")

ARCHIVE_BASE = Path(os.environ.get("ARCHIVE_DIR", os.path.join(
    os.environ.get("ARCHIVE_BASE", "/home/ned/data/scanner_calls/scanner_archive"),
    "clean",
)))
VALID_FEEDS = [
    "pd", "fd", "mpd", "mfd", "sfd", "bpd", "bfd",
    "mndfd", "mndpd", "uptfd", "uptpd", "blkpd", "blkfd",
    "milpd", "milfd", "medpd", "medfd", "foxpd", "frkpd", "frkfd",
]

def find_file(filename):
    for sub in VALID_FEEDS:
        f = ARCHIVE_BASE / sub / filename
        if f.exists():
            return f
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
    if isinstance(extra, dict) and extra.get("enhanced_transcript"):
        metadata["enhanced_transcript"] = extra["enhanced_transcript"]
    if metadata.get("derived_address") and not metadata.get("derived_full_address"):
        metadata["derived_full_address"] = metadata["derived_address"]
    return metadata




@api_scanner_bp.route("/api/latest_times_redis")
def api_latest_times():
    """
    Returns latest call timestamps for each scanner feed from Redis.
    Keys are expected as scanner:{feed}:latest_time → ISO string.
    """
    import redis, datetime
    r = redis.StrictRedis(host="localhost", port=6379, decode_responses=True)
    result = {}
    try:
        for key in r.scan_iter(match="scanner:*:latest_time"):
            feed = key.split(":")[1]
            ts = r.get(key)
            if ts:
                try:
                    if ts.endswith('Z'):
                        ts = ts[:-1] + '+00:00'
                    dt = datetime.datetime.fromisoformat(ts)
                    result[feed] = dt.strftime("%I:%M %p").lstrip("0")
                
                except Exception:
                    result[feed] = ts
        return jsonify(result)
    except Exception as e:
        return jsonify({"error": str(e)}), 500



@api_scanner_bp.route("/api/calls")
def list_calls():
    with get_conn(readonly=True) as conn:
        rows = conn.execute("""
            SELECT *
            FROM calls
            WHERE category IN ('pd', 'fd')
            ORDER BY timestamp DESC
        """).fetchall()

    calls = []
    for row in rows:
        metadata = _row_to_metadata(row)
        transcript = row["edited_transcript"] or row["transcript"] or ""
        calls.append({
            "id": Path(row["filename"]).stem.replace("rec_", ""),
            "feed": row["category"],
            "audio": f"/api/audio/{row['filename']}",
            "transcript": transcript,
            "filename": row["filename"],
            "edited": bool(row["edited_transcript"]),
            "metadata": metadata,
        })

    return jsonify(calls)

@api_scanner_bp.route("/api/call/<call_id>")
def get_call_details(call_id):
    filename = f"rec_{call_id}.wav"
    with get_conn(readonly=True) as conn:
        row = conn.execute("SELECT * FROM calls WHERE filename = ? LIMIT 1", (filename,)).fetchone()

    if not row:
        return abort(404, description="Call not found")

    metadata = _row_to_metadata(row)

    data = {
        "id": call_id,
        "audio": f"/api/audio/{row['filename']}",
        "filename": row["filename"],
        "transcript": row["edited_transcript"] or row["transcript"] or "",
        "metadata": metadata,
    }

    return jsonify(data)

@api_scanner_bp.route("/api/audio/<filename>")
def get_audio(filename):
    f = find_file(filename)
    if not f:
        return abort(404)
    return send_from_directory(f.parent, f.name)


