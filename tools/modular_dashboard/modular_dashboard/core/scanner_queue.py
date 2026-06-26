"""
core/scanner_queue.py — Redis scanner queue / backlog inspection.

Defensive: handles missing redis-py, unavailable Redis, non-stream keys,
consumer groups or plain streams, and unknown key types gracefully.

Backlog interpretation is controlled by SCANNER_QUEUE_BACKLOG_MODE:
  auto             — use consumer group pending if groups exist; else warn-only
  consumer_pending — only CG pending counts drive health
  stream_length    — XLEN drives health (legacy / simple behaviour)
"""
from __future__ import annotations

import time
from typing import Any, Optional

from modular_dashboard.core.config import (
    REDIS_URL,
    SCANNER_OLDEST_CRITICAL_SECONDS,
    SCANNER_OLDEST_WARN_SECONDS,
    SCANNER_PENDING_CRITICAL_COUNT,
    SCANNER_PENDING_WARN_COUNT,
    SCANNER_QUEUE_BACKLOG_MODE,
    SCANNER_REDIS_KEY_PATTERNS,
    SCANNER_REDIS_STREAM_KEY,
)
from modular_dashboard.core.logging_config import get_logger
from modular_dashboard.core.schemas import OverallStatus, RedisStatus, ScannerQueueStatus
from modular_dashboard.core.utils import utc_now_str, format_age_seconds

log = get_logger("scanner_queue")

# ---------------------------------------------------------------------------
# Redis client — optional dependency
# ---------------------------------------------------------------------------

try:
    import redis as _redis_lib  # type: ignore

    _REDIS_AVAILABLE = True
except ImportError:
    _REDIS_AVAILABLE = False
    log.warning("redis-py not installed; Redis checks will be unavailable")


def _get_client():
    """Return a redis client connected to REDIS_URL, or raise on failure."""
    if not _REDIS_AVAILABLE:
        raise RuntimeError("redis-py is not installed")
    return _redis_lib.from_url(REDIS_URL, socket_connect_timeout=3, socket_timeout=5)


# ---------------------------------------------------------------------------
# Redis connectivity
# ---------------------------------------------------------------------------

def check_redis() -> RedisStatus:
    """Ping Redis and return a RedisStatus."""
    if not _REDIS_AVAILABLE:
        return RedisStatus(
            reachable=False,
            url=REDIS_URL,
            error="redis-py not installed",
        )
    try:
        client = _get_client()
        client.ping()
        info = {}
        try:
            raw = client.info()
            # Keep a small, readable subset
            for key in ("redis_version", "uptime_in_seconds", "connected_clients",
                        "used_memory_human", "role"):
                if key in raw:
                    info[key] = raw[key]
        except Exception:
            pass
        return RedisStatus(reachable=True, url=REDIS_URL, server_info=info)
    except Exception as exc:
        return RedisStatus(reachable=False, url=REDIS_URL, error=str(exc))


# ---------------------------------------------------------------------------
# Stream / key helpers
# ---------------------------------------------------------------------------

def _stream_length(client, key: str) -> Optional[int]:
    try:
        return client.xlen(key)
    except Exception:
        return None


def _id_age_seconds(stream_id: Optional[str]) -> Optional[float]:
    """Convert a stream ID like '1714000000000-0' to age in seconds."""
    if not stream_id:
        return None
    try:
        ms = int(stream_id.split("-")[0])
        return max((time.time() * 1000 - ms) / 1000.0, 0)
    except Exception:
        return None


def _stream_first_last_ids(client, key: str) -> tuple[Optional[str], Optional[str]]:
    """Return (first_id, last_id) from XINFO STREAM, or (None, None)."""
    try:
        info = client.xinfo_stream(key)
        def _extract_id(entry):
            if entry is None:
                return None
            eid = entry[0] if isinstance(entry, (list, tuple)) else entry
            return eid.decode() if isinstance(eid, bytes) else str(eid)
        first = info.get("first-entry") or info.get("first_entry")
        last  = info.get("last-entry")  or info.get("last_entry")
        return _extract_id(first), _extract_id(last)
    except Exception:
        return None, None


def _oldest_entry_age(client, key: str) -> Optional[float]:
    """Return age in seconds of the oldest entry in a stream, or None."""
    try:
        entries = client.xrange(key, count=1)
        if not entries:
            return None
        entry_id, _ = entries[0]
        ms_str = entry_id.decode() if isinstance(entry_id, bytes) else str(entry_id)
        return _id_age_seconds(ms_str)
    except Exception:
        return None


def _consumer_groups_info(client, key: str) -> list[dict]:
    """Return list of consumer group detail dicts, or [] if none exist."""
    try:
        groups = client.xinfo_groups(key)
        result = []
        for g in groups:
            name = g.get("name", b"")
            if isinstance(name, bytes):
                name = name.decode()
            last_id = g.get("last-delivered-id", b"")
            if isinstance(last_id, bytes):
                last_id = last_id.decode()
            result.append({
                "name": name,
                "consumers": g.get("consumers", 0),
                "pending": g.get("pending", 0),
                "last_delivered_id": str(last_id),
            })
        return result
    except Exception:
        return []


def _pending_count(client, key: str) -> Optional[int]:
    """Total pending across all consumer groups; None if no groups."""
    groups = _consumer_groups_info(client, key)
    if not groups:
        return None
    return sum(g.get("pending", 0) for g in groups)


def _key_type_and_count(client, key: str) -> dict:
    """Return type and element count for an arbitrary key."""
    try:
        ktype = client.type(key)
        if isinstance(ktype, bytes):
            ktype = ktype.decode()
        count: Optional[int] = None
        if ktype == "stream":
            count = _stream_length(client, key)
        elif ktype == "list":
            count = client.llen(key)
        elif ktype == "set":
            count = client.scard(key)
        elif ktype == "zset":
            count = client.zcard(key)
        elif ktype == "hash":
            count = client.hlen(key)
        elif ktype == "string":
            count = 1
        return {"key": key, "type": ktype, "count": count}
    except Exception as exc:
        return {"key": key, "type": "unknown", "count": None, "error": str(exc)}


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def get_pending_count() -> Optional[int]:
    """Return pending count for the primary scanner stream, or None."""
    try:
        client = _get_client()
        return _pending_count(client, SCANNER_REDIS_STREAM_KEY)
    except Exception:
        return None


def get_oldest_pending_age_seconds() -> Optional[float]:
    """Return age (seconds) of the oldest entry in the primary scanner stream."""
    try:
        client = _get_client()
        return _oldest_entry_age(client, SCANNER_REDIS_STREAM_KEY)
    except Exception:
        return None


def get_recent_queue_items(limit: int = 20) -> list[dict]:
    """Return the most-recent entries from the primary scanner stream."""
    try:
        client = _get_client()
        entries = client.xrevrange(SCANNER_REDIS_STREAM_KEY, count=limit)
        result = []
        for entry_id, fields in entries:
            id_str = entry_id.decode() if isinstance(entry_id, bytes) else str(entry_id)
            decoded = {}
            for k, v in fields.items():
                k2 = k.decode() if isinstance(k, bytes) else str(k)
                v2 = v.decode() if isinstance(v, bytes) else str(v)
                decoded[k2] = v2
            result.append({"id": id_str, "fields": decoded})
        return result
    except Exception as exc:
        log.debug("get_recent_queue_items error: %s", exc)
        return []


def get_scanner_related_keys(limit: int = 100) -> list[dict]:
    """Scan for scanner-related keys using configured patterns."""
    try:
        client = _get_client()
        found: list[str] = []
        seen: set[str] = set()
        for pattern in SCANNER_REDIS_KEY_PATTERNS:
            for key in client.scan_iter(pattern, count=200):
                key_str = key.decode() if isinstance(key, bytes) else str(key)
                if key_str not in seen:
                    seen.add(key_str)
                    found.append(key_str)
                if len(found) >= limit:
                    break
        result = []
        for key in found:
            result.append(_key_type_and_count(client, key))
        return result
    except Exception as exc:
        log.debug("get_scanner_related_keys error: %s", exc)
        return []


def _compute_backlog_health(
    stream_length: Optional[int],
    groups: list[dict],
    oldest_retained_age: Optional[float],
    mode: str,
) -> tuple[OverallStatus, str, Optional[int]]:
    """
    Returns (health, note, effective_pending_count).

    In 'auto' mode (default):
      - If consumer groups exist, use their total pending as the backlog.
      - If no consumer groups, XLEN is *retained stream history* — emit
        a warning with explanation instead of critical.
    In 'consumer_pending' mode:
      - Require consumer groups; unknown if none.
    In 'stream_length' mode:
      - Use XLEN directly (legacy behaviour).
    """
    total_cg_pending: Optional[int] = None
    if groups:
        total_cg_pending = sum(g.get("pending", 0) for g in groups)

    if mode == "stream_length":
        eff_pending = stream_length
        eff_age = oldest_retained_age
        prefix = "[mode=stream_length] "
    elif mode == "consumer_pending":
        if total_cg_pending is None:
            return (
                OverallStatus.unknown,
                "No consumer groups detected. Cannot determine true pending backlog "
                "(SCANNER_QUEUE_BACKLOG_MODE=consumer_pending).",
                None,
            )
        eff_pending = total_cg_pending
        eff_age = None   # age thresholds not applied to CG mode
        prefix = "[CG pending] "
    else:
        # auto
        if total_cg_pending is not None:
            eff_pending = total_cg_pending
            eff_age = None
            prefix = "[CG pending] "
        else:
            # No consumer groups — large XLEN is normal retained history
            if (stream_length or 0) > 0:
                note = (
                    f"Stream length: {stream_length:,} entries (retained stream history — "
                    f"no consumer groups detected, so this is NOT proven pending work). "
                    f"Oldest retained entry: {format_age_seconds(oldest_retained_age)}. "
                    "To treat XLEN as backlog, set SCANNER_QUEUE_BACKLOG_MODE=stream_length."
                )
                return OverallStatus.warning, note, None
            return OverallStatus.healthy, "Stream is empty or has no consumer groups.", None

    problems: list[str] = []
    worst = OverallStatus.healthy

    if eff_pending is not None:
        if eff_pending >= SCANNER_PENDING_CRITICAL_COUNT:
            problems.append(
                f"{eff_pending:,} pending items ≥ critical threshold {SCANNER_PENDING_CRITICAL_COUNT:,}"
            )
            worst = OverallStatus.critical
        elif eff_pending >= SCANNER_PENDING_WARN_COUNT:
            problems.append(
                f"{eff_pending:,} pending items ≥ warning threshold {SCANNER_PENDING_WARN_COUNT:,}"
            )
            worst = OverallStatus.warning

    if eff_age is not None:
        if eff_age >= SCANNER_OLDEST_CRITICAL_SECONDS:
            problems.append(
                f"Oldest item is {format_age_seconds(eff_age)} old "
                f"(≥ critical {format_age_seconds(SCANNER_OLDEST_CRITICAL_SECONDS)})"
            )
            worst = OverallStatus.critical
        elif eff_age >= SCANNER_OLDEST_WARN_SECONDS:
            if worst != OverallStatus.critical:
                worst = OverallStatus.warning
            problems.append(
                f"Oldest item is {format_age_seconds(eff_age)} old "
                f"(≥ warning {format_age_seconds(SCANNER_OLDEST_WARN_SECONDS)})"
            )

    if problems:
        note = prefix + "; ".join(problems)
    else:
        note = prefix + "Queue backlog is within normal thresholds."

    return worst, note, eff_pending


def get_scanner_queue_status() -> ScannerQueueStatus:
    """Full scanner queue / backlog status report."""
    redis_status = check_redis()
    if not redis_status.reachable:
        return ScannerQueueStatus(
            stream_key=SCANNER_REDIS_STREAM_KEY,
            backlog_health=OverallStatus.unknown,
            backlog_note="Redis is unreachable.",
            error=redis_status.error,
        )

    try:
        client = _get_client()

        stream_len = _stream_length(client, SCANNER_REDIS_STREAM_KEY)
        first_id, last_id = _stream_first_last_ids(client, SCANNER_REDIS_STREAM_KEY)
        oldest_retained_age = _id_age_seconds(first_id)
        groups = _consumer_groups_info(client, SCANNER_REDIS_STREAM_KEY)

        mode = SCANNER_QUEUE_BACKLOG_MODE.lower().strip()
        health, health_note, eff_pending = _compute_backlog_health(
            stream_len, groups, oldest_retained_age, mode
        )

        oldest_note = ""
        if oldest_retained_age is None and (stream_len or 0) > 0:
            oldest_note = "Could not determine oldest entry age."
        elif not groups:
            oldest_note = (
                "No consumer groups — oldest age shown is the stream's first "
                "retained entry, not necessarily pending work."
            )

        recent = get_recent_queue_items(20)
        related_keys = get_scanner_related_keys(100)

        return ScannerQueueStatus(
            stream_key=SCANNER_REDIS_STREAM_KEY,
            stream_length=stream_len,
            pending_count=eff_pending,
            oldest_pending_age_seconds=oldest_retained_age,
            oldest_pending_note=oldest_note,
            first_entry_id=first_id,
            last_entry_id=last_id,
            consumer_groups=groups,
            recent_items=recent,
            related_keys=related_keys,
            backlog_health=health,
            backlog_note=health_note,
        )
    except Exception as exc:
        log.error("get_scanner_queue_status error: %s", exc)
        return ScannerQueueStatus(
            stream_key=SCANNER_REDIS_STREAM_KEY,
            backlog_health=OverallStatus.unknown,
            backlog_note="Error inspecting scanner queue.",
            error=str(exc),
        )


# ---------------------------------------------------------------------------
# Redis dashboard data — pending calls + transcribed history
# ---------------------------------------------------------------------------

def _decode_fields(fields: dict) -> dict:
    """Decode bytes keys/values from a Redis call."""
    return {
        (k.decode() if isinstance(k, bytes) else str(k)):
        (v.decode() if isinstance(v, bytes) else str(v))
        for k, v in fields.items()
    }


def _stream_id_to_ts(stream_id: str) -> str:
    """Convert '1714000000000-0' to a human-readable UTC timestamp string."""
    try:
        ms = int(stream_id.split("-")[0])
        import datetime
        dt = datetime.datetime.utcfromtimestamp(ms / 1000.0)
        return dt.strftime("%Y-%m-%d %H:%M:%S UTC")
    except Exception:
        return stream_id


def get_pending_calls(limit: int = 50) -> list[dict]:
    """Return stream entries not yet processed by the transcriber."""
    try:
        client = _get_client()
        last_id_bytes = client.get("scanner:transcriber:last_id")
        if not last_id_bytes:
            # No last_id → treat entire stream as unprocessed (unusual)
            entries = client.xrange(SCANNER_REDIS_STREAM_KEY, count=limit)
        else:
            last_id = last_id_bytes.decode() if isinstance(last_id_bytes, bytes) else str(last_id_bytes)
            # xrange exclusive start: use '(' prefix if supported, else filter manually
            entries = client.xrange(SCANNER_REDIS_STREAM_KEY, min=last_id, count=limit + 1)
            entries = [(eid, flds) for eid, flds in entries
                       if (eid.decode() if isinstance(eid, bytes) else str(eid)) > last_id]
            entries = entries[:limit]
        result = []
        for entry_id, fields in entries:
            id_str = entry_id.decode() if isinstance(entry_id, bytes) else str(entry_id)
            decoded = _decode_fields(fields)
            decoded["_id"] = id_str
            decoded["_ts"] = _stream_id_to_ts(id_str)
            result.append(decoded)
        return result
    except Exception as exc:
        log.debug("get_pending_calls error: %s", exc)
        return []


def get_recent_transcribed(limit: int = 30) -> list[dict]:
    """
    Return the most recent transcribed calls.
    Tries the API cache first; falls back to the most-recent stream entries
    (those at or before last_processed_id).
    """
    try:
        client = _get_client()
        import json as _json

        raw = client.get("scanner_api_cache:home_live_calls")
        if raw:
            data = _json.loads(raw)
            calls = data.get("calls", []) if isinstance(data, dict) else data
            if calls:
                result = []
                for call in calls[:limit]:
                    result.append({
                        "file": call.get("file", ""),
                        "feed": call.get("feed", ""),
                        "transcript": call.get("transcript", ""),
                        "edited_transcript": call.get("edited_transcript", ""),
                        "enhanced_transcript": call.get("enhanced_transcript", ""),
                        "timestamp": call.get("timestamp", ""),
                        "timestamp_human": call.get("timestamp_human", ""),
                        "duration": round(float(call.get("duration", 0) or 0), 1),
                        "town": (call.get("metadata") or {}).get("town", ""),
                        "dept": (call.get("metadata") or {}).get("dept", ""),
                        "source": "cache",
                    })
                return result

        # Fall back: pull recent stream entries at/before last_processed_id
        last_id_bytes = client.get("scanner:transcriber:last_id")
        max_id = (last_id_bytes.decode() if isinstance(last_id_bytes, bytes) else str(last_id_bytes)) if last_id_bytes else "+"
        entries = client.xrevrange(SCANNER_REDIS_STREAM_KEY, max=max_id, count=limit)
        result = []
        for entry_id, fields in entries:
            id_str = entry_id.decode() if isinstance(entry_id, bytes) else str(entry_id)
            decoded = _decode_fields(fields)
            ts = decoded.get("time", "")
            result.append({
                "file": decoded.get("file", "").split("/")[-1],
                "feed": decoded.get("tag", ""),
                "transcript": "",
                "edited_transcript": "",
                "enhanced_transcript": "",
                "timestamp": ts,
                "timestamp_human": ts[:16].replace("T", " ") if "T" in ts else ts[:16],
                "duration": 0,
                "town": "",
                "dept": "",
                "source": "stream",
            })
        return result
    except Exception as exc:
        log.debug("get_recent_transcribed error: %s", exc)
        return []


def get_feed_activity_today() -> list[dict]:
    """Return today's call counts per feed, sorted by count descending."""
    try:
        client = _get_client()
        import json as _json
        raw = client.get("scanner_api_cache:today_counts")
        if not raw:
            return []
        data = _json.loads(raw)
        result = []
        for feed, info in data.items():
            result.append({
                "feed": feed,
                "count": info.get("count", 0),
                "latest_time": info.get("latest_time", ""),
                "hooks_count": info.get("hooks_count", 0),
            })
        result.sort(key=lambda x: x["count"], reverse=True)
        return result
    except Exception as exc:
        log.debug("get_feed_activity_today error: %s", exc)
        return []


def get_latest_per_feed() -> dict:
    """Return the latest transcribed call per feed from the API cache."""
    try:
        client = _get_client()
        import json as _json
        raw = client.get("scanner_api_cache:latest")
        if not raw:
            return {}
        data = _json.loads(raw)
        return data if isinstance(data, dict) else {}
    except Exception as exc:
        log.debug("get_latest_per_feed error: %s", exc)
        return {}


def get_redis_dashboard_data() -> dict:
    """
    Aggregate all data needed for the Redis dashboard page:
      - connection info
      - stream stats (length, last processed ID, pending count)
      - pending_calls list
      - recent_transcribed list
      - feed_activity_today list
      - today_stats dict
    """
    redis_status = check_redis()
    if not redis_status.reachable:
        return {
            "reachable": False,
            "error": redis_status.error,
            "url": redis_status.url,
            "server_info": {},
            "stream_key": SCANNER_REDIS_STREAM_KEY,
            "stream_length": None,
            "last_processed_id": None,
            "last_processed_ts": None,
            "pending_calls": [],
            "pending_count": 0,
            "recent_transcribed": [],
            "feed_activity": [],
            "latest_per_feed": {},
            "today_stats": {},
        }

    try:
        client = _get_client()
        import json as _json

        stream_len = _stream_length(client, SCANNER_REDIS_STREAM_KEY)
        last_id_bytes = client.get("scanner:transcriber:last_id")
        last_id = (last_id_bytes.decode() if isinstance(last_id_bytes, bytes) else str(last_id_bytes)) if last_id_bytes else None
        last_ts = _stream_id_to_ts(last_id) if last_id else None

        pending = get_pending_calls(50)
        transcribed = get_recent_transcribed(50)
        activity = get_feed_activity_today()
        latest = get_latest_per_feed()

        # Overall today stats
        stats_raw = client.get("scanner_api_cache:stats")
        today_stats = _json.loads(stats_raw) if stats_raw else {}

        return {
            "reachable": True,
            "error": None,
            "url": redis_status.url,
            "server_info": redis_status.server_info,
            "stream_key": SCANNER_REDIS_STREAM_KEY,
            "stream_length": stream_len,
            "last_processed_id": last_id,
            "last_processed_ts": last_ts,
            "pending_calls": pending,
            "pending_count": len(pending),
            "recent_transcribed": transcribed,
            "feed_activity": activity,
            "latest_per_feed": latest,
            "today_stats": today_stats,
        }
    except Exception as exc:
        log.error("get_redis_dashboard_data error: %s", exc)
        return {
            "reachable": True,
            "error": str(exc),
            "url": redis_status.url,
            "server_info": redis_status.server_info,
            "stream_key": SCANNER_REDIS_STREAM_KEY,
            "stream_length": None,
            "last_processed_id": None,
            "last_processed_ts": None,
            "pending_calls": [],
            "pending_count": 0,
            "recent_transcribed": [],
            "feed_activity": [],
            "latest_per_feed": {},
            "today_stats": {},
        }
