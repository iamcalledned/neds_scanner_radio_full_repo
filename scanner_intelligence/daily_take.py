from __future__ import annotations

import hashlib
import json
import logging
import os
import re
import sqlite3
import threading
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Optional, Sequence, Set


DEFAULT_DB_PATH = os.environ.get(
    "SCANNER_DB_PATH",
    "/home/ned/data/scanner_calls/scanner_calls.db",
)
DEFAULT_INTELLIGENCE_DB_PATH = os.environ.get(
    "SCANNER_INTELLIGENCE_DB_PATH",
    "/home/ned/data/scanner_calls/scanner_intelligence.db",
)
PROMPT_VERSION = "lifecycle-v7-incident-pages"
MAX_INCIDENT_GAP_SECONDS = 10 * 60
MAX_ANCHORED_FIRE_GAP_SECONDS = 3 * 60 * 60
MAX_ANCHORED_POLICE_GAP_SECONDS = 60 * 60
MAX_POLICE_UNIT_GAP_SECONDS = 15 * 60
MAX_FIRE_CLOSED_FOLLOWUP_SECONDS = 30 * 60

_generation_locks: Dict[str, threading.Lock] = {}
_generation_locks_guard = threading.Lock()
logger = logging.getLogger("scanner_intelligence.daily_take")

UNIT_RE = re.compile(
    r"\b(engine|ladder|rescue|ambulance|squad|brush|car|bravo|alpha|"
    r"station|p|a|k|c|t)\s*[- ]?\s*(\d{1,3})\b",
    re.IGNORECASE,
)
REPORTED_UNIT_RE = re.compile(
    r"^(?:engine|ladder|rescue|ambulance|squad|brush|car|bravo|alpha|"
    r"station|p|a|k|c|t)[- ]?\d{1,3}$",
    re.IGNORECASE,
)
POLICE_SPEAKER_PATTERNS = (
    re.compile(r"^\s*(\d{1,3})\s+(?:to|for)\s+(?:control|station)\b", re.IGNORECASE),
    re.compile(r"\b(?:control|station)\s+to\s+(\d{1,3})\b", re.IGNORECASE),
    re.compile(r"^\s*(\d{1,3})\s*(?:,|\bis\b)\s+", re.IGNORECASE),
)

ADDRESS_RE = re.compile(
    r"\b\d{1,5}\s+[A-Za-z][A-Za-z0-9'’-]*(?:\s+[A-Za-z][A-Za-z0-9'’-]*){0,3}"
    r"\s+(?:Street|St|Road|Rd|Avenue|Ave|Lane|Ln|Drive|Dr|Court|Ct|Boulevard|Blvd)\b",
    re.IGNORECASE,
)
CITATION_OUTCOME_RE = re.compile(
    r"\b(?:citation issued|issued (?:a |the )?citation|"
    r"(?:one|two|three|\d+) (?:parking )?citations?|parking citations?|"
    r"(?:written|civil|criminal) citation|operator,? citation|"
    r"cited|ticketed|summons issued)\b",
    re.IGNORECASE,
)
WARNING_OUTCOME_RE = re.compile(
    r"\b(?:verbal warning|written warning|warning issued|issued .*? warning)\b",
    re.IGNORECASE,
)


def _local_naive(value: datetime) -> datetime:
    if value.tzinfo is not None:
        return value.astimezone().replace(tzinfo=None)
    return value


@dataclass
class CallRecord:
    call_id: int
    timestamp: datetime
    town: str
    dept: str
    category: str
    filename: str
    duration: float
    transcript: str
    call_type: str
    address: str
    units: Set[str]
    tone_detected: bool
    quality_flagged: bool
    play_count: int
    hook_request: bool
    service: str
    lifecycle_stage: str


@dataclass
class Incident:
    calls: List[CallRecord] = field(default_factory=list)
    addresses: Set[str] = field(default_factory=set)
    units: Set[str] = field(default_factory=set)
    call_types: Dict[str, int] = field(default_factory=dict)
    returned_units: Set[str] = field(default_factory=set)
    lifecycle_stages: List[str] = field(default_factory=list)
    closed: bool = False

    @property
    def first(self) -> CallRecord:
        return self.calls[0]

    @property
    def last(self) -> CallRecord:
        return self.calls[-1]

    @property
    def call_type(self) -> str:
        if not self.call_types:
            return ""
        return max(self.call_types.items(), key=lambda item: (item[1], item[0]))[0]

    @property
    def source_call_ids(self) -> List[int]:
        return [call.call_id for call in self.calls]

    def add(self, call: CallRecord) -> None:
        self.calls.append(call)
        if call.address:
            self.addresses.add(call.address)
        self.units.update(call.units)
        if call.call_type:
            self.call_types[call.call_type] = self.call_types.get(call.call_type, 0) + 1
        self.lifecycle_stages.append(call.lifecycle_stage)
        if call.service == "fire":
            if call.lifecycle_stage == "in_quarters":
                self.returned_units.update(call.units)
                self.closed = True
            elif call.lifecycle_stage == "terminated":
                self.closed = True
        elif call.service == "police" and call.lifecycle_stage == "cleared":
            self.closed = True


def parse_day(value: Optional[str], now: Optional[datetime] = None) -> str:
    """Resolve a supported day value to YYYY-MM-DD."""
    current = _local_naive(now or datetime.now()).date()
    normalized = (value or "today").strip().lower()
    if normalized == "today":
        return current.isoformat()
    if normalized == "yesterday":
        return (current - timedelta(days=1)).isoformat()
    try:
        return date.fromisoformat(normalized).isoformat()
    except ValueError as exc:
        raise ValueError("date must be today, yesterday, or YYYY-MM-DD") from exc


def _connect(db_path: str, readonly: bool = False) -> sqlite3.Connection:
    if readonly:
        uri = f"file:{Path(db_path).resolve()}?mode=ro"
        conn = sqlite3.connect(uri, timeout=15, uri=True)
    else:
        conn = sqlite3.connect(db_path, timeout=15)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout=15000")
    return conn


def _resolve_storage_path(source_db_path: str, storage_db_path: Optional[str]) -> str:
    if storage_db_path:
        return storage_db_path
    if source_db_path == DEFAULT_DB_PATH:
        return DEFAULT_INTELLIGENCE_DB_PATH
    return source_db_path


def ensure_intelligence_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS scanner_incidents (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            incident_key TEXT NOT NULL UNIQUE,
            day TEXT NOT NULL,
            town TEXT NOT NULL,
            dept TEXT,
            category TEXT,
            start_timestamp TEXT NOT NULL,
            end_timestamp TEXT NOT NULL,
            call_type TEXT,
            source_count INTEGER NOT NULL,
            source_call_ids_json TEXT NOT NULL,
            units_json TEXT NOT NULL,
            addresses_json TEXT NOT NULL,
            lifecycle_stages_json TEXT NOT NULL,
            returned_units_json TEXT NOT NULL,
            closed INTEGER NOT NULL DEFAULT 0,
            tone_detected INTEGER NOT NULL DEFAULT 0,
            quality_flagged INTEGER NOT NULL DEFAULT 0,
            notable_score REAL NOT NULL DEFAULT 0,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );

        CREATE INDEX IF NOT EXISTS idx_scanner_incidents_day_town
            ON scanner_incidents(day, town);

        CREATE TABLE IF NOT EXISTS scanner_daily_takes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            day TEXT NOT NULL,
            town_scope TEXT NOT NULL DEFAULT '',
            edition_type TEXT NOT NULL,
            status TEXT NOT NULL,
            fact_pack_json TEXT NOT NULL,
            content_json TEXT NOT NULL,
            source_call_ids_json TEXT NOT NULL,
            generated_at TEXT NOT NULL,
            generator TEXT NOT NULL,
            prompt_version TEXT NOT NULL,
            source_watermark_json TEXT NOT NULL DEFAULT '{}',
            UNIQUE(day, town_scope, edition_type)
        );

        CREATE INDEX IF NOT EXISTS idx_scanner_daily_takes_day
            ON scanner_daily_takes(day, town_scope, edition_type);

        CREATE TABLE IF NOT EXISTS scanner_incident_commentary (
            incident_key TEXT PRIMARY KEY,
            source_call_ids_json TEXT NOT NULL,
            commentary TEXT NOT NULL,
            generated_at TEXT NOT NULL,
            generator TEXT NOT NULL,
            prompt_version TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS scanner_call_enrichments (
            call_id INTEGER PRIMARY KEY,
            source_fingerprint TEXT NOT NULL,
            source_timestamp TEXT,
            town TEXT,
            dept TEXT,
            category TEXT,
            status TEXT NOT NULL DEFAULT 'pending',
            enhanced_transcript TEXT,
            factual_summary TEXT,
            commentary TEXT,
            classification_json TEXT NOT NULL DEFAULT '{}',
            evidence_json TEXT NOT NULL DEFAULT '[]',
            transcript_validation_json TEXT NOT NULL DEFAULT '{}',
            confidence REAL,
            model TEXT,
            prompt_version TEXT NOT NULL,
            attempts INTEGER NOT NULL DEFAULT 0,
            last_error TEXT,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            completed_at TEXT
        );

        CREATE INDEX IF NOT EXISTS idx_scanner_call_enrichments_status
            ON scanner_call_enrichments(status, updated_at);

        CREATE TABLE IF NOT EXISTS scanner_retranscription_requests (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            call_id INTEGER NOT NULL,
            source_fingerprint TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'queued',
            reasons_json TEXT NOT NULL DEFAULT '[]',
            explanation TEXT,
            validation_confidence REAL,
            requested_at TEXT NOT NULL,
            dispatched_at TEXT,
            completed_at TEXT,
            result_json TEXT,
            last_error TEXT,
            UNIQUE(call_id, source_fingerprint)
        );

        CREATE INDEX IF NOT EXISTS idx_scanner_retranscription_status
            ON scanner_retranscription_requests(status, requested_at);
        """
    )
    incident_columns = {
        row["name"]
        for row in conn.execute("PRAGMA table_info(scanner_incidents)").fetchall()
    }
    migrations = {
        "lifecycle_stages_json": "TEXT NOT NULL DEFAULT '[]'",
        "returned_units_json": "TEXT NOT NULL DEFAULT '[]'",
        "closed": "INTEGER NOT NULL DEFAULT 0",
    }
    for column, declaration in migrations.items():
        if column not in incident_columns:
            conn.execute(
                f"ALTER TABLE scanner_incidents ADD COLUMN {column} {declaration}"
            )
    take_columns = {
        row["name"]
        for row in conn.execute("PRAGMA table_info(scanner_daily_takes)").fetchall()
    }
    if "source_watermark_json" not in take_columns:
        conn.execute(
            "ALTER TABLE scanner_daily_takes "
            "ADD COLUMN source_watermark_json TEXT NOT NULL DEFAULT '{}'"
        )
    enrichment_columns = {
        row["name"]
        for row in conn.execute(
            "PRAGMA table_info(scanner_call_enrichments)"
        ).fetchall()
    }
    if "transcript_validation_json" not in enrichment_columns:
        conn.execute(
            "ALTER TABLE scanner_call_enrichments "
            "ADD COLUMN transcript_validation_json TEXT NOT NULL DEFAULT '{}'"
        )
    if "enhanced_transcript" not in enrichment_columns:
        conn.execute(
            "ALTER TABLE scanner_call_enrichments "
            "ADD COLUMN enhanced_transcript TEXT"
        )


def _json_object(value: Any) -> Dict[str, Any]:
    if isinstance(value, dict):
        return value
    if not value:
        return {}
    try:
        parsed = json.loads(value)
        return parsed if isinstance(parsed, dict) else {}
    except (TypeError, ValueError):
        return {}


def _parse_timestamp(value: str) -> datetime:
    normalized = (value or "").strip()
    if normalized.endswith("Z"):
        normalized = normalized[:-1] + "+00:00"
    parsed = datetime.fromisoformat(normalized)
    if parsed.tzinfo is not None:
        parsed = parsed.astimezone().replace(tzinfo=None)
    return parsed


def _normalize_text(value: Optional[str]) -> str:
    return re.sub(r"\s+", " ", (value or "").strip())


def _normalize_anchor(value: Optional[str]) -> str:
    return re.sub(r"[^a-z0-9]+", " ", (value or "").lower()).strip()


def _extract_call_type(classification: Dict[str, Any]) -> str:
    for key in ("call_type", "intent", "primary_intent"):
        value = classification.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
    return ""


def _extract_units(
    classification: Dict[str, Any],
    transcript: str,
    service: str,
) -> Set[str]:
    raw_units = classification.get("units") or []
    if isinstance(raw_units, str):
        raw_units = [raw_units]
    if not isinstance(raw_units, list):
        raw_units = []
    units: Set[str] = set()
    for raw_unit in raw_units:
        raw_text = str(raw_unit).strip()
        if service == "police" and re.fullmatch(r"\d{1,3}", raw_text):
            units.add(f"unit {int(raw_text)}")
        elif REPORTED_UNIT_RE.fullmatch(raw_text):
            units.add(_normalize_anchor(raw_text))
    for match in UNIT_RE.finditer(transcript):
        units.add(f"{match.group(1).lower()} {int(match.group(2))}")
    if service == "police":
        for pattern in POLICE_SPEAKER_PATTERNS:
            for match in pattern.finditer(transcript):
                units.add(f"unit {int(match.group(1))}")
    return units


def _service_for(dept: str, category: str) -> str:
    normalized_dept = (dept or "").lower()
    normalized_category = (category or "").lower()
    if normalized_dept == "fire" or normalized_category.endswith("fd"):
        return "fire"
    if normalized_dept == "police" or normalized_category.endswith("pd"):
        return "police"
    return "unknown"


def _fire_lifecycle_stage(transcript: str, tone_detected: bool) -> str:
    text = transcript.lower()
    if "command terminated" in text or "command is terminated" in text:
        return "terminated"
    if any(
        term in text
        for term in (
            "back in quarters",
            "back at quarters",
            "in quarters",
            "at quarters",
            "back in service",
            "available in quarters",
        )
    ):
        return "in_quarters"
    if any(term in text for term in ("clear returning", "returning to station", "returning")):
        return "returning"
    if tone_detected or "tone!!!" in text or "announcing a still alarm" in text:
        return "dispatched"
    if any(term in text for term in ("responding", "en route", "on the route")):
        return "responding"
    if any(term in text for term in ("on scene", "on-scene", "arriving", "arrival")):
        return "on_scene"
    return "update"


def _police_lifecycle_stage(transcript: str) -> str:
    text = transcript.lower()
    false_clear_phrases = (
        "loud and clear",
        "clear to copy",
        "when you're clear",
        "when you are clear",
        "first floor clear",
        "channel clear",
    )
    if (
        "clear" in text
        and not any(phrase in text for phrase in false_clear_phrases)
        and not text.rstrip().endswith("clear?")
    ):
        return "cleared"
    if any(term in text for term in ("respond to", "responding", "head to", "en route")):
        return "dispatched"
    if any(term in text for term in ("on scene", "on-scene", "arriving", "arrival")):
        return "on_scene"
    return "update"


def _lifecycle_stage(service: str, transcript: str, tone_detected: bool) -> str:
    if service == "fire":
        return _fire_lifecycle_stage(transcript, tone_detected)
    if service == "police":
        return _police_lifecycle_stage(transcript)
    return "update"


def _row_to_call(row: sqlite3.Row) -> Optional[CallRecord]:
    try:
        timestamp = _parse_timestamp(row["timestamp"])
    except (TypeError, ValueError):
        return None

    classification = _json_object(row["classification"])
    address = (
        row["derived_address"]
        or " ".join(
            part
            for part in (
                classification.get("address_number"),
                classification.get("address_street"),
            )
            if part
        )
    )
    transcript = _normalize_text(row["best_transcript"])
    dept = _normalize_text(row["dept"] or "unknown").lower()
    category = _normalize_text(row["category"] or "").lower()
    service = _service_for(dept, category)
    tone_detected = bool(classification.get("tone_detected"))
    return CallRecord(
        call_id=int(row["id"]),
        timestamp=timestamp,
        town=_normalize_text(row["town"] or row["derived_town"] or "Unknown"),
        dept=dept,
        category=category,
        filename=_normalize_text(row["filename"]),
        duration=float(row["duration"] or 0),
        transcript=transcript,
        call_type=_extract_call_type(classification),
        address=_normalize_anchor(address),
        units=_extract_units(classification, transcript, service),
        tone_detected=tone_detected,
        quality_flagged=bool(row["needs_review"] or row["needs_retry"]),
        play_count=int(row["play_count"] or 0),
        hook_request=bool(row["hook_request"]),
        service=service,
        lifecycle_stage=_lifecycle_stage(service, transcript, tone_detected),
    )


def _load_calls(
    conn: sqlite3.Connection,
    day: str,
    town: Optional[str] = None,
) -> List[CallRecord]:
    sql = """
        SELECT
            id, timestamp, town, derived_town, dept, category, filename,
            duration, classification, derived_address, needs_review,
            needs_retry, play_count, hook_request,
            COALESCE(
                NULLIF(TRIM(edited_transcript), ''),
                NULLIF(TRIM(transcript), ''),
                NULLIF(TRIM(normalized_transcript), ''),
                NULLIF(TRIM(raw_transcript), '')
            ) AS best_transcript
        FROM calls
        WHERE date(timestamp) = date(?)
    """
    params: List[Any] = [day]
    if town:
        sql += " AND LOWER(COALESCE(NULLIF(town, ''), derived_town, '')) = LOWER(?)"
        params.append(town.strip())
    sql += " ORDER BY timestamp ASC, id ASC"

    records: List[CallRecord] = []
    for row in conn.execute(sql, params):
        record = _row_to_call(row)
        if record:
            records.append(record)
    return records


def _load_calls_by_ids(
    conn: sqlite3.Connection,
    call_ids: Sequence[int],
) -> List[CallRecord]:
    if not call_ids:
        return []
    placeholders = ",".join("?" for _ in call_ids)
    sql = f"""
        SELECT
            id, timestamp, town, derived_town, dept, category, filename,
            duration, classification, derived_address, needs_review,
            needs_retry, play_count, hook_request,
            COALESCE(
                NULLIF(TRIM(edited_transcript), ''),
                NULLIF(TRIM(transcript), ''),
                NULLIF(TRIM(normalized_transcript), ''),
                NULLIF(TRIM(raw_transcript), '')
            ) AS best_transcript
        FROM calls
        WHERE id IN ({placeholders})
        ORDER BY timestamp ASC, id ASC
    """
    records: List[CallRecord] = []
    for row in conn.execute(sql, list(call_ids)):
        record = _row_to_call(row)
        if record:
            records.append(record)
    return records


def _source_watermark(
    conn: sqlite3.Connection,
    day: str,
    town: Optional[str] = None,
) -> Dict[str, Any]:
    sql = """
        SELECT COUNT(*) AS call_count, MAX(id) AS max_call_id,
               MAX(timestamp) AS max_timestamp
        FROM calls
        WHERE date(timestamp) = date(?)
    """
    params: List[Any] = [day]
    if town:
        sql += " AND LOWER(COALESCE(NULLIF(town, ''), derived_town, '')) = LOWER(?)"
        params.append(town.strip())
    row = conn.execute(sql, params).fetchone()
    return {
        "call_count": int(row["call_count"] or 0),
        "max_call_id": int(row["max_call_id"]) if row["max_call_id"] is not None else None,
        "max_timestamp": row["max_timestamp"] or None,
    }


def _generation_lock(key: str) -> threading.Lock:
    with _generation_locks_guard:
        return _generation_locks.setdefault(key, threading.Lock())


def _match_score(incident: Incident, call: CallRecord) -> float:
    if incident.closed:
        if (
            call.service == "fire"
            and call.lifecycle_stage == "in_quarters"
            and call.units
            and incident.units.intersection(call.units)
            and not call.units.issubset(incident.returned_units)
            and 0
            <= (call.timestamp - incident.last.timestamp).total_seconds()
            <= MAX_FIRE_CLOSED_FOLLOWUP_SECONDS
        ):
            return 8
        return -1
    if (
        incident.last.town.lower() != call.town.lower()
        or incident.last.dept != call.dept
        or incident.last.category != call.category
    ):
        return -1

    gap = (call.timestamp - incident.last.timestamp).total_seconds()
    if gap < 0:
        return -1

    address_match = bool(call.address and call.address in incident.addresses)
    unit_match = bool(call.units and incident.units.intersection(call.units))
    if call.service == "fire" and call.lifecycle_stage == "dispatched":
        if not (address_match and gap <= 120):
            return -1

    if gap > MAX_INCIDENT_GAP_SECONDS:
        if call.service == "fire":
            anchored = gap <= MAX_ANCHORED_FIRE_GAP_SECONDS and (
                address_match or unit_match
            )
        else:
            unit_gap = (
                MAX_ANCHORED_POLICE_GAP_SECONDS
                if call.lifecycle_stage == "cleared"
                else MAX_POLICE_UNIT_GAP_SECONDS
            )
            anchored = (
                address_match and gap <= MAX_ANCHORED_POLICE_GAP_SECONDS
            ) or (unit_match and gap <= unit_gap)
        if not anchored:
            return -1

    score = 0.0
    if address_match:
        score += 6
    if unit_match:
        score += 5
    if (
        call.call_type
        and call.call_type == incident.call_type
        and gap <= MAX_INCIDENT_GAP_SECONDS
    ):
        score += 2
    if gap <= 45:
        score += 1
    if gap <= 180 and call.lifecycle_stage in {
        "responding",
        "on_scene",
        "returning",
        "in_quarters",
        "cleared",
        "terminated",
    }:
        score += 1.5
    if unit_match and call.lifecycle_stage in {
        "returning",
        "in_quarters",
        "cleared",
        "terminated",
    }:
        score += 3
    return score


def group_calls_into_incidents(calls: Sequence[CallRecord]) -> List[Incident]:
    """Group transmission fragments using conservative shared-anchor heuristics."""
    incidents: List[Incident] = []
    for call in calls:
        best: Optional[Incident] = None
        best_score = -1.0
        for incident in reversed(incidents[-100:]):
            score = _match_score(incident, call)
            if score > best_score:
                best = incident
                best_score = score
        if best is not None and best_score >= 2.5:
            best.add(call)
            continue
        incident = Incident()
        incident.add(call)
        incidents.append(incident)
    return incidents


def _incident_score(incident: Incident) -> float:
    score = min(len(incident.calls), 5) * 0.8
    score += min(sum(call.duration for call in incident.calls) / 60.0, 3)
    score += 2 if any(call.tone_detected for call in incident.calls) else 0
    score += 2 if any(call.hook_request for call in incident.calls) else 0
    score += min(sum(call.play_count for call in incident.calls) / 5.0, 2)
    score -= 1.5 if all(call.quality_flagged for call in incident.calls) else 0
    return round(max(score, 0), 2)


def _incident_key(day: str, incident: Incident) -> str:
    raw = f"{day}:{incident.first.call_id}:{incident.first.category}"
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()[:20]


def _persist_incidents(
    conn: sqlite3.Connection,
    day: str,
    incidents: Sequence[Incident],
    town: Optional[str],
    generated_at: str,
) -> None:
    if town:
        conn.execute(
            "DELETE FROM scanner_incidents WHERE day = ? AND LOWER(town) = LOWER(?)",
            (day, town),
        )
    else:
        conn.execute("DELETE FROM scanner_incidents WHERE day = ?", (day,))

    for incident in incidents:
        conn.execute(
            """
            INSERT INTO scanner_incidents (
                incident_key, day, town, dept, category, start_timestamp,
                end_timestamp, call_type, source_count, source_call_ids_json,
                units_json, addresses_json, tone_detected, quality_flagged,
                lifecycle_stages_json, returned_units_json, closed,
                notable_score, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                _incident_key(day, incident),
                day,
                incident.first.town,
                incident.first.dept,
                incident.first.category,
                incident.first.timestamp.isoformat(),
                incident.last.timestamp.isoformat(),
                incident.call_type,
                len(incident.calls),
                json.dumps(incident.source_call_ids),
                json.dumps(sorted(incident.units)),
                json.dumps(sorted(incident.addresses)),
                int(any(call.tone_detected for call in incident.calls)),
                int(any(call.quality_flagged for call in incident.calls)),
                json.dumps(incident.lifecycle_stages),
                json.dumps(sorted(incident.returned_units)),
                int(incident.closed),
                _incident_score(incident),
                generated_at,
                generated_at,
            ),
        )


def _count_by(values: Iterable[str]) -> Dict[str, int]:
    counts: Dict[str, int] = {}
    for value in values:
        key = value or "Unclassified"
        counts[key] = counts.get(key, 0) + 1
    return dict(sorted(counts.items(), key=lambda item: (-item[1], item[0])))


def _citation(call: CallRecord) -> Dict[str, Any]:
    excerpt = call.transcript
    if len(excerpt) > 260:
        excerpt = excerpt[:257].rstrip() + "..."
    return {
        "call_id": call.call_id,
        "timestamp": call.timestamp.isoformat(),
        "town": call.town,
        "dept": call.dept,
        "excerpt": excerpt,
        "audio_url": f"/scanner/audio/{call.filename}",
        "archive_url": f"/scanner/call/{call.call_id}",
    }


def _highlight_priority(incident: Incident) -> float:
    combined = " ".join(call.transcript.lower() for call in incident.calls)
    specific_bonus = 3 if any(term in combined for term in ("raccoon", "toaster")) else 0
    outcome = _police_outcome(incident)
    detail_bonus = 0
    detail_bonus += 2 if incident.closed else 0
    detail_bonus += 2 if _incident_response_seconds(incident) is not None else 0
    detail_bonus += 3 if outcome.get("type") not in {None, "not_heard", "cleared"} else 0
    detail_bonus += 1 if incident.call_type else 0
    return _incident_score(incident) + specific_bonus + detail_bonus


def _incident_response_seconds(incident: Incident) -> Optional[int]:
    dispatched_at = next(
        (
            call.timestamp
            for call in incident.calls
            if call.lifecycle_stage == "dispatched"
        ),
        None,
    )
    if dispatched_at is None:
        return None
    on_scene_at = next(
        (
            call.timestamp
            for call in incident.calls
            if call.lifecycle_stage == "on_scene"
            and call.timestamp >= dispatched_at
        ),
        None,
    )
    if on_scene_at is None:
        return None
    return max(0, round((on_scene_at - dispatched_at).total_seconds()))


def _police_outcome(incident: Incident) -> Dict[str, Any]:
    if incident.first.service != "police":
        return {}
    combined = " ".join(call.transcript.lower() for call in incident.calls)
    citation_issued = bool(CITATION_OUTCOME_RE.search(combined))
    warning_issued = bool(WARNING_OUTCOME_RE.search(combined))
    patterns = (
        (
            "arrest",
            "Arrest / custody",
            r"\b(?:under arrest|placed .*? custody|in custody|arrested)\b",
        ),
        (
            "citation",
            "Citation issued",
            CITATION_OUTCOME_RE,
        ),
        (
            "warning",
            "Warning issued",
            WARNING_OUTCOME_RE,
        ),
        (
            "transport",
            "Transported",
            r"\b(?:transporting|transported|en route to the station)\b",
        ),
        (
            "report",
            "Report taken",
            r"\b(?:report taken|taking a report|filed a report)\b",
        ),
        (
            "gone_on_arrival",
            "Gone on arrival",
            r"\b(?:gone on arrival|goa|unable to locate)\b",
        ),
        (
            "no_action",
            "No enforcement action",
            r"\b(?:no action|negative service|no violation|all set|peace restored)\b",
        ),
    )
    outcome: Dict[str, Any] = {}
    for outcome_type, label, pattern in patterns:
        matched = (
            bool(pattern.search(combined))
            if hasattr(pattern, "search")
            else bool(re.search(pattern, combined))
        )
        if matched:
            outcome = {"type": outcome_type, "label": label}
            break
    if not outcome and incident.closed:
        outcome = {"type": "cleared", "label": "Cleared; outcome not stated"}
    if not outcome:
        outcome = {"type": "not_heard", "label": "Outcome not heard"}
    outcome["citation_issued"] = citation_issued
    outcome["warning_issued"] = warning_issued
    return outcome


def _highlight_for_incident(incident: Incident) -> Dict[str, Any]:
    combined = " ".join(call.transcript.lower() for call in incident.calls)
    police_outcome = _police_outcome(incident)
    if incident.call_type:
        call_type = incident.call_type
    elif police_outcome.get("type") in {"warning", "citation"}:
        call_type = "Traffic stop"
    elif police_outcome.get("type") == "arrest":
        call_type = "Police arrest"
    elif incident.first.service == "fire" and any(
        term in combined
        for term in ("accidental trip", "alarm company", "system has been reset")
    ):
        call_type = "Alarm investigation"
    elif incident.first.service == "fire" and any(
        term in combined
        for term in ("medical", "patient", "transporting")
    ):
        call_type = "Medical response"
    else:
        call_type = "Scanner activity"
    title = f"{incident.first.town}: {call_type}"
    summary = f"{call_type} radio traffic was recorded in {incident.first.town}."
    ned_note = ""

    if "raccoon" in combined:
        summary = f"Animal control received assistance with a raccoon in {incident.first.town}."
        ned_note = "The raccoon has declined to explain how the storm-drain plan was supposed to end."
    elif "toaster" in combined:
        summary = f"A toaster-related alarm was cleared in {incident.first.town}."
        ned_note = "Breakfast briefly achieved full command-staff attention."
    elif call_type == "Alarm investigation" and "accidental" in combined:
        summary = (
            f"Crews traced an alarm in {incident.first.town} to an accidental activation."
        )
        ned_note = "The alarm achieved a full audience before admitting it was an accident."
    elif incident.call_type.lower() == "animal":
        ned_note = "Animal control once again drew the shift’s most unpredictable assignment."
    elif incident.call_type.lower() == "parking":
        ned_note = "The curb remains undefeated, but enforcement put points on the board."
    elif incident.call_type.lower() == "alarm":
        ned_note = "An alarm system made sure nobody enjoyed an entirely quiet shift."
    elif incident.call_type.lower() == "medical":
        ned_note = "The radio version of “drop what you’re doing” stayed in regular rotation."
    elif incident.call_type.lower() == "fire":
        ned_note = "The apparatus got the assignment; the station chairs began the waiting game."
    elif incident.call_type.lower() == "mva":
        ned_note = "Traffic once again submitted an unscheduled incident report."
    elif call_type.lower() == "traffic stop" and police_outcome.get("citation_issued"):
        ned_note = "Holy shit, they gave a citation!!! The ticket book has entered active service."
    elif call_type.lower() == "traffic stop" and police_outcome.get("warning_issued"):
        ned_note = "A warning was issued, allowing the citation book to preserve its resale value."
    elif incident.call_type:
        ned_note = f"{incident.call_type} made its contribution to the day’s radio traffic."
    else:
        ned_note = "Another transmission joined the day’s increasingly ambitious call sheet."

    response_seconds = _incident_response_seconds(incident)
    incident_span_seconds = max(
        0,
        round((incident.last.timestamp - incident.first.timestamp).total_seconds()),
    )
    return {
        "incident_key": _incident_key(incident.first.timestamp.date().isoformat(), incident),
        "title": title,
        "summary": summary,
        "ned_note": ned_note,
        "call_type": call_type,
        "town": incident.first.town,
        "source_count": len(incident.calls),
        "source_call_ids": [call.call_id for call in incident.calls],
        "service": incident.first.service,
        "lifecycle_stages": list(incident.lifecycle_stages),
        "closed": incident.closed,
        "started_at": incident.first.timestamp.isoformat(),
        "ended_at": incident.last.timestamp.isoformat(),
        "incident_span_seconds": incident_span_seconds,
        "recorded_audio_seconds": round(
            sum(call.duration for call in incident.calls),
            1,
        ),
        "response_time_seconds": response_seconds,
        "response_time_basis": (
            "dispatch_to_first_on_scene" if response_seconds is not None else None
        ),
        "units": sorted(incident.units),
        "outcome": police_outcome.get("label"),
        "outcome_type": police_outcome.get("type"),
        "citation_issued": bool(police_outcome.get("citation_issued")),
        "warning_issued": bool(police_outcome.get("warning_issued")),
        "citations": [_citation(call) for call in incident.calls[:4]],
    }


def _selected_highlights(incidents: Sequence[Incident]) -> List[Incident]:
    """Select varied highlights without excluding any subject or call type."""
    meaningful = [
        incident
        for incident in incidents
        if (
            incident.call_type
            or incident.closed
            or any(call.tone_detected for call in incident.calls)
            or _incident_response_seconds(incident) is not None
            or _police_outcome(incident).get("type")
            not in {None, "not_heard", "cleared"}
            or len(incident.calls) > 1
            or any(len(call.transcript) >= 80 for call in incident.calls)
        )
    ]
    highlight_candidates = sorted(
        meaningful or list(incidents)[:1],
        key=_highlight_priority,
        reverse=True,
    )
    selected: List[Incident] = []
    selected_types: Set[str] = set()
    for incident in highlight_candidates:
        normalized_type = incident.call_type.lower() or "unclassified"
        if normalized_type in selected_types:
            continue
        selected.append(incident)
        selected_types.add(normalized_type)
        if len(selected) == 4:
            return selected
    for incident in highlight_candidates:
        if incident in selected:
            continue
        selected.append(incident)
        if len(selected) == 4:
            break
    return selected


def _build_scope_fact_pack(
    calls: Sequence[CallRecord],
    incidents: Sequence[Incident],
    day: str,
    generated_at: str,
    town: Optional[str] = None,
    department: Optional[str] = None,
) -> Dict[str, Any]:
    town_counts = _count_by(call.town for call in calls)
    feed_counts = _count_by(call.category.upper() for call in calls)
    type_counts = _count_by(incident.call_type for incident in incidents)
    hour_counts = _count_by(call.timestamp.strftime("%H:00") for call in calls)
    police_outcomes = [
        _police_outcome(incident)
        for incident in incidents
        if incident.first.service == "police"
    ]
    response_times = [
        response_seconds
        for incident in incidents
        if (response_seconds := _incident_response_seconds(incident)) is not None
    ]
    return {
        "day": day,
        "scope": {"town": town, "department": department},
        "generated_at": generated_at,
        "totals": {
            "transmissions": len(calls),
            "estimated_incidents": len(incidents),
            "closed_incidents": sum(incident.closed for incident in incidents),
            "recorded_minutes": round(sum(call.duration for call in calls) / 60.0, 1),
            "feeds": len({call.category for call in calls if call.category}),
            "quality_flagged": sum(call.quality_flagged for call in calls),
            "measured_responses": len(response_times),
            "average_response_seconds": (
                round(sum(response_times) / len(response_times))
                if response_times
                else None
            ),
        },
        "breakdowns": {
            "towns": town_counts,
            "feeds": feed_counts,
            "call_types": type_counts,
            "hours": hour_counts,
            "police_outcomes": _count_by(
                outcome.get("label", "Outcome not heard")
                for outcome in police_outcomes
            ),
            "enforcement": {
                "warnings": sum(
                    bool(outcome.get("warning_issued"))
                    for outcome in police_outcomes
                ),
                "citations": sum(
                    bool(outcome.get("citation_issued"))
                    for outcome in police_outcomes
                ),
                "arrests": sum(
                    outcome.get("type") == "arrest"
                    for outcome in police_outcomes
                ),
            },
        },
        "highlights": [
            _highlight_for_incident(incident)
            for incident in _selected_highlights(incidents)
        ],
        "method": {
            "incident_grouping": (
                "department-aware lifecycle grouping using feed, time, address, "
                "units, call type, and fire/police closure language"
            ),
            "incident_counts_are_estimates": True,
            "highlight_filtering": "none by call type or transcript subject",
        },
    }


def _department_sections(
    calls: Sequence[CallRecord],
    incidents: Sequence[Incident],
    day: str,
    generated_at: str,
    town: Optional[str] = None,
) -> List[Dict[str, Any]]:
    present_services = {call.service for call in calls}
    ordered_services = [
        service
        for service in ("police", "fire", "unknown")
        if service in present_services
    ]
    return [
        _build_scope_fact_pack(
            [call for call in calls if call.service == service],
            [incident for incident in incidents if incident.first.service == service],
            day,
            generated_at,
            town=town,
            department=service,
        )
        for service in ordered_services
    ]


def build_daily_fact_pack(
    db_path: str = DEFAULT_DB_PATH,
    storage_db_path: Optional[str] = None,
    day: Optional[str] = None,
    town: Optional[str] = None,
    now: Optional[datetime] = None,
    persist: bool = True,
) -> Dict[str, Any]:
    """Build every network, town, and department view from one loaded call set."""
    resolved_day = parse_day(day, now=now)
    current = _local_naive(now or datetime.now())
    generated_at = current.isoformat(timespec="seconds")
    normalized_town = (town or "").strip() or None
    resolved_storage_path = _resolve_storage_path(db_path, storage_db_path)

    with _connect(db_path, readonly=True) as conn:
        calls = _load_calls(conn, resolved_day, normalized_town)
        watermark = _source_watermark(conn, resolved_day, normalized_town)
    if calls:
        from .call_enrichment import get_call_enrichment_map

        enrichment_map = get_call_enrichment_map(
            [call.call_id for call in calls],
            db_path=db_path,
            storage_db_path=resolved_storage_path,
        )
        for call in calls:
            enrichment = enrichment_map.get(call.call_id)
            if not enrichment or call.call_type:
                continue
            suggested_type = (
                enrichment.get("classification", {}).get("call_type") or ""
            ).strip()
            if suggested_type and float(enrichment.get("confidence") or 0) >= 0.7:
                call.call_type = suggested_type
    incidents = group_calls_into_incidents(calls)
    if persist:
        Path(resolved_storage_path).parent.mkdir(parents=True, exist_ok=True)
        with _connect(resolved_storage_path) as conn:
            ensure_intelligence_schema(conn)
            _persist_incidents(
                conn,
                resolved_day,
                incidents,
                normalized_town,
                generated_at,
            )

    fact_pack = _build_scope_fact_pack(
        calls,
        incidents,
        resolved_day,
        generated_at,
        town=normalized_town,
    )
    fact_pack["source_watermark"] = watermark
    fact_pack["departments"] = _department_sections(
        calls,
        incidents,
        resolved_day,
        generated_at,
        town=normalized_town,
    )
    fact_pack["towns"] = []
    town_priority = {"hopedale": 0, "milford": 1}
    town_names = sorted(
        {call.town for call in calls},
        key=lambda value: (
            town_priority.get(value.casefold(), 2),
            value.casefold(),
        ),
    )
    for town_name in town_names:
        town_calls = [
            call for call in calls if call.town.casefold() == town_name.casefold()
        ]
        town_incidents = [
            incident
            for incident in incidents
            if incident.first.town.casefold() == town_name.casefold()
        ]
        town_pack = _build_scope_fact_pack(
            town_calls,
            town_incidents,
            resolved_day,
            generated_at,
            town=town_name,
        )
        town_pack["departments"] = _department_sections(
            town_calls,
            town_incidents,
            resolved_day,
            generated_at,
            town=town_name,
        )
        fact_pack["towns"].append(town_pack)
    return fact_pack


def _first_item(counts: Dict[str, int]) -> tuple[str, int]:
    return next(iter(counts.items()), ("None", 0))


def _department_label(value: Optional[str]) -> str:
    return {
        "police": "Police",
        "fire": "Fire",
        "unknown": "Other",
    }.get(value or "", (value or "Other").title())


def _human_duration(total_seconds: Any) -> str:
    seconds = max(0, round(float(total_seconds or 0)))
    minutes, remaining_seconds = divmod(seconds, 60)
    hours, remaining_minutes = divmod(minutes, 60)
    if hours:
        return (
            f"{hours}h {remaining_minutes}m"
            if remaining_minutes
            else f"{hours}h"
        )
    if minutes:
        return (
            f"{minutes}m {remaining_seconds}s"
            if remaining_seconds
            else f"{minutes}m"
        )
    return f"{remaining_seconds}s"


def _comic_review(
    fact_pack: Dict[str, Any],
    busiest_town: str,
    busiest_town_count: int,
) -> str:
    scope = fact_pack.get("scope", {})
    town = scope.get("town")
    department = scope.get("department")
    totals = fact_pack.get("totals", {})
    breakdowns = fact_pack.get("breakdowns", {})
    enforcement = breakdowns.get("enforcement", {})
    warning_count = int(enforcement.get("warnings") or 0)
    citation_count = int(enforcement.get("citations") or 0)
    arrest_count = int(enforcement.get("arrests") or 0)
    classified_types = {
        call_type: count
        for call_type, count in breakdowns.get("call_types", {}).items()
        if call_type != "Unclassified"
    }
    top_type, top_type_count = _first_item(classified_types)
    if not classified_types:
        top_type = "Radio traffic"
        top_type_count = int(totals.get("estimated_incidents") or 0)

    if town and town.casefold() == "hopedale" and department != "fire":
        if citation_count:
            review = "Holy shit, they gave a citation!!!"
            if warning_count:
                review += f" Explicit outcomes also included {warning_count} warnings."
            return review
        if warning_count:
            noun = "warning" if warning_count == 1 else "warnings"
            return (
                f"Hopedale police outcomes explicitly included {warning_count} "
                f"{noun} and no heard citations."
            )
        return "No Hopedale warnings or citations were explicitly heard."

    if department == "police":
        if citation_count or warning_count or arrest_count:
            return (
                f"Explicit police outcomes included {warning_count} warnings, "
                f"{citation_count} citations, and {arrest_count} arrests."
            )
        return "No warning, citation, or arrest outcome was explicitly heard."

    if department == "fire":
        average_response = totals.get("average_response_seconds")
        response_line = (
            f" Measured dispatch-to-scene time averaged {_human_duration(average_response)}."
            if average_response is not None
            else ""
        )
        return (
            f"{totals.get('closed_incidents', 0):,} incident threads made it to a "
            f"heard return-to-quarters or termination.{response_line}"
        )

    if town:
        review = (
            f"{top_type} led {town} with {top_type_count:,} estimated incident "
            "threads."
        )
    else:
        review = (
            f"{busiest_town} had the most radio traffic with "
            f"{busiest_town_count:,} transmissions."
        )
    return review


def _compose_scope_take(fact_pack: Dict[str, Any]) -> Dict[str, Any]:
    totals = fact_pack["totals"]
    scope_town = fact_pack.get("scope", {}).get("town")
    scope_department = fact_pack.get("scope", {}).get("department")
    department_label = _department_label(scope_department)
    if scope_town and scope_department:
        scope_name = f"{scope_town} {department_label}"
    elif scope_town:
        scope_name = scope_town
    elif scope_department:
        scope_name = f"the network’s {department_label.lower()} traffic"
    else:
        scope_name = "the scanner network"
    busiest_town, busiest_town_count = _first_item(
        fact_pack.get("breakdowns", {}).get("towns", {})
    )
    highlights = fact_pack.get("highlights", [])

    if totals["transmissions"] == 0:
        return {
            "headline": f"Ned’s Take: No recorded traffic for {scope_name}",
            "straight_summary": "No scanner transmissions were found for this scope and date.",
            "ned_take": "Even the radios appear to have taken the day off.",
            "highlights": [],
            "disclaimer": "Incident totals are estimates produced from individual radio transmissions.",
        }

    summary_scope_name = (
        f"The {scope_name[4:]}" if scope_name.startswith("the ") else scope_name
    )
    straight_summary = (
        f"{summary_scope_name} recorded {totals['transmissions']:,} transmissions "
        f"across {totals['feeds']} feeds, representing approximately "
        f"{totals['estimated_incidents']:,} incident threads and "
        f"{totals['recorded_minutes']:.1f} minutes of audio."
    )
    if not scope_town and busiest_town_count:
        straight_summary += (
            f" {busiest_town} had the most radio traffic with "
            f"{busiest_town_count:,} transmissions."
        )

    ned_take = _comic_review(
        fact_pack,
        busiest_town,
        busiest_town_count,
    )

    if scope_town and scope_department:
        headline = f"{scope_town} {department_label}"
    elif scope_town:
        headline = scope_town
    elif scope_department:
        headline = f"Network {department_label}"
    else:
        headline = f"Ned’s Take for {fact_pack['day']}"
    return {
        "headline": headline,
        "straight_summary": straight_summary,
        "ned_take": ned_take,
        "highlights": highlights,
        "disclaimer": (
            "Incident totals are estimates produced by grouping radio transmissions. "
            "Highlights and commentary are grounded in the cited transmissions."
        ),
    }


def _compose_section(fact_pack: Dict[str, Any]) -> Dict[str, Any]:
    content = _compose_scope_take(fact_pack)
    content["scope"] = fact_pack.get("scope", {})
    content["totals"] = fact_pack.get("totals", {})
    content["departments"] = [
        _compose_section(department)
        for department in fact_pack.get("departments", [])
    ]
    return content


def compose_daily_take(fact_pack: Dict[str, Any]) -> Dict[str, Any]:
    content = _compose_section(fact_pack)
    content["towns"] = [
        _compose_section(town)
        for town in fact_pack.get("towns", [])
    ]
    return content


def _collect_source_ids(content: Dict[str, Any]) -> List[int]:
    ids: List[int] = []
    seen: Set[int] = set()

    def visit(value: Any) -> None:
        if isinstance(value, dict):
            call_id = value.get("call_id")
            if (
                isinstance(call_id, int)
                and ("archive_url" in value or "audio_url" in value)
                and call_id not in seen
            ):
                seen.add(call_id)
                ids.append(call_id)
            for nested in value.values():
                visit(nested)
        elif isinstance(value, list):
            for nested in value:
                visit(nested)

    visit(content)
    return ids


def _persist_incident_commentary(
    conn: sqlite3.Connection,
    content: Dict[str, Any],
    generated_at: str,
) -> None:
    """Store generated incident riffs independently from mutable rollups."""
    seen: Set[str] = set()

    def visit(section: Any) -> None:
        if isinstance(section, dict):
            incident_key = section.get("incident_key")
            commentary = _normalize_text(section.get("ned_note"))
            citations = section.get("citations")
            if (
                isinstance(incident_key, str)
                and incident_key not in seen
                and commentary
                and isinstance(citations, list)
            ):
                source_ids = section.get("source_call_ids") or [
                    citation.get("call_id")
                    for citation in citations
                    if isinstance(citation, dict)
                    and isinstance(citation.get("call_id"), int)
                ]
                conn.execute(
                    """
                    INSERT INTO scanner_incident_commentary (
                        incident_key, source_call_ids_json, commentary,
                        generated_at, generator, prompt_version
                    ) VALUES (?, ?, ?, ?, ?, ?)
                    ON CONFLICT(incident_key) DO UPDATE SET
                        source_call_ids_json = excluded.source_call_ids_json,
                        commentary = excluded.commentary,
                        generated_at = excluded.generated_at,
                        generator = excluded.generator,
                        prompt_version = excluded.prompt_version
                    """,
                    (
                        incident_key,
                        json.dumps(source_ids),
                        commentary,
                        generated_at,
                        "local-llm",
                        PROMPT_VERSION,
                    ),
                )
                seen.add(incident_key)
            for nested in section.values():
                visit(nested)
        elif isinstance(section, list):
            for nested in section:
                visit(nested)

    visit(content)


def _cached_take(
    conn: sqlite3.Connection,
    day: str,
    town_scope: str,
    edition_type: str,
    source_watermark: Dict[str, Any],
) -> Optional[Dict[str, Any]]:
    row = conn.execute(
        """
        SELECT * FROM scanner_daily_takes
        WHERE day = ? AND town_scope = ? AND edition_type = ?
        """,
        (day, town_scope, edition_type),
    ).fetchone()
    if not row:
        return None
    fact_pack = json.loads(row["fact_pack_json"])
    stored_watermark = _json_object(row["source_watermark_json"])
    if not stored_watermark:
        stored_watermark = _json_object(fact_pack.get("source_watermark"))
    # Final editions are immutable historical snapshots. Rolling editions are
    # reusable only while both their generator contract and source rows match.
    if edition_type != "final":
        if row["prompt_version"] != PROMPT_VERSION:
            return None
        if stored_watermark != source_watermark:
            return None
    return {
        "ok": True,
        "day": row["day"],
        "town": row["town_scope"] or None,
        "edition_type": row["edition_type"],
        "status": row["status"],
        "generated_at": row["generated_at"],
        "generator": row["generator"],
        "prompt_version": row["prompt_version"],
        "fact_pack": fact_pack,
        "take": json.loads(row["content_json"]),
        "source_call_ids": json.loads(row["source_call_ids_json"]),
        "source_watermark": stored_watermark,
        "cached": True,
    }


def _get_or_generate_daily_take_unlocked(
    db_path: str = DEFAULT_DB_PATH,
    storage_db_path: Optional[str] = None,
    day: Optional[str] = None,
    town: Optional[str] = None,
    edition_type: Optional[str] = None,
    force: bool = False,
    now: Optional[datetime] = None,
) -> Dict[str, Any]:
    current = _local_naive(now or datetime.now())
    resolved_day = parse_day(day, now=current)
    normalized_town = (town or "").strip()
    resolved_edition = edition_type or (
        "rolling" if resolved_day == current.date().isoformat() else "final"
    )
    if resolved_edition not in {"rolling", "final"}:
        raise ValueError("edition_type must be rolling or final")

    resolved_storage_path = _resolve_storage_path(db_path, storage_db_path)
    Path(resolved_storage_path).parent.mkdir(parents=True, exist_ok=True)
    with _connect(db_path, readonly=True) as source_conn:
        source_watermark = _source_watermark(
            source_conn,
            resolved_day,
            normalized_town or None,
        )
    with _connect(resolved_storage_path) as conn:
        ensure_intelligence_schema(conn)
        # A published final is never silently replaced, including by a retried
        # scheduler job. Rolling force refreshes remain available to the worker.
        if not force or resolved_edition == "final":
            cached = _cached_take(
                conn,
                resolved_day,
                normalized_town,
                resolved_edition,
                source_watermark,
            )
            if cached:
                return cached

    fact_pack = build_daily_fact_pack(
        db_path=db_path,
        storage_db_path=resolved_storage_path,
        day=resolved_day,
        town=normalized_town or None,
        now=current,
        persist=True,
    )
    content = compose_daily_take(fact_pack)
    source_ids = _collect_source_ids(content)
    generated_at = current.isoformat(timespec="seconds")

    with _connect(resolved_storage_path) as conn:
        ensure_intelligence_schema(conn)
        conn.execute(
            """
            INSERT INTO scanner_daily_takes (
                day, town_scope, edition_type, status, fact_pack_json,
                content_json, source_call_ids_json, generated_at, generator,
                prompt_version, source_watermark_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(day, town_scope, edition_type) DO UPDATE SET
                status = excluded.status,
                fact_pack_json = excluded.fact_pack_json,
                content_json = excluded.content_json,
                source_call_ids_json = excluded.source_call_ids_json,
                generated_at = excluded.generated_at,
                generator = excluded.generator,
                prompt_version = excluded.prompt_version,
                source_watermark_json = excluded.source_watermark_json
            """,
            (
                resolved_day,
                normalized_town,
                resolved_edition,
                "published",
                json.dumps(fact_pack),
                json.dumps(content),
                json.dumps(source_ids),
                generated_at,
                "deterministic",
                PROMPT_VERSION,
                json.dumps(fact_pack["source_watermark"]),
            ),
        )

    return {
        "ok": True,
        "day": resolved_day,
        "town": normalized_town or None,
        "edition_type": resolved_edition,
        "status": "published",
        "generated_at": generated_at,
        "generator": "deterministic",
        "prompt_version": PROMPT_VERSION,
        "fact_pack": fact_pack,
        "take": content,
        "source_call_ids": source_ids,
        "source_watermark": fact_pack["source_watermark"],
        "cached": False,
    }


def get_or_generate_daily_take(
    db_path: str = DEFAULT_DB_PATH,
    storage_db_path: Optional[str] = None,
    day: Optional[str] = None,
    town: Optional[str] = None,
    edition_type: Optional[str] = None,
    force: bool = False,
    now: Optional[datetime] = None,
    commentary_generator: Optional[
        Callable[[Dict[str, Any]], Dict[str, Any]]
    ] = None,
) -> Dict[str, Any]:
    """Return a fresh rolling take or an immutable final historical edition."""
    current = _local_naive(now or datetime.now())
    resolved_day = parse_day(day, now=current)
    normalized_town = (town or "").strip().casefold()
    resolved_edition = edition_type or (
        "rolling" if resolved_day == current.date().isoformat() else "final"
    )
    storage_path = _resolve_storage_path(db_path, storage_db_path)
    lock_key = "|".join(
        (str(Path(storage_path).resolve()), resolved_day, normalized_town, resolved_edition)
    )
    with _generation_lock(lock_key):
        result = _get_or_generate_daily_take_unlocked(
            db_path=db_path,
            storage_db_path=storage_db_path,
            day=resolved_day,
            town=town,
            edition_type=resolved_edition,
            force=force,
            now=current,
        )
        should_generate_commentary = (
            commentary_generator is not None
            and (resolved_edition == "rolling" or not result.get("cached"))
        )
        if not should_generate_commentary:
            stored_generator = result.get("generator") or "deterministic"
            result["commentary_generator"] = (
                "stored-local-llm"
                if "local-llm" in stored_generator
                else "deterministic"
            )
            return result

        try:
            generated_take = commentary_generator(result)
            if not isinstance(generated_take, dict):
                raise ValueError("commentary generator did not return a take object")
            result["take"] = generated_take
            result["commentary_generator"] = "local-llm"
            result["commentary_generated_at"] = current.isoformat(timespec="seconds")
        except Exception:
            logger.exception(
                "commentary.generate_failed day=%s edition=%s",
                resolved_day,
                resolved_edition,
            )
            result["commentary_generator"] = "deterministic-fallback"
            return result

        with _connect(storage_path) as conn:
            ensure_intelligence_schema(conn)
            conn.execute(
                """
                UPDATE scanner_daily_takes
                SET content_json = ?, generator = ?
                WHERE day = ? AND town_scope = ? AND edition_type = ?
                """,
                (
                    json.dumps(generated_take),
                    "deterministic+local-llm",
                    resolved_day,
                    (town or "").strip(),
                    resolved_edition,
                ),
            )
            _persist_incident_commentary(
                conn,
                generated_take,
                current.isoformat(timespec="seconds"),
            )
        result["generator"] = "deterministic+local-llm"
        return result


def get_incident_detail(
    incident_key: str,
    db_path: str = DEFAULT_DB_PATH,
    storage_db_path: Optional[str] = None,
) -> Optional[Dict[str, Any]]:
    """Load one persisted grouped incident and all of its source transmissions."""
    normalized_key = (incident_key or "").strip()
    if not re.fullmatch(r"[a-f0-9]{20}", normalized_key):
        return None
    storage_path = _resolve_storage_path(db_path, storage_db_path)
    if not Path(storage_path).exists():
        return None
    with _connect(storage_path) as conn:
        ensure_intelligence_schema(conn)
        row = conn.execute(
            "SELECT * FROM scanner_incidents WHERE incident_key = ? LIMIT 1",
            (normalized_key,),
        ).fetchone()
        commentary_row = conn.execute(
            """
            SELECT *
            FROM scanner_incident_commentary
            WHERE incident_key = ?
            """,
            (normalized_key,),
        ).fetchone()
    if not row:
        return None

    source_call_ids = [
        int(call_id)
        for call_id in json.loads(row["source_call_ids_json"] or "[]")
    ]
    commentary = None
    commentary_generated_at = None
    if commentary_row:
        commentary_source_ids = [
            int(call_id)
            for call_id in json.loads(
                commentary_row["source_call_ids_json"] or "[]"
            )
        ]
        if commentary_source_ids == source_call_ids:
            commentary = commentary_row["commentary"]
            commentary_generated_at = commentary_row["generated_at"]
    with _connect(db_path, readonly=True) as conn:
        calls = _load_calls_by_ids(conn, source_call_ids)
    if not calls:
        return None
    # Imported lazily to avoid coupling the core deterministic grouper to the
    # optional model-enrichment worker.
    from .call_enrichment import get_call_enrichment_map

    call_enrichments = get_call_enrichment_map(
        source_call_ids,
        db_path=db_path,
        storage_db_path=storage_path,
    )

    incident = Incident()
    for call in calls:
        incident.add(call)
    highlight = _highlight_for_incident(incident)
    call_payloads = []
    for call in calls:
        enrichment = call_enrichments.get(call.call_id) or {}
        call_payloads.append(
            {
                "call_id": call.call_id,
                "timestamp": call.timestamp.isoformat(),
                "town": call.town,
                "department": call.dept,
                "feed": call.category,
                "filename": call.filename,
                "duration_seconds": round(call.duration, 1),
                "transcript": call.transcript,
                "lifecycle_stage": call.lifecycle_stage,
                "units": sorted(call.units),
                "audio_url": f"/scanner/audio/{call.filename}",
                "archive_url": f"/scanner/call/{call.call_id}",
                "ai_enrichment": enrichment or None,
            }
        )

    return {
        "ok": True,
        "incident_key": normalized_key,
        "day": row["day"],
        "town": incident.first.town,
        "department": incident.first.service,
        "feed": incident.first.category,
        "call_type": highlight["call_type"],
        "title": highlight["title"],
        "summary": highlight["summary"],
        "commentary": commentary or highlight["ned_note"] or highlight["summary"],
        "commentary_generator": (
            "stored-local-llm" if commentary else "deterministic"
        ),
        "commentary_generated_at": commentary_generated_at,
        "started_at": highlight["started_at"],
        "ended_at": highlight["ended_at"],
        "incident_span_seconds": highlight["incident_span_seconds"],
        "recorded_audio_seconds": highlight["recorded_audio_seconds"],
        "response_time_seconds": highlight["response_time_seconds"],
        "outcome": highlight["outcome"],
        "outcome_type": highlight["outcome_type"],
        "closed": highlight["closed"],
        "units": highlight["units"],
        "lifecycle_stages": highlight["lifecycle_stages"],
        "source_call_ids": source_call_ids,
        "source_watermark": {
            "source_call_ids": source_call_ids,
            "last_timestamp": highlight["ended_at"],
        },
        "calls": call_payloads,
    }


def find_incident_key_for_call(
    call_id: int,
    db_path: str = DEFAULT_DB_PATH,
    storage_db_path: Optional[str] = None,
) -> Optional[str]:
    """Return the newest persisted incident containing a source call."""
    storage_path = _resolve_storage_path(db_path, storage_db_path)
    if not Path(storage_path).exists():
        return None
    with _connect(storage_path) as conn:
        ensure_intelligence_schema(conn)
        row = conn.execute(
            """
            SELECT incident_key
            FROM scanner_incidents
            WHERE EXISTS (
                SELECT 1
                FROM json_each(scanner_incidents.source_call_ids_json)
                WHERE CAST(json_each.value AS INTEGER) = ?
            )
            ORDER BY updated_at DESC
            LIMIT 1
            """,
            (int(call_id),),
        ).fetchone()
    return row["incident_key"] if row else None
