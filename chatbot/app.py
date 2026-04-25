# -----------------------------------------------------------------------------
# Tool call extraction and execution from AI response
# -----------------------------------------------------------------------------
import json
import re

def extract_and_execute_tool_call_from_response(response: str) -> dict:
    """
    Detects a <tools>...</tools> block in the AI response, extracts the JSON tool call,
    executes it using execute_tool_call_from_dict, and returns the result.
    Returns None if no tool call is found.
    """
    # Look for <tools>...</tools> block
    match = re.search(r'<tools>\s*(\{.*?\})\s*</tools>', response, re.DOTALL)
    if not match:
        return None
    try:
        tool_call_json = match.group(1)
        tool_call = json.loads(tool_call_json)
        return execute_tool_call_from_dict(tool_call)
    except Exception as e:
        return {"ok": False, "error": f"Failed to extract/execute tool call: {e}"}

# Example usage:
# ai_response = "...<tools>\n{\n  \"name\": \"find_fire_announcements\", ...}\n</tools>..."
# result = extract_and_execute_tool_call_from_response(ai_response)
import json
import os
import sqlite3
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

import requests
from flask import Flask, jsonify, request

# -----------------------------------------------------------------------------
# Configuration
# -----------------------------------------------------------------------------

VLLM_BASE_URL = os.environ.get("VLLM_BASE_URL", "http://127.0.0.1:30000/v1")
MODEL_CATALOG_PATH = os.environ.get(
    "MODEL_CATALOG_PATH",
    "/home/ned/Documents/neds_scanner_radio_full_pipeline_with_git/transcriber/model_catalog.json",
)
SCANNER_DB_PATH = os.environ.get(
    "SCANNER_DB_PATH",
    "/home/ned/data/scanner_calls/scanner_calls.db",
)
CHAT_MAX_TOOL_ROUNDS = int(os.environ.get("CHAT_MAX_TOOL_ROUNDS", "4"))
REQUEST_TIMEOUT_SECONDS = int(os.environ.get("VLLM_TIMEOUT_SECONDS", "120"))


def load_default_chat_model_from_catalog() -> Optional[str]:
    try:
        with open(MODEL_CATALOG_PATH, "r", encoding="utf-8") as fh:
            catalog = json.load(fh)
    except Exception:
        return None

    models = catalog.get("models", {})
    preferred_key = os.environ.get("VLLM_MODEL_KEY") or os.environ.get("LOCAL_LLM_MODEL_KEY")
    if preferred_key and isinstance(models.get(preferred_key), dict):
        model_value = models[preferred_key].get("model")
        if model_value:
            return model_value

    default_key = catalog.get("default_chat_model") or catalog.get("default_model")
    if default_key and isinstance(models.get(default_key), dict):
        default_cfg = models[default_key]
        if default_cfg.get("kind") == "chat" and default_cfg.get("model"):
            return default_cfg["model"]

    for model_cfg in models.values():
        if isinstance(model_cfg, dict) and model_cfg.get("kind") == "chat" and model_cfg.get("model"):
            return model_cfg["model"]

    return None


VLLM_MODEL = (
    os.environ.get("VLLM_MODEL")
    or os.environ.get("LOCAL_LLM_MODEL")
    or load_default_chat_model_from_catalog()
    or "Qwen/Qwen2.5-14B-Instruct-AWQ"
)

# -----------------------------------------------------------------------------
# Flask app
# -----------------------------------------------------------------------------

app = Flask(__name__, static_folder="static", static_url_path="/static")

# -----------------------------------------------------------------------------
# Schema constants
# -----------------------------------------------------------------------------

TABLE_NAME = "calls"

COL_ID = "id"
COL_TOWN = "town"
COL_STATE = "state"
COL_DEPT = "dept"
COL_CATEGORY = "category"
COL_FILENAME = "filename"
COL_JSON_PATH = "json_path"
COL_WAV_PATH = "wav_path"
COL_DURATION = "duration"
COL_RMS = "rms"
COL_TRANSCRIPT = "transcript"
COL_EDITED_TRANSCRIPT = "edited_transcript"
COL_TIMESTAMP = "timestamp"
COL_REVIEWED = "reviewed"
COL_PLAY_COUNT = "play_count"
COL_CLASSIFICATION = "classification"
COL_INTENT_LABELED = "intent_labeled"
COL_INTENT_LABELED_AT = "intent_labeled_at"
COL_EMBEDDING = "embedding"
COL_EXTRA = "extra"
COL_RAW_TRANSCRIPT = "raw_transcript"
COL_NORMALIZED_TRANSCRIPT = "normalized_transcript"
COL_TRANSCRIPTION_SCORE = "transcription_score"
COL_NEEDS_RETRY = "needs_retry"
COL_NEEDS_REVIEW = "needs_review"
COL_QUALITY_REASONS = "quality_reasons"
COL_PROFILE_USED = "profile_used"
COL_RETRY_PROFILES_TRIED = "retry_profiles_tried"
COL_TRANSCRIPTION_ENGINE = "transcription_engine"
COL_TRANSCRIPTION_MODEL = "transcription_model"
COL_HOOK_REQUEST = "hook_request"
COL_DERIVED_ADDRESS = "derived_address"
COL_DERIVED_STREET = "derived_street"
COL_DERIVED_ADDR_NUM = "derived_addr_num"
COL_DERIVED_TOWN = "derived_town"
COL_DERIVED_LAT = "derived_lat"
COL_DERIVED_LNG = "derived_lng"
COL_ADDRESS_CONFIDENCE = "address_confidence"
COL_SAVE_FOR_EVAL = "save_for_eval"
COL_FREEZE_FOR_TESTING = "freeze_for_testing"

# -----------------------------------------------------------------------------
# Heuristic outcome patterns
# -----------------------------------------------------------------------------

WARNING_PATTERNS = [
    "%warning issued%",
    "%issued a warning%",
    "%verbal warning%",
    "%written warning%",
    "%given a warning%",
    "%warning for%",
    "%advised and warned%",
    "%operator warned%",
    "%warning to operator%",
    "%warning given%",
]

CITATION_PATTERNS = [
    "%citation issued%",
    "%issued a citation%",
    "%issued citation%",
    "%written citation%",
    "%citation for%",
    "%ticket issued%",
    "%issued a ticket%",
    "%issued ticket%",
    "%summons issued%",
    "%criminal application issued%",
    "%civil citation%",
]

FIRE_ANNOUNCEMENT_PATTERNS = [
    # recall / recalling
    "%recall%",
    "%recalling%",
    "%announcing a recall%",
    "%product recall%",
    "%consumer recall%",
    "%units recalled%",
    "%recalling units%",
    "%recall to station%",
    # coverage / covering
    "%coverage%",
    "%providing coverage%",
    "%covering%",
    "%cover assignment%",
    "%station coverage%",
    "%mutual aid coverage%",
    # broadcast style
    "%be advised%",
    "%all units%",
]

# -----------------------------------------------------------------------------
# Prompt
# -----------------------------------------------------------------------------

SYSTEM_PROMPT = """You are a local scanner-call assistant for a private scanner application.

Rules:
1. Use tools whenever the user asks for current or database-backed information.
2. Never invent calls, units, addresses, times, counts, outcomes, classifications, or citations.
3. If a tool returns no results, say so plainly.
4. Be concise, accurate, and direct.
5. Prefer tool use over guessing.
6. Never mention internal implementation details, SQL, schemas, or server internals.
7. If the user asks an ambiguous question, do your best with the available tools and explain any ambiguity briefly.
8. If the user asks about warnings, use count_warnings when appropriate.
9. If the user asks about citations, tickets, or summonses, use count_citations or count_tickets as appropriate.
10. If the user asks about fire recall announcements, recalling units, coverage, or fire broadcast traffic, use find_fire_announcements.
11. If a tool returns citations, include a short Evidence section with call IDs and timestamps. Do not invent citations.
12. For "tickets", treat that as enforcement outcome analysis and return a breakdown of likely warnings versus likely citations when available.
"""

# -----------------------------------------------------------------------------
# Tool definitions
# -----------------------------------------------------------------------------

TOOLS: List[Dict[str, Any]] = [
    {
        "type": "function",
        "function": {
            "name": "get_stats",
            "description": "Get high-level counts and basic stats from the scanner calls database.",
            "parameters": {
                "type": "object",
                "properties": {
                    "town": {"type": "string"},
                    "department": {"type": "string"},
                    "date": {
                        "type": "string",
                        "description": "Date in YYYY-MM-DD format or relative values like today or yesterday."
                    }
                },
                "additionalProperties": False
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "search_calls",
            "description": "Search scanner calls by town, department, date range, street, unit, or text.",
            "parameters": {
                "type": "object",
                "properties": {
                    "town": {"type": "string"},
                    "department": {"type": "string"},
                    "start_time": {
                        "type": "string",
                        "description": "ISO datetime, YYYY-MM-DD, or relative term like today or yesterday"
                    },
                    "end_time": {
                        "type": "string",
                        "description": "ISO datetime, YYYY-MM-DD, or relative term like today or yesterday"
                    },
                    "street": {"type": "string"},
                    "unit": {"type": "string"},
                    "text": {"type": "string"},
                    "limit": {
                        "type": "integer",
                        "minimum": 1,
                        "maximum": 50
                    }
                },
                "additionalProperties": False
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "get_call_details",
            "description": "Get details for one specific call by ID.",
            "parameters": {
                "type": "object",
                "properties": {
                    "call_id": {"type": "integer"}
                },
                "required": ["call_id"],
                "additionalProperties": False
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "semantic_search_transcripts",
            "description": "Search transcripts by meaning or keywords. This v1 implementation uses best-effort text matching across transcript fields.",
            "parameters": {
                "type": "object",
                "properties": {
                    "query": {"type": "string"},
                    "town": {"type": "string"},
                    "department": {"type": "string"},
                    "start_time": {
                        "type": "string",
                        "description": "ISO datetime, YYYY-MM-DD, or relative term like today or yesterday"
                    },
                    "end_time": {
                        "type": "string",
                        "description": "ISO datetime, YYYY-MM-DD, or relative term like today or yesterday"
                    },
                    "limit": {
                        "type": "integer",
                        "minimum": 1,
                        "maximum": 25
                    }
                },
                "required": ["query"],
                "additionalProperties": False
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "count_warnings",
            "description": "Count likely warnings using transcript text heuristics. Useful for questions like how many warnings were issued in Hopedale yesterday.",
            "parameters": {
                "type": "object",
                "properties": {
                    "town": {"type": "string"},
                    "department": {
                        "type": "string",
                        "description": "Usually police for warning-related enforcement questions."
                    },
                    "date": {
                        "type": "string",
                        "description": "Date in YYYY-MM-DD format or relative values like today or yesterday."
                    },
                    "limit_examples": {
                        "type": "integer",
                        "minimum": 0,
                        "maximum": 10
                    }
                },
                "additionalProperties": False
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "count_citations",
            "description": "Count likely citations or tickets using transcript text heuristics. Useful for questions about citations, tickets, or summonses.",
            "parameters": {
                "type": "object",
                "properties": {
                    "town": {"type": "string"},
                    "department": {
                        "type": "string",
                        "description": "Usually police for citation-related enforcement questions."
                    },
                    "date": {
                        "type": "string",
                        "description": "Date in YYYY-MM-DD format or relative values like today or yesterday."
                    },
                    "limit_examples": {
                        "type": "integer",
                        "minimum": 0,
                        "maximum": 10
                    }
                },
                "additionalProperties": False
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "count_tickets",
            "description": "For ticket-related questions, return an enforcement breakdown of likely warnings versus likely citations, with totals and evidence.",
            "parameters": {
                "type": "object",
                "properties": {
                    "town": {"type": "string"},
                    "department": {
                        "type": "string",
                        "description": "Usually police for ticket-related enforcement questions."
                    },
                    "date": {
                        "type": "string",
                        "description": "Date in YYYY-MM-DD format or relative values like today or yesterday."
                    },
                    "limit_examples": {
                        "type": "integer",
                        "minimum": 0,
                        "maximum": 10
                    }
                },
                "additionalProperties": False
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "find_fire_announcements",
            "description": "Find fire department announcement-style traffic mentioning recall, recalling, coverage, mutual aid coverage, or broadcast phrases like all units or be advised.",
            "parameters": {
                "type": "object",
                "properties": {
                    "town": {"type": "string"},
                    "department": {
                        "type": "string",
                        "description": "Defaults to fire if omitted."
                    },
                    "date": {
                        "type": "string",
                        "description": "Date in YYYY-MM-DD format or relative values like today or yesterday."
                    },
                    "query": {
                        "type": "string",
                        "description": "Optional extra keyword like battery, product, station, mutual aid, or recall."
                    },
                    "limit": {
                        "type": "integer",
                        "minimum": 1,
                        "maximum": 25
                    }
                },
                "additionalProperties": False
            }
        }
    },
]

# -----------------------------------------------------------------------------
# Utilities
# -----------------------------------------------------------------------------

def db_connect() -> sqlite3.Connection:
    conn = sqlite3.connect(SCANNER_DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def normalize_department(value: Optional[str]) -> Optional[str]:
    if not value:
        return None

    v = value.strip().lower()

    mapping = {
        "fd": "fire",
        "fire": "fire",
        "fire department": "fire",
        "pd": "police",
        "police": "police",
        "police department": "police",
        "ticket": "police",
        "tickets": "police",
        "warning": "police",
        "warnings": "police",
        "citation": "police",
        "citations": "police",
        "summons": "police",
        "summonses": "police",
        "recall": "fire",
        "coverage": "fire",
        "ems": "unknown",
        "medical": "unknown",
        "unknown": "unknown",
    }

    return mapping.get(v, v)


def normalize_town(value: Optional[str]) -> Optional[str]:
    if not value:
        return None
    return value.strip()


def resolve_relative_date(value: Optional[str]) -> Optional[str]:
    if not value:
        return None

    v = value.strip().lower()
    now = datetime.now()

    if v == "today":
        return now.strftime("%Y-%m-%d")
    if v == "yesterday":
        return (now - timedelta(days=1)).strftime("%Y-%m-%d")
    if v == "tomorrow":
        return (now + timedelta(days=1)).strftime("%Y-%m-%d")

    return value.strip()


def try_parse_dateish(value: Optional[str]) -> Optional[str]:
    if not value:
        return None

    value = resolve_relative_date(value)

    fmts = [
        "%Y-%m-%d",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%dT%H:%M",
    ]

    for fmt in fmts:
        try:
            dt = datetime.strptime(value, fmt)
            if fmt == "%Y-%m-%d":
                return dt.strftime("%Y-%m-%d")
            return dt.strftime("%Y-%m-%d %H:%M:%S")
        except ValueError:
            continue

    return value


def shorten_text(value: Optional[str], max_len: int = 400) -> Optional[str]:
    if not value:
        return value
    value = value.strip()
    if len(value) <= max_len:
        return value
    return value[:max_len].rstrip() + "..."


def get_best_transcript_expr() -> str:
    return (
        f"COALESCE(NULLIF(TRIM({COL_EDITED_TRANSCRIPT}), ''), "
        f"NULLIF(TRIM({COL_TRANSCRIPT}), ''), "
        f"NULLIF(TRIM({COL_NORMALIZED_TRANSCRIPT}), ''), "
        f"NULLIF(TRIM({COL_RAW_TRANSCRIPT}), ''))"
    )


def build_date_filters(
    where: List[str],
    params: List[Any],
    start_time: Optional[str],
    end_time: Optional[str],
) -> None:
    if start_time:
        if len(start_time) == 10:
            where.append(f"date({COL_TIMESTAMP}) >= date(?)")
        else:
            where.append(f"{COL_TIMESTAMP} >= ?")
        params.append(start_time)

    if end_time:
        if len(end_time) == 10:
            where.append(f"date({COL_TIMESTAMP}) <= date(?)")
        else:
            where.append(f"{COL_TIMESTAMP} <= ?")
        params.append(end_time)


def build_citations_from_rows(rows: List[sqlite3.Row], transcript_key: str = "best_transcript") -> List[Dict[str, Any]]:
    citations: List[Dict[str, Any]] = []
    for row in rows:
        item = dict(row)
        citations.append(
            {
                "call_id": item.get("id"),
                "timestamp": item.get("timestamp"),
                "town": item.get("town"),
                "dept": item.get("dept"),
                "derived_street": item.get("derived_street"),
                "derived_town": item.get("derived_town"),
                "wav_path": item.get("wav_path"),
                "excerpt": shorten_text(item.get(transcript_key), 180),
            }
        )
    return citations


def build_pattern_clause(expr: str, patterns: List[str]) -> Tuple[str, List[Any]]:
    clauses = []
    params: List[Any] = []
    for pattern in patterns:
        clauses.append(f"LOWER({expr}) LIKE LOWER(?)")
        params.append(pattern)
    return "(" + " OR ".join(clauses) + ")", params


def classify_fire_announcement(text: Optional[str]) -> str:
    if not text:
        return "unknown"

    t = text.lower()

    if "recall" in t or "recalling" in t:
        return "recall"
    if "coverage" in t or "covering" in t or "cover assignment" in t:
        return "coverage"
    if "be advised" in t or "all units" in t:
        return "broadcast"

    return "announcement"

# -----------------------------------------------------------------------------
# Tool implementations
# -----------------------------------------------------------------------------

def tool_get_stats(
    town: Optional[str] = None,
    department: Optional[str] = None,
    date: Optional[str] = None,
) -> Dict[str, Any]:
    town = normalize_town(town)
    department = normalize_department(department)
    date = try_parse_dateish(date)

    sql = f"SELECT COUNT(*) AS total_calls FROM {TABLE_NAME}"
    where: List[str] = []
    params: List[Any] = []

    if town:
        where.append(f"LOWER({COL_TOWN}) = LOWER(?)")
        params.append(town)

    if department:
        where.append(f"LOWER({COL_DEPT}) = LOWER(?)")
        params.append(department)

    if date:
        where.append(f"date({COL_TIMESTAMP}) = date(?)")
        params.append(date)

    if where:
        sql += " WHERE " + " AND ".join(where)

    transcript_expr = get_best_transcript_expr()
    transcript_sql = f"SELECT COUNT(*) AS transcript_count FROM {TABLE_NAME}"
    transcript_where = where.copy()
    transcript_where.append(f"{transcript_expr} IS NOT NULL")
    transcript_sql += " WHERE " + " AND ".join(transcript_where)

    print("\n[SQL:get_stats]")
    print(sql)
    print("PARAMS:", params)
    print("[SQL:get_stats transcripts]")
    print(transcript_sql)
    print("PARAMS:", params)

    with db_connect() as conn:
        total_calls = conn.execute(sql, params).fetchone()["total_calls"]
        transcript_count = conn.execute(transcript_sql, params).fetchone()["transcript_count"]

    return {
        "ok": True,
        "filters": {
            "town": town,
            "department": department,
            "date": date,
        },
        "total_calls": total_calls,
        "calls_with_transcript": transcript_count,
        "citations": [],
    }


def tool_search_calls(
    town: Optional[str] = None,
    department: Optional[str] = None,
    start_time: Optional[str] = None,
    end_time: Optional[str] = None,
    street: Optional[str] = None,
    unit: Optional[str] = None,
    text: Optional[str] = None,
    limit: int = 20,
) -> Dict[str, Any]:
    town = normalize_town(town)
    department = normalize_department(department)
    start_time = try_parse_dateish(start_time)
    end_time = try_parse_dateish(end_time)
    limit = max(1, min(int(limit or 20), 50))

    best_transcript_expr = get_best_transcript_expr()

    sql = f"""
        SELECT
            {COL_ID} AS id,
            {COL_TOWN} AS town,
            {COL_DEPT} AS dept,
            {COL_TIMESTAMP} AS timestamp,
            {COL_DERIVED_STREET} AS derived_street,
            {COL_DERIVED_TOWN} AS derived_town,
            {COL_WAV_PATH} AS wav_path,
            {COL_REVIEWED} AS reviewed,
            {COL_NEEDS_REVIEW} AS needs_review,
            {COL_NEEDS_RETRY} AS needs_retry,
            {best_transcript_expr} AS best_transcript
        FROM {TABLE_NAME}
    """

    where: List[str] = []
    params: List[Any] = []

    if town:
        where.append(f"LOWER({COL_TOWN}) = LOWER(?)")
        params.append(town)

    if department:
        where.append(f"LOWER({COL_DEPT}) = LOWER(?)")
        params.append(department)

    build_date_filters(where, params, start_time, end_time)

    if street:
        street_like = f"%{street}%"
        where.append(
            f"""(
                LOWER(COALESCE({COL_DERIVED_STREET}, '')) LIKE LOWER(?)
                OR LOWER(COALESCE({COL_TRANSCRIPT}, '')) LIKE LOWER(?)
                OR LOWER(COALESCE({COL_EDITED_TRANSCRIPT}, '')) LIKE LOWER(?)
                OR LOWER(COALESCE({COL_NORMALIZED_TRANSCRIPT}, '')) LIKE LOWER(?)
                OR LOWER(COALESCE({COL_RAW_TRANSCRIPT}, '')) LIKE LOWER(?)
            )"""
        )
        params.extend([street_like, street_like, street_like, street_like, street_like])

    if unit:
        unit_like = f"%{unit}%"
        where.append(
            f"""(
                LOWER(COALESCE({COL_TRANSCRIPT}, '')) LIKE LOWER(?)
                OR LOWER(COALESCE({COL_EDITED_TRANSCRIPT}, '')) LIKE LOWER(?)
                OR LOWER(COALESCE({COL_NORMALIZED_TRANSCRIPT}, '')) LIKE LOWER(?)
                OR LOWER(COALESCE({COL_RAW_TRANSCRIPT}, '')) LIKE LOWER(?)
            )"""
        )
        params.extend([unit_like, unit_like, unit_like, unit_like])

    if text:
        text_like = f"%{text}%"
        where.append(
            f"""(
                LOWER(COALESCE({COL_TRANSCRIPT}, '')) LIKE LOWER(?)
                OR LOWER(COALESCE({COL_EDITED_TRANSCRIPT}, '')) LIKE LOWER(?)
                OR LOWER(COALESCE({COL_NORMALIZED_TRANSCRIPT}, '')) LIKE LOWER(?)
                OR LOWER(COALESCE({COL_RAW_TRANSCRIPT}, '')) LIKE LOWER(?)
            )"""
        )
        params.extend([text_like, text_like, text_like, text_like])

    if where:
        sql += " WHERE " + " AND ".join(where)

    sql += f" ORDER BY {COL_TIMESTAMP} DESC LIMIT ?"
    params.append(limit)

    print("\n[SQL:search_calls]")
    print(sql)
    print("PARAMS:", params)

    with db_connect() as conn:
        rows = conn.execute(sql, params).fetchall()

    results = []
    for row in rows:
        item = dict(row)
        item["best_transcript"] = shorten_text(item.get("best_transcript"), 400)
        results.append(item)

    citations = build_citations_from_rows(rows)

    return {
        "ok": True,
        "filters": {
            "town": town,
            "department": department,
            "start_time": start_time,
            "end_time": end_time,
            "street": street,
            "unit": unit,
            "text": text,
            "limit": limit,
        },
        "count": len(results),
        "results": results,
        "citations": citations,
    }


def tool_get_call_details(call_id: int) -> Dict[str, Any]:
    best_transcript_expr = get_best_transcript_expr()

    sql = f"""
        SELECT
            *,
            {best_transcript_expr} AS best_transcript
        FROM {TABLE_NAME}
        WHERE {COL_ID} = ?
        LIMIT 1
    """

    print("\n[SQL:get_call_details]")
    print(sql)
    print("PARAMS:", [call_id])

    with db_connect() as conn:
        row = conn.execute(sql, (call_id,)).fetchone()

    if not row:
        return {
            "ok": True,
            "found": False,
            "call_id": call_id,
            "citations": [],
        }

    item = dict(row)
    item["best_transcript"] = shorten_text(item.get("best_transcript"), 800)

    citations = [
        {
            "call_id": item.get("id"),
            "timestamp": item.get("timestamp"),
            "town": item.get("town"),
            "dept": item.get("dept"),
            "derived_street": item.get("derived_street"),
            "derived_town": item.get("derived_town"),
            "wav_path": item.get("wav_path"),
            "excerpt": shorten_text(item.get("best_transcript"), 180),
        }
    ]

    return {
        "ok": True,
        "found": True,
        "call": item,
        "citations": citations,
    }


def tool_semantic_search_transcripts(
    query: str,
    town: Optional[str] = None,
    department: Optional[str] = None,
    start_time: Optional[str] = None,
    end_time: Optional[str] = None,
    limit: int = 10,
) -> Dict[str, Any]:
    return tool_search_calls(
        town=town,
        department=department,
        start_time=start_time,
        end_time=end_time,
        text=query,
        limit=max(1, min(int(limit or 10), 25)),
    )


def _count_outcome_by_patterns(
    patterns: List[str],
    town: Optional[str],
    department: Optional[str],
    date: Optional[str],
    limit_examples: int,
    label: str,
) -> Dict[str, Any]:
    town = normalize_town(town)
    department = normalize_department(department or "police")
    date = try_parse_dateish(date)
    limit_examples = max(0, min(int(limit_examples or 5), 10))

    transcript_expr = get_best_transcript_expr()
    pattern_clause, pattern_params = build_pattern_clause(transcript_expr, patterns)

    base_sql = f"""
        SELECT
            {COL_ID} AS id,
            {COL_TOWN} AS town,
            {COL_DEPT} AS dept,
            {COL_TIMESTAMP} AS timestamp,
            {COL_DERIVED_STREET} AS derived_street,
            {COL_DERIVED_TOWN} AS derived_town,
            {COL_WAV_PATH} AS wav_path,
            {transcript_expr} AS best_transcript
        FROM {TABLE_NAME}
    """

    where = [f"{transcript_expr} IS NOT NULL"]
    params: List[Any] = []

    if town:
        where.append(f"LOWER({COL_TOWN}) = LOWER(?)")
        params.append(town)

    if department:
        where.append(f"LOWER({COL_DEPT}) = LOWER(?)")
        params.append(department)

    if date:
        where.append(f"date({COL_TIMESTAMP}) = date(?)")
        params.append(date)

    where.append(pattern_clause)
    params.extend(pattern_params)

    filtered_sql = base_sql + " WHERE " + " AND ".join(where)
    count_sql = f"SELECT COUNT(*) AS outcome_count FROM ({filtered_sql})"

    print(f"\n[SQL:{label} count]")
    print(count_sql)
    print("PARAMS:", params)

    with db_connect() as conn:
        outcome_count = conn.execute(count_sql, params).fetchone()["outcome_count"]

        rows: List[sqlite3.Row] = []
        if limit_examples > 0:
            examples_sql = filtered_sql + f" ORDER BY {COL_TIMESTAMP} DESC LIMIT ?"
            example_params = params + [limit_examples]

            print(f"[SQL:{label} examples]")
            print(examples_sql)
            print("PARAMS:", example_params)

            rows = conn.execute(examples_sql, example_params).fetchall()

    examples = []
    for row in rows:
        item = dict(row)
        item["best_transcript"] = shorten_text(item.get("best_transcript"), 300)
        examples.append(item)

    citations = build_citations_from_rows(rows)

    return {
        "ok": True,
        "filters": {
            "town": town,
            "department": department,
            "date": date,
        },
        f"{label}_count": outcome_count,
        "heuristic": True,
        "note": f"{label.capitalize()} count is based on transcript text patterns and may miss cases if no transcript exists or the wording differs.",
        "examples": examples,
        "citations": citations,
    }


def tool_count_warnings(
    town: Optional[str] = None,
    department: Optional[str] = None,
    date: Optional[str] = None,
    limit_examples: int = 5,
) -> Dict[str, Any]:
    return _count_outcome_by_patterns(
        patterns=WARNING_PATTERNS,
        town=town,
        department=department,
        date=date,
        limit_examples=limit_examples,
        label="warning",
    )


def tool_count_citations(
    town: Optional[str] = None,
    department: Optional[str] = None,
    date: Optional[str] = None,
    limit_examples: int = 5,
) -> Dict[str, Any]:
    return _count_outcome_by_patterns(
        patterns=CITATION_PATTERNS,
        town=town,
        department=department,
        date=date,
        limit_examples=limit_examples,
        label="citation",
    )


def tool_count_tickets(
    town: Optional[str] = None,
    department: Optional[str] = None,
    date: Optional[str] = None,
    limit_examples: int = 5,
) -> Dict[str, Any]:
    town = normalize_town(town)
    department = normalize_department(department or "police")
    date = try_parse_dateish(date)
    limit_examples = max(0, min(int(limit_examples or 5), 10))

    warning_result = tool_count_warnings(
        town=town,
        department=department,
        date=date,
        limit_examples=limit_examples,
    )
    citation_result = tool_count_citations(
        town=town,
        department=department,
        date=date,
        limit_examples=limit_examples,
    )

    if not warning_result.get("ok", False):
        return warning_result
    if not citation_result.get("ok", False):
        return citation_result

    warning_count = warning_result.get("warning_count", 0)
    citation_count = citation_result.get("citation_count", 0)

    combined_citations = []
    seen_ids = set()

    for source in [warning_result.get("citations", []), citation_result.get("citations", [])]:
        for citation in source:
            key = citation.get("call_id")
            if key not in seen_ids:
                seen_ids.add(key)
                combined_citations.append(citation)

    combined_citations = combined_citations[: max(limit_examples, 1) * 2]

    return {
        "ok": True,
        "filters": {
            "town": town,
            "department": department,
            "date": date,
        },
        "heuristic": True,
        "note": "Ticket-related breakdown is based on transcript text patterns and may miss cases if no transcript exists or the wording differs.",
        "ticket_breakdown": {
            "warnings": warning_count,
            "citations": citation_count,
            "total_likely_enforcement_outcomes": warning_count + citation_count,
        },
        "warning_examples": warning_result.get("examples", []),
        "citation_examples": citation_result.get("examples", []),
        "citations": combined_citations,
    }


def tool_find_fire_announcements(
    town: Optional[str] = None,
    department: Optional[str] = None,
    date: Optional[str] = None,
    query: Optional[str] = None,
    limit: int = 10,
) -> Dict[str, Any]:
    town = normalize_town(town)
    department = normalize_department(department or "fire")
    date = try_parse_dateish(date)
    limit = max(1, min(int(limit or 10), 25))

    transcript_expr = get_best_transcript_expr()
    pattern_clause, pattern_params = build_pattern_clause(transcript_expr, FIRE_ANNOUNCEMENT_PATTERNS)

    sql = f"""
        SELECT
            {COL_ID} AS id,
            {COL_TOWN} AS town,
            {COL_DEPT} AS dept,
            {COL_TIMESTAMP} AS timestamp,
            {COL_DERIVED_STREET} AS derived_street,
            {COL_DERIVED_TOWN} AS derived_town,
            {COL_WAV_PATH} AS wav_path,
            {transcript_expr} AS best_transcript
        FROM {TABLE_NAME}
    """

    where = [f"{transcript_expr} IS NOT NULL"]
    params: List[Any] = []

    if town:
        where.append(f"LOWER({COL_TOWN}) = LOWER(?)")
        params.append(town)

    if department:
        where.append(f"LOWER({COL_DEPT}) = LOWER(?)")
        params.append(department)

    if date:
        where.append(f"date({COL_TIMESTAMP}) = date(?)")
        params.append(date)

    where.append(pattern_clause)
    params.extend(pattern_params)

    if query:
        q_like = f"%{query}%"
        where.append(f"LOWER({transcript_expr}) LIKE LOWER(?)")
        params.append(q_like)

    sql += " WHERE " + " AND ".join(where)
    sql += f" ORDER BY {COL_TIMESTAMP} DESC LIMIT ?"
    params.append(limit)

    print("\n[SQL:find_fire_announcements]")
    print(sql)
    print("PARAMS:", params)

    with db_connect() as conn:
        rows = conn.execute(sql, params).fetchall()

    results = []
    for row in rows:
        item = dict(row)
        item["best_transcript"] = shorten_text(item.get("best_transcript"), 300)
        item["announcement_type"] = classify_fire_announcement(item.get("best_transcript"))
        results.append(item)

    citations = build_citations_from_rows(rows)

    type_breakdown: Dict[str, int] = {}
    for item in results:
        atype = item["announcement_type"]
        type_breakdown[atype] = type_breakdown.get(atype, 0) + 1

    return {
        "ok": True,
        "filters": {
            "town": town,
            "department": department,
            "date": date,
            "query": query,
            "limit": limit,
        },
        "count": len(results),
        "type_breakdown": type_breakdown,
        "results": results,
        "citations": citations,
        "note": "Fire announcement matching is heuristic and based on transcript text patterns such as recall, recalling, coverage, all units, and be advised.",
    }


# -----------------------------------------------------------------------------
# Tool registry
# -----------------------------------------------------------------------------

TOOL_FUNCTIONS = {
    "get_stats": tool_get_stats,
    "search_calls": tool_search_calls,
    "get_call_details": tool_get_call_details,
    "semantic_search_transcripts": tool_semantic_search_transcripts,
    "count_warnings": tool_count_warnings,
    "count_citations": tool_count_citations,
    "count_tickets": tool_count_tickets,
    "find_fire_announcements": tool_find_fire_announcements,
}

# -----------------------------------------------------------------------------
# Tool call interpreter
# -----------------------------------------------------------------------------

def execute_tool_call_from_dict(tool_call: dict) -> dict:
    """
    Given a tool call dict like:
    {
        "name": "find_fire_announcements",
        "arguments": {
            "town": "Hopedale",
            "department": "fire",
            "date": "today"
        }
    }
    execute the corresponding tool and return the result.
    """
    name = tool_call.get("name")
    arguments = tool_call.get("arguments", {})
    if name not in TOOL_FUNCTIONS:
        return {"ok": False, "error": f"Unknown tool: {name}"}
    try:
        result = TOOL_FUNCTIONS[name](**arguments)
        return result
    except Exception as e:
        return {"ok": False, "error": str(e)}

# Example usage:
# tool_call = {
#     "name": "find_fire_announcements",
#     "arguments": {"town": "Hopedale", "department": "fire", "date": "today"}
# }
# result = execute_tool_call_from_dict(tool_call)

# -----------------------------------------------------------------------------
# vLLM client helpers
# -----------------------------------------------------------------------------

def get_served_vllm_models() -> List[str]:
    response = requests.get(
        f"{VLLM_BASE_URL}/models",
        headers={"Content-Type": "application/json"},
        timeout=10,
    )
    response.raise_for_status()
    payload = response.json()
    models = []
    for item in payload.get("data", []):
        if isinstance(item, dict) and item.get("id"):
            models.append(item["id"])
    return models


def call_vllm_chat(messages: List[Dict[str, Any]], tools: Optional[List[Dict[str, Any]]] = None) -> Dict[str, Any]:
    global VLLM_MODEL

    payload: Dict[str, Any] = {
        "model": VLLM_MODEL,
        "messages": messages,
        "temperature": 0,
    }

    if tools:
        payload["tools"] = tools
        payload["tool_choice"] = "auto"

    url = f"{VLLM_BASE_URL}/chat/completions"
    response = requests.post(
        url,
        headers={"Content-Type": "application/json"},
        json=payload,
        timeout=REQUEST_TIMEOUT_SECONDS,
    )

    if response.status_code == 404:
        try:
            served_models = get_served_vllm_models()
        except Exception:
            served_models = []

        if served_models and VLLM_MODEL not in served_models:
            old_model = VLLM_MODEL
            VLLM_MODEL = served_models[0]
            payload["model"] = VLLM_MODEL
            print(f"[vLLM] Model {old_model!r} was not served; retrying with {VLLM_MODEL!r}.")
            response = requests.post(
                url,
                headers={"Content-Type": "application/json"},
                json=payload,
                timeout=REQUEST_TIMEOUT_SECONDS,
            )

    response.raise_for_status()
    return response.json()


def execute_tool_call(tool_call: Dict[str, Any]) -> Dict[str, Any]:
    fn = tool_call.get("function", {})
    name = fn.get("name")
    raw_arguments = fn.get("arguments", "{}")

    print("\n================ TOOL CALL ================")
    print("Tool:", name)
    print("Args Raw:", raw_arguments)
    print("==========================================\n")

    if name not in TOOL_FUNCTIONS:
        return {
            "ok": False,
            "error": f"Unknown tool: {name}"
        }

    try:
        arguments = json.loads(raw_arguments) if isinstance(raw_arguments, str) else raw_arguments
    except json.JSONDecodeError as exc:
        return {
            "ok": False,
            "error": f"Invalid JSON arguments for tool {name}: {str(exc)}"
        }

    try:
        result = TOOL_FUNCTIONS[name](**arguments)
        print("[TOOL RESULT SUMMARY]")
        print(
            json.dumps(
                {
                    k: v
                    for k, v in result.items()
                    if k not in ["examples", "results", "warning_examples", "citation_examples"]
                },
                indent=2,
                default=str,
            )
        )
        return result
    except TypeError as exc:
        return {
            "ok": False,
            "error": f"Invalid arguments for tool {name}: {str(exc)}"
        }
    except Exception as exc:
        return {
            "ok": False,
            "error": f"Tool {name} failed: {str(exc)}"
        }

# -----------------------------------------------------------------------------
# Citation extraction
# -----------------------------------------------------------------------------

def extract_citations_from_tool_messages(messages: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    collected: List[Dict[str, Any]] = []
    seen: set = set()

    for msg in messages:
        if msg.get("role") != "tool":
            continue

        content = msg.get("content")
        if not content:
            continue

        try:
            payload = json.loads(content)
        except Exception:
            continue

        citations = payload.get("citations", [])
        if not isinstance(citations, list):
            continue

        for citation in citations:
            if not isinstance(citation, dict):
                continue

            key = (
                citation.get("call_id"),
                citation.get("timestamp"),
                citation.get("dept"),
            )

            if key in seen:
                continue

            seen.add(key)
            collected.append(citation)

    return collected

# -----------------------------------------------------------------------------
# Chat orchestration
# -----------------------------------------------------------------------------

def run_tool_loop(user_messages: List[Dict[str, Any]]) -> Dict[str, Any]:
    messages: List[Dict[str, Any]] = [{"role": "system", "content": SYSTEM_PROMPT}]
    messages.extend(user_messages)

    for round_num in range(CHAT_MAX_TOOL_ROUNDS):
        print(f"\n[run_tool_loop] === Tool round {round_num+1} ===")
        print("[run_tool_loop] Messages sent to vLLM:")
        print(json.dumps(messages, indent=2, default=str))
        response = call_vllm_chat(messages, tools=TOOLS)
        print("[run_tool_loop] vLLM response:")
        print(json.dumps(response, indent=2, default=str))
        choice = response["choices"][0]["message"]


        tool_calls = choice.get("tool_calls", [])
        content = choice.get("content")

        # --- Qwen-style tool call support ---
        qwen_tool_call = None
        if not tool_calls and content:
            try:
                parsed = json.loads(content)
                if (
                    isinstance(parsed, dict)
                    and "name" in parsed
                    and "arguments" in parsed
                ):
                    # Synthesize a tool_call dict compatible with execute_tool_call
                    qwen_tool_call = {
                        "id": "qwen-fake-id",
                        "function": {
                            "name": parsed["name"],
                            "arguments": json.dumps(parsed["arguments"]),
                        },
                    }
            except Exception:
                pass

        if not tool_calls and not qwen_tool_call:
            citations = extract_citations_from_tool_messages(messages)
            return {
                "ok": True,
                "answer": content or "",
                "citations": citations,
                "raw": response,
            }

        # If Qwen tool call detected, treat as single tool call

        if qwen_tool_call:
            # Qwen-style: execute tool and return result directly, skip follow-up LLM round
            tool_result = execute_tool_call(qwen_tool_call)
            return {
                "ok": True,
                "answer": None,
                "tool_result": tool_result,
                "citations": tool_result.get("citations", []),
                "raw": tool_result,
            }

        # OpenAI-style: continue as before
        assistant_message: Dict[str, Any] = {
            "role": "assistant",
            "content": content,
            "tool_calls": tool_calls,
        }
        messages.append(assistant_message)

        for tool_call in tool_calls:
            tool_result = execute_tool_call(tool_call)
            messages.append(
                {
                    "role": "tool",
                    "tool_call_id": tool_call.get("id", "unknown"),
                    "content": json.dumps(tool_result, default=str),
                }
            )

    citations = extract_citations_from_tool_messages(messages)
    return {
        "ok": False,
        "error": f"Exceeded max tool rounds ({CHAT_MAX_TOOL_ROUNDS}).",
        "citations": citations,
    }


# -----------------------------------------------------------------------------
# Routes
# -----------------------------------------------------------------------------

# Serve the chatbot UI at the root URL
@app.route("/")
def index():
    return app.send_static_file("index.html")

@app.route("/api/chat/local", methods=["POST"])
def api_chat_local():
    payload = request.get_json(silent=True) or {}
    user_messages = payload.get("messages")

    if not isinstance(user_messages, list) or not user_messages:
        return jsonify(
            {
                "ok": False,
                "error": "Body must include a non-empty 'messages' list."
            }
        ), 400

    try:
        result = run_tool_loop(user_messages)
        status = 200 if result.get("ok") else 500
        return jsonify(result), status
    except requests.HTTPError as exc:
        return jsonify(
            {
                "ok": False,
                "error": f"vLLM HTTP error: {str(exc)}",
                "details": getattr(exc.response, "text", None),
            }
        ), 502
    except Exception as exc:
        return jsonify(
            {
                "ok": False,
                "error": f"Unhandled server error: {str(exc)}"
            }
        ), 500


@app.route("/api/chat/local/health", methods=["GET"])
def api_chat_local_health():
    return jsonify(
        {
            "ok": True,
            "vllm_base_url": VLLM_BASE_URL,
            "model": VLLM_MODEL,
            "db_path": SCANNER_DB_PATH,
            "tools": [t["function"]["name"] for t in TOOLS],
        }
    )


@app.route("/api/chat/local/tools", methods=["GET"])
def api_chat_local_tools():
    return jsonify(
        {
            "ok": True,
            "tools": TOOLS,
        }
    )

# -----------------------------------------------------------------------------
# Entrypoint
# -----------------------------------------------------------------------------

if __name__ == "__main__":
    print("\nAVAILABLE TOOLS:")
    for tool in TOOLS:
        print("-", tool["function"]["name"])
    print()

    app.run(host="0.0.0.0", port=5011, debug=True)
