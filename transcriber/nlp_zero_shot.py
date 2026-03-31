#!/usr/bin/env python3
"""
nlp_zero_shot.py — Dictionary-backed metadata enrichment for Ned's Scanner Network

Extracts addresses, units, agency, tone, urgency, and town/state from
scanner transcripts using the MassGIS street dictionary for validation.

Layer 1: Street dictionary lookup (SQLite streets table)
Layer 2: Smart regex with street-suffix awareness + dictionary cross-ref
Layer 3: Coordinate lookup from addresses table

No ML model required — pure Python + SQLite.
"""

import os
import re
import logging
import logging.handlers
import sys
from pathlib import Path

from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(__file__), ".env"))

# ── Make shared/ importable ──
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

# ======================================================
# Logging
# ======================================================
LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO").upper()
LOG_DIR = Path(os.environ.get("LOG_DIR", "/home/ned/data/scanner_calls/logs/transcriber_logs"))
LOG_DIR.mkdir(parents=True, exist_ok=True)

_log_fmt = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")

_console_handler = logging.StreamHandler()
_console_handler.setFormatter(_log_fmt)

_file_handler = logging.handlers.RotatingFileHandler(
    LOG_DIR / "nlp_zero_shot.log", maxBytes=10_000_000, backupCount=5
)
_file_handler.setFormatter(_log_fmt)

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    handlers=[_console_handler, _file_handler],
)
log = logging.getLogger("nlp-zero-shot")

# ======================================================
# Configuration
# ======================================================

# Known agency names for detection
AGENCIES = [
    "Hopedale PD",
    "Hopedale Fire",
    "Milford PD",
    "Milford Fire",
    "Bellingham PD",
    "Bellingham Fire",
    "Mendon PD",
    "Mendon Fire",
    "Upton PD",
    "Upton Fire",
    "Blackstone PD",
    "Blackstone Fire",
    "Franklin PD",
    "Franklin Fire",
]

# Feed → Town mapping
SOURCE_MAP = {
    "pd": "Hopedale", "hpd": "Hopedale", "fd": "Hopedale", "hfd": "Hopedale",
    "mfd": "Milford", "mpd": "Milford",
    "bfd": "Bellingham", "bpd": "Bellingham",
    "mndfd": "Mendon", "mndpd": "Mendon",
    "uptfd": "Upton", "uptpd": "Upton",
    "blkfd": "Blackstone", "blkpd": "Blackstone",
    "frkfd": "Franklin", "frkpd": "Franklin",
}

# Recognized street suffixes — Whisper may output full or abbreviated forms
STREET_SUFFIXES = {
    # Full forms
    "STREET", "AVENUE", "ROAD", "DRIVE", "LANE", "COURT", "CIRCLE",
    "WAY", "PLACE", "TERRACE", "BOULEVARD", "TRAIL", "PATH", "PARK",
    "SQUARE", "TURNPIKE", "PIKE", "HIGHWAY", "EXTENSION",
    # Common abbreviations
    "ST", "AVE", "RD", "DR", "LN", "CT", "CIR", "BLVD", "TRL", "PL",
    "TER", "TPKE", "HWY", "EXT", "SQ", "PKWY", "PARKWAY",
}

# Pre-directionals that appear before street base names
PRE_DIRECTIONALS = {"NORTH", "SOUTH", "EAST", "WEST", "N", "S", "E", "W"}

# ======================================================
# Street Dictionary Cache (loaded once)
# ======================================================
_street_cache: dict[str, list[dict]] = {}   # town_upper → [street_rows]
_base_cache: dict[str, set[str]] = {}       # town_upper → {base_name_upper, ...}
_name_cache: dict[str, set[str]] = {}       # town_upper → {full_street_name_upper, ...}
_all_streets_loaded = False


def _load_streets():
    """Load the streets table into memory for fast matching (runs once)."""
    global _all_streets_loaded
    if _all_streets_loaded:
        return

    try:
        from shared.scanner_db import get_conn
        with get_conn(readonly=True) as conn:
            rows = conn.execute("""
                SELECT street_name, str_name_base, pre_dir, post_type, town,
                       min_addr_num, max_addr_num, addr_count
                FROM streets ORDER BY addr_count DESC
            """).fetchall()

        for r in rows:
            town = (r["town"] or "").upper()
            street_dict = dict(r)

            _street_cache.setdefault(town, []).append(street_dict)
            if r["str_name_base"]:
                _base_cache.setdefault(town, set()).add(r["str_name_base"].upper())
            if r["street_name"]:
                _name_cache.setdefault(town, set()).add(r["street_name"].upper())

        total = sum(len(v) for v in _street_cache.values())
        _all_streets_loaded = True
        log.info(f"[NLP] Street dictionary loaded: {total:,} streets across {len(_street_cache)} towns")

    except Exception as e:
        log.warning(f"[NLP] Could not load street dictionary: {e}")
        _all_streets_loaded = True  # Don't retry every call


# ======================================================
# Address Extraction — Layer 2 (Smart Regex + Dictionary)
# ======================================================

# Pattern: <number> <optional punctuation/space> <one or more capitalized words>
# Handles: "42 Main Street", "9, September Drive", "100 South Main Street",
#           "number 9, September Drive", "to 55 Cedar Street", "18A Country Club Lane"
_ADDR_PATTERN = re.compile(
    r"""
    \b
    (?:number\s+)?                               # Optional "number" prefix
    (\d{1,5}[A-Za-z]?)                           # Address number with optional letter suffix
    [,.\s]+                                      # Separator (comma, period, space)
    (                                            # Street name group
        (?:[A-Z][a-zA-Z']+\s+){0,4}             # Up to 4 words
        [A-Z][a-zA-Z']+                          # Final word
    )
    \b
    """,
    re.VERBOSE
)

# Same pattern but case-insensitive for Whisper lowercase output
_ADDR_PATTERN_CI = re.compile(
    r"""
    \b
    (?:number\s+)?
    (\d{1,5}[A-Za-z]?)
    [,.\s]+
    (
        (?:[a-zA-Z][a-zA-Z']+\s+){0,4}
        [a-zA-Z][a-zA-Z']+
    )
    \b
    """,
    re.VERBOSE
)

# Street-only pattern (no number): "on Main Street", "at Hartford Avenue"
# Also includes context words that commonly precede street names in scanner traffic
_STREET_TRIGGER_WORDS = r"(?:on|at|to|from|near|off|off\s+of|out|by|stop|respond(?:ing)?\s*(?:to)?|that|is)"

_STREET_ONLY_PATTERN = re.compile(
    r"""
    \b""" + _STREET_TRIGGER_WORDS + r"""\s+
    (
        (?:[A-Z][a-zA-Z']+\s+){0,3}
        [A-Z][a-zA-Z']+
    )
    \b
    """,
    re.VERBOSE
)

# Case-insensitive version for Whisper
_STREET_ONLY_PATTERN_CI = re.compile(
    r"""
    \b""" + _STREET_TRIGGER_WORDS + r"""\s+
    (
        (?:[a-zA-Z][a-zA-Z']+\s+){0,3}
        [a-zA-Z][a-zA-Z']+
    )
    \b
    """,
    re.VERBOSE
)

# Intersection pattern: "Main and Elm", "Main Street and Elm Avenue", "Main by Elm"
_INTERSECTION_PATTERN = re.compile(
    r"""
    \b
    ([A-Z][a-zA-Z']+(?:\s+[A-Z][a-zA-Z']+){0,2})  # Street 1
    \s+(?:and|at|&|/|by)\s+
    ([A-Z][a-zA-Z']+(?:\s+[A-Z][a-zA-Z']+){0,2})  # Street 2
    \b
    """,
    re.VERBOSE | re.IGNORECASE
)

# Words that look like addresses but aren't
_SKIP_FIRST_WORDS = {
    "UNIT", "APT", "APARTMENT", "FLOOR", "ROOM", "BUILDING", "BLDG",
    "COPY", "CODE", "RESPONDING", "RESPONSE", "REPORTED", "REPORT",
    "COMPLAINANT", "CALLER", "DISPATCH", "STATION", "HEADQUARTERS",
    "CHANNEL", "FREQUENCY", "BADGE", "CAR", "ENGINE", "LADDER", "TRUCK",
    "THE", "A", "AN", "IS", "IT", "ON", "IN", "AT", "TO", "OF", "FOR",
    "ALSO", "HER", "HIS", "ITS", "MY", "YOUR", "OUR", "THEIR",
    "WE", "THEY", "HE", "SHE", "WILL", "CAN", "DO", "HAVE",
    "CONTROL", "RECEIVED", "ROGER", "COPY", "CLEAR",
}


def _validate_street_name(street_words: str, town: str = None) -> dict | None:
    """Check if the extracted street name matches the street dictionary.
    Returns a match dict with confidence info, or None."""
    _load_streets()
    words = street_words.upper().strip()

    # Skip if the "street" is really just a common word
    first_word = words.split()[0] if words.split() else ""
    if first_word in _SKIP_FIRST_WORDS:
        return None

    # Treat "Unknown" as None (search all towns)
    if town and town.upper() == "UNKNOWN":
        town = None

    towns_to_check = [town.upper()] if town else list(_street_cache.keys())

    # Strategy 1: Exact full name match
    for t in towns_to_check:
        if words in _name_cache.get(t, set()):
            for s in _street_cache.get(t, []):
                if s["street_name"].upper() == words:
                    return {"street": s, "confidence": "high", "match_type": "exact"}

    # Parse the candidate
    word_list = words.split()
    last_word = word_list[-1] if word_list else ""
    if last_word in STREET_SUFFIXES and len(word_list) > 1:
        base_candidate = " ".join(word_list[:-1])
    else:
        base_candidate = words

    # Strategy 2: Match base name against str_name_base
    for t in towns_to_check:
        for s in _street_cache.get(t, []):
            s_base = (s["str_name_base"] or "").upper()
            if not s_base:
                continue

            # Direct base match: "MAIN" == "MAIN"
            if base_candidate == s_base:
                return {"street": s, "confidence": "high", "match_type": "base_match"}

            # Base + pre_dir: "SOUTH MAIN" → pre_dir=SOUTH, base=MAIN
            if len(word_list) >= 2:
                first = word_list[0].upper()
                rest_words = word_list[1:]
                if rest_words[-1] in STREET_SUFFIXES and len(rest_words) > 1:
                    rest_words = rest_words[:-1]
                rest = " ".join(rest_words)

                if first in PRE_DIRECTIONALS and rest.upper() == s_base:
                    pre = (s.get("pre_dir") or "").upper()
                    if pre and first.startswith(pre[0]):
                        return {"street": s, "confidence": "high", "match_type": "predir_base"}

            # Fuzzy: base name is IN the candidate (4+ chars to avoid false positives)
            if s_base in base_candidate.upper() and len(s_base) >= 4:
                return {"street": s, "confidence": "medium", "match_type": "base_in_candidate"}

    # Strategy 3: Prefix match against full street names
    for t in towns_to_check:
        for s in _street_cache.get(t, []):
            s_name = (s["street_name"] or "").upper()
            if s_name.startswith(words) and len(words) >= 4:
                return {"street": s, "confidence": "medium", "match_type": "prefix_match"}

    return None


def _normalize_text_for_address(text: str) -> str:
    """Light normalization to help address extraction."""
    t = text
    # Collapse hyphenated numbers: "1-2-3" → "123"
    t = re.sub(r"\b(\d+)-(\d+)\b", r"\1\2", t)
    # Convert number-words to digits (Whisper sometimes transcribes numbers as words)
    _NUMBER_WORDS = {
        "one": "1", "two": "2", "three": "3", "four": "4", "five": "5",
        "six": "6", "seven": "7", "eight": "8", "nine": "9", "ten": "10",
        "eleven": "11", "twelve": "12", "thirteen": "13", "fourteen": "14",
        "fifteen": "15", "sixteen": "16", "seventeen": "17", "eighteen": "18",
        "nineteen": "19", "twenty": "20", "thirty": "30", "forty": "40",
        "fifty": "50", "sixty": "60", "seventy": "70", "eighty": "80",
        "ninety": "90",
    }
    for word, digit in _NUMBER_WORDS.items():
        # Only replace when followed by a space and a capitalized word (likely street name)
        t = re.sub(
            rf"\b{word}\s+([A-Z])",
            f"{digit} \\1",
            t,
            flags=re.IGNORECASE,
        )
    return t


def extract_address(text: str, town: str = None) -> dict:
    """Extract the best address from a transcript using dictionary validation.

    Tries multiple strategies in priority order:
    1. Number + Street (case-sensitive, then case-insensitive)
    2. Intersection ("Main and Elm")
    3. Street-only mention ("on Main Street")

    Returns a dict with keys:
        address_number, address_street, full_address,
        latitude, longitude, confidence, match_type, intersection
    All keys present; values are None when not found.
    """
    _load_streets()

    result = {
        "address_number": None,
        "address_street": None,
        "full_address": None,
        "latitude": None,
        "longitude": None,
        "confidence": "none",
        "match_type": None,
        "intersection": None,
    }

    norm_text = _normalize_text_for_address(text)
    best_match = None
    best_score = 0

    # ── Strategy 1: Numeric address patterns (try both case variants) ──
    for pattern in [_ADDR_PATTERN, _ADDR_PATTERN_CI]:
        for m in pattern.finditer(norm_text):
            number_str = m.group(1)
            street_part = m.group(2).strip()
            # Strip letter suffix (e.g. "18A" → 18) for numeric comparisons
            number = int(re.sub(r'[A-Za-z]', '', number_str) or '0')

            # Skip numbers that are probably unit/radio codes, not addresses
            if number > 9999 or number == 0:
                continue

            validated = _validate_street_name(street_part, town)
            if validated:
                street_info = validated["street"]
                min_n = street_info.get("min_addr_num") or 0
                max_n = street_info.get("max_addr_num") or 99999
                addr_count = street_info.get("addr_count") or 1

                score = 0
                if validated["confidence"] == "high":
                    score += 100
                elif validated["confidence"] == "medium":
                    score += 50

                # Bonus for number being in the known range
                if min_n <= number <= max_n:
                    score += 50
                elif number <= max_n * 1.2:
                    score += 20

                # Bonus for street popularity
                score += min(addr_count // 10, 30)

                if score > best_score:
                    best_score = score
                    best_match = {
                        "number": number_str,
                        "street_part": street_part,
                        "street_info": street_info,
                        "validated": validated,
                        "score": score,
                    }

    if best_match:
        street_info = best_match["street_info"]
        official_street = street_info["street_name"]
        number = best_match["number"]
        match_town = street_info.get("town", town or "")

        result["address_number"] = number
        result["address_street"] = official_street
        result["full_address"] = f"{number} {official_street}"
        result["confidence"] = best_match["validated"]["confidence"]
        result["match_type"] = best_match["validated"]["match_type"]

        # Layer 3: Get coordinates from the addresses table
        num_int = int(re.sub(r'[A-Za-z]', '', number) or '0')
        _resolve_coordinates(result, num_int, official_street, match_town)
        return result

    # ── Strategy 2: Intersection patterns: "Main and Elm" ──
    for m in _INTERSECTION_PATTERN.finditer(norm_text):
        st1, st2 = m.group(1).strip(), m.group(2).strip()
        v1 = _validate_street_name(st1, town)
        v2 = _validate_street_name(st2, town)
        if v1 and v2:
            s1_name = v1["street"]["street_name"]
            s2_name = v2["street"]["street_name"]
            result["intersection"] = f"{s1_name} & {s2_name}"
            result["full_address"] = f"{s1_name} & {s2_name}"
            result["address_street"] = s1_name
            result["confidence"] = "medium"
            result["match_type"] = "intersection"
            # Approximate coords from the midpoint of both streets
            _resolve_street_midpoint(result, s1_name, v1["street"].get("town", town))
            return result
        elif v1:
            result["address_street"] = v1["street"]["street_name"]
            result["confidence"] = "low"
            result["match_type"] = "partial_intersection"
            result["full_address"] = v1["street"]["street_name"]
            _resolve_street_midpoint(result, v1["street"]["street_name"],
                                     v1["street"].get("town", town))
            return result

    # ── Strategy 3: Street-only mention ("on Main Street", "at Hartford Ave") ──
    for pattern in [_STREET_ONLY_PATTERN, _STREET_ONLY_PATTERN_CI]:
        for m in pattern.finditer(norm_text):
            street_part = m.group(1).strip()
            validated = _validate_street_name(street_part, town)
            if validated and validated["confidence"] in ("high", "medium"):
                s_info = validated["street"]
                result["address_street"] = s_info["street_name"]
                result["full_address"] = s_info["street_name"]
                result["confidence"] = "low"
                result["match_type"] = "street_only"
                _resolve_street_midpoint(result, s_info["street_name"],
                                         s_info.get("town", town))
                return result

    # ── Strategy 4: Unvalidated fallback — suffix-based extraction ──
    # If the street is NOT in MassGIS but clearly looks like an address
    # (ends with STREET, DRIVE, etc.), report it at low confidence.
    # This catches streets not in our 7-town database.
    best_unvalidated = None
    best_unval_len = 0

    for pattern in [_ADDR_PATTERN, _ADDR_PATTERN_CI]:
        for m in pattern.finditer(norm_text):
            number_str = m.group(1)
            street_part = m.group(2).strip()
            number = int(re.sub(r'[A-Za-z]', '', number_str) or '0')
            if number > 9999 or number == 0:
                continue

            words = street_part.upper().split()
            first_word = words[0] if words else ""

            if first_word in _SKIP_FIRST_WORDS:
                continue

            # Trim trailing non-suffix words (e.g., "Midway Road Medical" → "Midway Road")
            trimmed = list(words)
            while trimmed and trimmed[-1] not in STREET_SUFFIXES:
                trimmed.pop()

            if trimmed and len(trimmed) >= 2:
                street_clean = " ".join(w.title() for w in trimmed)
                length = len(street_clean)
                if length > best_unval_len:
                    best_unval_len = length
                    best_unvalidated = {
                        "number": number_str,
                        "street": street_clean,
                    }

    if best_unvalidated:
        result["address_number"] = best_unvalidated["number"]
        result["address_street"] = best_unvalidated["street"]
        result["full_address"] = f"{best_unvalidated['number']} {best_unvalidated['street']}"
        result["confidence"] = "low"
        result["match_type"] = "suffix_unvalidated"
        return result

    # ── Strategy 5: Unvalidated street-only with suffix ──
    for pattern in [_STREET_ONLY_PATTERN, _STREET_ONLY_PATTERN_CI]:
        for m in pattern.finditer(norm_text):
            street_part = m.group(1).strip()
            words = street_part.upper().split()
            first_word = words[0] if words else ""

            if first_word in _SKIP_FIRST_WORDS:
                continue

            # Trim trailing non-suffix words
            trimmed = list(words)
            while trimmed and trimmed[-1] not in STREET_SUFFIXES:
                trimmed.pop()

            if trimmed and len(trimmed) >= 2:
                street_clean = " ".join(w.title() for w in trimmed)
                result["address_street"] = street_clean
                result["full_address"] = street_clean
                result["confidence"] = "low"
                result["match_type"] = "street_only_unvalidated"
                return result

    # ── Strategy 6: Bare street name (no trigger word) validated against DB ──
    # Scans for any "<Word(s)> <Suffix>" pattern and checks the dictionary.
    # Lower priority because no contextual trigger = higher false positive risk.
    _BARE_STREET = re.compile(
        r"\b([A-Z][a-zA-Z']+(?:\s+[A-Z][a-zA-Z']+){0,3})\s+"
        r"(" + "|".join(sorted(STREET_SUFFIXES, key=len, reverse=True)) + r")\b",
        re.IGNORECASE
    )
    for m in _BARE_STREET.finditer(norm_text):
        candidate = f"{m.group(1)} {m.group(2)}".strip()
        # Try town-specific first, then fall back to all towns
        validated = _validate_street_name(candidate, town)
        if not validated:
            validated = _validate_street_name(candidate, None)
        if validated and validated["confidence"] in ("high", "medium"):
            s_info = validated["street"]
            result["address_street"] = s_info["street_name"]
            result["full_address"] = s_info["street_name"]
            result["confidence"] = "low"
            result["match_type"] = "bare_street_validated"
            _resolve_street_midpoint(result, s_info["street_name"],
                                     s_info.get("town", town))
            return result

    return result


def _resolve_coordinates(result: dict, number: int, street: str, town: str):
    """Look up exact or approximate coordinates for an address."""
    try:
        from shared.scanner_db import get_address_coords, get_conn
        coords = get_address_coords(number, street, town)
        if coords:
            result["latitude"] = coords["latitude"]
            result["longitude"] = coords["longitude"]
        else:
            _resolve_street_midpoint(result, street, town)
            if result["confidence"] == "high":
                result["confidence"] = "medium"
    except Exception as e:
        log.debug(f"[NLP] Coords lookup failed: {e}")


def _resolve_street_midpoint(result: dict, street: str, town: str):
    """Get approximate center of a street as fallback coordinates."""
    try:
        from shared.scanner_db import get_conn
        with get_conn(readonly=True) as conn:
            row = conn.execute("""
                SELECT AVG(latitude) as lat, AVG(longitude) as lng
                FROM addresses
                WHERE street_name = ? AND UPPER(town) = UPPER(?)
                  AND latitude IS NOT NULL
            """, (street, town or "")).fetchone()
            if row and row["lat"]:
                result["latitude"] = row["lat"]
                result["longitude"] = row["lng"]
    except Exception as e:
        log.debug(f"[NLP] Street midpoint lookup failed: {e}")


# ======================================================
# Main Enrichment
# ======================================================
def enrich_metadata(text: str, meta: dict) -> dict:
    """Extract address, units, tone, agency, urgency and add town/state.

    Now uses dictionary-backed address extraction with coordinate lookup.
    """
    extra = {}

    # === Town/State mapping based on feed source ===
    feed = meta.get("source") or ""
    town = SOURCE_MAP.get(feed.lower(), "Unknown")
    extra["town"] = town
    extra["state"] = "Massachusetts"

    # === Dictionary-backed address extraction ===
    addr = extract_address(text, town if town != "Unknown" else None)
    extra["location"] = addr["full_address"]
    extra["address_number"] = addr["address_number"]
    extra["address_street"] = addr["address_street"]
    extra["address_confidence"] = addr["confidence"]
    extra["address_match_type"] = addr["match_type"]
    extra["intersection"] = addr["intersection"]

    # Populate top-level derived fields for the calls table
    if addr["full_address"]:
        meta["derived_address"] = addr["full_address"]
        meta["derived_street"] = addr["address_street"]
        meta["derived_addr_num"] = addr["address_number"]
        meta["derived_town"] = addr.get("town") or town
        meta["derived_lat"] = addr["latitude"]
        meta["derived_lng"] = addr["longitude"]
        meta["address_confidence"] = addr["confidence"]

    # === Units (e.g., P-1, E-2, C-3, Car-1) ===
    units = re.findall(r"\b[A-Z]{1,3}-?\d{1,3}\b", text)
    # Filter out the address number if it was captured as a "unit"
    addr_num = addr.get("address_number")
    if addr_num and units:
        units = [u for u in units if u != addr_num]
    if units:
        extra["units"] = units

    # === Tone detection ===
    extra["tone_detected"] = bool(re.search(r"\bTONE\b", text, re.IGNORECASE))

    # === Agency detection ===
    for agency in AGENCIES:
        if agency.lower() in text.lower():
            extra["agency"] = agency
            break

    # === Call type patterns ===
    call_type_patterns = [
        (r"\b(motor vehicle accident|mva|car accident|crash)\b", "MVA"),
        (r"\b(domestic|domestic disturbance)\b", "Domestic"),
        (r"\b(breaking and entering|b&e|break.in)\b", "B&E"),
        (r"\b(larceny|theft|shoplifting|stolen)\b", "Larceny"),
        (r"\b(medical|ambulance|ems|rescue)\b", "Medical"),
        (r"\b(fire alarm|smoke|structure fire|brush fire)\b", "Fire"),
        (r"\b(disturbance|loud noise|noise complaint)\b", "Disturbance"),
        (r"\b(suspicious|suspicious person|suspicious vehicle)\b", "Suspicious"),
        (r"\b(traffic stop|motor vehicle stop)\b", "Traffic Stop"),
        (r"\b(well.?being|welfare check)\b", "Welfare Check"),
        (r"\b(trespassing|trespass)\b", "Trespass"),
        (r"\b(vandalism|criminal mischief)\b", "Vandalism"),
        (r"\b(missing person|missing child)\b", "Missing Person"),
        (r"\b(alarm|burglar alarm|security alarm)\b", "Alarm"),
        (r"\b(harassment|threats)\b", "Harassment"),
        (r"\b(overdose|narcotics|drugs)\b", "Overdose/Narcotics"),
        (r"\b(assault|fight|altercation)\b", "Assault"),
        (r"\b(warrant|arrest)\b", "Warrant/Arrest"),
        (r"\b(parking|parking complaint|illegally parked)\b", "Parking"),
        (r"\b(animal|dog|loose dog)\b", "Animal"),
    ]
    text_lower = text.lower()
    for pattern, call_type in call_type_patterns:
        if re.search(pattern, text_lower):
            extra["call_type"] = call_type
            break

    # === Urgency / priority markers ===
    urgency_map = {
        "code 3": "high",
        "priority 1": "high",
        "priority 2": "medium",
        "code 2": "medium",
        "routine": "low",
        "code 1": "low",
    }
    for k, v in urgency_map.items():
        if k in text_lower:
            extra["urgency"] = v
            break

    # Merge into classification dict
    meta["classification"].update(extra)
    return meta


# ======================================================
# Reusable in-memory API
# ======================================================
def enrich_meta_in_memory(meta: dict) -> dict:
    """Run dictionary-backed metadata enrichment directly on a Python dict.

    Extracts address (validated against MassGIS street dictionary), units,
    agency, tone, urgency, and town/state from the transcript.
    Merges into meta['classification'] and sets derived_* top-level fields.
    """
    text = meta.get("edited_transcript") or meta.get("transcript") or ""
    if not text:
        return meta

    meta.setdefault("classification", {})
    meta = enrich_metadata(text, meta)

    addr_conf = meta.get("address_confidence", "none")
    derived = meta.get("derived_address", "none")
    log.info(f"[NLP] Enriched {meta.get('filename', '?')} — "
             f"addr={derived} confidence={addr_conf}")
    return meta
