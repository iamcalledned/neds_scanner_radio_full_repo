#!/usr/bin/env python3
"""
import_massgis_addresses.py — Pull address data from MassGIS ArcGIS Feature Service

Downloads every address point for our 7 coverage towns from the Massachusetts
Master Address Database (MAD), inserts them into the `addresses` table, and
builds the `streets` summary table for fast transcript matching.

Data source:
  MassGIS Master Address Points (Feature Service)
  https://arcgisserver.digital.mass.gov/arcgisserver/rest/services/
    AGOL/MassGIS_Master_Address_Points/FeatureServer/0

Usage:
  python tools/import_massgis_addresses.py                  # import all 7 towns
  python tools/import_massgis_addresses.py --town Hopedale  # import one town
  python tools/import_massgis_addresses.py --stats           # show stats only
"""

import os
import sys
import math
import time
import json
import logging
import logging.handlers
import argparse
from pathlib import Path
from datetime import datetime

import requests

# ── Make shared/ importable ──
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from shared.scanner_db import (
    get_conn, create_tables, address_stats, log as db_log, DB_PATH
)

from dotenv import load_dotenv
load_dotenv(Path(__file__).resolve().parent.parent / ".env")

# ======================================================
#  Logging
# ======================================================
LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO").upper()
LOG_DIR = Path(os.environ.get("LOG_DIR", "/home/ned/data/scanner_calls/logs/transcriber_logs"))
LOG_DIR.mkdir(parents=True, exist_ok=True)

_log_fmt = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")
_console = logging.StreamHandler()
_console.setFormatter(_log_fmt)
_fh = logging.handlers.RotatingFileHandler(
    LOG_DIR / "massgis_import.log", maxBytes=10_000_000, backupCount=3
)
_fh.setFormatter(_log_fmt)
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    handlers=[_console, _fh],
)
log = logging.getLogger("massgis-import")

# ======================================================
#  MassGIS Feature Service Config
# ======================================================
FEATURE_SERVICE_URL = (
    "https://arcgisserver.digital.mass.gov/arcgisserver/rest/services/"
    "AGOL/MassGIS_Master_Address_Points/FeatureServer/0/query"
)

# The 7 towns we cover
COVERAGE_TOWNS = [
    "HOPEDALE", "MILFORD", "BELLINGHAM", "MENDON",
    "BLACKSTONE", "UPTON", "FRANKLIN",
]

# Fields to retrieve — everything we need for address matching + geocoding
OUT_FIELDS = ",".join([
    "MASTER_ADDRESS_ID",
    "FULL_NUMBER_STANDARDIZED",
    "ADDRESS_NUMBER",
    "STREET_NAME",
    "STREET_NAME_ID",
    "STR_NAME_BASE",
    "PRE_DIR",
    "PRE_TYPE",
    "PRE_MOD",
    "POST_TYPE",
    "POST_DIR",
    "POST_MOD",
    "UNIT",
    "FLOOR",
    "BUILDING_NAME",
    "GEOGRAPHIC_TOWN",
    "COMMUNITY_NAME",
    "POSTCODE",
    "COUNTY",
    "STATE",
    "POINT_TYPE",
])

# Max records per ArcGIS query (server limit = 2000)
PAGE_SIZE = 2000

# Request in WGS84 so we get lat/lng directly
OUT_SR = 4326


def fetch_town_count(town: str) -> int:
    """Get the total record count for a town before paginating."""
    params = {
        "where": f"GEOGRAPHIC_TOWN = '{town}'",
        "returnCountOnly": "true",
        "f": "json",
    }
    resp = requests.get(FEATURE_SERVICE_URL, params=params, timeout=30)
    resp.raise_for_status()
    data = resp.json()
    return data.get("count", 0)


def fetch_town_page(town: str, offset: int) -> list[dict]:
    """Fetch one page of address records for a town."""
    params = {
        "where": f"GEOGRAPHIC_TOWN = '{town}'",
        "outFields": OUT_FIELDS,
        "outSR": OUT_SR,
        "resultOffset": offset,
        "resultRecordCount": PAGE_SIZE,
        "orderByFields": "MASTER_ADDRESS_ID",
        "f": "json",
        "returnGeometry": "true",
    }
    resp = requests.get(FEATURE_SERVICE_URL, params=params, timeout=60)
    resp.raise_for_status()
    data = resp.json()

    if "error" in data:
        raise RuntimeError(f"ArcGIS error: {data['error']}")

    features = data.get("features", [])
    records = []
    for feat in features:
        attr = feat.get("attributes", {})
        geom = feat.get("geometry", {})
        attr["_longitude"] = geom.get("x")
        attr["_latitude"] = geom.get("y")
        records.append(attr)

    return records


def import_town(town: str, conn) -> int:
    """Import all addresses for a single town. Returns insert count."""
    town_upper = town.upper()
    total = fetch_town_count(town_upper)
    log.info(f"[{town_upper}] {total:,} address records on MassGIS")

    if total == 0:
        log.warning(f"[{town_upper}] No records found — check town spelling")
        return 0

    pages = math.ceil(total / PAGE_SIZE)
    inserted = 0
    skipped = 0
    now_iso = datetime.now().isoformat()

    for page in range(pages):
        offset = page * PAGE_SIZE
        try:
            records = fetch_town_page(town_upper, offset)
        except Exception as e:
            log.error(f"[{town_upper}] Page {page+1}/{pages} failed: {e}")
            time.sleep(2)
            continue

        batch = []
        for rec in records:
            master_id = rec.get("MASTER_ADDRESS_ID")
            if not master_id:
                skipped += 1
                continue

            # Parse integer address number
            addr_num_int = rec.get("ADDRESS_NUMBER")
            addr_num_str = rec.get("FULL_NUMBER_STANDARDIZED") or ""

            batch.append((
                master_id,
                rec.get("STREET_NAME") or "",
                rec.get("STREET_NAME_ID"),
                rec.get("STR_NAME_BASE") or "",
                rec.get("PRE_DIR") or "",
                rec.get("PRE_TYPE") or "",
                rec.get("PRE_MOD") or "",
                rec.get("POST_TYPE") or "",
                rec.get("POST_DIR") or "",
                rec.get("POST_MOD") or "",
                addr_num_str,
                addr_num_int,
                rec.get("UNIT") or "",
                rec.get("FLOOR") or "",
                rec.get("BUILDING_NAME") or "",
                rec.get("GEOGRAPHIC_TOWN") or town_upper,
                rec.get("COMMUNITY_NAME") or "",
                rec.get("POSTCODE") or "",
                rec.get("COUNTY") or "",
                rec.get("STATE") or "MA",
                rec.get("_latitude"),
                rec.get("_longitude"),
                rec.get("POINT_TYPE") or "",
                "massgis",
                now_iso,
            ))

        if batch:
            conn.executemany("""
                INSERT OR IGNORE INTO addresses (
                    master_addr_id, street_name, street_name_id,
                    str_name_base, pre_dir, pre_type, pre_mod,
                    post_type, post_dir, post_mod,
                    addr_num, addr_num_int,
                    unit, floor, building,
                    town, community, zipcode, county, state,
                    latitude, longitude, point_type,
                    source, imported_at
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """, batch)
            conn.commit()
            inserted += len(batch)

        log.info(
            f"  [{town_upper}] Page {page+1}/{pages} — "
            f"fetched={len(records)} inserted={inserted:,} skipped={skipped}"
        )

        # Be polite to the MassGIS server
        if page < pages - 1:
            time.sleep(0.5)

    log.info(f"[{town_upper}] Done — {inserted:,} addresses imported")
    return inserted


def rebuild_streets_table(conn):
    """Aggregate the addresses table into the streets summary table."""
    log.info("[STREETS] Rebuilding streets summary table...")

    conn.execute("DELETE FROM streets")

    conn.execute("""
        INSERT OR REPLACE INTO streets (
            street_name_id, street_name, str_name_base,
            pre_dir, pre_type, post_type, post_dir, post_mod,
            town, min_addr_num, max_addr_num, addr_count
        )
        SELECT
            street_name_id,
            street_name,
            str_name_base,
            pre_dir,
            pre_type,
            post_type,
            post_dir,
            post_mod,
            town,
            MIN(addr_num_int),
            MAX(addr_num_int),
            COUNT(*)
        FROM addresses
        WHERE street_name != ''
        GROUP BY street_name, town
    """)
    conn.commit()

    count = conn.execute("SELECT COUNT(*) FROM streets").fetchone()[0]
    log.info(f"[STREETS] Built {count:,} distinct street entries")
    return count


def main():
    parser = argparse.ArgumentParser(
        description="Import MassGIS Master Address Data for scanner coverage towns"
    )
    parser.add_argument("--town", type=str, help="Import only this town (default: all 7)")
    parser.add_argument("--stats", action="store_true", help="Show address stats and exit")
    parser.add_argument("--rebuild-streets", action="store_true",
                        help="Rebuild streets summary table from existing addresses")
    parser.add_argument("--clear", action="store_true",
                        help="Clear all address data before importing")
    args = parser.parse_args()

    # Ensure tables exist
    create_tables()

    if args.stats:
        stats = address_stats()
        log.info("═══════════════════════════════════════════")
        log.info(f"  Total addresses:  {stats['total_addresses']:,}")
        log.info(f"  Total streets:    {stats['total_streets']:,}")
        log.info(f"  Geocoded:         {stats['geocoded_addresses']:,}")
        log.info("───────────────────────────────────────────")
        for town, cnt in stats.get("by_town", {}).items():
            log.info(f"    {town:15} {cnt:>7,} addresses")
        log.info("═══════════════════════════════════════════")
        return

    conn = get_conn()

    if args.clear:
        log.warning("[CLEAR] Deleting all address + street data...")
        conn.execute("DELETE FROM addresses")
        conn.execute("DELETE FROM streets")
        conn.commit()
        log.info("[CLEAR] Done")

    if args.rebuild_streets:
        rebuild_streets_table(conn)
        conn.close()
        return

    # Determine which towns to import
    towns = [args.town.upper()] if args.town else COVERAGE_TOWNS

    log.info("═══════════════════════════════════════════")
    log.info(f"  MassGIS Address Import")
    log.info(f"  Database: {DB_PATH}")
    log.info(f"  Towns:    {', '.join(towns)}")
    log.info("═══════════════════════════════════════════")

    grand_total = 0
    start = time.time()

    for town in towns:
        try:
            count = import_town(town, conn)
            grand_total += count
        except Exception as e:
            log.error(f"[{town}] FAILED: {e}")

    # Rebuild the streets summary
    rebuild_streets_table(conn)

    elapsed = time.time() - start
    log.info("═══════════════════════════════════════════")
    log.info(f"  IMPORT COMPLETE")
    log.info(f"  Total addresses imported: {grand_total:,}")
    log.info(f"  Elapsed: {elapsed:.1f}s")
    log.info("═══════════════════════════════════════════")

    # Show final stats
    stats = address_stats()
    for town, cnt in stats.get("by_town", {}).items():
        log.info(f"    {town:15} {cnt:>7,} addresses")

    conn.close()


if __name__ == "__main__":
    main()
