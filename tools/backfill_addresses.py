#!/usr/bin/env python3
"""
backfill_addresses.py — One-time script to run address extraction on all
existing calls in the database and populate the derived_* columns.

Usage:
    python tools/backfill_addresses.py                  # Full run
    python tools/backfill_addresses.py --limit 1000     # Test with 1000 rows
    python tools/backfill_addresses.py --dry-run        # Show what would change
    python tools/backfill_addresses.py --force           # Re-process even if already set

Processes rows in batches of 500, commits after each batch, and prints
progress every 2000 rows. Safe to interrupt and re-run (skips rows that
already have a derived_address unless --force is used).
"""

import argparse
import os
import sys
import sqlite3
import time
from pathlib import Path

# ── Make project root importable ──
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from transcriber.nlp_zero_shot import extract_address, _load_streets

DB_PATH = os.environ.get(
    "SCANNER_DB",
    "/home/ned/data/scanner_calls/scanner_calls.db",
)

BATCH_SIZE = 500
PROGRESS_EVERY = 2000


def backfill(limit: int = 0, dry_run: bool = False, force: bool = False):
    """Run address extraction on all calls missing derived_address."""

    # Pre-load the street dictionary
    _load_streets()

    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=10000")

    # Count rows to process
    # After ALTER TABLE, all rows have address_confidence='none' and derived_address=NULL.
    # After backfill, rows with no address keep confidence='none' but get derived_town set
    # to mark them as processed.  Rows with an address get derived_address populated.
    where = "WHERE transcript IS NOT NULL AND LENGTH(transcript) > 10"
    if not force:
        where += " AND derived_town IS NULL"

    total = conn.execute(f"SELECT COUNT(*) FROM calls {where}").fetchone()[0]
    if limit:
        total = min(total, limit)

    print(f"Rows to process: {total:,}")
    if total == 0:
        print("Nothing to do.")
        conn.close()
        return

    if dry_run:
        # Just show a sample
        rows = conn.execute(
            f"SELECT id, transcript, town FROM calls {where} ORDER BY RANDOM() LIMIT 20"
        ).fetchall()
        found = 0
        for r in rows:
            town = (r["town"] or "Unknown").strip()
            addr = extract_address(r["transcript"], town if town.lower() != "unknown" else None)
            tag = "✓" if addr["confidence"] != "none" else "✗"
            if addr["confidence"] != "none":
                found += 1
            print(f"  {tag} [{addr['confidence']:6}] {addr['full_address'] or '—':35} {r['transcript'][:60]}")
        print(f"\nDry-run sample: {found}/{len(rows)} detected")
        conn.close()
        return

    # ── Process in batches ──
    query = f"""
        SELECT id, transcript, town
        FROM calls {where}
        ORDER BY id
        LIMIT ?
    """

    t0 = time.time()
    processed = 0
    updated = 0
    batch_updates = []

    cursor = conn.execute(query, (total,))

    for row in cursor:
        town = (row["town"] or "Unknown").strip()
        # Normalise town for address extraction
        town_for_extract = town if town.lower() != "unknown" else None
        text = row["transcript"]

        addr = extract_address(text, town_for_extract)

        if addr["confidence"] != "none":
            batch_updates.append((
                addr["full_address"],
                addr["address_street"],
                addr["address_number"],
                town if addr.get("town") is None else town,
                addr["latitude"],
                addr["longitude"],
                addr["confidence"],
                row["id"],
            ))
            updated += 1

        else:
            # Mark as processed so we skip on re-runs (derived_town set, address stays NULL)
            batch_updates.append((
                None, None, None, town, None, None, "none", row["id"]
            ))

        processed += 1

        # Commit in batches
        if len(batch_updates) >= BATCH_SIZE:
            _flush_batch(conn, batch_updates)
            batch_updates = []

        if processed % PROGRESS_EVERY == 0:
            elapsed = time.time() - t0
            rate = processed / elapsed if elapsed else 0
            eta = (total - processed) / rate if rate else 0
            pct = processed * 100 // total
            print(
                f"  [{pct:3d}%] {processed:>8,}/{total:,}  "
                f"addresses={updated:,}  "
                f"{rate:.0f} rows/sec  "
                f"ETA {eta:.0f}s"
            )

    # Final flush
    if batch_updates:
        _flush_batch(conn, batch_updates)

    elapsed = time.time() - t0
    print(f"\nDone in {elapsed:.1f}s")
    print(f"  Processed: {processed:,}")
    print(f"  Addresses found: {updated:,} ({updated * 100 // max(processed, 1)}%)")
    print(f"  No address: {processed - updated:,}")

    conn.close()


def _flush_batch(conn, updates):
    """Write a batch of updates to the DB."""
    conn.executemany(
        """
        UPDATE calls SET
            derived_address = ?,
            derived_street = ?,
            derived_addr_num = ?,
            derived_town = ?,
            derived_lat = ?,
            derived_lng = ?,
            address_confidence = ?
        WHERE id = ?
        """,
        updates,
    )
    conn.commit()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Backfill address extraction on existing calls")
    parser.add_argument("--limit", type=int, default=0, help="Max rows to process (0 = all)")
    parser.add_argument("--dry-run", action="store_true", help="Show sample results without writing")
    parser.add_argument("--force", action="store_true", help="Re-process rows that already have a value")
    args = parser.parse_args()

    backfill(limit=args.limit, dry_run=args.dry_run, force=args.force)
