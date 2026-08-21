#!/usr/bin/env python3
"""Add compact waveform envelopes to existing scanner call metadata."""

from __future__ import annotations

import argparse
import json
import os
import sqlite3
import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from shared.waveform import DEFAULT_WAVEFORM_POINTS, extract_waveform


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--db",
        default=os.environ.get(
            "SCANNER_DB_PATH",
            "/home/ned/data/scanner_calls/scanner_calls.db",
        ),
    )
    parser.add_argument("--day", help="Only process calls whose timestamp starts with YYYY-MM-DD")
    parser.add_argument("--limit", type=int, default=0, help="Maximum rows to inspect; zero means all")
    parser.add_argument("--points", type=int, default=DEFAULT_WAVEFORM_POINTS)
    parser.add_argument("--force", action="store_true", help="Replace an existing waveform")
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args()


def load_extra(raw: object) -> dict:
    if isinstance(raw, dict):
        return dict(raw)
    try:
        parsed = json.loads(raw or "{}")
        return parsed if isinstance(parsed, dict) else {}
    except (TypeError, json.JSONDecodeError):
        return {}


def main() -> int:
    args = parse_args()
    db_path = Path(args.db)
    if not db_path.exists():
        print(f"Database not found: {db_path}", file=sys.stderr)
        return 2

    clauses = ["wav_path IS NOT NULL", "TRIM(wav_path) != ''"]
    params: list[object] = []
    if args.day:
        clauses.append("timestamp LIKE ?")
        params.append(f"{args.day}%")
    limit_sql = " LIMIT ?" if args.limit > 0 else ""
    if args.limit > 0:
        params.append(args.limit)

    inspected = updated = skipped = failed = 0
    with sqlite3.connect(db_path, timeout=30) as conn:
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA busy_timeout=10000")
        rows = conn.execute(
            "SELECT id, filename, wav_path, json_path, extra FROM calls "
            f"WHERE {' AND '.join(clauses)} ORDER BY timestamp DESC{limit_sql}",
            params,
        ).fetchall()
        for row in rows:
            inspected += 1
            extra = load_extra(row["extra"])
            if not args.force and (extra.get("waveform") or {}).get("peaks"):
                skipped += 1
                continue
            wav_path = Path(row["wav_path"])
            if not wav_path.exists():
                failed += 1
                print(f"missing WAV: {wav_path}", file=sys.stderr)
                continue
            try:
                waveform = extract_waveform(wav_path, points=args.points)
                extra["waveform"] = waveform
                if not args.dry_run:
                    conn.execute(
                        "UPDATE calls SET extra = ? WHERE id = ?",
                        (json.dumps(extra, separators=(",", ":")), row["id"]),
                    )
                    # Release SQLite's writer lock before processing the next
                    # WAV so live transcription inserts are never held up by
                    # a long archive backfill.
                    conn.commit()
                    json_path = Path(row["json_path"] or "")
                    if json_path.is_file():
                        try:
                            sidecar = json.loads(json_path.read_text(encoding="utf-8"))
                            if isinstance(sidecar, dict):
                                sidecar["waveform"] = waveform
                                json_path.write_text(
                                    json.dumps(sidecar, indent=2),
                                    encoding="utf-8",
                                )
                        except (OSError, json.JSONDecodeError) as exc:
                            print(
                                f"sidecar warning {row['filename']}: {exc}",
                                file=sys.stderr,
                            )
                updated += 1
            except Exception as exc:
                failed += 1
                print(f"waveform failed {row['filename']}: {exc}", file=sys.stderr)
    mode = "would update" if args.dry_run else "updated"
    print(
        f"inspected={inspected} {mode}={updated} skipped={skipped} failed={failed}"
    )
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
