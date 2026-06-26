"""
scripts/validate_stack.py — Print a JSON status report of the scanner stack.

Usage:
    cd /home/ned/Documents/neds_scanner_radio_full_pipeline_with_git/tools/modular_dashboard
    pyscan
    python -m modular_dashboard.scripts.validate_stack
"""
from __future__ import annotations

import json
import sys

from modular_dashboard.core.logging_config import setup_logging
from modular_dashboard.core.scanner_actions import validate_scanner_stack
from modular_dashboard.core.scanner_health import get_scanner_health
from modular_dashboard.core.utils import utc_now_str


def main():
    setup_logging("modular_dashboard")

    print("=== Modular Scanner Dashboard — Stack Validation ===")
    print(f"Timestamp: {utc_now_str()}\n")

    result = validate_scanner_stack()

    # Pretty print services
    print("── Services ──")
    for name, info in result.get("services", {}).items():
        ok = "✓" if info.get("ok") else "✗"
        print(f"  {ok} {name}: {info.get('status', '?')}")

    print(f"\n── Redis ──")
    redis = result.get("redis", {})
    print(f"  Reachable: {redis.get('reachable', False)}")
    if redis.get("error"):
        print(f"  Error: {redis['error']}")

    print(f"\n── Paths ──")
    for ps in result.get("paths", []):
        ok = "✓" if ps.get("exists") else "✗"
        print(f"  {ok} {ps['label']}: {ps['path']}")

    problems = result.get("problems", [])
    print(f"\n── Problems ──")
    if problems:
        for p in problems:
            print(f"  ✗ {p}")
    else:
        print("  No problems detected.")

    print(f"\n── Overall OK: {'YES' if result.get('overall_ok') else 'NO'} ──\n")

    print("── Full JSON ──")
    print(json.dumps(result, indent=2, default=str))

    sys.exit(0 if result.get("overall_ok") else 1)


if __name__ == "__main__":
    main()
