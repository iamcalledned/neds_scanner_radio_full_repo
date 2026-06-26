"""
scripts/run_desktop.py — Launch the PySide6 desktop dashboard.

Usage:
    cd /home/ned/Documents/neds_scanner_radio_full_pipeline_with_git/tools/modular_dashboard
    pyscan
    python -m modular_dashboard.scripts.run_desktop
"""
from __future__ import annotations

import sys

from modular_dashboard.desktop.app import run


def main():
    sys.exit(run())


if __name__ == "__main__":
    main()
