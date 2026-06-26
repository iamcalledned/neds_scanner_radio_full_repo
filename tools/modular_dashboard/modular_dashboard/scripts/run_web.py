"""
scripts/run_web.py — Start the FastAPI web app with uvicorn.

Usage:
    cd /home/ned/Documents/neds_scanner_radio_full_pipeline_with_git/tools/modular_dashboard
    pyscan
    python -m modular_dashboard.scripts.run_web
"""
from __future__ import annotations

import uvicorn

from modular_dashboard.core.config import DEFAULT_WEB_HOST, DEFAULT_WEB_PORT
from modular_dashboard.core.logging_config import setup_logging


def main():
    setup_logging("modular_dashboard")
    uvicorn.run(
        "modular_dashboard.web.app:app",
        host=DEFAULT_WEB_HOST,
        port=DEFAULT_WEB_PORT,
        reload=False,
        log_level="info",
    )


if __name__ == "__main__":
    main()
