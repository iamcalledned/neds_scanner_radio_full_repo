"""
web/app.py — FastAPI application factory.

Call create_app() to get a configured FastAPI instance.
"""
from __future__ import annotations

from pathlib import Path

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from modular_dashboard.core.logging_config import setup_logging
from modular_dashboard.web.routes import api_router, page_router

_WEB_DIR = Path(__file__).parent
TEMPLATES_DIR = _WEB_DIR / "templates"
STATIC_DIR = _WEB_DIR / "static"


def create_app() -> FastAPI:
    setup_logging("modular_dashboard")

    app = FastAPI(
        title="Modular Scanner Dashboard",
        description="Phase 1 — Scanner Control Center",
        version="1.0.0",
    )

    # Mount static files
    if STATIC_DIR.exists():
        app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")

    # Include routers
    app.include_router(api_router)
    app.include_router(page_router)

    return app


# Module-level app instance used by uvicorn
app = create_app()
