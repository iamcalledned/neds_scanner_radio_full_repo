"""
gpu_model_manager/web/app.py

FastAPI application factory.
"""
from __future__ import annotations

from pathlib import Path

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles

from ..core.logging_config import setup_logging

_STATIC_DIR = Path(__file__).parent / "static"


def create_app() -> FastAPI:
    setup_logging()

    app = FastAPI(
        title="GPU Model Manager",
        description="RTX 5090 control plane — VRAM inventory, runtime registry, policy engine",
        version="0.1.0",
    )

    app.mount("/static", StaticFiles(directory=str(_STATIC_DIR)), name="static")

    from .routes import router
    app.include_router(router)

    return app
