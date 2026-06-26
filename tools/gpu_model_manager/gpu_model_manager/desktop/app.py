"""
gpu_model_manager/desktop/app.py

PySide6 application entry point.
"""
from __future__ import annotations

import sys

from PySide6.QtWidgets import QApplication

from ..core.logging_config import setup_logging
from .main_window import MainWindow
from .style import DARK_STYLESHEET


def run():
    setup_logging()
    app = QApplication(sys.argv)
    app.setApplicationName("GPU Model Manager")
    app.setStyleSheet(DARK_STYLESHEET)

    window = MainWindow()
    window.show()

    sys.exit(app.exec())
