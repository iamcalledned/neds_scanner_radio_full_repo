"""
desktop/app.py — Desktop application entry point.

Initialises logging and launches the PySide6 main window.
"""
from __future__ import annotations

import sys

from PySide6.QtWidgets import QApplication

from modular_dashboard.core.logging_config import setup_logging
from modular_dashboard.desktop.main_window import MainWindow


def run() -> int:
    setup_logging("modular_dashboard")
    app = QApplication(sys.argv)
    app.setApplicationName("Scanner Control Center")
    app.setOrganizationName("ned")

    window = MainWindow()
    window.show()

    return app.exec()


if __name__ == "__main__":
    sys.exit(run())
