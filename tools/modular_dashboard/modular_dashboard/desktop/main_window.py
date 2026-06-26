"""
desktop/main_window.py — Main PySide6 window with tabbed layout and global header.
"""
from __future__ import annotations

from datetime import datetime

from PySide6.QtCore import Qt, QTimer
from PySide6.QtWidgets import (
    QCheckBox,
    QFrame,
    QHBoxLayout,
    QLabel,
    QMainWindow,
    QPushButton,
    QStatusBar,
    QTabWidget,
    QVBoxLayout,
    QWidget,
)

from modular_dashboard.desktop import style as S
from modular_dashboard.desktop.widgets.dashboard_panel import DashboardPanel
from modular_dashboard.desktop.widgets.logs_panel import LogsPanel
from modular_dashboard.desktop.widgets.redis_panel import RedisPanel
from modular_dashboard.desktop.widgets.scanner_panel import ScannerPanel
from modular_dashboard.desktop.widgets.services_panel import ServicesPanel


class MainWindow(QMainWindow):
    """Top-level window for the Modular Scanner Dashboard desktop app."""

    AUTO_REFRESH_MS = 10_000   # global 10-second refresh

    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Scanner Control Center — Phase 1")
        self.resize(1400, 900)
        self.setMinimumSize(1100, 750)
        self.setStyleSheet(S.GLOBAL_STYLESHEET)
        self._build_ui()

        self._global_timer = QTimer(self)
        self._global_timer.timeout.connect(self._global_refresh)
        # Auto-refresh starts OFF; user can enable via checkbox

    def _build_ui(self):
        central = QWidget()
        self.setCentralWidget(central)
        root = QVBoxLayout(central)
        root.setContentsMargins(0, 0, 0, 0)
        root.setSpacing(0)

        # ── Global header bar ────────────────────────────────────────────────
        header = QFrame()
        header.setStyleSheet(
            f"background: {S.BG_CARD}; border-bottom: 1px solid {S.BORDER_DIM};"
        )
        header.setFixedHeight(52)
        hbar = QHBoxLayout(header)
        hbar.setContentsMargins(16, 0, 16, 0)
        hbar.setSpacing(14)

        title_lbl = QLabel("📡  Scanner Control Center")
        title_lbl.setStyleSheet(
            f"color: {S.ACCENT_BLUE}; font-size: 15px; font-weight: bold; background: transparent;"
        )
        hbar.addWidget(title_lbl)

        phase_lbl = QLabel("Phase 1")
        phase_lbl.setStyleSheet(
            f"color: {S.TEXT_DIM}; font-size: 12px; background: transparent;"
        )
        hbar.addWidget(phase_lbl)

        hbar.addStretch()

        self._last_refresh_lbl = QLabel("Not yet refreshed")
        self._last_refresh_lbl.setStyleSheet(
            f"color: {S.TEXT_DIM}; font-size: 11px; background: transparent;"
        )
        hbar.addWidget(self._last_refresh_lbl)

        self._auto_cb = QCheckBox("Auto-refresh")
        self._auto_cb.setToolTip("Refresh all panels every 10 seconds")
        self._auto_cb.stateChanged.connect(self._toggle_auto_refresh)
        hbar.addWidget(self._auto_cb)

        refresh_btn = QPushButton("↻  Refresh All")
        refresh_btn.setFixedHeight(32)
        refresh_btn.setStyleSheet(S.BTN_BLUE)
        refresh_btn.clicked.connect(self._global_refresh)
        hbar.addWidget(refresh_btn)

        root.addWidget(header)

        # ── Tabs ─────────────────────────────────────────────────────────────
        self._tabs = QTabWidget()
        self._tabs.setDocumentMode(True)

        self._dashboard = DashboardPanel()
        self._services  = ServicesPanel()
        self._scanner   = ScannerPanel()
        self._redis     = RedisPanel()
        self._logs      = LogsPanel()

        self._tabs.addTab(self._dashboard, "  Dashboard  ")
        self._tabs.addTab(self._services,  "  Services   ")
        self._tabs.addTab(self._scanner,   "  Scanner    ")
        self._tabs.addTab(self._redis,     "  Redis      ")
        self._tabs.addTab(self._logs,      "  Logs       ")

        root.addWidget(self._tabs)

        # ── Status bar ───────────────────────────────────────────────────────
        sb = QStatusBar()
        sb.showMessage("Scanner Control Center  |  Phase 1  |  redis://127.0.0.1:6379/0")
        self.setStatusBar(sb)

    def _toggle_auto_refresh(self, state: int):
        if state:
            self._global_timer.start(self.AUTO_REFRESH_MS)
        else:
            self._global_timer.stop()

    def _global_refresh(self):
        now = datetime.now().strftime("%H:%M:%S")
        self._last_refresh_lbl.setText(f"Last refresh: {now}")
        # Refresh whichever panel is currently visible for responsiveness,
        # and always refresh dashboard (it shows overall health)
        self._dashboard.refresh()
        current = self._tabs.currentWidget()
        if current is not self._dashboard:
            if hasattr(current, "refresh"):
                current.refresh()
