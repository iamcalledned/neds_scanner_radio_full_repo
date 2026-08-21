"""
gpu_model_manager/desktop/main_window.py

Main PySide6 window with tab layout.
"""
from __future__ import annotations

from PySide6.QtWidgets import (
    QLabel,
    QMainWindow,
    QStatusBar,
    QTabWidget,
)

from .widgets.dashboard_panel import DashboardPanel
from .widgets.gpu_panel import GpuPanel
from .widgets.leases_panel import LeasesPanel
from .widgets.logs_panel import LogsPanel
from .widgets.policy_panel import PolicyPanel
from .widgets.runtimes_panel import RuntimesPanel
from .widgets.services_panel import ServicesPanel


class MainWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("GPU Model Manager — RTX 5090 Control Plane")
        self.resize(1400, 900)
        self.setMinimumSize(1100, 750)

        self._tabs = QTabWidget()
        self._tabs.setDocumentMode(True)

        self._dashboard  = DashboardPanel()
        self._gpu        = GpuPanel()
        self._runtimes   = RuntimesPanel()
        self._services   = ServicesPanel()
        self._policy     = PolicyPanel()
        self._leases     = LeasesPanel()
        self._logs       = LogsPanel()

        self._tabs.addTab(self._dashboard, "Dashboard")
        self._tabs.addTab(self._gpu,       "GPU")
        self._tabs.addTab(self._runtimes,  "Runtimes")
        self._tabs.addTab(self._services,  "Services")
        self._tabs.addTab(self._policy,    "Policy")
        self._tabs.addTab(self._leases,    "Leases")
        self._tabs.addTab(self._logs,      "Logs")

        self.setCentralWidget(self._tabs)

        status_bar = QStatusBar()
        status_bar.addPermanentWidget(
            QLabel("GPU Model Manager v0.1 — Scanner pipeline unmodified")
        )
        self.setStatusBar(status_bar)

        # Refresh active tab on switch
        self._tabs.currentChanged.connect(self._on_tab_changed)

    def _on_tab_changed(self, index: int):
        widget = self._tabs.widget(index)
        if hasattr(widget, "refresh"):
            widget.refresh()
