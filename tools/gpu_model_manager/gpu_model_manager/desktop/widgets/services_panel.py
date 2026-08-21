"""
gpu_model_manager/desktop/widgets/services_panel.py

Services tab: model-related systemd services with start/stop/restart.
Protected services require explicit confirmation.
"""
from __future__ import annotations

from PySide6.QtWidgets import (
    QHBoxLayout,
    QLabel,
    QMessageBox,
    QPlainTextEdit,
    QPushButton,
    QTableWidget,
    QTableWidgetItem,
    QVBoxLayout,
    QWidget,
)

from ...core.actions import restart_runtime, start_runtime, stop_runtime
from ...core.runtime_registry import get_runtime_registry
from ...core.service_controller import discover_model_services
from ..workers import DataWorker
from .common import MutedLabel

_PROTECTED = {"scanner-mcp.service"}


def _fetch():
    return [s.model_dump() for s in discover_model_services()]


def _service_to_runtime_key(service_name: str) -> str | None:
    registry = get_runtime_registry()
    for key, defn in registry.items():
        if defn.service_name == service_name:
            return key
    return None


class ServicesPanel(QWidget):
    def __init__(self, parent=None):
        super().__init__(parent)
        self._worker: DataWorker | None = None
        self._services: list[dict] = []
        self._build_ui()
        self.refresh()

    def _build_ui(self):
        root = QVBoxLayout(self)
        root.setContentsMargins(16, 12, 16, 12)
        root.setSpacing(8)

        toolbar = QWidget()
        tl = QHBoxLayout(toolbar)
        tl.setContentsMargins(0, 0, 0, 0)
        tl.addWidget(QLabel("Model Services (systemctl --user)"))
        tl.addStretch()
        btn_refresh = QPushButton("Refresh")
        btn_refresh.setFixedWidth(90)
        btn_refresh.clicked.connect(self.refresh)
        tl.addWidget(btn_refresh)
        root.addWidget(toolbar)

        self._table = QTableWidget(0, 5)
        self._table.setHorizontalHeaderLabels([
            "Service", "Active", "Sub State", "PID", "Actions"
        ])
        self._table.setAlternatingRowColors(True)
        self._table.setEditTriggers(QTableWidget.EditTrigger.NoEditTriggers)
        self._table.setSelectionBehavior(QTableWidget.SelectionBehavior.SelectRows)
        self._table.horizontalHeader().setStretchLastSection(False)
        self._table.horizontalHeader().setMinimumSectionSize(80)
        root.addWidget(self._table, 1)

        self._result_label = MutedLabel("")
        root.addWidget(self._result_label)

        self._result_text = QPlainTextEdit()
        self._result_text.setReadOnly(True)
        self._result_text.setMaximumHeight(100)
        root.addWidget(self._result_text)

    def refresh(self):
        if self._worker and self._worker.isRunning():
            return
        self._worker = DataWorker(_fetch, parent=self)
        self._worker.data_ready.connect(self._on_data)
        self._worker.start()

    def _on_data(self, services: list[dict]):
        self._services = services
        self._table.setRowCount(len(services))
        for row, svc in enumerate(services):
            name = svc.get("name", "")
            is_protected = name in _PROTECTED

            name_item = QTableWidgetItem(name + (" [PROTECTED]" if is_protected else ""))
            self._table.setItem(row, 0, name_item)
            self._table.setItem(row, 1, QTableWidgetItem(svc.get("active_state", "")))
            self._table.setItem(row, 2, QTableWidgetItem(svc.get("sub_state", "")))
            pid = svc.get("pid")
            self._table.setItem(row, 3, QTableWidgetItem(str(pid) if pid else "—"))

            # Action buttons widget
            btn_widget = QWidget()
            btn_layout = QHBoxLayout(btn_widget)
            btn_layout.setContentsMargins(4, 2, 4, 2)
            btn_layout.setSpacing(4)

            btn_start = QPushButton("Start")
            btn_start.setObjectName("btn_green")
            btn_start.setFixedWidth(60)
            btn_start.clicked.connect(lambda _, n=name: self._do_action(n, "start"))

            btn_stop = QPushButton("Stop" + (" ⚠" if is_protected else ""))
            btn_stop.setObjectName("btn_red")
            btn_stop.setFixedWidth(75)
            btn_stop.clicked.connect(lambda _, n=name, p=is_protected: self._do_stop(n, p))

            btn_restart = QPushButton("Restart" + (" ⚠" if is_protected else ""))
            btn_restart.setObjectName("btn_yellow")
            btn_restart.setFixedWidth(85)
            btn_restart.clicked.connect(lambda _, n=name, p=is_protected: self._do_restart(n, p))

            btn_layout.addWidget(btn_start)
            btn_layout.addWidget(btn_stop)
            btn_layout.addWidget(btn_restart)
            self._table.setCellWidget(row, 4, btn_widget)

        self._table.resizeColumnsToContents()
        self._table.setColumnWidth(4, 250)

    def _do_action(self, service_name: str, action: str, confirm_protected: bool = False):
        runtime_key = _service_to_runtime_key(service_name)
        if not runtime_key:
            self._result_label.setText(f"No runtime registered for {service_name}")
            return

        self._result_label.setText(f"Running {action} on {runtime_key}…")
        if action == "start":
            fn = lambda k=runtime_key: start_runtime(k).model_dump()
        elif action == "stop":
            fn = lambda k=runtime_key, cp=confirm_protected: stop_runtime(k, confirm_protected=cp).model_dump()
        else:
            fn = lambda k=runtime_key, cp=confirm_protected: restart_runtime(k, confirm_protected=cp).model_dump()

        worker = DataWorker(fn, parent=self)
        worker.data_ready.connect(self._on_action_result)
        worker.start()

    def _do_stop(self, service_name: str, is_protected: bool):
        if is_protected:
            reply = QMessageBox.warning(
                self,
                "Protected Service",
                f"{service_name} is PROTECTED (scanner transcription).\n"
                "Are you absolutely sure you want to stop it?",
                QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
                QMessageBox.StandardButton.No,
            )
            if reply != QMessageBox.StandardButton.Yes:
                return
        self._do_action(service_name, "stop", confirm_protected=is_protected)

    def _do_restart(self, service_name: str, is_protected: bool):
        if is_protected:
            reply = QMessageBox.warning(
                self,
                "Protected Service",
                f"{service_name} is PROTECTED (scanner transcription).\n"
                "Are you absolutely sure you want to restart it?",
                QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
                QMessageBox.StandardButton.No,
            )
            if reply != QMessageBox.StandardButton.Yes:
                return
        self._do_action(service_name, "restart", confirm_protected=is_protected)

    def _on_action_result(self, result: dict):
        success = result.get("success", False)
        self._result_label.setText(
            f"{'OK' if success else 'FAILED'}: {result.get('action')} {result.get('runtime_key')}"
        )
        self._result_text.setPlainText(result.get("message", ""))
        self.refresh()
