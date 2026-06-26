"""
desktop/widgets/services_panel.py — Services tab.

Features:
  - Filter field (live filter by service name)
  - "Scanner only" checkbox
  - Classification column (required / optional / unknown)
  - Full-text coloured buttons: Start / Stop / Restart / Status
  - Wider rows
  - Restart Entire Stack (danger) button
"""
from __future__ import annotations

from PySide6.QtCore import Qt
from PySide6.QtGui import QColor
from PySide6.QtWidgets import (
    QCheckBox,
    QHBoxLayout,
    QHeaderView,
    QLabel,
    QLineEdit,
    QMessageBox,
    QPushButton,
    QTableWidget,
    QTableWidgetItem,
    QVBoxLayout,
    QWidget,
)

from modular_dashboard.core.scanner_actions import (
    restart_scanner_service,
    restart_scanner_stack,
    start_scanner_service,
    stop_scanner_service,
)
from modular_dashboard.core.scanner_services import get_scanner_services_status
from modular_dashboard.core.schemas import ServiceState
from modular_dashboard.desktop import style as S
from modular_dashboard.desktop.workers import BackgroundWorker, ServiceActionWorker, StackActionWorker


_STATE_COLORS = {
    ServiceState.active:       S.STATUS_ACTIVE,
    ServiceState.failed:       S.STATUS_FAILED,
    ServiceState.missing:      S.STATUS_MISSING,
    ServiceState.inactive:     S.STATUS_INACTIVE,
    ServiceState.unknown:      S.STATUS_UNKNOWN,
    ServiceState.activating:   S.STATUS_WARNING,
    ServiceState.deactivating: S.STATUS_WARNING,
}

_CLASS_COLORS = {
    "required": S.STATUS_CRITICAL,
    "optional": S.STATUS_OPTIONAL,
    "unknown":  S.TEXT_DIM,
}

_HEADERS = ["Service", "Class", "Status", "Sub-state", "Since", "Actions"]
_COL_SVC     = 0
_COL_CLASS   = 1
_COL_STATUS  = 2
_COL_SUB     = 3
_COL_SINCE   = 4
_COL_ACTIONS = 5


def _qcolor(hex_str: str) -> QColor:
    return QColor(hex_str)


def _make_action_buttons(svc_name: str, panel: "ServicesPanel") -> QWidget:
    w = QWidget()
    w.setStyleSheet("background: transparent;")
    row = QHBoxLayout(w)
    row.setContentsMargins(4, 2, 4, 2)
    row.setSpacing(6)

    start_btn = QPushButton("Start")
    start_btn.setFixedHeight(26)
    start_btn.setStyleSheet(S.BTN_START)
    start_btn.clicked.connect(lambda: panel._do_action(svc_name, "start"))
    row.addWidget(start_btn)

    stop_btn = QPushButton("Stop")
    stop_btn.setFixedHeight(26)
    stop_btn.setStyleSheet(S.BTN_STOP)
    stop_btn.clicked.connect(lambda: panel._do_action(svc_name, "stop"))
    row.addWidget(stop_btn)

    restart_btn = QPushButton("Restart")
    restart_btn.setFixedHeight(26)
    restart_btn.setStyleSheet(S.BTN_RESTART)
    restart_btn.clicked.connect(lambda: panel._do_action(svc_name, "restart"))
    row.addWidget(restart_btn)

    row.addStretch()
    return w


class ServicesPanel(QWidget):
    """The Services tab panel."""

    def __init__(self, parent=None):
        super().__init__(parent)
        self._worker = None
        self._action_worker = None
        self._all_rows: list[dict] = []   # raw data for filtering
        self._build_ui()
        self.refresh()

    def _build_ui(self):
        layout = QVBoxLayout(self)
        layout.setContentsMargins(14, 14, 14, 14)
        layout.setSpacing(10)

        # ── Toolbar ─────────────────────────────────────────────────────────
        toolbar = QHBoxLayout()

        self._filter_edit = QLineEdit()
        self._filter_edit.setPlaceholderText("Filter by name…")
        self._filter_edit.setFixedWidth(220)
        self._filter_edit.textChanged.connect(self._apply_filter)
        toolbar.addWidget(self._filter_edit)

        self._scanner_only_cb = QCheckBox("Scanner services only")
        self._scanner_only_cb.stateChanged.connect(self._apply_filter)
        toolbar.addWidget(self._scanner_only_cb)

        toolbar.addStretch()

        self._status_lbl = QLabel("")
        self._status_lbl.setWordWrap(True)
        self._status_lbl.setStyleSheet(f"color: {S.TEXT_MUTED}; font-size: 11px;")
        toolbar.addWidget(self._status_lbl)

        refresh_btn = QPushButton("↻  Refresh")
        refresh_btn.setStyleSheet(S.BTN_DEFAULT)
        refresh_btn.clicked.connect(self.refresh)
        toolbar.addWidget(refresh_btn)

        stack_btn = QPushButton("⚡  Restart Entire Stack")
        stack_btn.setStyleSheet(S.BTN_DANGER)
        stack_btn.clicked.connect(self._restart_stack)
        toolbar.addWidget(stack_btn)

        layout.addLayout(toolbar)

        # ── Table ────────────────────────────────────────────────────────────
        self._table = QTableWidget()
        self._table.setColumnCount(len(_HEADERS))
        self._table.setHorizontalHeaderLabels(_HEADERS)

        hdr = self._table.horizontalHeader()
        hdr.setSectionResizeMode(_COL_SVC,     QHeaderView.ResizeMode.Stretch)
        hdr.setSectionResizeMode(_COL_CLASS,   QHeaderView.ResizeMode.Fixed)
        hdr.setSectionResizeMode(_COL_STATUS,  QHeaderView.ResizeMode.Fixed)
        hdr.setSectionResizeMode(_COL_SUB,     QHeaderView.ResizeMode.Fixed)
        hdr.setSectionResizeMode(_COL_SINCE,   QHeaderView.ResizeMode.Fixed)
        hdr.setSectionResizeMode(_COL_ACTIONS, QHeaderView.ResizeMode.Fixed)

        self._table.setColumnWidth(_COL_CLASS,   90)
        self._table.setColumnWidth(_COL_STATUS,  90)
        self._table.setColumnWidth(_COL_SUB,     90)
        self._table.setColumnWidth(_COL_SINCE,   160)
        self._table.setColumnWidth(_COL_ACTIONS, 230)

        self._table.setEditTriggers(QTableWidget.EditTrigger.NoEditTriggers)
        self._table.setSelectionBehavior(QTableWidget.SelectionBehavior.SelectRows)
        self._table.setAlternatingRowColors(True)
        self._table.verticalHeader().setVisible(False)
        self._table.verticalHeader().setDefaultSectionSize(36)
        layout.addWidget(self._table)

    # ── Data load / refresh ─────────────────────────────────────────────────

    def refresh(self):
        if self._worker and self._worker.isRunning():
            return
        self._worker = BackgroundWorker(get_scanner_services_status)
        self._worker.finished.connect(self._on_services_received)
        self._worker.error.connect(lambda e: self._set_status(f"Error: {e}", "error"))
        self._worker.start()

    def _on_services_received(self, summary):
        self._all_rows = [
            {
                "name":           svc.name,
                "classification": svc.classification,
                "active_state":   svc.active_state,
                "sub_state":      svc.sub_state,
                "since":          svc.since,
                "is_missing":     svc.is_missing,
                "is_scanner":     "scanner" in svc.name or "rtl_tcp" in svc.name,
            }
            for svc in summary.services
        ]
        count = summary.active_count
        total = len(summary.services)
        self._set_status(f"{count}/{total} active", "info")
        self._apply_filter()

    def _apply_filter(self):
        text = self._filter_edit.text().strip().lower()
        scanner_only = self._scanner_only_cb.isChecked()

        rows = self._all_rows
        if text:
            rows = [r for r in rows if text in r["name"].lower()]
        if scanner_only:
            rows = [r for r in rows if r["is_scanner"]]

        self._table.setRowCount(0)
        for r in rows:
            row = self._table.rowCount()
            self._table.insertRow(row)

            # Service name
            item = QTableWidgetItem(r["name"])
            item.setForeground(_qcolor(S.TEXT_PRIMARY))
            self._table.setItem(row, _COL_SVC, item)

            # Classification
            cls = r["classification"]
            cls_item = QTableWidgetItem(cls)
            cls_item.setForeground(_qcolor(_CLASS_COLORS.get(cls, S.TEXT_DIM)))
            cls_item.setTextAlignment(Qt.AlignmentFlag.AlignCenter)
            self._table.setItem(row, _COL_CLASS, cls_item)

            # Status
            state = r["active_state"]
            color = _STATE_COLORS.get(state, S.STATUS_UNKNOWN)
            status_item = QTableWidgetItem(state.value)
            status_item.setForeground(_qcolor(color))
            status_item.setTextAlignment(Qt.AlignmentFlag.AlignCenter)
            self._table.setItem(row, _COL_STATUS, status_item)

            # Sub-state
            self._table.setItem(row, _COL_SUB, QTableWidgetItem(r["sub_state"]))

            # Since
            since = r["since"][:19] if r["since"] else ""
            self._table.setItem(row, _COL_SINCE, QTableWidgetItem(since))

            # Actions
            if r["is_missing"]:
                missing_lbl = QLabel("  not installed")
                missing_lbl.setStyleSheet(f"color: {S.STATUS_MISSING}; font-size: 11px;")
                self._table.setCellWidget(row, _COL_ACTIONS, missing_lbl)
            else:
                btn_widget = _make_action_buttons(r["name"], self)
                self._table.setCellWidget(row, _COL_ACTIONS, btn_widget)

    # ── Service actions ─────────────────────────────────────────────────────

    def _do_action(self, svc_name: str, action: str):
        if self._action_worker and self._action_worker.isRunning():
            return
        fn_map = {
            "start":   start_scanner_service,
            "stop":    stop_scanner_service,
            "restart": restart_scanner_service,
        }
        fn = fn_map.get(action)
        if fn is None:
            return
        self._set_status(f"{action} {svc_name}…", "info")
        self._action_worker = ServiceActionWorker(fn, svc_name)
        self._action_worker.finished.connect(self._on_action_done)
        self._action_worker.error.connect(lambda e: self._set_status(f"Error: {e}", "error"))
        self._action_worker.start()

    def _on_action_done(self, result):
        msg = "OK" if result.success else f"Failed (rc={result.returncode})"
        kind = "success" if result.success else "error"
        self._set_status(f"{result.action} {result.service_name}: {msg}", kind)
        self.refresh()

    def _restart_stack(self):
        reply = QMessageBox.question(
            self,
            "Restart Scanner Stack",
            "This will restart the entire scanner stack in order:\n"
            "scanner-mcp → scanner-transcriber → scanner-recorder → scanner-websocket\n\n"
            "Are you sure?",
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
        )
        if reply != QMessageBox.StandardButton.Yes:
            return
        self._set_status("Restarting full stack…", "info")
        self._action_worker = StackActionWorker(restart_scanner_stack)
        self._action_worker.finished.connect(
            lambda r: self._set_status(f"Stack restart: {'OK' if r.success else 'FAILED'}", "success" if r.success else "error")
        )
        self._action_worker.error.connect(lambda e: self._set_status(f"Stack error: {e}", "error"))
        self._action_worker.start()

    def _set_status(self, msg: str, kind: str = "info"):
        colors = {
            "info":    f"color: {S.TEXT_MUTED};",
            "success": f"color: {S.STATUS_HEALTHY};",
            "error":   f"color: {S.STATUS_CRITICAL};",
        }
        self._status_lbl.setStyleSheet(f"font-size: 11px; {colors.get(kind, '')}")
        self._status_lbl.setText(msg)
