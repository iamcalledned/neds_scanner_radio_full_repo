"""
gpu_model_manager/desktop/widgets/runtimes_panel.py

Runtimes tab: registry table + can-start checker + runtime detail.
"""
from __future__ import annotations

from PySide6.QtCore import Qt
from PySide6.QtWidgets import (
    QHBoxLayout,
    QLabel,
    QPlainTextEdit,
    QPushButton,
    QSplitter,
    QTableWidget,
    QTableWidgetItem,
    QVBoxLayout,
    QWidget,
)

from ...core.policy_engine import can_start_runtime
from ...core.runtime_registry import get_runtime_registry, get_runtime_status
from ..workers import DataWorker
from .common import MutedLabel, SectionLabel


def _fetch_registry():
    registry = get_runtime_registry()
    return {k: v.model_dump() for k, v in registry.items()}


class RuntimesPanel(QWidget):
    def __init__(self, parent=None):
        super().__init__(parent)
        self._worker: DataWorker | None = None
        self._registry: dict = {}
        self._build_ui()
        self.refresh()

    def _build_ui(self):
        root = QVBoxLayout(self)
        root.setContentsMargins(16, 12, 16, 12)
        root.setSpacing(8)

        # Toolbar
        toolbar = QWidget()
        tl = QHBoxLayout(toolbar)
        tl.setContentsMargins(0, 0, 0, 0)
        tl.addWidget(QLabel("Runtime Registry"))
        tl.addStretch()
        btn_refresh = QPushButton("Refresh")
        btn_refresh.setFixedWidth(90)
        btn_refresh.clicked.connect(self.refresh)
        tl.addWidget(btn_refresh)
        root.addWidget(toolbar)

        # Splitter: table top, detail bottom
        splitter = QSplitter(Qt.Orientation.Vertical)

        # Registry table
        self._table = QTableWidget(0, 7)
        self._table.setHorizontalHeaderLabels([
            "Key", "Name", "Kind", "Owner", "VRAM (MB)", "Priority", "Protected"
        ])
        self._table.setAlternatingRowColors(True)
        self._table.setEditTriggers(QTableWidget.EditTrigger.NoEditTriggers)
        self._table.setSelectionBehavior(QTableWidget.SelectionBehavior.SelectRows)
        self._table.horizontalHeader().setStretchLastSection(True)
        self._table.doubleClicked.connect(self._on_row_double_click)
        splitter.addWidget(self._table)

        # Detail / can-start panel
        detail_widget = QWidget()
        dl = QVBoxLayout(detail_widget)
        dl.setContentsMargins(0, 8, 0, 0)
        dl.setSpacing(6)

        can_start_row = QWidget()
        csr = QHBoxLayout(can_start_row)
        csr.setContentsMargins(0, 0, 0, 0)
        self._can_start_label = MutedLabel("Double-click a row or click Check to test admission")
        self._btn_check = QPushButton("Check Can Start")
        self._btn_check.setObjectName("btn_accent")
        self._btn_check.setFixedWidth(140)
        self._btn_check.clicked.connect(self._check_selected)
        csr.addWidget(self._can_start_label, 1)
        csr.addWidget(self._btn_check)
        dl.addWidget(can_start_row)

        self._detail_text = QPlainTextEdit()
        self._detail_text.setReadOnly(True)
        self._detail_text.setFixedHeight(180)
        self._detail_text.setPlaceholderText("Admission decision and runtime detail will appear here.")
        dl.addWidget(self._detail_text)
        splitter.addWidget(detail_widget)

        splitter.setSizes([400, 220])
        root.addWidget(splitter, 1)

    def refresh(self):
        if self._worker and self._worker.isRunning():
            return
        self._worker = DataWorker(_fetch_registry, parent=self)
        self._worker.data_ready.connect(self._on_data)
        self._worker.start()

    def _on_data(self, registry: dict):
        self._registry = registry
        self._table.setRowCount(len(registry))
        for row, (key, r) in enumerate(registry.items()):
            self._table.setItem(row, 0, QTableWidgetItem(key))
            self._table.setItem(row, 1, QTableWidgetItem(r.get("display_name", "")))
            self._table.setItem(row, 2, QTableWidgetItem(r.get("kind", "")))
            self._table.setItem(row, 3, QTableWidgetItem(r.get("owner", "")))
            self._table.setItem(row, 4, QTableWidgetItem(str(r.get("estimated_vram_mb", 0))))
            self._table.setItem(row, 5, QTableWidgetItem(r.get("priority", "")))
            protected = "YES" if r.get("protected") else "no"
            self._table.setItem(row, 6, QTableWidgetItem(protected))
        self._table.resizeColumnsToContents()

    def _selected_key(self) -> str | None:
        rows = self._table.selectedItems()
        if not rows:
            return None
        row = self._table.currentRow()
        item = self._table.item(row, 0)
        return item.text() if item else None

    def _on_row_double_click(self, index):
        key = self._selected_key()
        if key:
            self._run_can_start(key)

    def _check_selected(self):
        key = self._selected_key()
        if not key:
            self._detail_text.setPlainText("Select a runtime row first.")
            return
        self._run_can_start(key)

    def _run_can_start(self, key: str):
        self._can_start_label.setText(f"Checking {key}…")
        worker = DataWorker(lambda k=key: can_start_runtime(k).model_dump(), parent=self)
        worker.data_ready.connect(self._on_decision)
        worker.start()

    def _on_decision(self, decision: dict):
        key = decision.get("runtime_key", "?")
        self._can_start_label.setText(
            f"{key} — {'ALLOWED' if decision.get('allowed') else 'DENIED'}"
        )
        self._detail_text.setPlainText(decision.get("explanation", ""))
