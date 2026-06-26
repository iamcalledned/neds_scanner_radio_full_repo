"""
gpu_model_manager/desktop/widgets/leases_panel.py

Leases tab: active lease table, create/release controls.
"""
from __future__ import annotations

import uuid
from typing import Any

from PySide6.QtCore import Qt
from PySide6.QtWidgets import (
    QComboBox,
    QFormLayout,
    QGroupBox,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QMessageBox,
    QPushButton,
    QSpinBox,
    QTableWidget,
    QTableWidgetItem,
    QVBoxLayout,
    QWidget,
)

from ...core.lease_manager import create_lease, list_leases, release_lease
from ...core.runtime_registry import list_runtime_keys
from ..workers import DataWorker


def _fetch():
    return [l.model_dump() for l in list_leases()]


class LeasesPanel(QWidget):
    def __init__(self, parent=None):
        super().__init__(parent)
        self._worker: DataWorker | None = None
        self._build_ui()
        self.refresh()

    def _build_ui(self):
        root = QVBoxLayout(self)
        root.setContentsMargins(16, 12, 16, 12)
        root.setSpacing(12)

        # Toolbar
        toolbar = QWidget()
        tl = QHBoxLayout(toolbar)
        tl.setContentsMargins(0, 0, 0, 0)
        tl.addWidget(QLabel("Active GPU Leases"))
        tl.addStretch()
        btn_refresh = QPushButton("Refresh")
        btn_refresh.setFixedWidth(90)
        btn_refresh.clicked.connect(self.refresh)
        tl.addWidget(btn_refresh)
        root.addWidget(toolbar)

        # Active leases table
        self._table = QTableWidget(0, 6)
        self._table.setHorizontalHeaderLabels(
            ["Lease ID", "Runtime", "Owner", "Created", "Expires", "Release"]
        )
        self._table.horizontalHeader().setStretchLastSection(False)
        self._table.setSelectionBehavior(QTableWidget.SelectionBehavior.SelectRows)
        self._table.setEditTriggers(QTableWidget.EditTrigger.NoEditTriggers)
        self._table.setColumnWidth(0, 200)
        self._table.setColumnWidth(1, 160)
        self._table.setColumnWidth(2, 140)
        self._table.setColumnWidth(3, 160)
        self._table.setColumnWidth(4, 160)
        self._table.setColumnWidth(5, 90)
        root.addWidget(self._table, 1)

        # Create lease form
        create_group = QGroupBox("Create Lease")
        form = QFormLayout(create_group)

        self._combo_runtime = QComboBox()
        for key in list_runtime_keys():
            self._combo_runtime.addItem(key)

        self._edit_owner = QLineEdit()
        self._edit_owner.setPlaceholderText("e.g. scanner_mcp, my_script")

        self._spin_ttl = QSpinBox()
        self._spin_ttl.setRange(60, 86400)
        self._spin_ttl.setValue(3600)
        self._spin_ttl.setSuffix(" s")

        btn_create = QPushButton("Create Lease")
        btn_create.setObjectName("btn_accent")
        btn_create.clicked.connect(self._on_create)

        form.addRow("Runtime:", self._combo_runtime)
        form.addRow("Owner:", self._edit_owner)
        form.addRow("TTL:", self._spin_ttl)
        form.addRow("", btn_create)
        root.addWidget(create_group)

    def refresh(self):
        if self._worker and self._worker.isRunning():
            return
        self._worker = DataWorker(_fetch, parent=self)
        self._worker.data_ready.connect(self._on_data)
        self._worker.start()

    def _on_data(self, leases: list[dict]):
        self._table.setRowCount(0)
        for row, lease in enumerate(leases):
            self._table.insertRow(row)
            lease_id = str(lease.get("lease_id", ""))
            short_id = lease_id[:8] + "…" if len(lease_id) > 8 else lease_id
            self._table.setItem(row, 0, QTableWidgetItem(short_id))
            self._table.setItem(row, 1, QTableWidgetItem(str(lease.get("runtime_key", ""))))
            self._table.setItem(row, 2, QTableWidgetItem(str(lease.get("owner", ""))))
            self._table.setItem(row, 3, QTableWidgetItem(str(lease.get("created_at", ""))[:19]))
            self._table.setItem(row, 4, QTableWidgetItem(str(lease.get("expires_at", "") or "—")[:19]))

            btn_release = QPushButton("Release")
            btn_release.setObjectName("btn_danger")
            btn_release.setFixedWidth(80)
            btn_release.clicked.connect(lambda checked=False, lid=lease_id: self._on_release(lid))
            self._table.setCellWidget(row, 5, btn_release)

    def _on_create(self):
        key = self._combo_runtime.currentText()
        owner = self._edit_owner.text().strip() or "desktop_user"
        ttl = self._spin_ttl.value()

        def _create():
            return create_lease(key, owner=owner, ttl_seconds=ttl).model_dump()

        w = DataWorker(_create, parent=self)
        w.data_ready.connect(lambda _: self.refresh())
        w.error_occurred.connect(
            lambda e: QMessageBox.critical(self, "Lease Error", str(e))
        )
        w.start()

    def _on_release(self, lease_id: str):
        ok = QMessageBox.question(
            self,
            "Release Lease",
            f"Release lease {lease_id[:8]}…?",
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
        )
        if ok != QMessageBox.StandardButton.Yes:
            return

        def _release():
            return release_lease(lease_id)

        w = DataWorker(_release, parent=self)
        w.data_ready.connect(lambda _: self.refresh())
        w.error_occurred.connect(
            lambda e: QMessageBox.warning(self, "Release Error", str(e))
        )
        w.start()
