"""
gpu_model_manager/desktop/widgets/policy_panel.py

Policy tab: VRAM state, formula display, can-start tester.
"""
from __future__ import annotations

from PySide6.QtWidgets import (
    QCheckBox,
    QComboBox,
    QFormLayout,
    QGroupBox,
    QHBoxLayout,
    QLabel,
    QPlainTextEdit,
    QPushButton,
    QVBoxLayout,
    QWidget,
)

from ...core.policy_engine import can_start_runtime, get_policy_status
from ...core.runtime_registry import list_runtime_keys
from ..workers import DataWorker
from .common import VramBar


def _fetch():
    return get_policy_status().model_dump()


class PolicyPanel(QWidget):
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
        tl.addWidget(QLabel("VRAM Policy Engine"))
        tl.addStretch()
        btn_refresh = QPushButton("Refresh")
        btn_refresh.setFixedWidth(90)
        btn_refresh.clicked.connect(self.refresh)
        tl.addWidget(btn_refresh)
        root.addWidget(toolbar)

        # Stats group
        stats_group = QGroupBox("Current VRAM State")
        form = QFormLayout(stats_group)

        self._lbl_total = QLabel("—")
        self._lbl_used = QLabel("—")
        self._lbl_free = QLabel("—")
        self._lbl_safety = QLabel("—")
        self._lbl_reserve = QLabel("—")
        self._lbl_effective = QLabel("—")
        self._lbl_effective.setObjectName("label_accent")
        self._lbl_effective.setStyleSheet("font-size: 16px; font-weight: bold;")

        form.addRow("Total VRAM:", self._lbl_total)
        form.addRow("Used:", self._lbl_used)
        form.addRow("Free:", self._lbl_free)
        form.addRow("Safety margin (−):", self._lbl_safety)
        form.addRow("Scanner reserve (−):", self._lbl_reserve)
        form.addRow("= Effective available:", self._lbl_effective)

        self._vram_bar = VramBar()
        form.addRow("Usage:", self._vram_bar)
        root.addWidget(stats_group)

        # Formula group
        formula_group = QGroupBox("Policy Formula")
        fl = QVBoxLayout(formula_group)
        formula_lbl = QLabel(
            "effective_available_mb =\n"
            "    free_vram_mb\n"
            "    - GPU_SAFETY_MARGIN_MB\n"
            "    - SCANNER_WHISPER_RESERVED_MB\n\n"
            "allowed = effective_available_mb >= runtime.estimated_vram_mb"
        )
        formula_lbl.setObjectName("label_accent")
        formula_lbl.setStyleSheet(
            "font-family: Consolas, monospace; font-size: 12px; "
            "background: #0a0c10; padding: 8px; border-radius: 4px;"
        )
        fl.addWidget(formula_lbl)
        root.addWidget(formula_group)

        # Can-start tester
        tester_group = QGroupBox("Admission Tester")
        tl2 = QVBoxLayout(tester_group)

        input_row = QWidget()
        ir = QHBoxLayout(input_row)
        ir.setContentsMargins(0, 0, 0, 0)
        self._runtime_combo = QComboBox()
        for key in list_runtime_keys():
            self._runtime_combo.addItem(key)
        self._force_check = QCheckBox("Force override")
        btn_check = QPushButton("Check Can Start")
        btn_check.setObjectName("btn_accent")
        btn_check.setFixedWidth(140)
        btn_check.clicked.connect(self._run_check)
        ir.addWidget(self._runtime_combo, 1)
        ir.addWidget(self._force_check)
        ir.addWidget(btn_check)
        tl2.addWidget(input_row)

        self._result_text = QPlainTextEdit()
        self._result_text.setReadOnly(True)
        self._result_text.setFixedHeight(160)
        self._result_text.setPlaceholderText("Decision output will appear here.")
        tl2.addWidget(self._result_text)
        root.addWidget(tester_group)

        root.addStretch()

    def refresh(self):
        if self._worker and self._worker.isRunning():
            return
        self._worker = DataWorker(_fetch, parent=self)
        self._worker.data_ready.connect(self._on_data)
        self._worker.start()

    def _on_data(self, policy: dict):
        self._lbl_total.setText(f"{policy.get('total_vram_mb', 0):.0f} MB")
        self._lbl_used.setText(f"{policy.get('used_vram_mb', 0):.0f} MB")
        self._lbl_free.setText(f"{policy.get('free_vram_mb', 0):.0f} MB")
        self._lbl_safety.setText(f"{policy.get('gpu_safety_margin_mb', 0)} MB")
        self._lbl_reserve.setText(f"{policy.get('scanner_whisper_reserved_mb', 0)} MB")
        self._lbl_effective.setText(f"{policy.get('effective_available_mb', 0):.0f} MB")
        self._vram_bar.update(
            policy.get("used_vram_mb", 0),
            policy.get("total_vram_mb", 0),
        )

    def _run_check(self):
        key = self._runtime_combo.currentText()
        force = self._force_check.isChecked()
        self._result_text.setPlainText(f"Checking {key}…")

        worker = DataWorker(
            lambda k=key, f=force: can_start_runtime(k, force=f).model_dump(),
            parent=self,
        )
        worker.data_ready.connect(self._on_decision)
        worker.start()

    def _on_decision(self, decision: dict):
        self._result_text.setPlainText(decision.get("explanation", ""))
