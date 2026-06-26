"""
desktop/widgets/scanner_panel.py — Scanner operational health tab.

Shows:
  - Pipeline flow visualization (Recorder → Redis → Transcriber → MCP → Archive/DB)
  - Overall health badge
  - Plain-English health summary
  - Problems & Suggested Actions
  - Path Checks table
  - Stack Validation result
"""
from __future__ import annotations

from PySide6.QtCore import Qt
from PySide6.QtWidgets import (
    QFrame,
    QGroupBox,
    QHBoxLayout,
    QHeaderView,
    QLabel,
    QPushButton,
    QScrollArea,
    QSizePolicy,
    QTableWidget,
    QTableWidgetItem,
    QTextEdit,
    QVBoxLayout,
    QWidget,
)

from modular_dashboard.core.scanner_actions import validate_scanner_stack
from modular_dashboard.core.scanner_health import get_scanner_health
from modular_dashboard.core.schemas import OverallStatus, ServiceState
from modular_dashboard.desktop import style as S
from modular_dashboard.desktop.workers import BackgroundWorker, StackActionWorker
from modular_dashboard.desktop.widgets.common import ActionButton, BigStatusBanner


# ── Pipeline stages ─────────────────────────────────────────────────────────
_PIPELINE_STAGES = [
    ("scanner-recorder.service",    "Recorder",    "Captures SDR audio"),
    ("scanner:stream:new_call",     "Redis Stream","Audio segments queued"),
    ("scanner-transcriber.service", "Transcriber", "Whisper STT"),
    ("scanner-mcp.service",         "MCP Service", "Analysis & enrichment"),
    ("scanner-websocket.service",   "WebSocket",   "Live push to UI"),
]


class _PipelineStageWidget(QFrame):
    """A single pipeline stage card."""

    def __init__(self, stage_name: str, sub: str, parent=None):
        super().__init__(parent)
        self.setMinimumWidth(130)
        self.setMinimumHeight(72)
        self.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Fixed)
        self.setStyleSheet(
            f"background: {S.BG_CARD}; border: 1px solid {S.BORDER_MED}; border-radius: 8px;"
        )
        layout = QVBoxLayout(self)
        layout.setContentsMargins(10, 8, 10, 8)
        layout.setSpacing(3)

        self._name_lbl = QLabel(stage_name)
        self._name_lbl.setStyleSheet(
            f"color: {S.TEXT_PRIMARY}; font-weight: bold; font-size: 12px; "
            "background: transparent; border: none;"
        )
        layout.addWidget(self._name_lbl)

        self._sub_lbl = QLabel(sub)
        self._sub_lbl.setStyleSheet(
            f"color: {S.TEXT_DIM}; font-size: 10px; background: transparent; border: none;"
        )
        layout.addWidget(self._sub_lbl)

        self._status_lbl = QLabel("—")
        self._status_lbl.setStyleSheet(
            f"color: {S.TEXT_MUTED}; font-size: 11px; font-weight: bold; "
            "background: transparent; border: none;"
        )
        layout.addWidget(self._status_lbl)

    def set_status(self, status: str, color: str):
        self._status_lbl.setText(status.upper())
        self._status_lbl.setStyleSheet(
            f"color: {color}; font-size: 11px; font-weight: bold; "
            "background: transparent; border: none;"
        )
        self.setStyleSheet(
            f"background: {S.BG_CARD}; border: 1px solid {color}; border-radius: 8px;"
        )


class _Arrow(QLabel):
    def __init__(self, parent=None):
        super().__init__("→", parent)
        self.setStyleSheet(
            f"color: {S.BORDER_MED}; font-size: 20px; background: transparent;"
        )
        self.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self.setFixedWidth(28)


class ScannerPanel(QWidget):
    """The Scanner tab panel."""

    def __init__(self, parent=None):
        super().__init__(parent)
        self._health_worker = None
        self._validate_worker = None
        self._pipeline_stages: dict[str, _PipelineStageWidget] = {}
        self._build_ui()
        self.refresh()

    def _build_ui(self):
        layout = QVBoxLayout(self)
        layout.setContentsMargins(14, 14, 14, 14)
        layout.setSpacing(12)

        # ── Toolbar ─────────────────────────────────────────────────────────
        toolbar = QHBoxLayout()
        refresh_btn = QPushButton("↻  Refresh")
        refresh_btn.setStyleSheet(S.BTN_DEFAULT)
        refresh_btn.clicked.connect(self.refresh)
        toolbar.addWidget(refresh_btn)
        toolbar.addStretch()
        validate_btn = ActionButton("✓  Validate Stack", "blue")
        validate_btn.clicked.connect(self.validate_stack)
        toolbar.addWidget(validate_btn)
        layout.addLayout(toolbar)

        # ── Status banner ────────────────────────────────────────────────────
        self._banner = BigStatusBanner()
        layout.addWidget(self._banner)

        scroll = QScrollArea()
        scroll.setWidgetResizable(True)
        scroll.setStyleSheet("QScrollArea { border: none; }")
        content = QWidget()
        cl = QVBoxLayout(content)
        cl.setContentsMargins(0, 0, 0, 0)
        cl.setSpacing(14)
        scroll.setWidget(content)
        layout.addWidget(scroll)

        # ── Pipeline flow ────────────────────────────────────────────────────
        flow_grp = QGroupBox("Pipeline Flow")
        flow_grp.setStyleSheet(
            f"QGroupBox {{ border: 1px solid {S.BORDER_DIM}; border-radius: 8px; "
            f"margin-top: 10px; padding-top: 10px; color: {S.TEXT_MUTED}; font-weight: bold; }}"
        )
        flow_row = QHBoxLayout(flow_grp)
        flow_row.setContentsMargins(12, 8, 12, 8)
        flow_row.setSpacing(0)

        for i, (svc_key, label, sub) in enumerate(_PIPELINE_STAGES):
            stage = _PipelineStageWidget(label, sub)
            self._pipeline_stages[svc_key] = stage
            flow_row.addWidget(stage)
            if i < len(_PIPELINE_STAGES) - 1:
                flow_row.addWidget(_Arrow())

        cl.addWidget(flow_grp)

        # ── Problems & Actions ───────────────────────────────────────────────
        detail_row = QHBoxLayout()
        detail_row.setSpacing(12)

        problems_grp = QGroupBox("Problems")
        problems_grp.setStyleSheet(flow_grp.styleSheet())
        pl = QVBoxLayout(problems_grp)
        self._problems_text = QTextEdit()
        self._problems_text.setReadOnly(True)
        self._problems_text.setMinimumHeight(110)
        self._problems_text.setStyleSheet(
            "background: #1a0808; color: #fca5a5; font-size: 12px; border: none;"
        )
        pl.addWidget(self._problems_text)
        detail_row.addWidget(problems_grp)

        actions_grp = QGroupBox("Suggested Next Actions")
        actions_grp.setStyleSheet(flow_grp.styleSheet())
        al = QVBoxLayout(actions_grp)
        self._actions_text = QTextEdit()
        self._actions_text.setReadOnly(True)
        self._actions_text.setMinimumHeight(110)
        self._actions_text.setStyleSheet(
            "background: #0a1628; color: #93c5fd; font-size: 12px; "
            "font-family: 'Courier New', monospace; border: none;"
        )
        al.addWidget(self._actions_text)
        detail_row.addWidget(actions_grp)

        cl.addLayout(detail_row)

        # ── Paths table ──────────────────────────────────────────────────────
        paths_grp = QGroupBox("Filesystem Path Checks")
        paths_grp.setStyleSheet(flow_grp.styleSheet())
        paths_inner = QVBoxLayout(paths_grp)
        self._paths_table = QTableWidget()
        self._paths_table.setColumnCount(5)
        self._paths_table.setHorizontalHeaderLabels(
            ["Label", "Path", "Exists?", "Type", "Access"]
        )
        hdr = self._paths_table.horizontalHeader()
        hdr.setSectionResizeMode(1, QHeaderView.ResizeMode.Stretch)
        self._paths_table.setColumnWidth(0, 160)
        self._paths_table.setColumnWidth(2, 70)
        self._paths_table.setColumnWidth(3, 60)
        self._paths_table.setColumnWidth(4, 120)
        self._paths_table.setEditTriggers(QTableWidget.EditTrigger.NoEditTriggers)
        self._paths_table.verticalHeader().setVisible(False)
        self._paths_table.setMaximumHeight(220)
        self._paths_table.setAlternatingRowColors(True)
        paths_inner.addWidget(self._paths_table)
        cl.addWidget(paths_grp)

        # ── Validation result ────────────────────────────────────────────────
        val_grp = QGroupBox("Stack Validation Result")
        val_grp.setStyleSheet(flow_grp.styleSheet())
        val_inner = QVBoxLayout(val_grp)
        self._validate_text = QTextEdit()
        self._validate_text.setReadOnly(True)
        self._validate_text.setMinimumHeight(130)
        self._validate_text.setStyleSheet(
            f"font-family: 'Courier New', monospace; font-size: 11px; "
            f"background: {S.BG_CARD_DARK}; border: none;"
        )
        self._validate_text.setPlaceholderText("Click '✓ Validate Stack' to run full diagnostics.")
        val_inner.addWidget(self._validate_text)
        cl.addWidget(val_grp)

        cl.addStretch()

        # ── Timestamp ────────────────────────────────────────────────────────
        self._ts_lbl = QLabel("")
        self._ts_lbl.setStyleSheet(f"color: {S.TEXT_DIM}; font-size: 10px;")
        layout.addWidget(self._ts_lbl)

    # ── Refresh ──────────────────────────────────────────────────────────────

    def refresh(self):
        if self._health_worker and self._health_worker.isRunning():
            return
        self._health_worker = BackgroundWorker(get_scanner_health)
        self._health_worker.finished.connect(self._on_health)
        self._health_worker.error.connect(
            lambda e: self._banner.update("unknown", f"Error: {e}")
        )
        self._health_worker.start()

    def _on_health(self, health):
        self._banner.update(health.overall.value, health.summary)

        svc_by_name = {}
        if health.services:
            svc_by_name = {s.name: s for s in health.services.services}

        for svc_key, stage_widget in self._pipeline_stages.items():
            if svc_key == "scanner:stream:new_call":
                # Redis stream status from queue
                if health.queue:
                    qh = health.queue.backlog_health
                    color = S.status_color(qh.value)
                    if health.queue.pending_count is not None:
                        txt = f"{health.queue.pending_count:,} pending"
                    elif health.queue.stream_length is not None:
                        txt = f"XLEN {health.queue.stream_length:,}"
                    else:
                        txt = qh.value
                    stage_widget.set_status(txt, color)
                else:
                    stage_widget.set_status("unknown", S.STATUS_UNKNOWN)
            else:
                svc = svc_by_name.get(svc_key)
                if svc:
                    color = S.status_color(svc.active_state.value)
                    stage_widget.set_status(svc.active_state.value, color)
                else:
                    stage_widget.set_status("not found", S.STATUS_MISSING)

        self._problems_text.setPlainText(
            "\n".join(f"• {p}" for p in health.problems) if health.problems else "✓ No problems detected."
        )
        self._actions_text.setPlainText(
            "\n".join(f"→ {a}" for a in health.next_actions) if health.next_actions else "No actions needed."
        )

        self._paths_table.setRowCount(0)
        for ps in health.paths:
            row = self._paths_table.rowCount()
            self._paths_table.insertRow(row)
            self._paths_table.setItem(row, 0, QTableWidgetItem(ps.label))
            self._paths_table.setItem(row, 1, QTableWidgetItem(ps.path))

            exists_item = QTableWidgetItem("yes" if ps.exists else "NO")
            exists_item.setForeground(
                _qcolor(S.STATUS_HEALTHY if ps.exists else S.STATUS_CRITICAL)
            )
            exists_item.setTextAlignment(Qt.AlignmentFlag.AlignCenter)
            self._paths_table.setItem(row, 2, exists_item)

            kind = "dir" if ps.is_dir else ("file" if ps.is_file else "—")
            self._paths_table.setItem(row, 3, QTableWidgetItem(kind))

            parts = []
            if ps.readable:
                parts.append("read")
            if ps.writable:
                parts.append("write")
            access = "/".join(parts) if parts else ("—" if ps.exists else "n/a")
            self._paths_table.setItem(row, 4, QTableWidgetItem(access))

        self._ts_lbl.setText(f"Last updated: {health.timestamp}")

    # ── Validation ───────────────────────────────────────────────────────────

    def validate_stack(self):
        if self._validate_worker and self._validate_worker.isRunning():
            return
        self._validate_text.setPlainText("Running validation…")
        self._validate_worker = StackActionWorker(validate_scanner_stack)
        self._validate_worker.finished.connect(self._on_validate_done)
        self._validate_worker.error.connect(
            lambda e: self._validate_text.setPlainText(f"Error: {e}")
        )
        self._validate_worker.start()

    def _on_validate_done(self, result):
        lines = [f"Exit code: {result.returncode}"]
        if result.stdout:
            lines.append("\n--- stdout ---")
            lines.extend(result.stdout.splitlines()[-80:])
        if result.stderr:
            lines.append("\n--- stderr ---")
            lines.extend(result.stderr.splitlines()[-40:])
        self._validate_text.setPlainText("\n".join(lines))


def _qcolor(hex_str: str):
    from PySide6.QtGui import QColor
    return QColor(hex_str)
