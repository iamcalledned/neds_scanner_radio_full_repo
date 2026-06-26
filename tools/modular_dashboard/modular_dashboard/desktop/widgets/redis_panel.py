"""
desktop/widgets/redis_panel.py — Redis / Queue inspection tab.

Key improvements:
  - Clearly distinguishes stream XLEN (retained history) from true pending (CG pending)
  - Shows first/last entry IDs so you can see how old the stream is
  - Shows consumer group info if groups exist
  - Plain-English interpretation panel explaining what the numbers mean
  - Related scanner keys table
"""
from __future__ import annotations

from PySide6.QtCore import Qt
from PySide6.QtGui import QColor
from PySide6.QtWidgets import (
    QFrame,
    QGroupBox,
    QHBoxLayout,
    QHeaderView,
    QLabel,
    QPushButton,
    QScrollArea,
    QTableWidget,
    QTableWidgetItem,
    QTextEdit,
    QVBoxLayout,
    QWidget,
)

from modular_dashboard.core.scanner_queue import (
    check_redis,
    get_scanner_queue_status,
    get_scanner_related_keys,
)
from modular_dashboard.core.schemas import OverallStatus
from modular_dashboard.core.utils import format_age_seconds
from modular_dashboard.desktop import style as S
from modular_dashboard.desktop.workers import BackgroundWorker
from modular_dashboard.desktop.widgets.common import InfoRow, SectionHeader


def _load_all():
    redis_status = check_redis()
    queue = None
    keys = []
    if redis_status.reachable:
        queue = get_scanner_queue_status()
        keys = get_scanner_related_keys(100)
    return redis_status, queue, keys


def _qcolor(hex_str: str) -> QColor:
    return QColor(hex_str)


class RedisPanel(QWidget):
    """The Redis / Queue tab panel."""

    def __init__(self, parent=None):
        super().__init__(parent)
        self._worker = None
        self._build_ui()
        self.refresh()

    def _build_ui(self):
        layout = QVBoxLayout(self)
        layout.setContentsMargins(14, 14, 14, 14)
        layout.setSpacing(12)

        # ── Toolbar ──────────────────────────────────────────────────────────
        toolbar = QHBoxLayout()
        refresh_btn = QPushButton("↻  Refresh")
        refresh_btn.setStyleSheet(S.BTN_DEFAULT)
        refresh_btn.clicked.connect(self.refresh)
        toolbar.addWidget(refresh_btn)
        toolbar.addStretch()
        layout.addLayout(toolbar)

        # ── Connection info bar ──────────────────────────────────────────────
        conn_frame = QFrame()
        conn_frame.setStyleSheet(
            f"background: {S.BG_CARD}; border: 1px solid {S.BORDER_DIM}; border-radius: 8px;"
        )
        conn_row = QHBoxLayout(conn_frame)
        conn_row.setContentsMargins(16, 10, 16, 10)
        conn_row.setSpacing(20)

        self._conn_status_lbl = QLabel("● Checking…")
        self._conn_status_lbl.setStyleSheet(
            f"color: {S.TEXT_MUTED}; font-size: 14px; font-weight: bold; background: transparent; border: none;"
        )
        conn_row.addWidget(self._conn_status_lbl)

        self._conn_info_lbl = QLabel("")
        self._conn_info_lbl.setStyleSheet(
            f"color: {S.TEXT_DIM}; font-size: 11px; font-family: monospace; background: transparent; border: none;"
        )
        conn_row.addWidget(self._conn_info_lbl, 1)

        layout.addWidget(conn_frame)

        # ── Scrollable content ───────────────────────────────────────────────
        scroll = QScrollArea()
        scroll.setWidgetResizable(True)
        scroll.setStyleSheet("QScrollArea { border: none; }")
        content = QWidget()
        cl = QVBoxLayout(content)
        cl.setContentsMargins(0, 0, 0, 0)
        cl.setSpacing(14)
        scroll.setWidget(content)
        layout.addWidget(scroll)

        grp_style = (
            f"QGroupBox {{ border: 1px solid {S.BORDER_DIM}; border-radius: 8px; "
            f"margin-top: 10px; padding-top: 10px; color: {S.TEXT_MUTED}; font-weight: bold; }}"
        )

        # ── Stream diagnostics ───────────────────────────────────────────────
        stream_grp = QGroupBox("Stream Diagnostics — scanner:stream:new_call")
        stream_grp.setStyleSheet(grp_style)
        stream_layout = QVBoxLayout(stream_grp)
        stream_layout.setSpacing(4)
        stream_layout.setContentsMargins(12, 8, 12, 12)

        self._xlen_row     = InfoRow("Stream length (XLEN)")
        self._first_id_row = InfoRow("First entry ID (oldest)")
        self._last_id_row  = InfoRow("Last entry ID (newest)")
        self._first_age_row= InfoRow("Oldest entry age")
        self._last_age_row = InfoRow("Newest entry age")
        self._cg_count_row = InfoRow("Consumer groups")
        self._pending_row  = InfoRow("Total CG pending")
        self._health_row   = InfoRow("Backlog health")

        for row in (
            self._xlen_row, self._first_id_row, self._last_id_row,
            self._first_age_row, self._last_age_row,
            self._cg_count_row, self._pending_row, self._health_row,
        ):
            stream_layout.addWidget(row)

        cl.addWidget(stream_grp)

        # ── Interpretation text ──────────────────────────────────────────────
        interp_grp = QGroupBox("Interpretation — What Do These Numbers Mean?")
        interp_grp.setStyleSheet(grp_style)
        interp_layout = QVBoxLayout(interp_grp)
        self._interp_text = QTextEdit()
        self._interp_text.setReadOnly(True)
        self._interp_text.setMinimumHeight(100)
        self._interp_text.setMaximumHeight(150)
        self._interp_text.setStyleSheet(
            f"background: #0d1f35; color: #93c5fd; font-size: 12px; "
            f"font-family: 'Segoe UI', sans-serif; border: none;"
        )
        interp_layout.addWidget(self._interp_text)
        cl.addWidget(interp_grp)

        # ── Consumer Groups table ────────────────────────────────────────────
        cg_grp = QGroupBox("Consumer Groups")
        cg_grp.setStyleSheet(grp_style)
        cg_inner = QVBoxLayout(cg_grp)
        self._cg_table = QTableWidget()
        self._cg_table.setColumnCount(4)
        self._cg_table.setHorizontalHeaderLabels(
            ["Group", "Consumers", "Pending", "Last Delivered ID"]
        )
        self._cg_table.horizontalHeader().setSectionResizeMode(
            3, QHeaderView.ResizeMode.Stretch
        )
        self._cg_table.setEditTriggers(QTableWidget.EditTrigger.NoEditTriggers)
        self._cg_table.verticalHeader().setVisible(False)
        self._cg_table.setMaximumHeight(160)
        self._cg_table.setAlternatingRowColors(True)
        cg_inner.addWidget(self._cg_table)
        cl.addWidget(cg_grp)

        # ── Recent queue entries ──────────────────────────────────────────────
        recent_grp = QGroupBox("20 Most Recent Stream Entries (newest first)")
        recent_grp.setStyleSheet(grp_style)
        recent_inner = QVBoxLayout(recent_grp)
        self._recent_table = QTableWidget()
        self._recent_table.setColumnCount(2)
        self._recent_table.setHorizontalHeaderLabels(["Entry ID", "Fields"])
        self._recent_table.horizontalHeader().setSectionResizeMode(
            1, QHeaderView.ResizeMode.Stretch
        )
        self._recent_table.setEditTriggers(QTableWidget.EditTrigger.NoEditTriggers)
        self._recent_table.verticalHeader().setVisible(False)
        self._recent_table.setMaximumHeight(240)
        self._recent_table.setAlternatingRowColors(True)
        recent_inner.addWidget(self._recent_table)
        cl.addWidget(recent_grp)

        # ── Related keys ─────────────────────────────────────────────────────
        keys_grp = QGroupBox("Scanner-Related Redis Keys")
        keys_grp.setStyleSheet(grp_style)
        keys_inner = QVBoxLayout(keys_grp)
        self._keys_table = QTableWidget()
        self._keys_table.setColumnCount(3)
        self._keys_table.setHorizontalHeaderLabels(["Key", "Type", "Count"])
        self._keys_table.horizontalHeader().setSectionResizeMode(
            0, QHeaderView.ResizeMode.Stretch
        )
        self._keys_table.setEditTriggers(QTableWidget.EditTrigger.NoEditTriggers)
        self._keys_table.verticalHeader().setVisible(False)
        self._keys_table.setMaximumHeight(200)
        self._keys_table.setAlternatingRowColors(True)
        keys_inner.addWidget(self._keys_table)
        cl.addWidget(keys_grp)

        cl.addStretch()

    # ── Refresh ──────────────────────────────────────────────────────────────

    def refresh(self):
        if self._worker and self._worker.isRunning():
            return
        self._worker = BackgroundWorker(_load_all)
        self._worker.finished.connect(self._on_data)
        self._worker.error.connect(
            lambda e: self._conn_status_lbl.setText(f"Error: {e}")
        )
        self._worker.start()

    def _on_data(self, data):
        redis_status, queue, keys = data

        # Connection row
        if redis_status.reachable:
            self._conn_status_lbl.setText("● Redis reachable")
            self._conn_status_lbl.setStyleSheet(
                f"color: {S.STATUS_HEALTHY}; font-size: 14px; font-weight: bold; "
                "background: transparent; border: none;"
            )
            info_parts = [f"{k}: {v}" for k, v in redis_status.server_info.items()]
            self._conn_info_lbl.setText("  |  ".join(info_parts))
        else:
            self._conn_status_lbl.setText(f"✗ Redis unreachable: {redis_status.error or ''}")
            self._conn_status_lbl.setStyleSheet(
                f"color: {S.STATUS_CRITICAL}; font-size: 14px; font-weight: bold; "
                "background: transparent; border: none;"
            )
            self._conn_info_lbl.setText(f"URL: {redis_status.url}")

        # Stream diagnostics
        if queue:
            xlen = queue.stream_length
            self._xlen_row.set_value(
                f"{xlen:,}" if xlen is not None else "n/a",
                S.TEXT_PRIMARY
            )

            self._first_id_row.set_value(queue.first_entry_id or "—", S.TEXT_DIM)
            self._last_id_row.set_value(queue.last_entry_id or "—", S.TEXT_DIM)

            # Ages from IDs
            first_age = None
            last_age  = None
            if queue.first_entry_id:
                try:
                    import time
                    ms = int(queue.first_entry_id.split("-")[0])
                    first_age = max((time.time() * 1000 - ms) / 1000.0, 0)
                except Exception:
                    pass
            if queue.last_entry_id:
                try:
                    import time
                    ms = int(queue.last_entry_id.split("-")[0])
                    last_age = max((time.time() * 1000 - ms) / 1000.0, 0)
                except Exception:
                    pass

            self._first_age_row.set_value(
                format_age_seconds(first_age) if first_age is not None else "—"
            )
            self._last_age_row.set_value(
                format_age_seconds(last_age) if last_age is not None else "—",
                S.STATUS_HEALTHY if (last_age is not None and last_age < 60) else S.TEXT_PRIMARY
            )

            cg_count = len(queue.consumer_groups)
            self._cg_count_row.set_value(
                str(cg_count) if cg_count else "0 (no consumer groups)",
                S.TEXT_MUTED if cg_count == 0 else S.TEXT_PRIMARY
            )

            pending = queue.pending_count
            if pending is not None:
                pen_color = (
                    S.STATUS_CRITICAL if pending > 500
                    else S.STATUS_WARNING if pending > 100
                    else S.STATUS_HEALTHY
                )
                self._pending_row.set_value(f"{pending:,}", pen_color)
            else:
                self._pending_row.set_value(
                    "N/A — no consumer groups",
                    S.TEXT_MUTED
                )

            health = queue.backlog_health
            h_color = S.status_color(health.value)
            self._health_row.set_value(health.value.upper(), h_color)

            # Interpretation text
            self._interp_text.setPlainText(self._build_interpretation(queue, first_age, last_age))

            # Consumer groups table
            self._cg_table.setRowCount(0)
            for g in queue.consumer_groups:
                row = self._cg_table.rowCount()
                self._cg_table.insertRow(row)
                self._cg_table.setItem(row, 0, QTableWidgetItem(g.get("name", "")))
                self._cg_table.setItem(row, 1, QTableWidgetItem(str(g.get("consumers", 0))))
                pen_item = QTableWidgetItem(str(g.get("pending", 0)))
                if g.get("pending", 0) > 0:
                    pen_item.setForeground(_qcolor(S.STATUS_WARNING))
                self._cg_table.setItem(row, 2, pen_item)
                self._cg_table.setItem(row, 3, QTableWidgetItem(g.get("last_delivered_id", "")))

            # Recent items
            self._recent_table.setRowCount(0)
            for item in queue.recent_items:
                row = self._recent_table.rowCount()
                self._recent_table.insertRow(row)
                self._recent_table.setItem(row, 0, QTableWidgetItem(item.get("id", "")))
                fields_str = ", ".join(
                    f"{k}={v}" for k, v in (item.get("fields") or {}).items()
                )[:200]
                self._recent_table.setItem(row, 1, QTableWidgetItem(fields_str))

        # Related keys
        self._keys_table.setRowCount(0)
        for entry in keys:
            row = self._keys_table.rowCount()
            self._keys_table.insertRow(row)
            self._keys_table.setItem(row, 0, QTableWidgetItem(entry.get("key", "")))
            self._keys_table.setItem(row, 1, QTableWidgetItem(entry.get("type", "")))
            cnt = entry.get("count")
            self._keys_table.setItem(row, 2, QTableWidgetItem(str(cnt) if cnt is not None else "—"))

    def _build_interpretation(self, queue, first_age, last_age) -> str:
        xlen = queue.stream_length or 0
        cg_count = len(queue.consumer_groups)
        lines = []

        if cg_count == 0:
            lines.append(
                f"Stream has {xlen:,} entries but NO consumer groups are registered.\n"
                "This means:\n"
                "  • The stream length (XLEN) is RETAINED HISTORY, not a backlog of pending work.\n"
                "  • Nothing is consuming from this stream via XREAD/XREADGROUP consumer groups.\n"
                "  • High XLEN is expected if the stream has been running for a long time without trimming.\n"
            )
            if first_age is not None:
                lines.append(
                    f"  • Oldest entry is {format_age_seconds(first_age)} old — this tells you how long "
                    "the stream has been accumulating retained data.\n"
                )
            lines.append(
                "  • Health is reported as WARNING (not CRITICAL) in auto mode because there is no\n"
                "    proven pending backlog without consumer groups.\n"
                "\n"
                "To treat XLEN as backlog (legacy behaviour), set:\n"
                "    SCANNER_QUEUE_BACKLOG_MODE=stream_length"
            )
        else:
            total_pending = sum(g.get("pending", 0) for g in queue.consumer_groups)
            lines.append(
                f"Stream has {xlen:,} total entries and {cg_count} consumer group(s).\n"
                f"Total consumer-group pending: {total_pending:,} messages.\n"
                "\n"
                "  • XLEN = total retained stream entries (including already-processed ones)\n"
                "  • CG pending = messages delivered to a group but not yet acknowledged (true backlog)\n"
                "  • Health is based on CG pending count, not XLEN.\n"
            )
            if total_pending == 0:
                lines.append("  ✓ No pending work — all delivered messages have been acknowledged.")
            else:
                lines.append(f"  ⚠ {total_pending:,} messages are pending acknowledgment.")

        return "\n".join(lines)
