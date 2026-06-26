"""
gpu_model_manager/desktop/widgets/logs_panel.py

Logs tab: tail log file with level filter and text search.
"""
from __future__ import annotations

from PySide6.QtCore import Qt
from PySide6.QtGui import QTextCharFormat, QColor, QTextCursor
from PySide6.QtWidgets import (
    QCheckBox,
    QComboBox,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QPlainTextEdit,
    QPushButton,
    QSpinBox,
    QVBoxLayout,
    QWidget,
)

from ...core.config import LOG_FILE
from ...core.utils import read_log_tail
from ..workers import DataWorker


def _fetch(n: int):
    return read_log_tail(LOG_FILE, n=n)


class LogsPanel(QWidget):
    def __init__(self, parent=None):
        super().__init__(parent)
        self._worker: DataWorker | None = None
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

        tl.addWidget(QLabel("Lines:"))
        self._spin_lines = QSpinBox()
        self._spin_lines.setRange(50, 2000)
        self._spin_lines.setValue(300)
        self._spin_lines.setSingleStep(50)
        tl.addWidget(self._spin_lines)

        tl.addWidget(QLabel("Level:"))
        self._combo_level = QComboBox()
        self._combo_level.addItems(["ALL", "DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"])
        tl.addWidget(self._combo_level)

        tl.addWidget(QLabel("Filter:"))
        self._edit_filter = QLineEdit()
        self._edit_filter.setPlaceholderText("text search…")
        self._edit_filter.setFixedWidth(200)
        tl.addWidget(self._edit_filter)

        tl.addStretch()
        btn_refresh = QPushButton("Refresh")
        btn_refresh.setFixedWidth(90)
        btn_refresh.clicked.connect(self.refresh)
        tl.addWidget(btn_refresh)
        root.addWidget(toolbar)

        # Log path label
        self._path_lbl = QLabel(f"Log: {LOG_FILE}")
        self._path_lbl.setStyleSheet("color: #6b7280; font-size: 11px;")
        root.addWidget(self._path_lbl)

        # Log display
        self._log_box = QPlainTextEdit()
        self._log_box.setReadOnly(True)
        self._log_box.setLineWrapMode(QPlainTextEdit.LineWrapMode.NoWrap)
        self._log_box.setStyleSheet(
            "QPlainTextEdit { font-family: Consolas, Monospace; font-size: 11px; "
            "background: #0a0c10; color: #cbd5e0; border: 1px solid #2d3748; }"
        )
        root.addWidget(self._log_box, 1)

        # Connect filter controls to re-display without re-fetch
        self._combo_level.currentTextChanged.connect(self._apply_filter)
        self._edit_filter.textChanged.connect(self._apply_filter)

        self._raw_lines: list[str] = []

    def refresh(self):
        if self._worker and self._worker.isRunning():
            return
        n = self._spin_lines.value()
        self._worker = DataWorker(lambda n=n: _fetch(n), parent=self)
        self._worker.data_ready.connect(self._on_data)
        self._worker.start()

    def _on_data(self, lines: list[str]):
        self._raw_lines = lines
        self._apply_filter()

    def _apply_filter(self):
        level = self._combo_level.currentText()
        text_filter = self._edit_filter.text().lower()

        filtered = self._raw_lines
        if level != "ALL":
            filtered = [l for l in filtered if level in l.upper()]
        if text_filter:
            filtered = [l for l in filtered if text_filter in l.lower()]

        self._log_box.setPlainText("\n".join(filtered))
        # Scroll to bottom
        cursor = self._log_box.textCursor()
        cursor.movePosition(QTextCursor.MoveOperation.End)
        self._log_box.setTextCursor(cursor)
