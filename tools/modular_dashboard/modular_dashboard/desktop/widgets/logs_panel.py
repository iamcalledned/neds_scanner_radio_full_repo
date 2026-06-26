"""
desktop/widgets/logs_panel.py — Logs tab.

Improvements:
  - Shows file path, existence, size, last-modified time
  - Friendly empty-state message when log file doesn't exist yet
  - "Clear View" button (clears text display, does NOT delete the file)
  - "Open Log Folder" button
  - Refresh button
  - Last N lines (default 300)
"""
from __future__ import annotations

from datetime import datetime
from pathlib import Path

from PySide6.QtCore import Qt
from PySide6.QtWidgets import (
    QHBoxLayout,
    QLabel,
    QPushButton,
    QTextEdit,
    QVBoxLayout,
    QWidget,
)

from modular_dashboard.core.config import LOG_FILE
from modular_dashboard.desktop import style as S
from modular_dashboard.desktop.workers import BackgroundWorker

_LOG_LINES = 300


def _read_logs(n: int = _LOG_LINES) -> dict:
    log_path = Path(LOG_FILE)
    info = {
        "path":    str(log_path),
        "exists":  log_path.exists(),
        "size":    None,
        "mtime":   None,
        "lines":   [],
        "error":   None,
    }
    if not log_path.exists():
        info["error"] = f"Log file not yet created: {log_path}"
        return info
    try:
        stat = log_path.stat()
        info["size"]  = stat.st_size
        info["mtime"] = datetime.fromtimestamp(stat.st_mtime).strftime("%Y-%m-%d %H:%M:%S")
        all_lines = log_path.read_text(encoding="utf-8", errors="replace").splitlines()
        info["lines"] = all_lines[-n:]
    except Exception as exc:
        info["error"] = str(exc)
    return info


def _fmt_size(b: int) -> str:
    if b < 1024:
        return f"{b} B"
    if b < 1024 * 1024:
        return f"{b / 1024:.1f} KB"
    return f"{b / 1024 / 1024:.1f} MB"


class LogsPanel(QWidget):
    """The Logs tab panel."""

    def __init__(self, parent=None):
        super().__init__(parent)
        self._worker = None
        self._build_ui()
        self.refresh()

    def _build_ui(self):
        layout = QVBoxLayout(self)
        layout.setContentsMargins(14, 14, 14, 14)
        layout.setSpacing(10)

        # ── Toolbar ──────────────────────────────────────────────────────────
        toolbar = QHBoxLayout()

        refresh_btn = QPushButton("↻  Refresh")
        refresh_btn.setStyleSheet(S.BTN_DEFAULT)
        refresh_btn.clicked.connect(self.refresh)
        toolbar.addWidget(refresh_btn)

        clear_btn = QPushButton("✕  Clear View")
        clear_btn.setStyleSheet(S.BTN_DEFAULT)
        clear_btn.setToolTip("Clears the text display — does NOT delete the file")
        clear_btn.clicked.connect(lambda: self._log_text.clear())
        toolbar.addWidget(clear_btn)

        open_btn = QPushButton("📁  Open Log Folder")
        open_btn.setStyleSheet(S.BTN_BLUE)
        open_btn.clicked.connect(self._open_folder)
        toolbar.addWidget(open_btn)

        toolbar.addStretch()

        self._ts_lbl = QLabel("")
        self._ts_lbl.setStyleSheet(f"color: {S.TEXT_DIM}; font-size: 10px;")
        toolbar.addWidget(self._ts_lbl)

        layout.addLayout(toolbar)

        # ── File info bar ─────────────────────────────────────────────────────
        info_row = QHBoxLayout()
        info_row.setSpacing(20)

        self._path_lbl = QLabel(f"Path: {LOG_FILE}")
        self._path_lbl.setStyleSheet(
            f"color: {S.TEXT_MUTED}; font-size: 11px; font-family: monospace;"
        )
        self._path_lbl.setTextInteractionFlags(
            Qt.TextInteractionFlag.TextSelectableByMouse
        )
        info_row.addWidget(self._path_lbl)

        self._size_lbl = QLabel("")
        self._size_lbl.setStyleSheet(f"color: {S.TEXT_DIM}; font-size: 11px;")
        info_row.addWidget(self._size_lbl)

        self._mtime_lbl = QLabel("")
        self._mtime_lbl.setStyleSheet(f"color: {S.TEXT_DIM}; font-size: 11px;")
        info_row.addWidget(self._mtime_lbl)

        info_row.addStretch()
        layout.addLayout(info_row)

        # ── Log text area ──────────────────────────────────────────────────────
        self._log_text = QTextEdit()
        self._log_text.setReadOnly(True)
        self._log_text.setStyleSheet(
            f"background: #070d1a; color: {S.TEXT_PRIMARY}; "
            f"font-family: 'Fira Mono', 'Courier New', monospace; font-size: 11px; "
            f"border: 1px solid {S.BORDER_DIM}; border-radius: 4px;"
        )
        layout.addWidget(self._log_text)

    # ── Refresh ───────────────────────────────────────────────────────────────

    def refresh(self):
        if self._worker and self._worker.isRunning():
            return
        self._worker = BackgroundWorker(_read_logs)
        self._worker.finished.connect(self._on_logs)
        self._worker.error.connect(
            lambda e: self._log_text.setPlainText(f"Error reading log: {e}")
        )
        self._worker.start()

    def _on_logs(self, info: dict):
        now = datetime.now().strftime("%H:%M:%S")
        self._ts_lbl.setText(f"Loaded at {now}")

        if not info["exists"]:
            self._size_lbl.setText("File not found")
            self._size_lbl.setStyleSheet(f"color: {S.STATUS_WARNING}; font-size: 11px;")
            self._mtime_lbl.setText("")
            self._log_text.setPlainText(
                "─── Log file not yet created ───\n\n"
                f"Expected path: {info['path']}\n\n"
                "The dashboard creates its log file on first write.\n"
                "If the web server has not started, run:\n"
                "    python -m modular_dashboard.scripts.run_web\n\n"
                "Or refresh once the service is running."
            )
            return

        if info.get("size") is not None:
            self._size_lbl.setText(f"Size: {_fmt_size(info['size'])}")
            self._size_lbl.setStyleSheet(f"color: {S.TEXT_DIM}; font-size: 11px;")
        if info.get("mtime"):
            self._mtime_lbl.setText(f"Modified: {info['mtime']}")

        if info.get("error"):
            self._log_text.setPlainText(f"Error: {info['error']}")
            return

        lines = info.get("lines", [])
        if lines:
            self._log_text.setPlainText("\n".join(lines))
            sb = self._log_text.verticalScrollBar()
            sb.setValue(sb.maximum())
        else:
            self._log_text.setPlainText("(Log file exists but is empty)")

    def _open_folder(self):
        import subprocess
        folder = str(Path(LOG_FILE).parent)
        try:
            subprocess.Popen(["xdg-open", folder])
        except Exception:
            try:
                subprocess.Popen(["nautilus", folder])
            except Exception:
                pass

