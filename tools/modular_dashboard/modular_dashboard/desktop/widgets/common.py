"""
desktop/widgets/common.py — Shared reusable widgets for the dashboard.
"""
from __future__ import annotations

from PySide6.QtCore import Qt, Signal
from PySide6.QtGui import QCursor
from PySide6.QtWidgets import (
    QFrame,
    QHBoxLayout,
    QLabel,
    QPushButton,
    QSizePolicy,
    QVBoxLayout,
    QWidget,
)

from modular_dashboard.desktop import style as S


class SectionHeader(QLabel):
    """A bold section heading label."""

    def __init__(self, text: str, parent=None):
        super().__init__(text, parent)
        self.setStyleSheet(
            f"color: {S.TEXT_MUTED}; font-weight: bold; font-size: 11px; "
            f"text-transform: uppercase; letter-spacing: 1px; padding: 2px 0;"
        )


class StatusBadge(QLabel):
    """A small pill/badge label coloured by status."""

    def __init__(self, status: str = "unknown", parent=None):
        super().__init__(parent)
        self.set_status(status)
        self.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self.setFixedHeight(22)
        self.setMinimumWidth(70)

    def set_status(self, status: str):
        color = S.status_color(status)
        self.setText(status.upper())
        self.setStyleSheet(
            f"background: {color}22; color: {color}; border: 1px solid {color}; "
            f"border-radius: 10px; padding: 0 10px; font-size: 11px; font-weight: bold;"
        )


class StatusCard(QFrame):
    """A card widget displaying a title, large status value, and optional note.

    Emits ``clicked`` when the user clicks the card (if clickable).
    """

    clicked = Signal()

    def __init__(self, title: str, parent=None):
        super().__init__(parent)
        self.setMinimumWidth(170)
        self.setMinimumHeight(90)
        self.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Fixed)
        self.setStyleSheet(
            f"background: {S.BG_CARD}; border: 1px solid {S.BORDER_MED}; border-radius: 8px;"
        )
        self.setCursor(QCursor(Qt.CursorShape.PointingHandCursor))

        layout = QVBoxLayout(self)
        layout.setContentsMargins(12, 10, 12, 10)
        layout.setSpacing(4)

        self._title_lbl = QLabel(title)
        self._title_lbl.setStyleSheet(f"color: {S.TEXT_MUTED}; font-size: 11px; border: none; background: transparent;")
        layout.addWidget(self._title_lbl)

        self._value_lbl = QLabel("—")
        f = self._value_lbl.font()
        f.setBold(True)
        f.setPointSize(13)
        self._value_lbl.setFont(f)
        self._value_lbl.setStyleSheet(f"color: {S.TEXT_PRIMARY}; border: none; background: transparent;")
        layout.addWidget(self._value_lbl)

        self._note_lbl = QLabel("")
        self._note_lbl.setWordWrap(True)
        self._note_lbl.setStyleSheet(f"color: {S.TEXT_DIM}; font-size: 10px; border: none; background: transparent;")
        layout.addWidget(self._note_lbl)

    def update_value(self, value: str, color: str, note: str = ""):
        self._value_lbl.setText(value)
        self._value_lbl.setStyleSheet(
            f"color: {color}; font-weight: bold; font-size: 13px; border: none; background: transparent;"
        )
        self._note_lbl.setText(note)
        self.setStyleSheet(
            f"background: {S.BG_CARD}; border: 1px solid {color}; border-radius: 8px;"
        )

    def mousePressEvent(self, event):
        self.clicked.emit()
        super().mousePressEvent(event)


class BigStatusBanner(QFrame):
    """A large full-width status banner for the top of the dashboard."""

    def __init__(self, parent=None):
        super().__init__(parent)
        self.setMinimumHeight(70)
        layout = QVBoxLayout(self)
        layout.setContentsMargins(20, 12, 20, 12)
        layout.setSpacing(4)

        self._status_lbl = QLabel("● UNKNOWN")
        f = self._status_lbl.font()
        f.setBold(True)
        f.setPointSize(18)
        self._status_lbl.setFont(f)
        layout.addWidget(self._status_lbl)

        self._detail_lbl = QLabel("Loading...")
        self._detail_lbl.setWordWrap(True)
        self._detail_lbl.setStyleSheet(f"color: {S.TEXT_PRIMARY}; font-size: 12px; background: transparent;")
        layout.addWidget(self._detail_lbl)

    def update(self, status: str, detail: str):
        color = S.status_color(status)
        self._status_lbl.setText(f"● {status.upper()}")
        self._status_lbl.setStyleSheet(
            f"color: {color}; font-size: 18px; font-weight: bold; background: transparent;"
        )
        self._detail_lbl.setText(detail)
        self.setStyleSheet(
            f"background: {color}18; border: 1px solid {color}44; border-radius: 8px;"
        )


class ActionButton(QPushButton):
    """A QPushButton with pre-applied action style."""

    def __init__(self, text: str, action: str = "default", parent=None):
        super().__init__(text, parent)
        style_map = {
            "start":   S.BTN_START,
            "stop":    S.BTN_STOP,
            "restart": S.BTN_RESTART,
            "danger":  S.BTN_DANGER,
            "blue":    S.BTN_BLUE,
            "default": S.BTN_DEFAULT,
        }
        self.setStyleSheet(style_map.get(action, S.BTN_DEFAULT))


class InfoRow(QWidget):
    """A horizontal key/value info row."""

    def __init__(self, label: str, value: str = "", parent=None):
        super().__init__(parent)
        layout = QHBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(8)

        lbl = QLabel(label + ":")
        lbl.setFixedWidth(180)
        lbl.setStyleSheet(f"color: {S.TEXT_MUTED}; font-size: 12px;")
        lbl.setAlignment(Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter)
        layout.addWidget(lbl)

        self._val = QLabel(value)
        self._val.setStyleSheet(f"color: {S.TEXT_PRIMARY}; font-family: monospace; font-size: 12px;")
        self._val.setTextInteractionFlags(Qt.TextInteractionFlag.TextSelectableByMouse)
        layout.addWidget(self._val, 1)

    def set_value(self, value: str, color: str = ""):
        self._val.setText(value)
        if color:
            self._val.setStyleSheet(
                f"color: {color}; font-family: monospace; font-size: 12px;"
            )
