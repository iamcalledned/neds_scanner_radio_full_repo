"""
gpu_model_manager/desktop/widgets/common.py

Shared widget helpers and base classes used across all panels.
"""
from __future__ import annotations

from PySide6.QtWidgets import (
    QFrame,
    QHBoxLayout,
    QLabel,
    QProgressBar,
    QVBoxLayout,
    QWidget,
)


class SectionLabel(QLabel):
    """Small uppercase section title."""
    def __init__(self, text: str, parent=None):
        super().__init__(text.upper(), parent)
        self.setObjectName("label_title")


class ValueLabel(QLabel):
    """Large bold value display."""
    def __init__(self, text: str = "—", parent=None):
        super().__init__(text, parent)
        self.setObjectName("label_value")


class MutedLabel(QLabel):
    def __init__(self, text: str = "", parent=None):
        super().__init__(text, parent)
        self.setObjectName("label_muted")


class StatusLabel(QLabel):
    """Colored status — healthy/warning/critical."""
    _COLORS = {
        "healthy":  "label_green",
        "warning":  "label_yellow",
        "critical": "label_red",
        "unknown":  "label_muted",
        "active":   "label_green",
        "inactive": "label_muted",
        "not-found": "label_red",
    }

    def __init__(self, parent=None):
        super().__init__("—", parent)

    def set_status(self, status: str):
        self.setText(status.upper())
        obj_name = self._COLORS.get(status.lower(), "label_muted")
        self.setObjectName(obj_name)
        self.style().unpolish(self)
        self.style().polish(self)


class VramBar(QWidget):
    """VRAM usage bar with label."""
    def __init__(self, parent=None):
        super().__init__(parent)
        layout = QVBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(4)

        self._bar = QProgressBar()
        self._bar.setRange(0, 100)
        self._bar.setTextVisible(False)
        self._bar.setFixedHeight(12)

        self._label = MutedLabel("— / — MB")

        layout.addWidget(self._bar)
        layout.addWidget(self._label)

    def update(self, used_mb: float, total_mb: float):
        if total_mb > 0:
            pct = int(used_mb / total_mb * 100)
            self._bar.setValue(pct)
            self._label.setText(
                f"{used_mb:.0f} / {total_mb:.0f} MB  ({pct}%)"
            )
        else:
            self._bar.setValue(0)
            self._label.setText("— / — MB")


class CardFrame(QFrame):
    """A simple bordered card container."""
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setFrameShape(QFrame.Shape.StyledPanel)
        self._layout = QVBoxLayout(self)
        self._layout.setContentsMargins(12, 10, 12, 10)
        self._layout.setSpacing(6)

    def add_title(self, text: str):
        self._layout.addWidget(SectionLabel(text))

    def add_widget(self, widget: QWidget):
        self._layout.addWidget(widget)

    def content_layout(self) -> QVBoxLayout:
        return self._layout


def make_hbox(*widgets) -> QWidget:
    w = QWidget()
    layout = QHBoxLayout(w)
    layout.setContentsMargins(0, 0, 0, 0)
    for widget in widgets:
        layout.addWidget(widget)
    return w


def make_vbox(*widgets) -> QWidget:
    w = QWidget()
    layout = QVBoxLayout(w)
    layout.setContentsMargins(0, 0, 0, 0)
    for widget in widgets:
        layout.addWidget(widget)
    return w


def h_separator() -> QFrame:
    sep = QFrame()
    sep.setFrameShape(QFrame.Shape.HLine)
    sep.setFrameShadow(QFrame.Shadow.Sunken)
    return sep
