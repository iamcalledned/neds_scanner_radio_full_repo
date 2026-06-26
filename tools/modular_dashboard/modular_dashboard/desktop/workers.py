"""
desktop/workers.py — QThread-based workers for non-blocking backend calls.

Each worker runs a core function in a background thread and emits
signals with the result. UI slots connect to those signals.
"""
from __future__ import annotations

from typing import Any, Callable

from PySide6.QtCore import QThread, Signal


class BackgroundWorker(QThread):
    """Generic worker that calls a callable in a background thread.

    Emits finished(result) on success, error(str) on exception.
    """

    finished: Signal = Signal(object)
    error: Signal = Signal(str)

    def __init__(self, fn: Callable, *args, **kwargs):
        super().__init__()
        self._fn = fn
        self._args = args
        self._kwargs = kwargs

    def run(self) -> None:
        try:
            result = self._fn(*self._args, **self._kwargs)
            self.finished.emit(result)
        except Exception as exc:  # noqa: BLE001
            self.error.emit(str(exc))


class ServiceActionWorker(QThread):
    """Worker for service start/stop/restart actions."""

    done: Signal = Signal(object)   # emits ScannerActionResponse
    error: Signal = Signal(str)

    def __init__(self, fn: Callable, service_name: str):
        super().__init__()
        self._fn = fn
        self._service_name = service_name

    def run(self) -> None:
        try:
            result = self._fn(self._service_name)
            self.done.emit(result)
        except Exception as exc:  # noqa: BLE001
            self.error.emit(str(exc))


class StackActionWorker(QThread):
    """Worker for stack-level actions (restart, validate)."""

    done: Signal = Signal(object)
    error: Signal = Signal(str)

    def __init__(self, fn: Callable):
        super().__init__()
        self._fn = fn

    def run(self) -> None:
        try:
            result = self._fn()
            self.done.emit(result)
        except Exception as exc:  # noqa: BLE001
            self.error.emit(str(exc))
