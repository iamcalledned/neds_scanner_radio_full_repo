"""
gpu_model_manager/desktop/workers.py

Background QThread workers for polling core modules without blocking the UI.
"""
from __future__ import annotations

from typing import Any, Callable

from PySide6.QtCore import QThread, Signal


class DataWorker(QThread):
    """
    Generic background worker. Runs fetch_fn() in a thread and emits results.
    """
    data_ready = Signal(object)
    error_occurred = Signal(str)

    def __init__(self, fetch_fn: Callable[[], Any], parent=None):
        super().__init__(parent)
        self.fetch_fn = fetch_fn

    def run(self):
        try:
            result = self.fetch_fn()
            self.data_ready.emit(result)
        except Exception as exc:
            self.error_occurred.emit(str(exc))


class PollingWorker(QThread):
    """
    Runs fetch_fn() repeatedly on a timer (interval_ms).
    Each result is emitted via data_ready.
    """
    data_ready = Signal(object)
    error_occurred = Signal(str)

    def __init__(self, fetch_fn: Callable[[], Any], interval_ms: int = 15000, parent=None):
        super().__init__(parent)
        self.fetch_fn = fetch_fn
        self.interval_ms = interval_ms
        self._running = True

    def run(self):
        import time
        while self._running:
            try:
                result = self.fetch_fn()
                self.data_ready.emit(result)
            except Exception as exc:
                self.error_occurred.emit(str(exc))
            # Sleep in small chunks so stop() is responsive
            elapsed = 0
            while self._running and elapsed < self.interval_ms:
                time.sleep(0.25)
                elapsed += 250

    def stop(self):
        self._running = False
