"""
gpu_model_manager/desktop/widgets/gpu_panel.py

GPU tab: VRAM, utilization, temperature, power, process table.
"""
from __future__ import annotations

from PySide6.QtWidgets import (
    QHBoxLayout,
    QPushButton,
    QTableWidget,
    QTableWidgetItem,
    QVBoxLayout,
    QWidget,
)

from ...core.gpu_inventory import get_gpu_processes, get_gpu_status
from ...core.process_inspector import classify_gpu_process
from ..workers import DataWorker
from .common import CardFrame, MutedLabel, ValueLabel, VramBar


def _fetch():
    gpu = get_gpu_status()
    procs = get_gpu_processes()
    enriched = []
    for p in procs:
        d = p.model_dump()
        d["classification"] = classify_gpu_process(p)
        enriched.append(d)
    return {"gpu": gpu.model_dump(), "processes": enriched}


class GpuPanel(QWidget):
    def __init__(self, parent=None):
        super().__init__(parent)
        self._worker: DataWorker | None = None
        self._build_ui()
        self.refresh()

    def _build_ui(self):
        root = QVBoxLayout(self)
        root.setContentsMargins(16, 12, 16, 12)
        root.setSpacing(12)

        # Refresh button
        btn_row = QWidget()
        btn_layout = QHBoxLayout(btn_row)
        btn_layout.setContentsMargins(0, 0, 0, 0)
        btn_layout.addStretch()
        btn = QPushButton("Refresh")
        btn.setFixedWidth(90)
        btn.clicked.connect(self.refresh)
        btn_layout.addWidget(btn)
        root.addWidget(btn_row)

        # Stats row
        stats_row = QWidget()
        stats_layout = QHBoxLayout(stats_row)
        stats_layout.setContentsMargins(0, 0, 0, 0)
        stats_layout.setSpacing(12)

        self._card_name = CardFrame()
        self._card_name.add_title("GPU")
        self._lbl_name = ValueLabel("—")
        self._lbl_driver = MutedLabel("Driver: —")
        self._card_name.add_widget(self._lbl_name)
        self._card_name.add_widget(self._lbl_driver)
        stats_layout.addWidget(self._card_name, 2)

        self._card_vram = CardFrame()
        self._card_vram.add_title("VRAM")
        self._vram_bar = VramBar()
        self._card_vram.add_widget(self._vram_bar)
        self._lbl_free = MutedLabel("Free: —")
        self._card_vram.add_widget(self._lbl_free)
        stats_layout.addWidget(self._card_vram, 2)

        self._card_util = CardFrame()
        self._card_util.add_title("Utilization / Temp / Power")
        self._lbl_util = ValueLabel("—%")
        self._lbl_temp = MutedLabel("Temp: —°C")
        self._lbl_power = MutedLabel("Power: —W / —W")
        self._card_util.add_widget(self._lbl_util)
        self._card_util.add_widget(self._lbl_temp)
        self._card_util.add_widget(self._lbl_power)
        stats_layout.addWidget(self._card_util, 1)

        root.addWidget(stats_row)

        # Process table
        proc_card = CardFrame()
        proc_card.add_title("GPU Compute Processes")
        self._proc_table = QTableWidget(0, 4)
        self._proc_table.setHorizontalHeaderLabels(
            ["PID", "Process Name", "VRAM (MB)", "Classification"]
        )
        self._proc_table.horizontalHeader().setStretchLastSection(True)
        self._proc_table.setAlternatingRowColors(True)
        self._proc_table.setEditTriggers(QTableWidget.EditTrigger.NoEditTriggers)
        self._proc_table.setSelectionBehavior(QTableWidget.SelectionBehavior.SelectRows)
        proc_card.add_widget(self._proc_table)
        root.addWidget(proc_card, 1)

    def refresh(self):
        if self._worker and self._worker.isRunning():
            return
        self._worker = DataWorker(_fetch, parent=self)
        self._worker.data_ready.connect(self._on_data)
        self._worker.start()

    def _on_data(self, data: dict):
        gpu = data.get("gpu", {})
        procs = data.get("processes", [])

        if gpu.get("available"):
            self._lbl_name.setText(gpu.get("name", "—"))
            self._lbl_driver.setText(f"Driver: {gpu.get('driver_version', '—')}")
            self._vram_bar.update(gpu.get("memory_used_mb", 0), gpu.get("memory_total_mb", 0))
            self._lbl_free.setText(f"Free: {gpu.get('memory_free_mb', 0):.0f} MB")
            self._lbl_util.setText(f"{gpu.get('utilization_pct', 0)}%")
            self._lbl_temp.setText(f"Temp: {gpu.get('temperature_c', '—')}°C")
            p_draw = gpu.get("power_draw_w")
            p_lim = gpu.get("power_limit_w")
            self._lbl_power.setText(
                f"Power: {p_draw}W / {p_lim}W" if p_draw else "Power: N/A"
            )
        else:
            self._lbl_name.setText("GPU Unavailable")
            self._lbl_driver.setText(gpu.get("error", ""))

        # Processes
        self._proc_table.setRowCount(len(procs))
        for row, p in enumerate(procs):
            self._proc_table.setItem(row, 0, QTableWidgetItem(str(p.get("pid", ""))))
            self._proc_table.setItem(row, 1, QTableWidgetItem(p.get("process_name", "")))
            self._proc_table.setItem(row, 2, QTableWidgetItem(str(p.get("used_memory_mb", ""))))
            self._proc_table.setItem(row, 3, QTableWidgetItem(p.get("classification", "unknown")))
        self._proc_table.resizeColumnsToContents()
