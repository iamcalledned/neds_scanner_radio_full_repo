"""
gpu_model_manager/desktop/widgets/dashboard_panel.py

Dashboard tab: overall health, GPU summary, scanner status, policy, leases.
"""
from __future__ import annotations

from PySide6.QtCore import Qt
from PySide6.QtWidgets import (
    QGridLayout,
    QLabel,
    QPushButton,
    QScrollArea,
    QVBoxLayout,
    QWidget,
)

from ...core.health import get_dashboard_status
from ..workers import DataWorker
from .common import (
    CardFrame,
    MutedLabel,
    StatusLabel,
    ValueLabel,
    VramBar,
)


class DashboardPanel(QWidget):
    def __init__(self, parent=None):
        super().__init__(parent)
        self._worker: DataWorker | None = None
        self._build_ui()
        self.refresh()

    def _build_ui(self):
        root = QVBoxLayout(self)
        root.setContentsMargins(16, 12, 16, 12)
        root.setSpacing(12)

        # Title row
        title_row = QWidget()
        tr_layout = QGridLayout(title_row)
        tr_layout.setContentsMargins(0, 0, 0, 0)
        lbl = QLabel("GPU Model Manager — Dashboard")
        lbl.setObjectName("label_accent")
        lbl.setStyleSheet("font-size: 16px; font-weight: bold;")
        self._btn_refresh = QPushButton("Refresh")
        self._btn_refresh.setFixedWidth(90)
        self._btn_refresh.clicked.connect(self.refresh)
        tr_layout.addWidget(lbl, 0, 0)
        tr_layout.addWidget(self._btn_refresh, 0, 1, Qt.AlignmentFlag.AlignRight)
        root.addWidget(title_row)

        # Scroll area
        scroll = QScrollArea()
        scroll.setWidgetResizable(True)
        inner = QWidget()
        self._grid = QGridLayout(inner)
        self._grid.setSpacing(12)
        self._grid.setContentsMargins(0, 0, 0, 0)
        scroll.setWidget(inner)
        root.addWidget(scroll)

        # -- Health card
        self._health_card = CardFrame()
        self._health_card.add_title("Manager Health")
        self._health_status = StatusLabel()
        self._health_card.add_widget(self._health_status)
        self._health_detail = MutedLabel("Loading…")
        self._health_card.add_widget(self._health_detail)
        self._grid.addWidget(self._health_card, 0, 0)

        # -- GPU card
        self._gpu_card = CardFrame()
        self._gpu_card.add_title("GPU Hardware")
        self._gpu_name = ValueLabel("—")
        self._gpu_vram = VramBar()
        self._gpu_temp = MutedLabel("—")
        self._gpu_card.add_widget(self._gpu_name)
        self._gpu_card.add_widget(self._gpu_vram)
        self._gpu_card.add_widget(self._gpu_temp)
        self._grid.addWidget(self._gpu_card, 0, 1)

        # -- Scanner card
        self._scanner_card = CardFrame()
        self._scanner_card.add_title("Scanner Whisper  [PROTECTED]")
        self._scanner_status = QLabel("Checking…")
        self._scanner_status.setObjectName("label_protected")
        self._scanner_detail = MutedLabel("scanner-mcp.service  |  Reserved: — MB")
        self._scanner_card.add_widget(self._scanner_status)
        self._scanner_card.add_widget(self._scanner_detail)
        self._grid.addWidget(self._scanner_card, 1, 0)

        # -- Policy card
        self._policy_card = CardFrame()
        self._policy_card.add_title("VRAM Policy")
        self._policy_value = ValueLabel("—")
        self._policy_detail = MutedLabel("effective free MB")
        self._policy_card.add_widget(self._policy_value)
        self._policy_card.add_widget(self._policy_detail)
        self._grid.addWidget(self._policy_card, 1, 1)

        # -- Leases card
        self._lease_card = CardFrame()
        self._lease_card.add_title("Active Leases")
        self._lease_count = ValueLabel("0")
        self._lease_detail = MutedLabel("No active leases")
        self._lease_card.add_widget(self._lease_count)
        self._lease_card.add_widget(self._lease_detail)
        self._grid.addWidget(self._lease_card, 2, 0)

        # -- Services card
        self._svc_card = CardFrame()
        self._svc_card.add_title("Services")
        self._svc_detail = MutedLabel("—/— active")
        self._svc_card.add_widget(self._svc_detail)
        self._grid.addWidget(self._svc_card, 2, 1)

        # -- Validation card (new)
        self._validation_card = CardFrame()
        self._validation_card.add_title("Validation")
        self._validation_status = ValueLabel("Not run")
        self._validation_meta = MutedLabel("Click Validate to run registry & endpoint checks")
        self._validation_btn = QPushButton("Validate")
        self._validation_btn.setFixedWidth(100)
        self._validation_btn.clicked.connect(self._on_validate_clicked)
        self._validation_card.add_widget(self._validation_status)
        self._validation_card.add_widget(self._validation_meta)
        self._validation_card.add_widget(self._validation_btn)
        self._grid.addWidget(self._validation_card, 3, 0)

        self._grid.setColumnStretch(0, 1)
        self._grid.setColumnStretch(1, 1)

    def refresh(self):
        if self._worker and self._worker.isRunning():
            return
        self._worker = DataWorker(get_dashboard_status, parent=self)
        self._worker.data_ready.connect(self._on_data)
        self._worker.error_occurred.connect(self._on_error)
        self._worker.start()

    def _on_data(self, data: dict):
        health = data.get("health", {})
        gpu = data.get("gpu", {})
        policy = data.get("policy", {})
        leases = data.get("leases", {})

        # Health
        self._health_status.set_status(health.get("status", "unknown"))
        svc_checked = health.get("services_checked", 0)
        svc_active = health.get("services_active", 0)
        self._health_detail.setText(
            f"Services: {svc_active}/{svc_checked} active  |  "
            f"Leases: {health.get('active_leases', 0)}"
        )

        # GPU
        if gpu.get("available"):
            self._gpu_name.setText(gpu.get("name", "—"))
            self._gpu_vram.update(gpu.get("memory_used_mb", 0), gpu.get("memory_total_mb", 0))
            self._gpu_temp.setText(
                f"Temp: {gpu.get('temperature_c', '—')}°C  |  "
                f"Util: {gpu.get('utilization_pct', '—')}%  |  "
                f"Driver: {gpu.get('driver_version', '—')}"
            )
        else:
            self._gpu_name.setText("GPU Unavailable")
            self._gpu_temp.setText(gpu.get("error", "nvidia-smi failed"))

        # Scanner
        if health.get("scanner_protected_ok"):
            self._scanner_status.setText("Running (Protected)")
            self._scanner_status.setObjectName("label_green")
        else:
            self._scanner_status.setText("Not Running — Check scanner-mcp.service")
            self._scanner_status.setObjectName("label_yellow")
        reserved = policy.get("scanner_whisper_reserved_mb", 0)
        self._scanner_detail.setText(f"scanner-mcp.service  |  Reserved: {reserved} MB")

        # Policy
        eff = policy.get("effective_available_mb", 0)
        self._policy_value.setText(f"{eff:.0f} MB")
        free = policy.get("free_vram_mb", 0)
        safety = policy.get("gpu_safety_margin_mb", 0)
        self._policy_detail.setText(
            f"Free {free:.0f} − safety {safety} − scanner {reserved} MB"
        )

        # Leases
        self._lease_count.setText(str(leases.get("active_count", 0)))
        lease_list = leases.get("leases", [])
        if lease_list:
            names = ", ".join(l["runtime_key"] for l in lease_list[:3])
            self._lease_detail.setText(names)
        else:
            self._lease_detail.setText("No active leases")

        # Services
        self._svc_detail.setText(
            f"{svc_active}/{svc_checked} model services active"
        )

    def _on_error(self, error: str):
        self._health_detail.setText(f"Error: {error}")

    def _on_validate_clicked(self):
        # Run registry validation and reconciliation in the background
        # reuse DataWorker to run the validation task
        def task():
            from ...core.registry_validator import validate_runtime_registry
            from ...core.reconciler import get_runtime_reconciliation
            val = validate_runtime_registry()
            rec = get_runtime_reconciliation()
            return {"val": val, "rec": rec}

        worker = DataWorker(task, parent=self)
        worker.data_ready.connect(self._on_validate_result)
        worker.error_occurred.connect(lambda e: self._validation_meta.setText(f"Error: {e}"))
        worker.start()

    def _on_validate_result(self, result: dict):
        val = result.get("val")
        rec = result.get("rec")
        # Summary status
        if not val.ok:
            self._validation_status.setText("Registry INVALID")
            self._validation_meta.setText("; ".join(val.errors[:3]) or "See logs")
            return
        # check reconcile warnings/errors
        notes = []
        for e in rec.runtimes:
            for n in (e.notes or []):
                if "not reachable" in n or "not found" in n or "WARNING" in n:
                    notes.append(f"{e.key}: {n}")
        if notes:
            self._validation_status.setText(f"Issues: {len(notes)}")
            self._validation_meta.setText("; ".join(notes[:3]))
        else:
            self._validation_status.setText("OK")
            self._validation_meta.setText("Registry and endpoints healthy")
