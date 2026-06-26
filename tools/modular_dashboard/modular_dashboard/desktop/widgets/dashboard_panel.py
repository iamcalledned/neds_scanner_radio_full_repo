"""
desktop/widgets/dashboard_panel.py — Dashboard tab.

Sections:
  1. Big status banner (pipeline overall health + plain-English summary)
  2. Key service cards (8 critical services)
  3. Problems & recommended actions
  4. Quick controls (Start All / Restart MCP / Restart Stack)
"""
from __future__ import annotations

from PySide6.QtCore import Qt, QTimer
from PySide6.QtWidgets import (
    QCheckBox,
    QDialog,
    QDialogButtonBox,
    QFrame,
    QGridLayout,
    QGroupBox,
    QHBoxLayout,
    QLabel,
    QPushButton,
    QScrollArea,
    QSizePolicy,
    QTabWidget,
    QTextEdit,
    QTreeWidget,
    QTreeWidgetItem,
    QVBoxLayout,
    QWidget,
)

from modular_dashboard.core.scanner_actions import (
    get_service_detail,
    restart_scanner_service,
    restart_scanner_stack,
    start_scanner_service,
    stop_scanner_service,
)
from modular_dashboard.core.scanner_health import get_scanner_health
from modular_dashboard.core.schemas import OverallStatus, ServiceState
from modular_dashboard.desktop import style as S
from modular_dashboard.desktop.workers import BackgroundWorker, ServiceActionWorker, StackActionWorker
from modular_dashboard.desktop.widgets.common import (
    ActionButton,
    BigStatusBanner,
    SectionHeader,
    StatusCard,
)


# ── Key services to show as cards on the dashboard ─────────────────────────
_KEY_SERVICES = [
    ("scanner-mcp.service",         "MCP Service"),
    ("scanner-transcriber.service", "Listener/Transcriber"),
    ("scanner-recorder.service",    "Recorder"),
    ("scanner-websocket.service",   "WebSocket"),
    ("rtl_tcp@12000.service",       "RTL-TCP :12000"),
    ("rtl_tcp@12001.service",       "RTL-TCP :12001"),
    ("rtl_tcp@12002.service",       "RTL-TCP :12002"),
    ("rtl_tcp@12003.service",       "RTL-TCP :12003"),
]


# ── Service Detail Dialog ────────────────────────────────────────────────────

class ServiceDetailDialog(QDialog):
    """A dialog showing journal logs, systemctl status, and properties for a service."""

    def __init__(self, service_name: str, parent=None):
        super().__init__(parent)
        self._service_name = service_name
        self._detail_worker = None
        self._action_worker = None

        self.setWindowTitle(f"Service Detail: {service_name}")
        self.setMinimumSize(760, 560)
        self.setAttribute(Qt.WidgetAttribute.WA_DeleteOnClose)

        layout = QVBoxLayout(self)
        layout.setContentsMargins(12, 12, 12, 12)
        layout.setSpacing(8)

        # ── Title row ──
        title_row = QHBoxLayout()
        self._title_lbl = QLabel(service_name)
        self._title_lbl.setStyleSheet(
            f"font-size: 14px; font-weight: bold; color: {S.ACCENT_BLUE}; font-family: monospace;"
        )
        title_row.addWidget(self._title_lbl)
        title_row.addStretch()

        self._start_btn  = QPushButton("▶ Start")
        self._stop_btn   = QPushButton("■ Stop")
        self._restart_btn = QPushButton("↻ Restart")
        self._start_btn.setStyleSheet(f"color: {S.STATUS_HEALTHY}; border: 1px solid {S.STATUS_HEALTHY}; padding: 3px 10px; border-radius: 4px;")
        self._stop_btn.setStyleSheet(f"color: {S.STATUS_WARNING}; border: 1px solid {S.STATUS_WARNING}; padding: 3px 10px; border-radius: 4px;")
        self._restart_btn.setStyleSheet(f"color: {S.TEXT_MUTED}; border: 1px solid {S.BORDER_MED}; padding: 3px 10px; border-radius: 4px;")
        for btn in (self._start_btn, self._stop_btn, self._restart_btn):
            title_row.addWidget(btn)
        layout.addLayout(title_row)

        # ── Meta row ──
        self._meta_lbl = QLabel("Loading…")
        self._meta_lbl.setStyleSheet(f"color: {S.TEXT_MUTED}; font-size: 11px;")
        layout.addWidget(self._meta_lbl)

        # ── Action feedback ──
        self._action_lbl = QLabel("")
        self._action_lbl.setStyleSheet(f"color: {S.STATUS_HEALTHY}; font-size: 11px;")
        self._action_lbl.setVisible(False)
        layout.addWidget(self._action_lbl)

        # ── Tabs ──
        tabs = QTabWidget()
        tabs.setStyleSheet(
            f"QTabBar::tab {{ background: {S.BG_CARD}; color: {S.TEXT_MUTED}; "
            f"padding: 5px 14px; border: 1px solid {S.BORDER_DIM}; border-bottom: none; }}"
            f"QTabBar::tab:selected {{ color: {S.TEXT_PRIMARY}; background: {S.BG_CARD_DARK}; }}"
        )

        self._journal_edit = QTextEdit()
        self._journal_edit.setReadOnly(True)
        self._journal_edit.setStyleSheet(
            f"background: {S.BG_DEEP}; color: {S.TEXT_PRIMARY}; "
            f"font-family: 'Courier New', monospace; font-size: 11px; border: none;"
        )
        tabs.addTab(self._journal_edit, "Journal Logs")

        self._status_edit = QTextEdit()
        self._status_edit.setReadOnly(True)
        self._status_edit.setStyleSheet(
            f"background: {S.BG_DEEP}; color: {S.TEXT_PRIMARY}; "
            f"font-family: 'Courier New', monospace; font-size: 11px; border: none;"
        )
        tabs.addTab(self._status_edit, "systemctl status")

        self._props_tree = QTreeWidget()
        self._props_tree.setHeaderLabels(["Property", "Value"])
        self._props_tree.setColumnWidth(0, 240)
        self._props_tree.setStyleSheet(
            f"background: {S.BG_DEEP}; color: {S.TEXT_PRIMARY}; font-size: 11px; border: none;"
        )
        tabs.addTab(self._props_tree, "Properties")

        layout.addWidget(tabs)

        # ── Close button ──
        btn_box = QDialogButtonBox(QDialogButtonBox.StandardButton.Close)
        btn_box.rejected.connect(self.close)
        layout.addWidget(btn_box)

        # Wire action buttons
        self._start_btn.clicked.connect(lambda: self._do_action("start"))
        self._stop_btn.clicked.connect(lambda: self._do_action("stop"))
        self._restart_btn.clicked.connect(lambda: self._do_action("restart"))

        # Load data
        self._load_detail()

    def _load_detail(self):
        if self._detail_worker and self._detail_worker.isRunning():
            return
        self._journal_edit.setPlainText("Loading…")
        self._detail_worker = BackgroundWorker(get_service_detail, self._service_name)
        self._detail_worker.finished.connect(self._on_detail)
        self._detail_worker.error.connect(lambda e: self._journal_edit.setPlainText(f"Error: {e}"))
        self._detail_worker.start()

    def _on_detail(self, data: dict):
        # Meta
        state = data.get("active_state", "?")
        sub   = data.get("sub_state", "?")
        cls   = data.get("classification", "?")
        pid   = data.get("pid", "")
        since = data.get("since", "")
        parts = [f"State: {state}/{sub}", f"Class: {cls}"]
        if pid:
            parts.append(f"PID: {pid}")
        if since:
            parts.append(f"Since: {since}")
        self._meta_lbl.setText("   ·   ".join(parts))

        # Journal
        logs = data.get("journal_logs") or ""
        err  = data.get("journal_error")
        self._journal_edit.setPlainText(logs if logs else (f"[journalctl error: {err}]" if err else "(no output)"))

        # systemctl status
        self._status_edit.setPlainText(data.get("systemctl_status") or "(no output)")

        # Properties
        self._props_tree.clear()
        for k, v in (data.get("properties") or {}).items():
            item = QTreeWidgetItem([k, str(v)])
            self._props_tree.addTopLevelItem(item)

    def _do_action(self, action: str):
        if self._action_worker and self._action_worker.isRunning():
            return
        fn_map = {
            "start":   start_scanner_service,
            "stop":    stop_scanner_service,
            "restart": restart_scanner_service,
        }
        fn = fn_map.get(action)
        if fn is None:
            return
        self._action_lbl.setText(f"Running {action}…")
        self._action_lbl.setVisible(True)
        self._action_worker = ServiceActionWorker(fn, self._service_name)
        self._action_worker.finished.connect(self._on_action_done)
        self._action_worker.error.connect(lambda e: self._action_lbl.setText(f"Error: {e}"))
        self._action_worker.start()

    def _on_action_done(self, result):
        msg = "OK" if result.success else f"Failed (rc={result.returncode})"
        self._action_lbl.setText(f"{result.action} {result.service_name}: {msg}")
        # Reload detail after a short delay
        QTimer.singleShot(1200, self._load_detail)


class DashboardPanel(QWidget):
    """The Dashboard tab panel."""

    AUTO_REFRESH_MS = 8000

    def __init__(self, parent=None):
        super().__init__(parent)
        self._worker = None
        self._action_worker = None
        self._service_cards: dict[str, StatusCard] = {}
        self._timer = QTimer(self)
        self._timer.timeout.connect(self.refresh)

        self._build_ui()
        self.refresh()

    def _build_ui(self):
        outer = QVBoxLayout(self)
        outer.setContentsMargins(14, 14, 14, 14)
        outer.setSpacing(12)

        # ── Toolbar (auto-refresh + refresh button) ─────────────────────────
        toolbar = QHBoxLayout()
        self._auto_cb = QCheckBox("Auto-refresh (8s)")
        self._auto_cb.stateChanged.connect(self._toggle_auto)
        toolbar.addWidget(self._auto_cb)
        toolbar.addStretch()
        self._action_lbl = QLabel("")
        self._action_lbl.setStyleSheet(f"color: {S.TEXT_MUTED}; font-size: 11px;")
        toolbar.addWidget(self._action_lbl)
        btn = QPushButton("↻  Refresh")
        btn.setStyleSheet(S.BTN_DEFAULT)
        btn.clicked.connect(self.refresh)
        toolbar.addWidget(btn)
        outer.addLayout(toolbar)

        # ── Big status banner ───────────────────────────────────────────────
        self._banner = BigStatusBanner()
        outer.addWidget(self._banner)

        # Scrollable lower content
        scroll = QScrollArea()
        scroll.setWidgetResizable(True)
        scroll.setStyleSheet("QScrollArea { border: none; }")
        content = QWidget()
        cl = QVBoxLayout(content)
        cl.setContentsMargins(0, 0, 0, 0)
        cl.setSpacing(14)
        scroll.setWidget(content)
        outer.addWidget(scroll)

        # ── Key service cards ───────────────────────────────────────────────
        cards_grp = QGroupBox("Pipeline Services")
        cards_grp.setStyleSheet(
            f"QGroupBox {{ border: 1px solid {S.BORDER_DIM}; border-radius: 8px; "
            f"margin-top: 10px; padding-top: 10px; color: {S.TEXT_MUTED}; font-weight: bold; }}"
        )
        cards_layout = QGridLayout(cards_grp)
        cards_layout.setSpacing(10)
        for i, (svc_name, label) in enumerate(_KEY_SERVICES):
            card = StatusCard(label)
            card.clicked.connect(lambda sn=svc_name: self._open_service_detail(sn))
            self._service_cards[svc_name] = card
            cards_layout.addWidget(card, i // 4, i % 4)
        cl.addWidget(cards_grp)

        # ── Redis / Queue status cards ──────────────────────────────────────
        infra_grp = QGroupBox("Infrastructure")
        infra_grp.setStyleSheet(cards_grp.styleSheet())
        infra_row = QHBoxLayout(infra_grp)
        infra_row.setSpacing(10)

        self._redis_card  = StatusCard("Redis")
        self._queue_card  = StatusCard("Queue / Backlog")
        self._paths_card  = StatusCard("Filesystem Paths")
        for c in (self._redis_card, self._queue_card, self._paths_card):
            infra_row.addWidget(c)
        infra_row.addStretch()
        cl.addWidget(infra_grp)

        # ── Problems & Actions ──────────────────────────────────────────────
        bottom_row = QHBoxLayout()
        bottom_row.setSpacing(12)

        problems_grp = QGroupBox("Problems Detected")
        problems_grp.setStyleSheet(cards_grp.styleSheet())
        pl = QVBoxLayout(problems_grp)
        self._problems_text = QTextEdit()
        self._problems_text.setReadOnly(True)
        self._problems_text.setMinimumHeight(100)
        self._problems_text.setStyleSheet(
            f"background: #1a0808; color: #fca5a5; font-size: 12px; border: none;"
        )
        pl.addWidget(self._problems_text)
        bottom_row.addWidget(problems_grp, 2)

        actions_grp = QGroupBox("Recommended Actions")
        actions_grp.setStyleSheet(cards_grp.styleSheet())
        al = QVBoxLayout(actions_grp)
        self._actions_text = QTextEdit()
        self._actions_text.setReadOnly(True)
        self._actions_text.setMinimumHeight(100)
        self._actions_text.setStyleSheet(
            f"background: #0a1628; color: #93c5fd; font-size: 12px; "
            f"font-family: 'Courier New', monospace; border: none;"
        )
        al.addWidget(self._actions_text)
        bottom_row.addWidget(actions_grp, 3)

        cl.addLayout(bottom_row)

        # ── Quick Controls ──────────────────────────────────────────────────
        ctrl_grp = QGroupBox("Quick Controls")
        ctrl_grp.setStyleSheet(cards_grp.styleSheet())
        ctrl_row = QHBoxLayout(ctrl_grp)
        ctrl_row.setSpacing(10)

        start_mcp_btn = ActionButton("Start MCP", "start")
        start_mcp_btn.setToolTip("systemctl --user start scanner-mcp.service")
        start_mcp_btn.clicked.connect(lambda: self._do_action("scanner-mcp.service", "start"))
        ctrl_row.addWidget(start_mcp_btn)

        restart_mcp_btn = ActionButton("Restart MCP", "restart")
        restart_mcp_btn.clicked.connect(lambda: self._do_action("scanner-mcp.service", "restart"))
        ctrl_row.addWidget(restart_mcp_btn)

        start_rec_btn = ActionButton("Start Recorder", "start")
        start_rec_btn.clicked.connect(lambda: self._do_action("scanner-recorder.service", "start"))
        ctrl_row.addWidget(start_rec_btn)

        start_trans_btn = ActionButton("Start Transcriber", "start")
        start_trans_btn.clicked.connect(lambda: self._do_action("scanner-transcriber.service", "start"))
        ctrl_row.addWidget(start_trans_btn)

        ctrl_row.addStretch()

        restart_stack_btn = ActionButton("⚡  Restart Entire Stack", "danger")
        restart_stack_btn.setToolTip("Restarts all scanner services in order")
        restart_stack_btn.clicked.connect(self._restart_stack)
        ctrl_row.addWidget(restart_stack_btn)

        cl.addWidget(ctrl_grp)

        # ── Timestamp ───────────────────────────────────────────────────────
        self._ts_lbl = QLabel("")
        self._ts_lbl.setStyleSheet(f"color: {S.TEXT_DIM}; font-size: 10px;")
        outer.addWidget(self._ts_lbl)

    # ── Refresh ─────────────────────────────────────────────────────────────

    def refresh(self):
        if self._worker and self._worker.isRunning():
            return
        self._worker = BackgroundWorker(get_scanner_health)
        self._worker.finished.connect(self._on_health)
        self._worker.error.connect(lambda e: self._banner.update("unknown", f"Error: {e}"))
        self._worker.start()

    def _toggle_auto(self, state):
        if state:
            self._timer.start(self.AUTO_REFRESH_MS)
        else:
            self._timer.stop()

    def _on_health(self, health):
        # Banner
        self._banner.update(health.overall.value, health.summary)

        # Service cards
        svc_by_name = {}
        if health.services:
            svc_by_name = {s.name: s for s in health.services.services}

        for svc_name, card in self._service_cards.items():
            svc = svc_by_name.get(svc_name)
            if svc is None:
                card.update_value("not found", S.STATUS_MISSING, "")
            else:
                color = S.status_color(svc.active_state.value)
                note = svc.since[:16] if svc.since else ""
                card.update_value(svc.active_state.value.upper(), color, note)

        # Redis card
        if health.redis:
            if health.redis.reachable:
                ver = health.redis.server_info.get("redis_version", "")
                self._redis_card.update_value("REACHABLE", S.STATUS_HEALTHY, f"v{ver}" if ver else "")
            else:
                self._redis_card.update_value("UNREACHABLE", S.STATUS_CRITICAL, health.redis.error or "")

        # Queue card
        if health.queue:
            q = health.queue
            color = S.status_color(q.backlog_health.value)
            if q.pending_count is not None:
                val = f"{q.pending_count:,} pending"
            elif q.stream_length is not None:
                val = f"XLEN {q.stream_length:,}"
            else:
                val = q.backlog_health.value.upper()
            # Shorten the note for the card
            note = q.backlog_note[:90] + "…" if len(q.backlog_note) > 90 else q.backlog_note
            self._queue_card.update_value(val, color, note)
        else:
            self._queue_card.update_value("—", S.STATUS_UNKNOWN, "Redis not reachable")

        # Paths card
        path_missing = sum(1 for p in health.paths if not p.exists)
        if path_missing == 0:
            self._paths_card.update_value("All OK", S.STATUS_HEALTHY, f"{len(health.paths)} paths checked")
        else:
            self._paths_card.update_value(
                f"{path_missing} missing", S.STATUS_WARNING,
                f"{len(health.paths)} paths total"
            )

        # Problems & actions
        self._problems_text.setPlainText(
            "\n".join(f"• {p}" for p in health.problems) if health.problems else "✓ No problems detected."
        )
        self._actions_text.setPlainText(
            "\n".join(f"→ {a}" for a in health.next_actions) if health.next_actions else "No actions needed."
        )

        self._ts_lbl.setText(f"Last updated: {health.timestamp}")

    # ── Actions ─────────────────────────────────────────────────────────────

    def _do_action(self, service: str, action: str):
        if self._action_worker and self._action_worker.isRunning():
            return
        fn_map = {
            "start":   start_scanner_service,
            "restart": restart_scanner_service,
        }
        fn = fn_map.get(action)
        if fn is None:
            return
        self._action_lbl.setText(f"{action} {service}…")
        self._action_worker = ServiceActionWorker(fn, service)
        self._action_worker.finished.connect(self._on_action_done)
        self._action_worker.error.connect(lambda e: self._action_lbl.setText(f"Error: {e}"))
        self._action_worker.start()

    def _on_action_done(self, result):
        msg = "OK" if result.success else f"Failed (rc={result.returncode})"
        self._action_lbl.setText(f"{result.action} {result.service_name}: {msg}")
        self.refresh()

    def _restart_stack(self):
        from PySide6.QtWidgets import QMessageBox
        reply = QMessageBox.question(
            self,
            "Restart Scanner Stack",
            "This will restart the entire scanner stack in order.\n\n"
            "MCP → Transcriber → Recorder → WebSocket\n\n"
            "Continue?",
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
        )
        if reply != QMessageBox.StandardButton.Yes:
            return
        self._action_lbl.setText("Restarting full stack…")
        self._action_worker = StackActionWorker(restart_scanner_stack)
        self._action_worker.finished.connect(
            lambda r: self._action_lbl.setText(f"Stack restart: {'OK' if r.success else 'FAILED'}")
        )
        self._action_worker.error.connect(lambda e: self._action_lbl.setText(f"Stack error: {e}"))
        self._action_worker.start()

    def _open_service_detail(self, service_name: str):
        dlg = ServiceDetailDialog(service_name, parent=self)
        dlg.show()
