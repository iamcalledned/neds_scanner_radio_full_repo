#!/usr/bin/env python3
"""
scanner_dashboard.py — Textual dashboard to start/stop/monitor:
  - scanner-mcp.service
  - scanner-recorder.service
  - scanner-transcriber.service

Features:
  - Service status table (Active/Sub/PID/Since)
  - Health checks: Redis ping, MCP TCP, GPU mem (nvidia-smi)
  - Live log tail for selected service
  - Keybindings: start/stop/restart selected, start/stop/restart all

Deps:
  pip install textual redis
"""

from __future__ import annotations

import os
import socket
import subprocess
import time
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

import redis
from textual.app import App, ComposeResult
from textual.containers import Container, Horizontal, Vertical
from textual.reactive import reactive
from textual.widgets import DataTable, Footer, Header, Static, Log

# -----------------------------
# Config
# -----------------------------
ENV_FILE = os.path.expanduser("~/.config/scanner/env")
REFRESH_SEC = 1.0
LOG_LINES = 800  # pull last N log lines each refresh


@dataclass(frozen=True)
class ServiceDef:
    unit: str
    label: str


SERVICES: List[ServiceDef] = [
    ServiceDef("scanner-mcp.service", "MCP Server"),
    ServiceDef("scanner-recorder.service", "Recorder"),
    ServiceDef("scanner-transcriber.service", "Transcriber Listener"),
    ServiceDef("rtl_tcp@12000.service", "BPD, MPD, FRNKFD"),
    ServiceDef("rtl_tcp@12001.service", "BFD, HFD"),
    ServiceDef("rtl_tcp@12002.service", "HPD, BLKFD"),
    ServiceDef("rtl_tcp@12003.service", "MFD, MNDFD"),
    ServiceDef("rtl_tcp@12004.service", "MNDPD, BLKPD"),
    ServiceDef("scanner-websocket.service", "Websocket Server"),
]


# -----------------------------
# Helpers
# -----------------------------
def run(cmd: List[str], timeout: Optional[int] = None) -> Tuple[int, str]:
    p = subprocess.run(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        timeout=timeout,
    )
    return p.returncode, (p.stdout or "").rstrip()


def parse_env_file(path: str) -> Dict[str, str]:
    env: Dict[str, str] = {}
    if not os.path.exists(path):
        return env
    for raw in open(path, "r", encoding="utf-8").read().splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, v = line.split("=", 1)
        env[k.strip()] = v.strip()
    return env


def parse_mcp_host_port(mcp_url: str) -> Tuple[str, int]:
    # crude parse: http://host:port/...
    host = "127.0.0.1"
    port = 8000
    try:
        tmp = mcp_url.split("://", 1)[1]
        hostport = tmp.split("/", 1)[0]
        if ":" in hostport:
            host, p = hostport.split(":", 1)
            port = int(p)
        else:
            host = hostport
    except Exception:
        pass
    return host, port


def tcp_check(host: str, port: int, timeout: float = 0.5) -> bool:
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except Exception:
        return False


def redis_ping(redis_url: str) -> bool:
    try:
        r = redis.from_url(redis_url)
        return bool(r.ping())
    except Exception:
        return False


def gpu_mem() -> str:
    code, out = run(
        [
            "nvidia-smi",
            "--query-gpu=memory.used,memory.total",
            "--format=csv,noheader,nounits",
        ]
    )
    if code != 0 or not out.strip():
        return "n/a"
    first = out.splitlines()[0].strip()
    parts = [p.strip() for p in first.split(",")]
    if len(parts) != 2:
        return "n/a"
    return f"{parts[0]} MiB / {parts[1]} MiB"


def systemctl_show(unit: str) -> Dict[str, str]:
    props = [
        "ActiveState",
        "SubState",
        "ExecMainPID",
        "MainPID",
        "ExecMainStartTimestamp",
    ]
    code, out = run(
        ["systemctl", "--user", "show", unit, "--property=" + ",".join(props)]
    )
    d: Dict[str, str] = {}
    if code != 0:
        d["ActiveState"] = "unknown"
        d["SubState"] = "unknown"
        d["ExecMainPID"] = ""
        d["MainPID"] = ""
        d["ExecMainStartTimestamp"] = ""
        d["error"] = out
        return d
    for line in out.splitlines():
        if "=" in line:
            k, v = line.split("=", 1)
            d[k] = v
    return d


def journal_tail(unit: str, lines: int) -> str:
    _, out = run(
        [
            "journalctl",
            "--user",
            "-u",
            unit,
            "-n",
            str(lines),
            "--no-pager",
            "--output=short",
        ]
    )
    return out


def systemctl_action(unit: str, verb: str) -> Tuple[bool, str]:
    code, out = run(["systemctl", "--user", verb, unit])
    if code == 0:
        return True, f"{verb} {unit}: OK"
    return False, f"{verb} {unit}: FAILED\n{out}"


# -----------------------------
# Widgets
# -----------------------------
class HealthPanel(Static):
    def update_health(self, env: Dict[str, str]) -> None:
        redis_url = env.get("REDIS_URL", "redis://127.0.0.1:6379/0")
        mcp_url = env.get("MCP_URL", "http://127.0.0.1:8000/mcp")
        host, port = parse_mcp_host_port(mcp_url)

        redis_ok = redis_ping(redis_url)
        mcp_ok = tcp_check(host, port)
        gpu = gpu_mem()

        lines = [
            "[b]Health[/b]",
            f"Redis: {'[green]OK[/green]' if redis_ok else '[red]DOWN[/red]'}",
            f"MCP TCP ({host}:{port}): {'[green]OK[/green]' if mcp_ok else '[red]DOWN[/red]'}",
            f"GPU Mem: {gpu}",
            "",
            "[b]Env[/b]",
            f"REDIS_URL={redis_url}",
            f"MCP_URL={mcp_url}",
        ]
        self.update("\n".join(lines))


class ServiceTable(DataTable):
    def on_mount(self) -> None:
        self.add_columns("Service", "Active", "Sub", "PID", "Since")
        self.cursor_type = "row"
        self.zebra_stripes = True


# -----------------------------
# App
# -----------------------------
class ScannerControlRoom(App):
    CSS = """
    Screen { layout: vertical; }

    #top { height: 12; }
    #services { width: 60%; padding: 0 1; }
    #health { width: 40%; padding: 0 1; }
    #main { height: 1fr; }
    #logs { height: 1fr; border: round $accent; padding: 0 1; }
    #statusbar { height: 3; border: round $secondary; padding: 0 1; }
    """

    BINDINGS = [
        ("q", "quit", "Quit"),
        ("r", "restart_selected", "Restart Selected"),
        ("s", "start_selected", "Start Selected"),
        ("x", "stop_selected", "Stop Selected"),
        ("R", "restart_all", "Restart All"),
        ("S", "start_all", "Start All"),
        ("X", "stop_all", "Stop All"),
        ("l", "toggle_follow", "Toggle Log Follow"),
        ("f", "force_refresh", "Refresh Now"),
    ]

    env: Dict[str, str] = {}
    selected_unit: reactive[str] = reactive(SERVICES[0].unit)
    follow_logs: reactive[bool] = reactive(True)

    def compose(self) -> ComposeResult:
        yield Header(show_clock=True)

        with Horizontal(id="top"):
            with Container(id="services"):
                yield Static("[b]Services[/b]")
                yield ServiceTable(id="svc_table")
            with Container(id="health"):
                yield HealthPanel(id="health_panel")

        with Vertical(id="main"):
            yield Log(id="logs", highlight=True)
            yield Static("", id="statusbar")

        yield Footer()

    def on_mount(self) -> None:
        self.env = parse_env_file(ENV_FILE)
        self._init_table()
        self.set_interval(REFRESH_SEC, self.refresh_all)
        self.refresh_all()

    def _init_table(self) -> None:
        table = self.query_one("#svc_table", ServiceTable)
        table.clear()
        for svc in SERVICES:
            table.add_row(svc.label, "…", "…", "…", "…", key=svc.unit)
        table.focus()
        table.cursor_coordinate = (0, 0)
        self.selected_unit = SERVICES[0].unit

    def _set_status(self, msg: str) -> None:
        self.query_one("#statusbar", Static).update(msg)

    def refresh_all(self) -> None:
        table = self.query_one("#svc_table", ServiceTable)
        for svc in SERVICES:
            info = systemctl_show(svc.unit)
            active = info.get("ActiveState", "unknown")
            sub = info.get("SubState", "unknown")
            pid = info.get("ExecMainPID") or info.get("MainPID") or ""
            since = info.get("ExecMainStartTimestamp", "")

            if active == "active":
                active_txt = "[green]active[/green]"
            elif active == "inactive":
                active_txt = "[yellow]inactive[/yellow]"
            else:
                active_txt = f"[red]{active}[/red]"

            try:
                row_index = table.get_row_index(svc.unit)
                table.update_cell_at((row_index, 1), active_txt)
                table.update_cell_at((row_index, 2), sub)
                table.update_cell_at((row_index, 3), pid)
                table.update_cell_at((row_index, 4), since)
            except Exception:
                self._init_table()
                break

        self.query_one("#health_panel", HealthPanel).update_health(self.env)

        if self.follow_logs:
            self.refresh_logs()

    def refresh_logs(self) -> None:
        unit = self.selected_unit
        log_text = journal_tail(unit, LOG_LINES)

        logs = self.query_one("#logs", Log)
        logs.clear()

        if log_text.strip():
            for line in log_text.splitlines():
                logs.write_line(line)
        else:
            logs.write_line("(no logs)")

        self._set_status(
            f"Selected: [b]{unit}[/b] | follow_logs={'ON' if self.follow_logs else 'OFF'}"
        )

    def on_data_table_row_highlighted(self, event: DataTable.RowHighlighted) -> None:
        if event.row_key is None:
            return

        # Textual wraps keys in RowKey; unwrap it safely
        key = event.row_key
        if hasattr(key, "value"):
            key = key.value

        self.selected_unit = str(key)
        self.refresh_logs()

    # -----------------------------
    # Actions
    # -----------------------------
    def action_force_refresh(self) -> None:
        self.refresh_all()
        self._set_status("Refreshed.")

    def action_toggle_follow(self) -> None:
        self.follow_logs = not self.follow_logs
        self.refresh_logs()

    def _act_selected(self, verb: str) -> None:
        unit = self.selected_unit
        ok, msg = systemctl_action(unit, verb)
        self._set_status(msg if ok else f"[red]{msg}[/red]")
        time.sleep(0.15)
        self.refresh_all()

    def _act_all(self, verb: str) -> None:
        msgs = []
        all_ok = True
        for svc in SERVICES:
            ok, msg = systemctl_action(svc.unit, verb)
            all_ok = all_ok and ok
            msgs.append(msg)
        joined = " | ".join(msgs)
        self._set_status(joined if all_ok else f"[red]{joined}[/red]")
        time.sleep(0.2)
        self.refresh_all()

    def action_start_selected(self) -> None:
        self._act_selected("start")

    def action_stop_selected(self) -> None:
        self._act_selected("stop")

    def action_restart_selected(self) -> None:
        self._act_selected("restart")

    def action_start_all(self) -> None:
        self._act_all("start")

    def action_stop_all(self) -> None:
        self._act_all("stop")

    def action_restart_all(self) -> None:
        self._act_all("restart")


if __name__ == "__main__":
    ScannerControlRoom().run()