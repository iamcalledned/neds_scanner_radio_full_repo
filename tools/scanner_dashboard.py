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

import sys
import os

venv_python = os.path.expanduser("~/venv/bin/python3")
if sys.executable != venv_python:
    os.execv(venv_python, [venv_python] + sys.argv)
    
import socket
import subprocess
import time
import json
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

import redis
from textual.app import App, ComposeResult
from textual.containers import Container, Horizontal, Vertical, VerticalScroll
from textual.reactive import reactive
from textual.widgets import DataTable, Footer, Header, Static, Log, TabbedContent, TabPane, Select, Checkbox, Button, Label

# -----------------------------
# Config
# -----------------------------
ENV_FILE = os.path.expanduser("~/.config/scanner/env")
REFRESH_SEC = 1.0
LOG_LINES = 800  # pull last N log lines each refresh

TRANSCRIBER_ENV_FILE = "/home/ned/Documents/neds_scanner_radio_full_pipeline_with_git/transcriber/.env"
MODEL_CATALOG_FILE = "/home/ned/Documents/neds_scanner_radio_full_pipeline_with_git/transcriber/model_catalog.json"

def clean_proc(p):
    parts = p.split()

    pid = parts[0] if parts else "?"
    cmd = parts[1] if len(parts) > 1 else "unknown"

    name = os.path.basename(cmd)

    # Optional: detect GPU-related chrome process
    if "--type=gpu-process" in p:
        name += " (GPU)"

    return f"{name} (pid: {pid})"

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
    ServiceDef("scanner-archive-sweep.service", "Archive Sweeper"),
    ServiceDef("scanner-archive-sweep.timer", "Archive Sweep Timer"),
    ServiceDef("vllm.service", "vLLM Local Model Server"),
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


def gpu_processes(max_rows: int = 5) -> List[str]:
    """
    Return up to max_rows processes using GPU with their memory usage.
    """
    code, out = run(
        [
            "nvidia-smi",
            "--query-compute-apps=pid,process_name,used_memory",
            "--format=csv,noheader,nounits",
        ]
    )
    if code != 0 or not out.strip():
        return ["n/a"]

    rows = []
    for line in out.splitlines():
        parts = [p.strip() for p in line.split(",")]
        if len(parts) != 3:
            continue
        pid, name, mem = parts
        rows.append(f"{pid} {name} ({mem} MiB)")
        if len(rows) >= max_rows:
            break
    return rows or ["none"]


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
# Model Configuration Helpers
# -----------------------------
def get_available_models() -> List[str]:
    try:
        with open(MODEL_CATALOG_FILE, "r") as f:
            cat = json.load(f)
        return list(cat.get("models", {}).keys())
    except Exception:
        return []

def read_env_settings() -> Tuple[str, bool, List[str], bool]:
    default_model = ""
    default_enabled = False
    secondary_models = []
    secondary_enabled = False
    
    if not os.path.exists(TRANSCRIBER_ENV_FILE):
        return default_model, default_enabled, secondary_models, secondary_enabled
        
    with open(TRANSCRIBER_ENV_FILE, "r") as f:
        for line in f:
            sline = line.strip()
            
            if sline.startswith("DEFAULT_MODEL_KEY="):
                default_enabled = True
                default_model = sline.split("=", 1)[1].strip()
            elif sline.startswith("#DEFAULT_MODEL_KEY=") or sline.startswith("# DEFAULT_MODEL_KEY="):
                default_enabled = False
                default_model = sline.split("=", 1)[1].strip()
                
            elif sline.startswith("SECONDARY_MODELS="):
                secondary_enabled = True
                val = sline.split("=", 1)[1].strip()
                if val:
                    secondary_models = [x.strip() for x in val.split(",")]
            elif sline.startswith("#SECONDARY_MODELS=") or sline.startswith("# SECONDARY_MODELS="):
                secondary_enabled = False
                val = sline.split("=", 1)[1].strip()
                if val:
                    secondary_models = [x.strip() for x in val.split(",")]
                    
    return default_model, default_enabled, secondary_models, secondary_enabled

def update_transcriber_env(default_model: str, default_enabled: bool, secondary_models_str: str, secondary_enabled: bool) -> bool:
    if not os.path.exists(TRANSCRIBER_ENV_FILE):
        lines = []
    else:
        with open(TRANSCRIBER_ENV_FILE, "r") as f:
            lines = f.readlines()
            
    out = []
    found_default = False
    found_secondary = False
    
    for line in lines:
        sline = line.strip()
        
        if sline.startswith("DEFAULT_MODEL_KEY=") or sline.startswith("#DEFAULT_MODEL_KEY=") or sline.startswith("# DEFAULT_MODEL_KEY="):
            out.append(f"{'' if default_enabled else '# '}DEFAULT_MODEL_KEY={default_model}\n")
            found_default = True
            continue
            
        if sline.startswith("SECONDARY_MODELS=") or sline.startswith("#SECONDARY_MODELS=") or sline.startswith("# SECONDARY_MODELS="):
            out.append(f"{'' if secondary_enabled else '# '}SECONDARY_MODELS={secondary_models_str}\n")
            found_secondary = True
            continue
            
        out.append(line)
        
    if not found_default:
        out.append(f"{'' if default_enabled else '# '}DEFAULT_MODEL_KEY={default_model}\n")
            
    if not found_secondary:
        out.append(f"{'' if secondary_enabled else '# '}SECONDARY_MODELS={secondary_models_str}\n")
            
    try:
        with open(TRANSCRIBER_ENV_FILE, "w") as f:
            f.writelines(out)
        return True
    except Exception:
        return False


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
        gpu_procs = gpu_processes()

        # Limit the number of GPU processes to display
        max_gpu_procs = 5
        gpu_procs_display = gpu_procs[:max_gpu_procs]

        lines = [
            "[b]Health[/b]",
            f"Redis: {'[green]OK[/green]' if redis_ok else '[red]DOWN[/red]'}",
            f"MCP TCP ({host}:{port}): {'[green]OK[/green]' if mcp_ok else '[red]DOWN[/red]'}",
            f"GPU Mem: {gpu}",
            "",
            "[b]GPU Processes[/b]",
            *["  - " + clean_proc(p) for p in gpu_procs_display],
            "" if len(gpu_procs) <= max_gpu_procs else f"  - ... (and {len(gpu_procs) - max_gpu_procs} more)",
            "",
            "[b]Environment Variables[/b]",
            f"REDIS_URL={redis_url}",
            f"MCP_URL={mcp_url}",
        ]
        self.update("\n".join(lines))

class ServiceTable(DataTable):
    def on_mount(self) -> None:
        self.add_columns("Service", "Active", "Sub", "PID", "Since")
        self.cursor_type = "row"
        self.zebra_stripes = True

class ModelConfigPanel(Vertical):
    def compose(self) -> ComposeResult:
        default_model, default_enabled, sec_models, sec_enabled = read_env_settings()
        
        models = get_available_models()
        for m in [default_model] + sec_models:
            if m and m not in models:
                models.append(m)
        
        with Horizontal(id="settings_header"):
            yield Label("Model Configuration", id="settings_title")
            yield Button("Reload Options", id="refresh_models_btn")
            
        yield Label("Default Model", classes="lbl")
        with Horizontal(classes="toggle_row"):
            yield Checkbox("Enable", value=default_enabled, id="enable_default_cb")
            yield Select([(m, m) for m in models], value=default_model if default_model else getattr(Select, "BLANK", None), id="default_model_select")
        
        yield Label("Secondary Models", classes="lbl")
        yield Checkbox("Enable", value=sec_enabled, id="enable_secondary_cb")
        
        with Vertical(id="secondary_models_list"):
            for m in models:
                yield Checkbox(m, value=(m in sec_models), id=f"sec_cb_{m}", classes="sec_cb")
                
        yield Button("Save Changes", id="save_config_btn", variant="primary")
        yield Label("", id="settings_status")

    def refresh_settings_ui(self) -> None:
        default_model, default_enabled, sec_models, sec_enabled = read_env_settings()
        
        models = get_available_models()
        for m in [default_model] + sec_models:
            if m and m not in models:
                models.append(m)
        
        sel = self.query_one("#default_model_select", Select)
        if hasattr(sel, "set_options"):
            sel.set_options([(m, m) for m in models])
            
        blank_val = getattr(Select, "BLANK", object())
        sel.value = default_model if default_model else blank_val
            
        self.query_one("#enable_default_cb", Checkbox).value = default_enabled
        self.query_one("#enable_secondary_cb", Checkbox).value = sec_enabled
        
        sec_list = self.query_one("#secondary_models_list", Vertical)
        for child in list(sec_list.children):
            child.remove()
            
        for m in models:
            sec_list.mount(Checkbox(m, value=(m in sec_models), id=f"sec_cb_{m}", classes="sec_cb"))

    def on_button_pressed(self, event: Button.Pressed) -> None:
        if event.button.id == "save_config_btn":
            sel = self.query_one("#default_model_select", Select)
            val = getattr(sel, "value", None)
            blank_val = getattr(Select, "BLANK", object())
            default_model = str(val) if val and val != blank_val else ""
            
            default_enabled = self.query_one("#enable_default_cb", Checkbox).value
            sec_enabled = self.query_one("#enable_secondary_cb", Checkbox).value
            
            sec_models = [cb.id[7:] for cb in self.query(Checkbox).filter(".sec_cb") if cb.value]
            sec_str = ",".join(sec_models)
            
            success = update_transcriber_env(default_model, default_enabled, sec_str, sec_enabled)
            status = self.query_one("#settings_status", Label)
            if success:
                status.update(f"[{time.strftime('%H:%M:%S')}] [green]Saved successfully![/green]")
            else:
                status.update(f"[{time.strftime('%H:%M:%S')}] [red]Error saving file[/red]")
                
        elif event.button.id == "refresh_models_btn":
            self.refresh_settings_ui()
            status = self.query_one("#settings_status", Label)
            status.update(f"[{time.strftime('%H:%M:%S')}] [yellow]Reloaded models from disk[/yellow]")


# -----------------------------
# App
# -----------------------------
class ScannerControlRoom(App):
    CSS = """
    Screen { layout: vertical; }
    #services_scroll { 
    height: 1fr; 
    overflow-y: auto; 
        }   
    #top { height: 12; }
    #services { width: 60%; padding: 0 1; }
    #health { width: 40%; padding: 0 1; }
    #main { height: 1fr; }
    #logs { height: 1fr; border: round $accent; padding: 0 1; }
    #statusbar { height: 3; border: round $secondary; padding: 0 1; }

    /* Model Config Settings */
    #model_config_panel { padding: 1 2; height: 100%; width: 100%; }
    #settings_header { height: 3; align: left middle; }
    #settings_title { text-style: bold; width: 1fr; content-align: left middle; }
    .lbl { margin-top: 1; text-style: bold; }
    .toggle_row { height: 3; }
    #default_model_select { width: 60; margin-left: 2; }
    #secondary_models_list { 
        height: 1fr; 
        border: solid $accent; 
        padding: 0 1; 
        margin-top: 1;
        margin-bottom: 1; 
        overflow-y: auto;
    }
    #save_config_btn { margin-top: 1; }
    #settings_status { margin-top: 1; }
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

        with TabbedContent(initial="dashboard_tab"):
            with TabPane("Dashboard", id="dashboard_tab"):
                    with Horizontal(id="top"):
                        with Container(id="services"):
                            yield Static("[b]Services[/b]")
                            with VerticalScroll(id="services_scroll"):
                                yield ServiceTable(id="svc_table")
                        with Container(id="health"):
                            with VerticalScroll(id="health_scroll"):
                                yield HealthPanel(id="health_panel")
                    with Vertical(id="main"):
                        yield Log(id="logs", highlight=True)
                        yield Static("", id="statusbar")
                    
            with TabPane("Model Settings", id="settings_tab"):
                yield ModelConfigPanel(id="model_config_panel")

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