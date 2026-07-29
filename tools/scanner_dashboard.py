#!/usr/bin/env python3
"""
scanner_dashboard.py — Textual dashboard to start/stop/monitor:
  - scanner-mcp.service
  - scanner-recorder.service
  - gpu-gatekeeper.service
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
import math
import shlex
import threading
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

import redis
from rich.markup import escape
from textual.app import App, ComposeResult
from textual.containers import Container, Horizontal, Vertical, VerticalScroll
from textual.reactive import reactive
from textual.widgets import DataTable, Footer, Header, Static, Log, TabbedContent, TabPane, Select, Checkbox, Button, Label, Input

from llm_launcher import (
    BASE_MODELS_DIR,
    VllmRunSettings,
    assess_gatekeeper_plan,
    build_vllm_command,
    discover_local_llm_models,
    estimate_model_vram_mb,
    format_bytes,
    model_size_bytes,
    parse_vllm_command,
    recommended_download_dir,
    validate_extra_args,
    validate_model_path,
)
from scanner_dashboard_health import (
    format_gatekeeper_leases,
    format_gatekeeper_services,
    gatekeeper_is_healthy,
)

# -----------------------------
# Config
# -----------------------------
ENV_FILE = os.path.expanduser("~/.config/scanner/env")
REFRESH_SEC = 1.0
LOG_LINES = 800  # pull last N log lines each refresh

TRANSCRIBER_ENV_FILE = "/home/ned/Documents/neds_scanner_radio_full_pipeline_with_git/transcriber/.env"
MODEL_CATALOG_FILE = "/home/ned/Documents/neds_scanner_radio_full_pipeline_with_git/transcriber/model_catalog.json"
VLLM_UNIT = "vllm.service"
VLLM_OVERRIDE_FILE = os.path.expanduser(f"~/.config/systemd/user/{VLLM_UNIT}.d/override.conf")
VLLM_DEFAULT_MODEL = "Qwen/Qwen2.5-14B-Instruct-AWQ"
VLLM_DEFAULT_EXEC = (
    "/home/ned/vllm_stack/bin/vllm serve "
    f"{VLLM_DEFAULT_MODEL} "
    "--gpu-memory-utilization 0.55 --max-num-seqs 2 --port 30000 "
    "--enable-auto-tool-choice --tool-call-parser hermes"
)
VLLM_BINARY = shlex.split(VLLM_DEFAULT_EXEC)[0]

LLAMA_UNIT = "llama-server.service"
LLAMA_OVERRIDE_FILE = os.path.expanduser(f"~/.config/systemd/user/{LLAMA_UNIT}.d/override.conf")
LLAMA_BINARY = os.path.expanduser("~/Documents/llama.cpp/build/bin/llama-server")
LLAMA_MODELS_DIR = BASE_MODELS_DIR
LLAMA_DEFAULT_MODEL = os.path.expanduser(
    "~/models/qwen36/Qwen3.6-35B-A3B-Uncensored-HauhauCS-Aggressive-Q4_K_M.gguf"
)
LLAMA_DEFAULT_NGL = "999"
LLAMA_DEFAULT_CTX = "32768"
LLAMA_DEFAULT_HOST = "127.0.0.1"
LLAMA_DEFAULT_PORT = "30000"
LLAMA_DEFAULT_EXTRA = ""
LLAMA_STATE_FILE = os.path.expanduser("~/.config/scanner/llama_state.json")

def clean_proc(p):
    parts = p.split()

    pid = parts[0] if parts else "?"
    cmd = parts[1] if len(parts) > 1 else "unknown"

    name = os.path.basename(cmd)

    # Optional: detect GPU-related chrome process
    if "--type=gpu-process" in p:
        name += " (GPU)"

    return f"{name} (pid: {pid})"


def configure_gatekeeper_llm_runtime(
    runtime_key: str,
    port: int,
    estimated_vram_mb: int,
) -> Tuple[bool, str]:
    """Synchronize dynamic LLM controls with gatekeeper readiness and policy."""
    try:
        gk_path = "/home/ned/Documents/GPU_agent_gatekeeper"
        if gk_path not in sys.path:
            sys.path.append(gk_path)
        from gpu_gatekeeper.core import runtime_registry

        runtimes = runtime_registry.load_runtime_registry()
        runtime = runtimes.get(runtime_key)
        if not runtime:
            return False, f"Gatekeeper runtime '{runtime_key}' is not registered."
        runtime["endpoint"] = f"http://127.0.0.1:{port}/v1"
        runtime["estimated_vram_mb"] = int(estimated_vram_mb)
        if not runtime_registry.save_runtime_registry(runtimes):
            return False, "Could not update the GPU gatekeeper runtime registry."
        return True, "Gatekeeper runtime synchronized."
    except Exception as exc:
        return False, f"Could not synchronize GPU gatekeeper runtime: {exc}"

@dataclass(frozen=True)
class ServiceDef:
    unit: str
    label: str
    is_user: bool = True
    log_file: Optional[str] = None


SCANNER_SERVICES: List[ServiceDef] = [
    ServiceDef("scanner-mcp.service", "MCP Server"),
    ServiceDef("scanner-recorder.service", "Recorder"),
    ServiceDef("gpu-gatekeeper.service", "Scanner Gatekeeper"),
    ServiceDef("scanner-transcriber.service", "Transcriber Listener"),
    ServiceDef("rtl_tcp@12000.service", "BPD, MPD, FRNKFD"),
    ServiceDef("rtl_tcp@12001.service", "BFD, HFD"),
    ServiceDef("rtl_tcp@12002.service", "HPD, BLKFD"),
    ServiceDef("rtl_tcp@12003.service", "MFD, MNDFD"),
    ServiceDef("rtl_tcp@12004.service", "MNDPD, BLKPD"),
    ServiceDef("scanner-websocket.service", "Websocket Server"),
    ServiceDef("scanner-archive-sweep.service", "Archive Sweeper"),
    ServiceDef("scanner-archive-sweep.timer", "Archive Sweep Timer"),
]

CHATBOT_SERVICES: List[ServiceDef] = [
    ServiceDef("nedbot.service", "NedBot Chatbot", is_user=True, log_file=os.path.expanduser("~/.local/share/nedbot/chat.log")),
]

LLM_SERVICES: List[ServiceDef] = [
    ServiceDef("vllm.service", "vLLM Local Model Server"),
]

LLAMA_SERVICES: List[ServiceDef] = [
    ServiceDef(LLAMA_UNIT, "llama-server"),
]

SERVICES: List[ServiceDef] = SCANNER_SERVICES + LLM_SERVICES + LLAMA_SERVICES + CHATBOT_SERVICES


def _unit_is_user(unit: str) -> bool:
    for svc in SERVICES:
        if svc.unit == unit:
            return svc.is_user
    return True


def _unit_log_file(unit: str) -> Optional[str]:
    for svc in SERVICES:
        if svc.unit == unit:
            return svc.log_file
    return None


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
    user_flag = ["--user"] if _unit_is_user(unit) else []
    code, out = run(
        ["systemctl"] + user_flag + ["show", unit, "--property=" + ",".join(props)]
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
    user_flag = ["--user"] if _unit_is_user(unit) else []
    _, journal_out = run(
        [
            "journalctl",
        ]
        + user_flag
        + [
            "-u",
            unit,
            "-n",
            str(lines),
            "--no-pager",
            "--output=short",
        ]
    )

    log_file = _unit_log_file(unit)
    if log_file and os.path.exists(log_file):
        try:
            with open(log_file, "r", encoding="utf-8", errors="replace") as f:
                all_lines = f.readlines()
            file_out = "".join(all_lines[-lines:])
        except Exception as e:
            file_out = f"(could not read {log_file}: {e})"
        parts = []
        if journal_out.strip():
            parts.append(journal_out)
        parts.append(f"--- {log_file} ---")
        parts.append(file_out)
        return "\n".join(parts)

    return journal_out


def systemctl_action(unit: str, verb: str) -> Tuple[bool, str]:
    user_flag = ["--user"] if _unit_is_user(unit) else []
    code, out = run(["systemctl"] + user_flag + [verb, unit])
    if code == 0:
        return True, f"{verb} {unit}: OK"
    return False, f"{verb} {unit}: FAILED\n{out}"


def systemctl_daemon_reload() -> Tuple[bool, str]:
    code, out = run(["systemctl", "--user", "daemon-reload"])
    if code == 0:
        return True, "systemctl --user daemon-reload: OK"
    return False, f"systemctl --user daemon-reload: FAILED\n{out}"


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
# vLLM Service Configuration Helpers
# -----------------------------
def _dedupe(items: List[str]) -> List[str]:
    seen = set()
    out = []
    for item in items:
        if item and item not in seen:
            seen.add(item)
            out.append(item)
    return out


def get_available_vllm_models() -> List[str]:
    models = [VLLM_DEFAULT_MODEL]
    try:
        with open(MODEL_CATALOG_FILE, "r", encoding="utf-8") as f:
            cat = json.load(f)
        raw_models = cat.get("models", {})
        if isinstance(raw_models, dict):
            for key, value in raw_models.items():
                if isinstance(key, str) and "/" in key:
                    models.append(key)
                if isinstance(value, dict):
                    for field in ("model", "model_id", "repo_id", "hf_model_id", "path"):
                        val = value.get(field)
                        if isinstance(val, str) and val:
                            models.append(val)
    except Exception:
        pass
    current_model = get_vllm_model_from_command(get_current_vllm_exec_command())
    if current_model:
        models.append(current_model)
    return _dedupe(models)


def get_current_vllm_exec_command() -> str:
    code, out = run(["systemctl", "--user", "cat", VLLM_UNIT])
    if code == 0 and out.strip():
        exec_start = _last_exec_start_from_unit_text(out)
        if exec_start:
            return exec_start

    exec_start = _last_exec_start_from_file(VLLM_OVERRIDE_FILE)
    return exec_start or VLLM_DEFAULT_EXEC


def _last_exec_start_from_file(path: str) -> str:
    if not os.path.exists(path):
        return ""
    try:
        with open(path, "r", encoding="utf-8") as f:
            return _last_exec_start_from_unit_text(f.read())
    except Exception:
        return ""


def _last_exec_start_from_unit_text(text: str) -> str:
    exec_start = ""
    for raw in text.splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or not line.startswith("ExecStart="):
            continue
        value = line.split("=", 1)[1].strip()
        if value:
            exec_start = value
    return exec_start


def get_vllm_model_from_command(command: str) -> str:
    try:
        parts = shlex.split(command)
    except ValueError:
        return ""
    try:
        serve_index = parts.index("serve")
    except ValueError:
        return ""
    if len(parts) <= serve_index + 1:
        return ""
    return parts[serve_index + 1]


def replace_vllm_model_in_command(command: str, model: str) -> str:
    try:
        parts = shlex.split(command)
    except ValueError:
        parts = shlex.split(VLLM_DEFAULT_EXEC)

    try:
        serve_index = parts.index("serve")
    except ValueError:
        parts = shlex.split(VLLM_DEFAULT_EXEC)
        serve_index = parts.index("serve")

    if len(parts) <= serve_index + 1:
        parts.append(model)
    else:
        parts[serve_index + 1] = model
    return shlex.join(parts)


# -----------------------------
# llama-server Helpers
# -----------------------------

def get_gguf_models() -> List[str]:
    models = []
    try:
        for root, _dirs, files in os.walk(LLAMA_MODELS_DIR):
            for fname in sorted(files):
                if fname.endswith(".gguf"):
                    models.append(os.path.join(root, fname))
    except Exception:
        pass
    if not models:
        models.append(LLAMA_DEFAULT_MODEL)
    return models


def _load_llama_state() -> Dict[str, str]:
    defaults: Dict[str, str] = {
        "model": LLAMA_DEFAULT_MODEL,
        "ngl": LLAMA_DEFAULT_NGL,
        "ctx": LLAMA_DEFAULT_CTX,
        "host": LLAMA_DEFAULT_HOST,
        "port": LLAMA_DEFAULT_PORT,
        "extra": LLAMA_DEFAULT_EXTRA,
    }
    try:
        if os.path.exists(LLAMA_STATE_FILE):
            with open(LLAMA_STATE_FILE, "r", encoding="utf-8") as f:
                saved = json.load(f)
            defaults.update({k: str(v) for k, v in saved.items() if k in defaults})
    except Exception:
        pass
    return defaults


def _save_llama_state(state: Dict[str, str]) -> None:
    try:
        os.makedirs(os.path.dirname(LLAMA_STATE_FILE), exist_ok=True)
        with open(LLAMA_STATE_FILE, "w", encoding="utf-8") as f:
            json.dump(state, f, indent=2)
    except Exception:
        pass


def build_llama_command(model: str, ngl: str, ctx: str, host: str, port: str, extra: str) -> str:
    parts = [
        LLAMA_BINARY,
        "-m", model,
        "-ngl", ngl,
        "-c", ctx,
        "--host", host,
        "--port", port,
    ]
    if extra.strip():
        try:
            parts.extend(shlex.split(extra.strip()))
        except ValueError:
            parts.append(extra.strip())
    return shlex.join(parts)


def write_llama_exec_override(model: str, ngl: str, ctx: str, host: str, port: str, extra: str) -> Tuple[bool, str]:
    command = build_llama_command(model, ngl, ctx, host, port, extra)
    override_dir = os.path.dirname(LLAMA_OVERRIDE_FILE)
    try:
        os.makedirs(override_dir, exist_ok=True)
        with open(LLAMA_OVERRIDE_FILE, "w", encoding="utf-8") as f:
            f.write("[Service]\n")
            f.write("ExecStart=\n")
            f.write(f"ExecStart={command}\n")
    except Exception as e:
        return False, f"Failed writing override: {e}"
    ok, msg = systemctl_daemon_reload()
    if not ok:
        return False, msg
    _save_llama_state({"model": model, "ngl": ngl, "ctx": ctx, "host": host, "port": port, "extra": extra})
    return True, f"Saved llama-server override."


def write_vllm_exec_override(command: str) -> Tuple[bool, str]:
    command = command.strip()
    if not command:
        return False, "vLLM ExecStart command is empty."

    try:
        parts = shlex.split(command)
    except ValueError as e:
        return False, f"vLLM ExecStart command has invalid quoting: {e}"

    if "serve" not in parts:
        return False, "vLLM ExecStart command must include the 'serve' subcommand."

    try:
        os.makedirs(os.path.dirname(VLLM_OVERRIDE_FILE), exist_ok=True)
        with open(VLLM_OVERRIDE_FILE, "w", encoding="utf-8") as f:
            f.write("[Service]\n")
            # FlashInfer's sampler JIT consults the host CUDA toolkit. This
            # workstation's RTX 5090 is supported by the CUDA 12.9 PyTorch
            # runtime, but the older system nvcc cannot compile SM 12.0.
            # vLLM's native sampler avoids that optional JIT path.
            f.write("Environment=VLLM_USE_FLASHINFER_SAMPLER=0\n")
            f.write("ExecStart=\n")
            f.write(f"ExecStart={command}\n")
    except Exception as e:
        return False, f"Failed writing {VLLM_OVERRIDE_FILE}: {e}"

    ok, msg = systemctl_daemon_reload()
    if not ok:
        return False, msg
    return True, f"Saved vLLM override: {VLLM_OVERRIDE_FILE}"


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

        gk_status_str = "[red]DOWN[/red]"
        gk_leases_str = ""
        gk_services_str = ""
        try:
            import sys
            gk_path = "/home/ned/Documents/GPU_agent_gatekeeper"
            if gk_path not in sys.path:
                sys.path.append(gk_path)
            from gpu_gatekeeper.client import GpuGatekeeperClient
            gk_client = GpuGatekeeperClient()
            gk_data = gk_client.status()
            
            if gatekeeper_is_healthy(gk_data):
                gk_status_str = "[green]OK[/green]"
            else:
                gk_status_str = "[yellow]DEGRADED[/yellow]"
                
            leases = gk_data.get("leases", {}).get("leases", [])
            gk_leases_str = format_gatekeeper_leases(leases)
            
            svcs = gk_data.get("services", {})
            gk_services_str = format_gatekeeper_services(svcs)
        except Exception as e:
            gk_status_str = f"[red]DOWN ({type(e).__name__})[/red]"
            gk_leases_str = f"  • Connection error"
            gk_services_str = f"  • Connection error"

        lines = [
            "[b]Health[/b]",
            f"Redis: {'[green]OK[/green]' if redis_ok else '[red]DOWN[/red]'}",
            f"MCP TCP ({host}:{port}): {'[green]OK[/green]' if mcp_ok else '[red]DOWN[/red]'}",
            f"GPU Mem: {gpu}",
            "",
            "[b]GPU Gatekeeper[/b]",
            f"Status: {gk_status_str}",
            "Active Leases:",
            gk_leases_str,
            "Tracked Services:",
            gk_services_str,
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


class LLMLauncherPanel(Vertical):
    """One scanner-safe workflow for selecting and launching a local LLM."""

    _refreshing_models = False
    _busy = False

    BACKENDS = {
        "llama": {
            "label": "llama.cpp (GGUF)",
            "runtime_key": "local_llm",
            "unit": LLAMA_UNIT,
            "registry_floor_mb": 22_000,
        },
        "vllm": {
            "label": "vLLM (Transformers)",
            "runtime_key": "vllm_runtime",
            "unit": VLLM_UNIT,
            "registry_floor_mb": 16_000,
        },
    }

    def compose(self) -> ComposeResult:
        current_vllm_settings = parse_vllm_command(
            get_current_vllm_exec_command()
        )
        with VerticalScroll(id="llm_launcher_scroll"):
            yield Static(
                "[b]Scanner-safe LLM launcher[/b]\n"
                "Enter a Hugging Face model ID to download it, or choose an existing "
                "local model. Every launch goes through the GPU gatekeeper.",
                id="llm_launcher_intro",
            )

            yield Static("[b]LLM Services[/b]", classes="llm_section_title")
            yield ServiceTable(id="llm_svc_table")

            yield Static(
                "[b]MODEL ACQUISITION CARD[/b]",
                id="llm_acquisition_card_title",
                classes="llm_card_title",
            )
            with Horizontal(classes="llm_launcher_row"):
                yield Label("HF model", classes="llm_launcher_label")
                yield Input(
                    placeholder="openai/gpt-oss-20b",
                    id="llm_hf_repo_input",
                )
            with Horizontal(classes="llm_launcher_row"):
                yield Label("HF filename", classes="llm_launcher_label")
                yield Input(
                    placeholder="Optional; required for GGUF repositories",
                    id="llm_hf_filename_input",
                )
                yield Button(
                    "Download with HF",
                    id="llm_hf_download_btn",
                    variant="primary",
                )
            yield Static(
                f"Destination: {LLAMA_MODELS_DIR}/organization__model-name",
                id="llm_hf_destination",
            )

            yield Static(
                "[b]LLM RUN CONTROL CARD[/b]",
                id="llm_run_card_title",
                classes="llm_card_title",
            )
            with Horizontal(classes="llm_launcher_row"):
                yield Label("Backend", classes="llm_launcher_label")
                yield Select(
                    [
                        (spec["label"], key)
                        for key, spec in self.BACKENDS.items()
                    ],
                    value="vllm",
                    id="llm_backend_select",
                )
                yield Button("Refresh models", id="llm_models_refresh_btn")

            with Horizontal(classes="llm_launcher_row"):
                yield Label("Downloaded", classes="llm_launcher_label")
                yield Select(
                    [("No compatible local models found", "__none__")],
                    value="__none__",
                    id="llm_local_model_select",
                )

            with Horizontal(classes="llm_launcher_row"):
                yield Label("Model path", classes="llm_launcher_label")
                yield Input(
                    placeholder="Download a model or enter its local path",
                    id="llm_launch_model_input",
                )

            yield Static("", id="llm_model_details")

            with Horizontal(classes="llm_launcher_row"):
                yield Label("Host", classes="llm_launcher_label")
                yield Input(
                    value=current_vllm_settings.host,
                    id="llm_launch_host_input",
                    classes="llm_launcher_medium",
                )
                yield Label("Port", classes="llm_launcher_inline_label")
                yield Input(
                    value=str(current_vllm_settings.port),
                    id="llm_launch_port_input",
                    classes="llm_launcher_short",
                )

            with Horizontal(classes="llm_launcher_row"):
                yield Label("GPU utilization", classes="llm_launcher_label")
                yield Input(
                    value=f"{current_vllm_settings.gpu_memory_utilization:.4g}",
                    id="llm_launch_gpu_util_input",
                    classes="llm_launcher_short",
                )
                yield Label("Max model len", classes="llm_launcher_inline_label")
                yield Input(
                    value=str(current_vllm_settings.max_model_len),
                    id="llm_launch_max_len_input",
                    classes="llm_launcher_short",
                )

            with Horizontal(classes="llm_launcher_row"):
                yield Label("Extra args", classes="llm_launcher_label")
                yield Input(
                    value=current_vllm_settings.extra_args,
                    placeholder="Optional backend arguments",
                    id="llm_launch_extra_args_input",
                )

            yield Static("", id="llm_command_preview")

            with Horizontal(classes="llm_launcher_row"):
                yield Button(
                    "Check GPU capacity",
                    id="llm_capacity_btn",
                    variant="primary",
                )
                yield Button(
                    "Launch safely",
                    id="llm_safe_launch_btn",
                    variant="success",
                )
                yield Button(
                    "Stop selected backend",
                    id="llm_safe_stop_btn",
                    variant="error",
                )

        with Vertical(id="llm_launcher_info"):
            yield Static(
                "Capacity has not been checked yet.",
                id="llm_capacity_result",
            )
            yield Label("", id="llm_launcher_status")

    def on_mount(self) -> None:
        os.makedirs(LLAMA_MODELS_DIR, exist_ok=True)
        self.refresh_models()
        self._refresh_hf_destination()

    def _backend(self) -> str:
        value = self.query_one("#llm_backend_select", Select).value
        return str(value) if value in self.BACKENDS else "llama"

    def _model_path(self) -> str:
        return os.path.expanduser(
            self.query_one("#llm_launch_model_input", Input).value.strip()
        )

    def _hf_repo(self) -> str:
        return self.query_one("#llm_hf_repo_input", Input).value.strip()

    def _run_settings(self) -> Tuple[Optional[VllmRunSettings], str]:
        host = self.query_one("#llm_launch_host_input", Input).value.strip()
        port_raw = self.query_one("#llm_launch_port_input", Input).value.strip()
        util_raw = self.query_one(
            "#llm_launch_gpu_util_input",
            Input,
        ).value.strip()
        max_len_raw = self.query_one(
            "#llm_launch_max_len_input",
            Input,
        ).value.strip()
        extra = self.query_one(
            "#llm_launch_extra_args_input",
            Input,
        ).value.strip()

        if not host or any(char.isspace() for char in host):
            return None, "Host must be a hostname or bind address without spaces."
        try:
            port = int(port_raw)
        except ValueError:
            return None, "Port must be a whole number."
        if not 1024 <= port <= 65535:
            return None, "Port must be between 1024 and 65535."
        try:
            utilization = float(util_raw)
        except ValueError:
            return None, "GPU utilization must be a decimal number."
        if not 0.05 <= utilization <= 0.95:
            return None, "GPU utilization must be between 0.05 and 0.95."
        try:
            max_model_len = int(max_len_raw)
        except ValueError:
            return None, "Max model length must be a whole number."
        if not 512 <= max_model_len <= 262_144:
            return None, "Max model length must be between 512 and 262144."
        extra_error = validate_extra_args(extra, self._backend())
        if extra_error:
            return None, extra_error

        return VllmRunSettings(
            host=host,
            port=port,
            gpu_memory_utilization=utilization,
            max_model_len=max_model_len,
            extra_args=extra,
        ), ""

    def refresh_models(self, preferred_path: str = "") -> None:
        backend = self._backend()
        models = [
            model
            for model in discover_local_llm_models(LLAMA_MODELS_DIR)
            if model.backend == backend
        ]
        select = self.query_one("#llm_local_model_select", Select)
        current_path = preferred_path or self._model_path()
        available_paths = {model.path for model in models}

        if current_path not in available_paths:
            current_path = models[0].path if models else ""

        options = (
            [(model.display_name, model.path) for model in models]
            if models
            else [("No compatible local models found", "__none__")]
        )
        self._refreshing_models = True
        try:
            select.set_options(options)
            select.value = current_path or "__none__"
            self.query_one("#llm_launch_model_input", Input).value = current_path
        finally:
            self._refreshing_models = False
        self._refresh_model_details()
        self._refresh_command_preview()

    def select_model(self, model_path: str) -> None:
        model_path = os.path.expanduser(model_path)
        backend = "llama" if model_path.lower().endswith(".gguf") else "vllm"
        self.query_one("#llm_backend_select", Select).value = backend
        self.refresh_models(preferred_path=model_path)

    def _refresh_hf_destination(self) -> None:
        repo = self._hf_repo()
        destination = (
            recommended_download_dir(LLAMA_MODELS_DIR, repo)
            if repo
            else f"{LLAMA_MODELS_DIR}/organization__model-name"
        )
        self.query_one("#llm_hf_destination", Static).update(
            f"Destination: {escape(destination)}"
        )

    def _refresh_command_preview(self) -> None:
        settings, error = self._run_settings()
        model = self._model_path() or self._hf_repo() or "<model>"
        if error or settings is None:
            preview = f"Control card error: {error}"
        elif self._backend() == "vllm":
            try:
                preview = build_vllm_command(
                    VLLM_BINARY,
                    model,
                    settings,
                )
            except ValueError as exc:
                preview = f"Control card error: {exc}"
        else:
            state = _load_llama_state()
            preview = build_llama_command(
                model=model,
                ngl=state["ngl"],
                ctx=str(settings.max_model_len),
                host=settings.host,
                port=str(settings.port),
                extra=settings.extra_args,
            )
        self.query_one("#llm_command_preview", Static).update(
            f"[b]Command preview[/b]\n{escape(preview)}"
        )

    def _refresh_model_details(self) -> None:
        backend = self._backend()
        model_path = self._model_path()
        settings, _error = self._run_settings()
        max_model_len = settings.max_model_len if settings else 32_768
        size = model_size_bytes(model_path, backend) if model_path else 0
        estimate = (
            estimate_model_vram_mb(
                model_path,
                backend,
                context_size=max_model_len,
            )
            if model_path
            else 0
        )
        if not model_path:
            message = "No compatible downloaded model is selected."
        else:
            message = (
                f"Selected: {escape(model_path)}\n"
                f"Downloaded weights: {format_bytes(size)} · "
                f"conservative VRAM estimate: {estimate} MB"
            )
        self.query_one("#llm_model_details", Static).update(message)
        self.query_one("#llm_capacity_result", Static).update(
            "Capacity must be checked for this selection."
        )
        self._refresh_command_preview()

    def _set_status(self, message: str, color: str = "green") -> None:
        timestamp = time.strftime("%H:%M:%S")
        formatted = f"({timestamp}) [{color}]{escape(message)}[/{color}]"
        self.query_one("#llm_launcher_status", Label).update(formatted)
        try:
            self.app.query_one("#llm_global_info", Static).update(
                f"[b]INFO[/b] {formatted}"
            )
        except Exception:
            pass

    def _set_capacity(self, message: str, allowed: bool) -> None:
        color = "green" if allowed else "red"
        self.query_one("#llm_capacity_result", Static).update(
            f"[{color}]{escape(message)}[/{color}]"
        )

    def _start_hf_download(self) -> None:
        repo = self._hf_repo()
        filename = self.query_one(
            "#llm_hf_filename_input",
            Input,
        ).value.strip()
        if (
            repo.count("/") != 1
            or any(char.isspace() for char in repo)
            or repo.startswith(("-", "/"))
            or repo.endswith("/")
        ):
            self._set_status(
                "Enter a Hugging Face model ID such as openai/gpt-oss-20b.",
                "red",
            )
            return
        if filename.startswith("-") or ".." in filename.split("/"):
            self._set_status("The Hugging Face filename is invalid.", "red")
            return
        if "gguf" in repo.lower() and not filename:
            self._set_status(
                "Choose one GGUF filename to avoid downloading every quantization.",
                "red",
            )
            return

        try:
            downloads = self.app.query_one("#models_panel", ModelMaintenancePanel)
            if downloads._dl_active:
                self._set_status("A Hugging Face download is already running.", "yellow")
                return
            destination = recommended_download_dir(LLAMA_MODELS_DIR, repo)
            backend = (
                "llama"
                if filename.lower().endswith(".gguf")
                or "gguf" in repo.lower()
                else "vllm"
            )
            self.query_one("#llm_backend_select", Select).value = backend
            downloads.query_one("#mm_repo_input", Input).value = repo
            downloads.query_one("#mm_filename_input", Input).value = filename
            downloads.query_one("#mm_localdir_input", Input).value = LLAMA_MODELS_DIR
            self.app.query_one("#llm_workspace", TabbedContent).active = (
                "llm_download_subtab"
            )
            downloads._start_download(repo, filename, destination)
            self._set_status(
                f"Downloading {repo} into {destination}.",
                "yellow",
            )
        except Exception as exc:
            self._set_status(f"Could not start Hugging Face download: {exc}", "red")

    def _request_plan(
        self,
        backend: str,
        model_path: str,
        settings: VllmRunSettings,
    ) -> Tuple[Dict[str, object], object]:
        gk_path = "/home/ned/Documents/GPU_agent_gatekeeper"
        if gk_path not in sys.path:
            sys.path.append(gk_path)
        from gpu_gatekeeper.client import GpuGatekeeperClient

        spec = self.BACKENDS[backend]
        estimate = estimate_model_vram_mb(
            model_path,
            backend,
            context_size=settings.max_model_len,
        )
        client = GpuGatekeeperClient()
        status = client.status()
        total_vram_mb = int(
            float(status.get("gpu", {}).get("total_vram_mb", 0) or 0)
        )
        requested_runtime_vram_mb = 0
        if backend == "vllm":
            requested_runtime_vram_mb = math.ceil(
                total_vram_mb * settings.gpu_memory_utilization
            )
        dynamic_requirement = max(
            int(spec["registry_floor_mb"]),
            estimate,
            requested_runtime_vram_mb,
        )
        registry_ok, registry_message = configure_gatekeeper_llm_runtime(
            str(spec["runtime_key"]),
            settings.port,
            dynamic_requirement,
        )
        if not registry_ok:
            raise RuntimeError(registry_message)

        plan = client.plan_runtime(
            capability="chat",
            owner="scanner-dashboard",
            runtime_key=spec["runtime_key"],
            ttl_seconds=21_600,
            allow_start=True,
            force=False,
            metadata={
                "model_path": model_path,
                "selected_model_vram_mb": estimate,
                "host": settings.host,
                "port": settings.port,
                "gpu_memory_utilization": settings.gpu_memory_utilization,
                "max_model_len": settings.max_model_len,
            },
        )
        assessment = assess_gatekeeper_plan(
            plan,
            estimate,
            requested_runtime_vram_mb=requested_runtime_vram_mb,
        )
        return {
            "client": client,
            "plan": plan,
            "estimate": estimate,
            "requested_runtime_vram_mb": requested_runtime_vram_mb,
        }, assessment

    def _validate_selection(
        self,
    ) -> Tuple[str, str, Optional[VllmRunSettings], str]:
        backend = self._backend()
        model_path = self._model_path()
        error = validate_model_path(
            model_path,
            backend,
            model_root=LLAMA_MODELS_DIR,
        )
        settings, settings_error = self._run_settings()
        if settings_error:
            error = settings_error
        return backend, model_path, settings, error

    def _run_capacity_check(self) -> None:
        backend, model_path, settings, error = self._validate_selection()
        if error or settings is None:
            self._set_status(error, "red")
            self._set_capacity(error, False)
            return
        if self._busy:
            self._set_status("Another LLM operation is already running.", "yellow")
            return

        self._busy = True
        self._set_status("Asking GPU gatekeeper for a launch plan…", "yellow")

        def worker() -> None:
            try:
                _context, assessment = self._request_plan(
                    backend,
                    model_path,
                    settings,
                )
                self.app.call_from_thread(
                    self._set_capacity,
                    assessment.explanation,
                    assessment.allowed,
                )
                self.app.call_from_thread(
                    self._set_status,
                    "Capacity check passed." if assessment.allowed else "Launch denied.",
                    "green" if assessment.allowed else "red",
                )
            except Exception as exc:
                self.app.call_from_thread(
                    self._set_capacity,
                    f"Gatekeeper check failed: {exc}",
                    False,
                )
                self.app.call_from_thread(
                    self._set_status,
                    f"Gatekeeper check failed: {exc}",
                    "red",
                )
            finally:
                self._busy = False

        threading.Thread(target=worker, daemon=True).start()

    def _launch(self) -> None:
        backend, model_path, settings, error = self._validate_selection()
        if error or settings is None:
            self._set_status(error, "red")
            self._set_capacity(error, False)
            return
        if self._busy:
            self._set_status("Another LLM operation is already running.", "yellow")
            return

        selected_unit = self.BACKENDS[backend]["unit"]
        other_units = {
            spec["unit"] for key, spec in self.BACKENDS.items() if key != backend
        }
        for unit in {selected_unit, *other_units}:
            if systemctl_show(unit).get("ActiveState") == "active":
                self._set_status(
                    f"{unit} is already running. Stop it before changing models.",
                    "red",
                )
                return

        self._busy = True
        self._set_status("Checking capacity before launch…", "yellow")

        def worker() -> None:
            try:
                context_data, assessment = self._request_plan(
                    backend,
                    model_path,
                    settings,
                )
                self.app.call_from_thread(
                    self._set_capacity,
                    assessment.explanation,
                    assessment.allowed,
                )
                if not assessment.allowed:
                    self.app.call_from_thread(self._set_status, "Launch denied.", "red")
                    return

                # Build the override from the state captured above.
                if backend == "llama":
                    state = _load_llama_state()
                    ok, message = write_llama_exec_override(
                        model=model_path,
                        ngl=state["ngl"],
                        ctx=str(settings.max_model_len),
                        host=settings.host,
                        port=str(settings.port),
                        extra=settings.extra_args,
                    )
                else:
                    command = build_vllm_command(
                        VLLM_BINARY,
                        model_path,
                        settings,
                    )
                    ok, message = write_vllm_exec_override(command)
                if not ok:
                    self.app.call_from_thread(self._set_status, message, "red")
                    return

                client = context_data["client"]
                spec = self.BACKENDS[backend]
                # Re-check immediately before the mutating request. This closes
                # the window for another cooperative GPU workload to consume a
                # lease after the first plan was calculated.
                _latest_context, latest_assessment = self._request_plan(
                    backend,
                    model_path,
                    settings,
                )
                if not latest_assessment.allowed:
                    self.app.call_from_thread(
                        self._set_capacity,
                        latest_assessment.explanation,
                        False,
                    )
                    self.app.call_from_thread(
                        self._set_status,
                        "GPU capacity changed; launch was cancelled.",
                        "red",
                    )
                    return
                self.app.call_from_thread(
                    self._set_status,
                    "Gatekeeper approved; starting the model service…",
                    "yellow",
                )
                response = client.ensure_runtime(
                    capability="chat",
                    owner="scanner-dashboard",
                    runtime_key=spec["runtime_key"],
                    ttl_seconds=21_600,
                    allow_start=True,
                    force=False,
                    metadata={
                        "model_path": model_path,
                        "selected_model_vram_mb": assessment.required_vram_mb,
                        "host": settings.host,
                        "port": settings.port,
                        "gpu_memory_utilization": settings.gpu_memory_utilization,
                        "max_model_len": settings.max_model_len,
                    },
                )
                if not response.get("ok"):
                    self.app.call_from_thread(
                        self._set_status,
                        str(response.get("message") or "Gatekeeper launch failed."),
                        "red",
                    )
                    return
                lease = response.get("lease") or {}
                lease_id = lease.get("lease_id", "unknown")
                self.app.call_from_thread(
                    self._set_status,
                    f"Model started safely. Gatekeeper lease: {lease_id}",
                    "green",
                )
                self.app.call_from_thread(self.app.refresh_all)
            except Exception as exc:
                self.app.call_from_thread(
                    self._set_status,
                    f"Launch failed: {exc}",
                    "red",
                )
            finally:
                self._busy = False

        threading.Thread(target=worker, daemon=True).start()

    def _stop(self) -> None:
        backend = self._backend()
        if self._busy:
            self._set_status("Another LLM operation is already running.", "yellow")
            return
        self._busy = True
        self._set_status("Releasing the selected runtime through gatekeeper…", "yellow")

        def worker() -> None:
            try:
                gk_path = "/home/ned/Documents/GPU_agent_gatekeeper"
                if gk_path not in sys.path:
                    sys.path.append(gk_path)
                from gpu_gatekeeper.client import GpuGatekeeperClient

                client = GpuGatekeeperClient()
                runtime_key = self.BACKENDS[backend]["runtime_key"]
                active = client.status().get("leases", {}).get("leases", [])
                ours = [
                    lease
                    for lease in active
                    if lease.get("runtime_key") == runtime_key
                    and lease.get("owner") == "scanner-dashboard"
                    and lease.get("status") in ("active", "pending")
                ]

                if not ours:
                    unit = self.BACKENDS[backend]["unit"]
                    if systemctl_show(unit).get("ActiveState") != "active":
                        self.app.call_from_thread(
                            self._set_status,
                            f"{unit} is already stopped.",
                            "yellow",
                        )
                        return
                    acquired = client.ensure_runtime(
                        capability="chat",
                        owner="scanner-dashboard",
                        runtime_key=runtime_key,
                        ttl_seconds=300,
                        allow_start=False,
                        force=False,
                        metadata={"purpose": "safe-dashboard-stop"},
                    )
                    if acquired.get("ok") and acquired.get("lease"):
                        ours = [acquired["lease"]]

                if not ours:
                    self.app.call_from_thread(
                        self._set_status,
                        "Gatekeeper could not obtain a releasable runtime lease.",
                        "red",
                    )
                    return

                messages = []
                for lease in ours:
                    result = client.release_runtime(str(lease["lease_id"]))
                    messages.append(str(result.get("stop_reason", "")))
                self.app.call_from_thread(
                    self._set_status,
                    "Lease released. " + " ".join(filter(None, messages)),
                    "green",
                )
                self.app.call_from_thread(self.app.refresh_all)
            except Exception as exc:
                self.app.call_from_thread(
                    self._set_status,
                    f"Stop failed: {exc}",
                    "red",
                )
            finally:
                self._busy = False

        threading.Thread(target=worker, daemon=True).start()

    def on_select_changed(self, event: Select.Changed) -> None:
        if self._refreshing_models:
            return
        if event.select.id == "llm_backend_select":
            self.refresh_models()
        elif event.select.id == "llm_local_model_select":
            value = event.select.value
            if value and value != "__none__":
                self.query_one("#llm_launch_model_input", Input).value = str(value)
                self._refresh_model_details()

    def on_input_changed(self, event: Input.Changed) -> None:
        if event.input.id == "llm_hf_repo_input":
            self._refresh_hf_destination()
            self._refresh_command_preview()
        elif event.input.id in (
            "llm_launch_model_input",
            "llm_launch_host_input",
            "llm_launch_port_input",
            "llm_launch_gpu_util_input",
            "llm_launch_max_len_input",
            "llm_launch_extra_args_input",
        ) and not self._refreshing_models:
            self._refresh_model_details()

    def on_button_pressed(self, event: Button.Pressed) -> None:
        if event.button.id == "llm_models_refresh_btn":
            self.refresh_models()
            self._set_status("Downloaded model list refreshed.", "yellow")
        elif event.button.id == "llm_hf_download_btn":
            self._start_hf_download()
        elif event.button.id == "llm_capacity_btn":
            self._run_capacity_check()
        elif event.button.id == "llm_safe_launch_btn":
            self._launch()
        elif event.button.id == "llm_safe_stop_btn":
            self._stop()


class ServiceTable(DataTable):
    def on_mount(self) -> None:
        self.add_columns("Service", "Active", "Sub", "PID", "Since")
        self.cursor_type = "row"
        self.zebra_stripes = True


class VllmServiceConfigPanel(Vertical):
    def compose(self) -> ComposeResult:
        command = get_current_vllm_exec_command()
        current_model = get_vllm_model_from_command(command) or VLLM_DEFAULT_MODEL
        models = get_available_vllm_models()
        if current_model not in models:
            models.append(current_model)

        with Horizontal(id="vllm_config_header"):
            yield Label("vLLM Service Start Command", id="vllm_config_title")
            yield Button("Reload Service", id="reload_vllm_exec_btn")

        with Horizontal(classes="vllm_row"):
            yield Label("Known", classes="vllm_lbl")
            yield Select(
                [(m, m) for m in models],
                value=current_model,
                id="vllm_model_select",
            )

        with Horizontal(classes="vllm_row"):
            yield Label("Model", classes="vllm_lbl")
            yield Input(value=current_model, id="vllm_model_input")

        with Horizontal(classes="vllm_row"):
            yield Label("ExecStart", classes="vllm_lbl")
            yield Input(value=command, id="vllm_exec_input")

        with Horizontal(id="vllm_apply_row"):
            yield Button(
                "Apply (launch from the safe Launch tab)",
                id="apply_vllm_exec_btn",
                variant="primary",
            )
            yield Label("", id="vllm_config_status")

    def refresh_vllm_ui(self) -> None:
        command = get_current_vllm_exec_command()
        current_model = get_vllm_model_from_command(command) or VLLM_DEFAULT_MODEL
        models = get_available_vllm_models()
        if current_model not in models:
            models.append(current_model)

        sel = self.query_one("#vllm_model_select", Select)
        if hasattr(sel, "set_options"):
            sel.set_options([(m, m) for m in models])
        sel.value = current_model

        self.query_one("#vllm_model_input", Input).value = current_model
        self.query_one("#vllm_exec_input", Input).value = command

    def _set_config_status(self, msg: str, color: str = "green") -> None:
        status = self.query_one("#vllm_config_status", Label)
        status.update(
            f"({time.strftime('%H:%M:%S')}) "
            f"[{color}]{escape(msg)}[/{color}]"
        )

    def _selected_model(self) -> str:
        model = self.query_one("#vllm_model_input", Input).value.strip()
        return model or VLLM_DEFAULT_MODEL

    def _apply(self) -> None:
        model = self._selected_model()
        error = validate_model_path(
            model,
            "vllm",
            model_root=LLAMA_MODELS_DIR,
        )
        if error:
            self._set_config_status(error, "red")
            return
        command_input = self.query_one("#vllm_exec_input", Input)
        command = replace_vllm_model_in_command(command_input.value, model)
        command_input.value = command
        ok, msg = write_vllm_exec_override(command)
        if not ok:
            self._set_config_status(msg, "red")
            return

        self._set_config_status(msg)

    def on_select_changed(self, event: Select.Changed) -> None:
        if event.select.id != "vllm_model_select":
            return
        val = getattr(event.select, "value", None)
        blank_val = getattr(Select, "BLANK", object())
        if val and val != blank_val:
            self.query_one("#vllm_model_input", Input).value = str(val)
        command_input = self.query_one("#vllm_exec_input", Input)
        command_input.value = replace_vllm_model_in_command(
            command_input.value,
            self._selected_model(),
        )

    def on_input_changed(self, event: Input.Changed) -> None:
        if event.input.id != "vllm_model_input":
            return
        model = event.value.strip()
        if not model:
            return
        command_input = self.query_one("#vllm_exec_input", Input)
        command_input.value = replace_vllm_model_in_command(command_input.value, model)

    def on_button_pressed(self, event: Button.Pressed) -> None:
        if event.button.id == "reload_vllm_exec_btn":
            self.refresh_vllm_ui()
            self._set_config_status("Reloaded vLLM service command.", "yellow")
        elif event.button.id == "apply_vllm_exec_btn":
            self._apply()


class LlamaConfigPanel(VerticalScroll):
    _current_dir: str = LLAMA_MODELS_DIR

    def compose(self) -> ComposeResult:
        state = _load_llama_state()
        model = state["model"]

        with Horizontal(id="llama_config_header"):
            yield Label("llama-server Configuration", id="llama_config_title")
            yield Button("Reload", id="llama_reload_btn")

        yield Static("[b]Browse & Select Model[/b]", id="llama_browser_title")
        with Horizontal(id="llama_browser_nav"):
            yield Button("\u2b06 Up", id="llama_up_btn")
            yield Button("Root", id="llama_root_btn")
            yield Static("", id="llama_browser_path")
        with Horizontal(id="llama_browser"):
            with Vertical(id="llama_dirs_pane"):
                yield Static("[b]Folders[/b]", id="llama_dirs_label")
                with VerticalScroll(id="llama_dirs_scroll"):
                    yield DataTable(id="llama_dirs_table")
            with Vertical(id="llama_files_pane"):
                yield Static("[b]GGUF Files[/b]", id="llama_files_label")
                with VerticalScroll(id="llama_files_scroll"):
                    yield DataTable(id="llama_models_file_table")

        with Horizontal(classes="llama_row"):
            yield Label("Model Path", classes="llama_lbl")
            yield Input(value=model, id="llama_model_input")

        with Horizontal(classes="llama_row"):
            yield Label("GPU Layers", classes="llama_lbl")
            yield Input(value=state["ngl"], id="llama_ngl_input", classes="llama_short")
            yield Label("Context Size", classes="llama_lbl")
            yield Input(value=state["ctx"], id="llama_ctx_input", classes="llama_short")

        with Horizontal(classes="llama_row"):
            yield Label("Host", classes="llama_lbl")
            yield Input(value=state["host"], id="llama_host_input", classes="llama_short")
            yield Label("Port", classes="llama_lbl")
            yield Input(value=state["port"], id="llama_port_input", classes="llama_short")

        with Horizontal(classes="llama_row"):
            yield Label("Extra Args", classes="llama_lbl")
            yield Input(value=state["extra"], id="llama_extra_input")

        with Horizontal(classes="llama_row"):
            yield Label("Command Preview", classes="llama_lbl")
            yield Static(
                build_llama_command(model, state["ngl"], state["ctx"], state["host"], state["port"], state["extra"]),
                id="llama_cmd_preview",
            )

        with Horizontal(id="llama_apply_row"):
            yield Button(
                "Apply (launch from the safe Launch tab)",
                id="llama_apply_btn",
                variant="primary",
            )
            yield Label("", id="llama_config_status")

    def on_mount(self) -> None:
        os.makedirs(LLAMA_MODELS_DIR, exist_ok=True)
        self._current_dir = LLAMA_MODELS_DIR

        dirs_table = self.query_one("#llama_dirs_table", DataTable)
        dirs_table.add_column("Subdirectory", key="name")
        dirs_table.cursor_type = "row"
        dirs_table.zebra_stripes = True

        files_table = self.query_one("#llama_models_file_table", DataTable)
        files_table.add_column("Model File", key="name")
        files_table.add_column("Size", key="size")
        files_table.cursor_type = "row"
        files_table.zebra_stripes = True

        self._refresh_llama_browser()

    def _refresh_llama_browser(self) -> None:
        cur = self._current_dir
        self.query_one("#llama_browser_path", Static).update(f"[dim]{cur}[/dim]")

        dirs_table = self.query_one("#llama_dirs_table", DataTable)
        dirs_table.clear()
        try:
            for entry in sorted(os.listdir(cur)):
                full = os.path.join(cur, entry)
                if os.path.isdir(full):
                    dirs_table.add_row(entry, key=full)
        except Exception:
            pass

        files_table = self.query_one("#llama_models_file_table", DataTable)
        files_table.clear()
        try:
            for entry in sorted(os.listdir(cur)):
                full = os.path.join(cur, entry)
                if os.path.isfile(full) and entry.endswith(".gguf"):
                    try:
                        size_bytes = os.path.getsize(full)
                        size_str = (
                            f"{size_bytes / 1024**3:.1f} GB"
                            if size_bytes >= 1024**3
                            else f"{size_bytes / 1024**2:.1f} MB"
                            if size_bytes >= 1024**2
                            else f"{size_bytes / 1024:.0f} KB"
                        )
                    except Exception:
                        size_str = "?"
                    files_table.add_row(entry, size_str, key=full)
        except Exception:
            pass

    def on_data_table_row_selected(self, event: DataTable.RowSelected) -> None:
        key = event.row_key
        if hasattr(key, "value"):
            key = key.value
        key = str(key)
        if event.data_table.id == "llama_dirs_table":
            if os.path.isdir(key):
                self._current_dir = key
                self._refresh_llama_browser()
                event.stop()
        elif event.data_table.id == "llama_models_file_table":
            if os.path.isfile(key):
                self.query_one("#llama_model_input", Input).value = key
                self._refresh_preview()
                event.stop()

    def _state_from_inputs(self) -> Dict[str, str]:
        return {
            "model": self.query_one("#llama_model_input", Input).value.strip() or LLAMA_DEFAULT_MODEL,
            "ngl": self.query_one("#llama_ngl_input", Input).value.strip() or LLAMA_DEFAULT_NGL,
            "ctx": self.query_one("#llama_ctx_input", Input).value.strip() or LLAMA_DEFAULT_CTX,
            "host": self.query_one("#llama_host_input", Input).value.strip() or LLAMA_DEFAULT_HOST,
            "port": self.query_one("#llama_port_input", Input).value.strip() or LLAMA_DEFAULT_PORT,
            "extra": self.query_one("#llama_extra_input", Input).value.strip(),
        }

    def _refresh_preview(self) -> None:
        s = self._state_from_inputs()
        cmd = build_llama_command(s["model"], s["ngl"], s["ctx"], s["host"], s["port"], s["extra"])
        self.query_one("#llama_cmd_preview", Static).update(cmd)

    def _set_status(self, msg: str, color: str = "green") -> None:
        self.query_one("#llama_config_status", Label).update(
            f"({time.strftime('%H:%M:%S')}) "
            f"[{color}]{escape(msg)}[/{color}]"
        )

    def _apply(self) -> None:
        s = self._state_from_inputs()
        error = validate_model_path(
            s["model"],
            "llama",
            model_root=LLAMA_MODELS_DIR,
        )
        if error:
            self._set_status(error, "red")
            return
        ok, msg = write_llama_exec_override(**s)
        if not ok:
            self._set_status(msg, "red")
            return
        self._set_status(msg)

    def _reload_ui(self) -> None:
        state = _load_llama_state()
        self.query_one("#llama_model_input", Input).value = state["model"]
        self.query_one("#llama_ngl_input", Input).value = state["ngl"]
        self.query_one("#llama_ctx_input", Input).value = state["ctx"]
        self.query_one("#llama_host_input", Input).value = state["host"]
        self.query_one("#llama_port_input", Input).value = state["port"]
        self.query_one("#llama_extra_input", Input).value = state["extra"]
        self._refresh_preview()
        self._refresh_llama_browser()

    def on_input_changed(self, event: Input.Changed) -> None:
        if event.input.id in (
            "llama_model_input", "llama_ngl_input", "llama_ctx_input",
            "llama_host_input", "llama_port_input", "llama_extra_input",
        ):
            self._refresh_preview()

    def on_button_pressed(self, event: Button.Pressed) -> None:
        if event.button.id == "llama_reload_btn":
            self._reload_ui()
            self._set_status("Reloaded.", "yellow")
        elif event.button.id == "llama_up_btn":
            parent = os.path.dirname(self._current_dir)
            if parent and os.path.isdir(parent) and parent != self._current_dir:
                self._current_dir = parent
                self._refresh_llama_browser()
        elif event.button.id == "llama_root_btn":
            self._current_dir = LLAMA_MODELS_DIR
            self._refresh_llama_browser()
        elif event.button.id == "llama_apply_btn":
            self._apply()


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
                yield Checkbox(m, value=(m in sec_models), name=m, classes="sec_cb")
                
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
            sec_list.mount(Checkbox(m, value=(m in sec_models), name=m, classes="sec_cb"))

    def on_button_pressed(self, event: Button.Pressed) -> None:
        if event.button.id == "save_config_btn":
            sel = self.query_one("#default_model_select", Select)
            val = getattr(sel, "value", None)
            blank_val = getattr(Select, "BLANK", object())
            default_model = str(val) if val and val != blank_val else ""
            
            default_enabled = self.query_one("#enable_default_cb", Checkbox).value
            sec_enabled = self.query_one("#enable_secondary_cb", Checkbox).value
            
            sec_models = [
                str(cb.name or cb.id[7:])
                for cb in self.query(Checkbox).filter(".sec_cb")
                if cb.value
            ]
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


class ModelMaintenancePanel(Vertical):
    _dl_proc: Optional[subprocess.Popen] = None
    _dl_cancelled = False
    _dl_active = False
    _dl_repo = ""
    _dl_local_dir = ""
    _dl_initial_size = 0
    _dl_started_at = 0.0
    _dl_last_size = 0
    _dl_last_sample = 0.0

    def compose(self) -> ComposeResult:
        with VerticalScroll(id="mm_content_scroll"):
            with Horizontal(id="mm_header"):
                yield Static("[b]Models[/b]", id="mm_title")
                yield Button("\u2b06 Up", id="mm_up_btn")
                yield Button("Root", id="mm_root_btn")
                yield Button("Refresh", id="mm_refresh_btn")
            yield Static("", id="mm_current_path")
            with Horizontal(id="mm_browser"):
                with Vertical(id="mm_dirs_pane"):
                    yield Static("[b]Folders[/b]", id="mm_dirs_label")
                    with VerticalScroll(id="mm_dirs_scroll"):
                        yield DataTable(id="mm_dirs_table")
                with Vertical(id="mm_files_pane"):
                    yield Static("[b]Files[/b]", id="mm_files_label")
                    with VerticalScroll(id="mm_files_scroll"):
                        yield DataTable(id="mm_model_table")
            yield Static("[b]Download Model from Hugging Face[/b]", id="mm_dl_title")
            yield Static(
                "Downloads do not use the GPU. For GGUF repositories, enter one "
                "specific filename so every quantization is not downloaded.",
                id="mm_dl_help",
            )
            with Horizontal(classes="mm_row"):
                yield Label("HF Repo", classes="mm_lbl")
                yield Input(placeholder="org/repo-name", id="mm_repo_input")
            with Horizontal(classes="mm_row"):
                yield Label("Filename", classes="mm_lbl")
                yield Input(
                    placeholder="model.gguf  (leave blank for full repo)",
                    id="mm_filename_input",
                )
            with Horizontal(classes="mm_row"):
                yield Label("Model root", classes="mm_lbl")
                yield Input(
                    value=LLAMA_MODELS_DIR,
                    id="mm_localdir_input",
                    disabled=True,
                )
        with Vertical(id="mm_download_info"):
            with Horizontal(id="mm_dl_row"):
                yield Button("Download", id="mm_download_btn", variant="primary")
                yield Button("Cancel", id="mm_cancel_btn", variant="error")
                yield Label("", id="mm_dl_status")
            with Vertical(id="mm_dl_output_scroll"):
                yield Log(id="mm_dl_log", highlight=False)

    def on_mount(self) -> None:
        os.makedirs(LLAMA_MODELS_DIR, exist_ok=True)
        self._current_dir = LLAMA_MODELS_DIR

        dirs_table = self.query_one("#mm_dirs_table", DataTable)
        dirs_table.add_column("Subdirectory", key="name")
        dirs_table.cursor_type = "row"
        dirs_table.zebra_stripes = True

        files_table = self.query_one("#mm_model_table", DataTable)
        files_table.add_columns("Name", "Size", "Type")
        files_table.cursor_type = "row"
        files_table.zebra_stripes = True

        self._refresh_browser()
        self.set_interval(1.0, self._refresh_download_status)

    def _refresh_browser(self) -> None:
        cur = self._current_dir
        self.query_one("#mm_current_path", Static).update(f"[dim]{cur}[/dim]")
        self.query_one("#mm_localdir_input", Input).value = LLAMA_MODELS_DIR

        dirs_table = self.query_one("#mm_dirs_table", DataTable)
        dirs_table.clear()
        try:
            for entry in sorted(os.listdir(cur)):
                full = os.path.join(cur, entry)
                if os.path.isdir(full):
                    dirs_table.add_row(entry, key=full)
        except Exception:
            pass

        files_table = self.query_one("#mm_model_table", DataTable)
        files_table.clear()
        try:
            for entry in sorted(os.listdir(cur)):
                full = os.path.join(cur, entry)
                if os.path.isfile(full):
                    try:
                        size_bytes = os.path.getsize(full)
                        size_str = (
                            f"{size_bytes / 1024**3:.1f} GB"
                            if size_bytes >= 1024**3
                            else f"{size_bytes / 1024**2:.1f} MB"
                            if size_bytes >= 1024**2
                            else f"{size_bytes / 1024:.0f} KB"
                        )
                    except Exception:
                        size_str = "?"
                    ext = os.path.splitext(entry)[1].lower() or "(none)"
                    files_table.add_row(entry, size_str, ext, key=full)
        except Exception:
            pass

    def on_data_table_row_selected(self, event: DataTable.RowSelected) -> None:
        key = event.row_key
        if hasattr(key, "value"):
            key = key.value
        if event.data_table.id == "mm_dirs_table" and key and os.path.isdir(str(key)):
            self._current_dir = str(key)
            self._refresh_browser()
        elif event.data_table.id == "mm_model_table" and key:
            model_path = str(key)
            if model_path.lower().endswith(".gguf"):
                try:
                    self.app.query_one(
                        "#llm_launcher_panel",
                        LLMLauncherPanel,
                    ).select_model(model_path)
                    self._set_dl_status(
                        "Selected for launch. Open the Launch tab to continue.",
                        "green",
                    )
                except Exception:
                    pass

    def _set_dl_status(self, msg: str, color: str = "green") -> None:
        timestamp = time.strftime("%H:%M:%S")
        formatted = f"({timestamp}) [{color}]{escape(msg)}[/{color}]"
        self.query_one("#mm_dl_status", Label).update(formatted)
        try:
            self.app.query_one("#llm_global_info", Static).update(
                f"[b]INFO[/b] {formatted}"
            )
        except Exception:
            pass

    def _download_complete(self, preferred_model: str) -> None:
        self._current_dir = (
            preferred_model
            if os.path.isdir(preferred_model)
            else os.path.dirname(preferred_model)
        )
        self._refresh_browser()
        try:
            self.app.query_one(
                "#llm_launcher_panel",
                LLMLauncherPanel,
            ).refresh_models(preferred_model)
        except Exception:
            pass

    @staticmethod
    def _fmt_size(n: int) -> str:
        if n >= 1024 ** 3:
            return f"{n / 1024**3:.2f} GB"
        if n >= 1024 ** 2:
            return f"{n / 1024**2:.1f} MB"
        return f"{n / 1024:.0f} KB"

    @staticmethod
    def _downloaded_bytes(local_dir: str) -> int:
        """Count completed model files plus active Hugging Face partial files."""
        total = 0
        try:
            for root, _dirs, files in os.walk(local_dir):
                relative_parts = os.path.relpath(root, local_dir).split(os.sep)
                in_hf_cache = ".cache" in relative_parts
                for filename in files:
                    is_partial = (
                        ".incomplete" in filename
                        or filename.endswith((".tmp", ".part"))
                    )
                    if in_hf_cache and not is_partial:
                        continue
                    try:
                        stat = os.stat(os.path.join(root, filename))
                        total += (
                            min(stat.st_size, stat.st_blocks * 512)
                            if is_partial
                            else stat.st_size
                        )
                    except OSError:
                        pass
        except OSError:
            pass
        return total

    def _refresh_download_status(self) -> None:
        """Update progress on Textual's UI thread once per second."""
        if not self._dl_active:
            return
        now = time.monotonic()
        current_size = max(
            0,
            self._downloaded_bytes(self._dl_local_dir) - self._dl_initial_size,
        )
        elapsed = now - self._dl_started_at
        mm, ss = divmod(int(elapsed), 60)
        sample_seconds = max(now - self._dl_last_sample, 0.001)
        delta = max(current_size - self._dl_last_size, 0)
        rate = delta / sample_seconds
        rate_text = (
            f" · {self._fmt_size(int(rate))}/s"
            if rate > 0
            else " · waiting for data"
        )
        self._set_dl_status(
            f"Downloading {self._dl_repo} · {self._fmt_size(current_size)} "
            f"received · {mm}m{ss:02d}s{rate_text}",
            "yellow",
        )
        self._dl_last_size = current_size
        self._dl_last_sample = now

    def _start_download(self, repo: str, filename: str, local_dir: str) -> None:
        if self._dl_active:
            self._set_dl_status("A Hugging Face download is already running.", "yellow")
            return

        local_dir_exp = os.path.expanduser(local_dir)
        try:
            os.makedirs(local_dir_exp, exist_ok=True)
        except OSError as exc:
            self._set_dl_status(f"Could not create model directory: {exc}", "red")
            return

        started_at = time.monotonic()
        initial_size = self._downloaded_bytes(local_dir_exp)
        self._dl_cancelled = False
        self._dl_active = True
        self._dl_repo = repo
        self._dl_local_dir = local_dir_exp
        self._dl_initial_size = initial_size
        self._dl_started_at = started_at
        self._dl_last_size = 0
        self._dl_last_sample = started_at

        log = self.query_one("#mm_dl_log", Log)
        log.clear()
        self._set_dl_status(f"Starting download of {repo}…", "yellow")

        def _worker() -> None:
            hf_bin = os.path.expanduser("~/vllm_stack/bin/hf")
            if not os.path.exists(hf_bin):
                hf_bin = os.path.expanduser("~/venv/bin/hf")
            if not os.path.exists(hf_bin):
                hf_bin = "hf"
            cmd = [hf_bin, "download", repo]
            if filename:
                cmd.append(filename)
            else:
                # A Transformers/vLLM snapshot does not need alternate Metal
                # or reference checkpoints. Those folders can each duplicate
                # the model weights and make a routine download tens of GB
                # larger than the runnable snapshot.
                cmd += [
                    "--exclude",
                    "metal/*",
                    "--exclude",
                    "original/*",
                ]
            cmd += ["--local-dir", local_dir_exp, "--max-workers", "1"]

            self.app.call_from_thread(log.write_line, "$ " + " ".join(shlex.quote(c) for c in cmd))

            try:
                env = os.environ.copy()
                env["PYTHONUNBUFFERED"] = "1"
                # Direct HTTP reliably resumes partial files on this host; the
                # previous Xet transfer left sockets in CLOSE_WAIT indefinitely.
                env["HF_HUB_DISABLE_XET"] = "1"
                proc = subprocess.Popen(
                    cmd,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.STDOUT,
                    text=True,
                    bufsize=1,
                    env=env,
                )
                self._dl_proc = proc

                assert proc.stdout is not None
                for line in proc.stdout:
                    clean = line.strip()
                    if clean:
                        self.app.call_from_thread(log.write_line, clean)

                proc.wait()
                elapsed = time.monotonic() - started_at
                mm, ss = divmod(int(elapsed), 60)
                received = max(
                    0,
                    self._downloaded_bytes(local_dir_exp) - initial_size,
                )
                self._dl_active = False
                if proc.returncode == 0:
                    self.app.call_from_thread(
                        self._set_dl_status,
                        f"Download complete · {self._fmt_size(received)} received "
                        f"· {mm}m{ss:02d}s",
                        "green",
                    )
                    preferred = (
                        os.path.join(local_dir_exp, filename)
                        if filename
                        else local_dir_exp
                    )
                    self.app.call_from_thread(self._download_complete, preferred)
                elif self._dl_cancelled:
                    self.app.call_from_thread(
                        self._set_dl_status,
                        f"Download cancelled · {self._fmt_size(received)} received "
                        f"· {mm}m{ss:02d}s",
                        "yellow",
                    )
                else:
                    self.app.call_from_thread(
                        self._set_dl_status,
                        f"Download failed (exit {proc.returncode}) · "
                        f"{self._fmt_size(received)} received · {mm}m{ss:02d}s",
                        "red",
                    )
            except Exception as exc:
                self._dl_active = False
                self.app.call_from_thread(log.write_line, f"Error: {exc}")
                self.app.call_from_thread(self._set_dl_status, f"Error: {exc}", "red")
            finally:
                self._dl_proc = None

        threading.Thread(target=_worker, daemon=True).start()

    def on_button_pressed(self, event: Button.Pressed) -> None:
        if event.button.id == "mm_refresh_btn":
            self._refresh_browser()
        elif event.button.id == "mm_up_btn":
            parent = os.path.dirname(self._current_dir)
            if parent and os.path.isdir(parent) and parent != self._current_dir:
                self._current_dir = parent
                self._refresh_browser()
        elif event.button.id == "mm_root_btn":
            self._current_dir = LLAMA_MODELS_DIR
            self._refresh_browser()
        elif event.button.id == "mm_download_btn":
            repo = self.query_one("#mm_repo_input", Input).value.strip()
            filename = self.query_one("#mm_filename_input", Input).value.strip()
            if not repo:
                self._set_dl_status("HF Repo is required.", "red")
                return
            if "gguf" in repo.lower() and not filename:
                self._set_dl_status(
                    "Choose one GGUF filename to avoid downloading the entire repository.",
                    "red",
                )
                return
            local_dir = recommended_download_dir(LLAMA_MODELS_DIR, repo)
            self.query_one("#mm_localdir_input", Input).value = LLAMA_MODELS_DIR
            self._start_download(repo, filename, local_dir)
        elif event.button.id == "mm_cancel_btn":
            proc = self._dl_proc
            if proc is not None:
                self._dl_cancelled = True
                proc.terminate()
                self._set_dl_status("Cancelling download…", "yellow")
            else:
                self._set_dl_status("No download in progress.", "yellow")


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
    #llm_services_scroll { 
    height: 1fr; 
    overflow-y: auto; 
        }
    #llama_services_scroll {
    height: 1fr;
    overflow-y: auto;
        }
    #top { height: 12; }
    #services { width: 60%; padding: 0 1; }
    #health { width: 40%; padding: 0 1; }
    #main { height: 1fr; }
    #logs { height: 1fr; border: round $accent; padding: 0 1; }
    #llm_top { height: 10; }
    #llm_services { width: 60%; padding: 0 1; }
    #llm_controls { height: 3; align: left middle; padding: 0 1; }
    #llm_main { height: 1fr; }
    #llm_logs { height: 1fr; border: round $accent; padding: 0 1; }
    #llm_statusbar { height: 6; border: round $secondary; padding: 0 1; }
    #llm_page { height: 1fr; }
    #llm_workspace { height: 1fr; }
    #llm_global_info {
        height: 3;
        border: round $accent;
        padding: 0 1;
        content-align: left middle;
    }
    #llm_launcher_panel { height: 1fr; }
    #llm_launcher_scroll {
        height: 1fr;
        padding: 1 2 0 2;
        overflow-y: auto;
    }
    #llm_launcher_info {
        height: 8;
        padding: 0 2;
        border-top: solid $accent;
    }
    #llm_launcher_intro {
        height: 4;
        border: round $accent;
        padding: 0 1;
        margin-bottom: 1;
    }
    .llm_section_title { height: 2; margin-top: 1; }
    .llm_card_title {
        height: 3;
        border: round $accent;
        padding: 0 1;
        margin-top: 1;
        content-align: left middle;
    }
    #llm_svc_table { height: 7; border: solid $accent; margin-bottom: 1; }
    .llm_launcher_row { height: 3; align: left middle; }
    .llm_launcher_label {
        width: 14;
        text-style: bold;
        content-align: left middle;
    }
    .llm_launcher_inline_label {
        width: 8;
        margin-left: 2;
        text-style: bold;
        content-align: left middle;
    }
    .llm_launcher_short { width: 18; }
    .llm_launcher_medium { width: 32; }
    #llm_hf_repo_input { width: 1fr; }
    #llm_hf_filename_input { width: 1fr; }
    #llm_hf_destination {
        height: 2;
        color: $text-muted;
        padding: 0 1;
    }
    #llm_backend_select { width: 40; }
    #llm_local_model_select { width: 1fr; }
    #llm_launch_model_input { width: 1fr; }
    #llm_model_details {
        height: 4;
        border: solid $secondary;
        padding: 0 1;
        color: $text-muted;
    }
    #llm_launch_extra_args_input { width: 1fr; }
    #llm_command_preview {
        height: 5;
        border: solid $secondary;
        padding: 0 1;
        color: $text-muted;
    }
    #llm_capacity_result {
        height: 4;
        border: round $secondary;
        padding: 0 1;
    }
    #llm_launcher_status { height: 3; padding: 0 1; }
    #llm_download_subtab #models_panel { height: 1fr; }
    #llm_logs_subtab #llm_main { height: 1fr; }
    #statusbar { height: 3; border: round $secondary; padding: 0 1; }

    /* vLLM Service Config */
    #vllm_service_config_panel { padding: 0 2 1 2; height: 18; width: 100%; }
    #vllm_config_header { height: 3; align: left middle; }
    #vllm_config_title { text-style: bold; width: 1fr; content-align: left middle; }
    .vllm_row { height: 3; align: left middle; }
    .vllm_lbl { width: 10; text-style: bold; content-align: left middle; }
    #vllm_model_select { width: 70; }
    #vllm_model_input { width: 1fr; }
    #vllm_exec_input { width: 1fr; }
    #vllm_apply_row { height: 6; align: left top; }
    #vllm_config_status { margin-left: 2; width: 1fr; height: 5; content-align: left top; }

    /* Model Config Settings */
    #model_config_panel { padding: 1 2; height: 1fr; width: 100%; }
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

    /* llama-server tab */
    #llama_top { height: 12; }
    #llama_services { width: 60%; padding: 0 1; }
    #llama_health { width: 40%; padding: 0 1; }
    #llama_controls { height: 3; align: left middle; padding: 0 1; }
    #llama_config_panel { padding: 0 2 1 2; height: 1fr; width: 100%; }
    #llama_config_header { height: 3; align: left middle; }
    #llama_config_title { text-style: bold; width: 1fr; content-align: left middle; }
    #llama_browser_title { margin-top: 1; height: 2; text-style: bold; }
    #llama_browser_nav { height: 3; align: left middle; }
    #llama_browser_path { height: 1; width: 1fr; color: $text-muted; content-align: left middle; margin-left: 2; }
    #llama_browser { height: 12; }
    #llama_dirs_pane { width: 30%; }
    #llama_files_pane { width: 70%; }
    #llama_dirs_label { height: 1; text-style: bold; padding: 0 1; }
    #llama_files_label { height: 1; text-style: bold; padding: 0 1; }
    #llama_dirs_scroll { height: 1fr; border: solid $accent; }
    #llama_files_scroll { height: 1fr; border: solid $accent; }
    .llama_row { height: 3; align: left middle; }
    .llama_lbl { width: 14; text-style: bold; content-align: left middle; }
    .llama_short { width: 14; }
    #llama_model_input { width: 1fr; }
    #llama_extra_input { width: 1fr; }
    #llama_cmd_preview { width: 1fr; height: 3; content-align: left top; color: $text-muted; }
    #llama_apply_row { height: 3; align: left middle; }
    #llama_config_status { margin-left: 2; width: 1fr; content-align: left middle; }
    #llama_main { height: 12; }
    #llama_logs { height: 1fr; border: round $accent; padding: 0 1; }
    #llama_statusbar { height: 3; border: round $secondary; padding: 0 1; }

    /* Chatbot tab */
    #chatbot_services_scroll { height: 1fr; overflow-y: auto; }
    #chatbot_top { height: 8; }
    #chatbot_services { width: 100%; padding: 0 1; }
    #chatbot_controls { height: 3; align: left middle; padding: 0 1; }
    #chatbot_main { height: 1fr; }
    #chatbot_logs { height: 1fr; border: round $accent; padding: 0 1; }
    #chatbot_statusbar { height: 3; border: round $secondary; padding: 0 1; }

    /* Models tab */
    #models_panel { height: 1fr; }
    #mm_content_scroll {
        height: 1fr;
        padding: 0 1;
        overflow-y: auto;
    }
    #mm_download_info {
        height: 13;
        padding: 0 1;
        border-top: solid $accent;
    }
    #mm_header { height: 3; align: left middle; }
    #mm_title { width: 1fr; text-style: bold; content-align: left middle; }
    #mm_current_path { height: 1; color: $text-muted; }
    #mm_browser { height: 14; }
    #mm_dirs_pane { width: 30%; }
    #mm_files_pane { width: 70%; }
    #mm_dirs_label { height: 1; text-style: bold; padding: 0 1; }
    #mm_files_label { height: 1; text-style: bold; padding: 0 1; }
    #mm_dirs_scroll { height: 1fr; border: solid $accent; }
    #mm_files_scroll { height: 1fr; border: solid $accent; }
    #mm_dl_title { margin-top: 1; height: 2; text-style: bold; }
    #mm_dl_help { height: 3; color: $text-muted; padding: 0 1; }
    .mm_row { height: 3; align: left middle; }
    .mm_lbl { width: 12; text-style: bold; content-align: left middle; }
    #mm_repo_input { width: 1fr; }
    #mm_filename_input { width: 1fr; }
    #mm_localdir_input { width: 1fr; }
    #mm_dl_row { height: 3; align: left middle; }
    #mm_dl_status { margin-left: 2; width: 1fr; content-align: left middle; }
    #mm_dl_output_scroll { height: 1fr; border: round $accent; }
    #mm_dl_log { height: 1fr; }
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
    selected_unit: reactive[str] = reactive(SCANNER_SERVICES[0].unit)
    follow_logs: reactive[bool] = reactive(True)

    def compose(self) -> ComposeResult:
        yield Header(show_clock=True)

        with TabbedContent(initial="scanner_tab"):
            with TabPane("Scanner", id="scanner_tab"):
                    with Horizontal(id="top"):
                        with Container(id="services"):
                            yield Static("[b]Services[/b]")
                            with VerticalScroll(id="services_scroll"):
                                yield ServiceTable(id="scanner_svc_table")
                        with Container(id="health"):
                            with VerticalScroll(id="health_scroll"):
                                yield HealthPanel(id="health_panel")
                    with Vertical(id="main"):
                        yield Log(id="logs", highlight=True)
                        yield Static("", id="statusbar")

            with TabPane("LLM", id="llm_tab"):
                with Vertical(id="llm_page"):
                    with TabbedContent(initial="llm_launch_subtab", id="llm_workspace"):
                        with TabPane("Launch", id="llm_launch_subtab"):
                            yield LLMLauncherPanel(id="llm_launcher_panel")
                        with TabPane("Download", id="llm_download_subtab"):
                            yield ModelMaintenancePanel(id="models_panel")
                        with TabPane("vLLM advanced", id="llm_vllm_subtab"):
                            yield VllmServiceConfigPanel(id="vllm_service_config_panel")
                        with TabPane("llama.cpp advanced", id="llm_llama_subtab"):
                            yield LlamaConfigPanel(id="llama_config_panel")
                        with TabPane("Logs", id="llm_logs_subtab"):
                            with Vertical(id="llm_main"):
                                yield Log(id="llm_logs", highlight=True)
                                yield Static("", id="llm_statusbar")
                    yield Static(
                        "[b]INFO[/b] Ready.",
                        id="llm_global_info",
                    )

            with TabPane("Chatbot", id="chatbot_tab"):
                with Horizontal(id="chatbot_top"):
                    with Container(id="chatbot_services"):
                        yield Static("[b]NedBot[/b]")
                        with VerticalScroll(id="chatbot_services_scroll"):
                            yield ServiceTable(id="chatbot_svc_table")
                with Horizontal(id="chatbot_controls"):
                    yield Button("Start", id="chatbot_start_btn", variant="success")
                    yield Button("Stop", id="chatbot_stop_btn", variant="error")
                    yield Button("Restart", id="chatbot_restart_btn", variant="warning")
                with Vertical(id="chatbot_main"):
                    yield Log(id="chatbot_logs", highlight=True)
                    yield Static("", id="chatbot_statusbar")

        yield Footer()

    def on_mount(self) -> None:
        self.env = parse_env_file(ENV_FILE)
        self._init_tables()
        self.set_interval(REFRESH_SEC, self.refresh_all)
        self.refresh_all()

    def _init_table(self, table_id: str, services: List[ServiceDef], focus: bool = False) -> None:
        table = self.query_one(f"#{table_id}", ServiceTable)
        table.clear()
        for svc in services:
            table.add_row(svc.label, "…", "…", "…", "…", key=svc.unit)
        table.cursor_coordinate = (0, 0)
        if focus:
            table.focus()

    def _init_tables(self) -> None:
        self._init_table("scanner_svc_table", SCANNER_SERVICES, focus=True)
        self._init_table("llm_svc_table", LLM_SERVICES + LLAMA_SERVICES)
        self._init_table("chatbot_svc_table", CHATBOT_SERVICES)
        self.selected_unit = SCANNER_SERVICES[0].unit

    def _set_status(self, msg: str) -> None:
        if self._is_llm_unit(self.selected_unit):
            status_id = "#llm_statusbar"
        elif self._is_llama_unit(self.selected_unit):
            status_id = "#llm_statusbar"
        elif self._is_chatbot_unit(self.selected_unit):
            status_id = "#chatbot_statusbar"
        else:
            status_id = "#statusbar"
        self.query_one(status_id, Static).update(msg)

    def _is_llm_unit(self, unit: str) -> bool:
        return any(svc.unit == unit for svc in LLM_SERVICES)

    def _is_llama_unit(self, unit: str) -> bool:
        return any(svc.unit == unit for svc in LLAMA_SERVICES)

    def _is_chatbot_unit(self, unit: str) -> bool:
        return any(svc.unit == unit for svc in CHATBOT_SERVICES)

    def _services_for_unit(self, unit: str) -> List[ServiceDef]:
        if self._is_llm_unit(unit):
            return LLM_SERVICES
        if self._is_llama_unit(unit):
            return LLAMA_SERVICES
        if self._is_chatbot_unit(unit):
            return CHATBOT_SERVICES
        return SCANNER_SERVICES

    def _update_service_table(self, table_id: str, services: List[ServiceDef]) -> None:
        table = self.query_one(f"#{table_id}", ServiceTable)
        for svc in services:
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
                self._init_table(table_id, services)
                break

    def on_button_pressed(self, event: Button.Pressed) -> None:
        if event.button.id == "chatbot_start_btn":
            self.selected_unit = CHATBOT_SERVICES[0].unit
            self._act_selected("start")
        elif event.button.id == "chatbot_stop_btn":
            self.selected_unit = CHATBOT_SERVICES[0].unit
            self._act_selected("stop")
        elif event.button.id == "chatbot_restart_btn":
            self.selected_unit = CHATBOT_SERVICES[0].unit
            self._act_selected("restart")

    def refresh_all(self) -> None:
        self._update_service_table("scanner_svc_table", SCANNER_SERVICES)
        self._update_service_table("llm_svc_table", LLM_SERVICES + LLAMA_SERVICES)
        self._update_service_table("chatbot_svc_table", CHATBOT_SERVICES)

        self.query_one("#health_panel", HealthPanel).update_health(self.env)

        if self.follow_logs:
            self.refresh_logs()
            if not (
                self._is_llm_unit(self.selected_unit)
                or self._is_llama_unit(self.selected_unit)
            ):
                self.refresh_llm_logs()
            if not self._is_chatbot_unit(self.selected_unit):
                self.refresh_chatbot_logs()

    def refresh_logs(self) -> None:
        unit = self.selected_unit
        if self._is_llm_unit(unit) or self._is_llama_unit(unit):
            self.refresh_llm_logs()
            return
        if self._is_chatbot_unit(unit):
            self.refresh_chatbot_logs()
            return
        self._write_logs(unit, "#logs", "#statusbar")

    def refresh_llm_logs(self) -> None:
        unit = self.selected_unit
        if not (self._is_llm_unit(unit) or self._is_llama_unit(unit)):
            unit = LLM_SERVICES[0].unit
        self._write_logs(unit, "#llm_logs", "#llm_statusbar")

    def refresh_chatbot_logs(self) -> None:
        self._write_logs(CHATBOT_SERVICES[0].unit, "#chatbot_logs", "#chatbot_statusbar")

    def _write_logs(self, unit: str, logs_id: str, status_id: str) -> None:
        log_text = journal_tail(unit, LOG_LINES)

        logs = self.query_one(logs_id, Log)
        logs.clear()

        if log_text.strip():
            for line in log_text.splitlines():
                logs.write_line(line)
        else:
            logs.write_line("(no logs)")

        self.query_one(status_id, Static).update(
            f"Selected: [b]{unit}[/b] | follow_logs={'ON' if self.follow_logs else 'OFF'}"
        )

    def on_data_table_row_highlighted(self, event: DataTable.RowHighlighted) -> None:
        if event.row_key is None:
            return

        # Skip file-browser tables — their keys are paths, not unit names
        _browser_tables = {"mm_dirs_table", "mm_model_table", "llama_dirs_table", "llama_models_file_table"}
        if event.data_table.id in _browser_tables:
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
        for svc in self._services_for_unit(self.selected_unit):
            ok, msg = systemctl_action(svc.unit, verb)
            all_ok = all_ok and ok
            msgs.append(msg)
        joined = " | ".join(msgs)
        self._set_status(joined if all_ok else f"[red]{joined}[/red]")
        time.sleep(0.2)
        self.refresh_all()

    def action_start_selected(self) -> None:
        if self._is_llm_unit(self.selected_unit) or self._is_llama_unit(
            self.selected_unit
        ):
            self.query_one("#llm_launcher_panel", LLMLauncherPanel)._set_status(
                "Use Launch safely so GPU gatekeeper can protect the scanner.",
                "red",
            )
            return
        self._act_selected("start")

    def action_stop_selected(self) -> None:
        if self._is_llm_unit(self.selected_unit) or self._is_llama_unit(
            self.selected_unit
        ):
            launcher = self.query_one("#llm_launcher_panel", LLMLauncherPanel)
            launcher.query_one("#llm_backend_select", Select).value = (
                "vllm" if self._is_llm_unit(self.selected_unit) else "llama"
            )
            launcher._stop()
            return
        self._act_selected("stop")

    def action_restart_selected(self) -> None:
        if self._is_llm_unit(self.selected_unit) or self._is_llama_unit(
            self.selected_unit
        ):
            self.query_one("#llm_launcher_panel", LLMLauncherPanel)._set_status(
                "Stop the current backend, then use Launch safely.",
                "red",
            )
            return
        self._act_selected("restart")

    def action_start_all(self) -> None:
        if self._is_llm_unit(self.selected_unit) or self._is_llama_unit(
            self.selected_unit
        ):
            self.action_start_selected()
            return
        self._act_all("start")

    def action_stop_all(self) -> None:
        if self._is_llm_unit(self.selected_unit) or self._is_llama_unit(
            self.selected_unit
        ):
            self.action_stop_selected()
            return
        self._act_all("stop")

    def action_restart_all(self) -> None:
        if self._is_llm_unit(self.selected_unit) or self._is_llama_unit(
            self.selected_unit
        ):
            self.action_restart_selected()
            return
        self._act_all("restart")


if __name__ == "__main__":
    ScannerControlRoom().run()
