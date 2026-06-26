"""
gpu_model_manager/core/process_inspector.py

Inspect GPU processes via ps and classify them heuristically.
Classification is humble — if uncertain, returns "unknown_gpu_process".
"""
from __future__ import annotations

from .command_runner import run_command
from .logging_config import get_logger
from .schemas import GpuProcess, ProcessDetails

log = get_logger("process_inspector")

# (keywords_to_match_in_cmdline, classification_label)
_RULES: list[tuple[list[str], str]] = [
    (["scanner_transcriber_mcp", "scanner-mcp", "scanner_mcp"], "scanner_whisper"),
    (["faster_whisper", "faster-whisper"], "faster_whisper"),
    (["whisper"], "whisper_generic"),
    (["vllm.entrypoints", "vllm"], "vllm"),
    (["llama_server", "llama-server", "llama.cpp", "llamacpp", "llama_cpp"], "llama_cpp"),
    (["openai_server", "openai-api-server"], "openai_compatible"),
    (["python3", "python"], "python_gpu"),  # broad fallback for Python GPU processes
]


def _classify_cmdline(cmdline: str) -> str:
    lower = cmdline.lower()
    for keywords, label in _RULES:
        if any(kw in lower for kw in keywords):
            return label
    return "unknown_gpu_process"


def get_process_details(pid: int) -> ProcessDetails:
    result = run_command(
        ["ps", "-p", str(pid), "-o", "pid,ppid,user,comm,args", "--no-headers"],
        timeout=5,
    )
    if result.returncode != 0 or not result.stdout:
        return ProcessDetails(pid=pid, classification="unknown")

    try:
        # ps output: pid ppid user comm args...
        parts = result.stdout.split(None, 4)
        args_str = parts[4] if len(parts) > 4 else ""
        return ProcessDetails(
            pid=int(parts[0]),
            ppid=int(parts[1]) if len(parts) > 1 else None,
            user=parts[2] if len(parts) > 2 else None,
            comm=parts[3] if len(parts) > 3 else None,
            args=args_str,
            classification=_classify_cmdline(args_str),
        )
    except Exception as exc:
        log.debug("Failed to parse ps output for pid %d: %s", pid, exc)
        return ProcessDetails(pid=pid, classification="unknown")


def classify_gpu_process(process: GpuProcess) -> str:
    details = get_process_details(process.pid)
    return details.classification


def find_model_processes() -> list[ProcessDetails]:
    """
    Get all current GPU processes and return detailed + classified info for each.
    """
    from .gpu_inventory import get_gpu_processes

    gpu_procs = get_gpu_processes()
    results: list[ProcessDetails] = []
    for proc in gpu_procs:
        details = get_process_details(proc.pid)
        results.append(details)
    return results
