"""Model discovery and scanner-safe LLM launch planning helpers."""

from __future__ import annotations

import json
import math
import os
import re
import shlex
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List

MIB = 1024 * 1024
BASE_MODELS_DIR = "/home/ned/models/base_models"
MODEL_WEIGHT_SUFFIXES = (".safetensors", ".bin", ".pt", ".pth")
IGNORED_DIRS = {".cache", ".git", "__pycache__"}
GGUF_SHARD_RE = re.compile(
    r"^(?P<prefix>.+)-(?P<part>\d{5})-of-(?P<total>\d{5})\.gguf$",
    re.IGNORECASE,
)
MANAGED_EXTRA_OPTIONS = {
    "vllm": {
        "--host",
        "--port",
        "--gpu-memory-utilization",
        "--max-model-len",
    },
    "llama": {
        "-m",
        "--model",
        "-ngl",
        "--n-gpu-layers",
        "-c",
        "--ctx-size",
        "--host",
        "--port",
    },
}


@dataclass(frozen=True)
class LocalLLMModel:
    path: str
    backend: str
    size_bytes: int

    @property
    def size_label(self) -> str:
        return format_bytes(self.size_bytes)

    @property
    def display_name(self) -> str:
        return f"{Path(self.path).name}  ({self.size_label})"


@dataclass(frozen=True)
class LaunchAssessment:
    allowed: bool
    required_vram_mb: int
    effective_available_mb: int
    gatekeeper_required_mb: int
    explanation: str


@dataclass(frozen=True)
class VllmRunSettings:
    host: str = "0.0.0.0"
    port: int = 30_001
    gpu_memory_utilization: float = 0.90
    max_model_len: int = 32_768
    extra_args: str = ""


def format_bytes(size_bytes: int) -> str:
    if size_bytes >= 1024**3:
        return f"{size_bytes / 1024**3:.1f} GB"
    if size_bytes >= 1024**2:
        return f"{size_bytes / 1024**2:.0f} MB"
    return f"{size_bytes / 1024:.0f} KB"


def _walk_model_root(root: Path) -> Iterable[tuple[Path, List[str]]]:
    if not root.is_dir():
        return
    for current, dirs, files in os.walk(root):
        dirs[:] = [name for name in dirs if name not in IGNORED_DIRS]
        yield Path(current), files


def _is_text_generation_config(path: Path) -> bool:
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return False
    architectures = data.get("architectures") or []
    return any("CausalLM" in str(name) for name in architectures)


def discover_local_llm_models(model_root: str) -> List[LocalLLMModel]:
    """Discover launchable GGUF and local Transformers/vLLM models."""
    root = Path(model_root).expanduser()
    models: List[LocalLLMModel] = []

    for current, files in _walk_model_root(root):
        gguf_groups: Dict[str, List[Path]] = {}
        for filename in files:
            lower = filename.lower()
            path = current / filename
            if (
                lower.endswith(".gguf")
                and not lower.startswith(("mmproj-", "ggml-vocab-"))
            ):
                try:
                    size = path.stat().st_size
                except OSError:
                    continue
                if size >= 100 * MIB:
                    shard = GGUF_SHARD_RE.match(filename)
                    group = shard.group("prefix") if shard else filename
                    gguf_groups.setdefault(group, []).append(path)

        for paths in gguf_groups.values():
            paths.sort()
            try:
                size = sum(path.stat().st_size for path in paths)
            except OSError:
                continue
            models.append(LocalLLMModel(str(paths[0]), "llama", size))

        if "config.json" not in files:
            continue
        config_path = current / "config.json"
        if not _is_text_generation_config(config_path):
            continue
        weight_size = 0
        for filename in files:
            if filename.lower().endswith(MODEL_WEIGHT_SUFFIXES):
                try:
                    weight_size += (current / filename).stat().st_size
                except OSError:
                    pass
        if weight_size:
            models.append(LocalLLMModel(str(current), "vllm", weight_size))

    return sorted(models, key=lambda model: (model.backend, model.path.lower()))


def model_size_bytes(model_path: str, backend: str) -> int:
    path = Path(model_path).expanduser()
    if backend == "llama":
        if not path.is_file() or path.suffix.lower() != ".gguf":
            return 0
        try:
            shard = GGUF_SHARD_RE.match(path.name)
            if not shard:
                return path.stat().st_size
            pattern = (
                f"{shard.group('prefix')}-*-of-{shard.group('total')}.gguf"
            )
            return sum(part.stat().st_size for part in path.parent.glob(pattern))
        except OSError:
            return 0

    if not path.is_dir():
        return 0
    total = 0
    try:
        for child in path.iterdir():
            if child.is_file() and child.name.lower().endswith(MODEL_WEIGHT_SUFFIXES):
                total += child.stat().st_size
    except OSError:
        return 0
    return total


def validate_model_path(
    model_path: str,
    backend: str,
    model_root: str | None = None,
) -> str:
    path = Path(model_path).expanduser()
    if model_root:
        try:
            path.resolve().relative_to(Path(model_root).expanduser().resolve())
        except (OSError, ValueError):
            return f"Model must be stored under {model_root}."

    if backend == "llama":
        if not path.is_file():
            return "Select an existing GGUF model file."
        if path.suffix.lower() != ".gguf":
            return "llama.cpp requires a .gguf model file."
        if path.name.lower().startswith(("mmproj-", "ggml-vocab-")):
            return "That GGUF is a support file, not a launchable language model."
        return ""

    if not path.is_dir():
        return "Select a downloaded Hugging Face model directory for vLLM."
    if not (path / "config.json").is_file():
        return "The vLLM model directory must contain config.json."
    if not _is_text_generation_config(path / "config.json"):
        return "The selected directory is not a causal language model."
    if model_size_bytes(str(path), backend) <= 0:
        return "No model weight files were found in the selected directory."
    return ""


def estimate_model_vram_mb(
    model_path: str,
    backend: str,
    context_size: int = 32_768,
) -> int:
    """Conservative launch estimate based on downloaded weight size."""
    size_mb = model_size_bytes(model_path, backend) / MIB
    if size_mb <= 0:
        return 0
    if backend == "llama":
        # Account for weights, runtime buffers, and a context-dependent KV cache.
        kv_cache_mb = max(2048, math.ceil(context_size / 32_768 * 2048))
        estimate = math.ceil(size_mb * 1.12 + kv_cache_mb)
    else:
        estimate = math.ceil(size_mb * 1.20 + 2048)
    return max(4096 if backend == "llama" else 6000, estimate)


def assess_gatekeeper_plan(
    plan: Dict[str, Any],
    selected_model_vram_mb: int,
    requested_runtime_vram_mb: int = 0,
) -> LaunchAssessment:
    """Combine gatekeeper policy with the selected model's real size estimate."""
    math_data = plan.get("vram_math", {})
    available = int(float(math_data.get("effective_available_mb", 0) or 0))
    gatekeeper_required = int(float(math_data.get("runtime_required_mb", 0) or 0))
    required = max(
        selected_model_vram_mb,
        gatekeeper_required,
        requested_runtime_vram_mb,
    )

    if not plan.get("ok", False):
        reason = str(plan.get("explanation") or "Gatekeeper denied the launch.")
        return LaunchAssessment(
            False,
            required,
            available,
            gatekeeper_required,
            reason,
        )
    if selected_model_vram_mb <= 0:
        return LaunchAssessment(
            False,
            required,
            available,
            gatekeeper_required,
            "Could not estimate the selected model's VRAM requirement.",
        )
    if (
        requested_runtime_vram_mb
        and requested_runtime_vram_mb < selected_model_vram_mb
    ):
        return LaunchAssessment(
            False,
            required,
            available,
            gatekeeper_required,
            (
                f"Requested vLLM allocation is {requested_runtime_vram_mb} MB, "
                f"but this model needs about {selected_model_vram_mb} MB. "
                "Increase GPU memory utilization."
            ),
        )
    if required > available:
        total = int(float(math_data.get("gpu_total_mb", 0) or 0))
        safe_hint = ""
        if requested_runtime_vram_mb and total:
            safe_hint = (
                f" The current maximum safe GPU utilization is about "
                f"{available / total:.2f}."
            )
        return LaunchAssessment(
            False,
            required,
            available,
            gatekeeper_required,
            (
                f"Selected model needs about {required} MB, but only {available} MB "
                "is available after scanner protection and the GPU safety margin."
                f"{safe_hint}"
            ),
        )
    return LaunchAssessment(
        True,
        required,
        available,
        gatekeeper_required,
        (
            f"Safe to launch: about {required} MB required and {available} MB "
            "available after scanner protection."
        ),
    )


def parse_vllm_command(command: str) -> VllmRunSettings:
    """Read the dashboard-controlled options from an existing vLLM command."""
    try:
        parts = shlex.split(command)
    except ValueError:
        parts = []

    known_with_value = {
        "--host": "host",
        "--port": "port",
        "--gpu-memory-utilization": "gpu_memory_utilization",
        "--max-model-len": "max_model_len",
    }
    values: Dict[str, Any] = {
        "host": "0.0.0.0",
        "port": 30_001,
        "gpu_memory_utilization": 0.90,
        "max_model_len": 32_768,
    }
    extra: List[str] = []
    try:
        serve_index = parts.index("serve")
    except ValueError:
        serve_index = -1
    index = serve_index + 2 if serve_index >= 0 else len(parts)
    while index < len(parts):
        token = parts[index]
        field = known_with_value.get(token)
        if field and index + 1 < len(parts):
            raw = parts[index + 1]
            try:
                if field in ("port", "max_model_len"):
                    values[field] = int(raw)
                elif field == "gpu_memory_utilization":
                    values[field] = float(raw)
                else:
                    values[field] = raw
            except ValueError:
                pass
            index += 2
            continue
        extra.append(token)
        index += 1

    return VllmRunSettings(
        host=str(values["host"]),
        port=int(values["port"]),
        gpu_memory_utilization=float(values["gpu_memory_utilization"]),
        max_model_len=int(values["max_model_len"]),
        extra_args=shlex.join(extra) if extra else "",
    )


def validate_extra_args(extra_args: str, backend: str) -> str:
    """Reject extra arguments that can override gatekeeper-checked controls."""
    try:
        parts = shlex.split(extra_args)
    except ValueError as exc:
        return f"Extra arguments have invalid quoting: {exc}"

    managed = MANAGED_EXTRA_OPTIONS.get(backend, set())
    for token in parts:
        option = token.split("=", 1)[0]
        if option in managed:
            return (
                f"{option} is controlled by the run control card and cannot "
                "also appear in Extra args."
            )
    return ""


def build_vllm_command(
    binary: str,
    model: str,
    settings: VllmRunSettings,
) -> str:
    """Build a shell-safe vLLM service command from the run control card."""
    extra_error = validate_extra_args(settings.extra_args, "vllm")
    if extra_error:
        raise ValueError(extra_error)
    parts = [
        binary,
        "serve",
        model,
        "--host",
        settings.host,
        "--port",
        str(settings.port),
        "--gpu-memory-utilization",
        f"{settings.gpu_memory_utilization:.4g}",
        "--max-model-len",
        str(settings.max_model_len),
    ]
    if settings.extra_args.strip():
        parts.extend(shlex.split(settings.extra_args))
    return shlex.join(parts)


def recommended_download_dir(model_root: str, repo_id: str) -> str:
    """Put each Hub repository in its own predictable local directory."""
    repo_id = repo_id.strip().strip("/")
    safe_parts = [
        re.sub(r"[^A-Za-z0-9._-]+", "-", part).strip(".-")
        for part in repo_id.split("/")
        if part
    ]
    folder_name = "__".join(part for part in safe_parts if part)
    return str(
        Path(model_root).expanduser()
        / (folder_name or "downloaded-model")
    )
