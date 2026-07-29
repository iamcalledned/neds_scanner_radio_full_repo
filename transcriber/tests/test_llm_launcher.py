import json
from pathlib import Path

from tools.llm_launcher import (
    BASE_MODELS_DIR,
    VllmRunSettings,
    assess_gatekeeper_plan,
    build_vllm_command,
    discover_local_llm_models,
    estimate_model_vram_mb,
    model_size_bytes,
    parse_vllm_command,
    recommended_download_dir,
    validate_extra_args,
    validate_model_path,
)


def test_discovers_launchable_gguf_and_causal_model(tmp_path: Path) -> None:
    gguf = tmp_path / "chat-q4.gguf"
    with gguf.open("wb") as handle:
        handle.truncate(101 * 1024 * 1024)
    with (tmp_path / "mmproj-Q8.gguf").open("wb") as handle:
        handle.truncate(101 * 1024 * 1024)

    causal = tmp_path / "causal"
    causal.mkdir()
    (causal / "config.json").write_text(
        json.dumps({"architectures": ["QwenForCausalLM"]}),
        encoding="utf-8",
    )
    (causal / "model.safetensors").write_bytes(b"x" * 1024)

    whisper = tmp_path / "whisper"
    whisper.mkdir()
    (whisper / "config.json").write_text(
        json.dumps({"architectures": ["WhisperForConditionalGeneration"]}),
        encoding="utf-8",
    )
    (whisper / "model.safetensors").write_bytes(b"x" * 1024)

    models = discover_local_llm_models(str(tmp_path))

    assert [(Path(m.path).name, m.backend) for m in models] == [
        ("chat-q4.gguf", "llama"),
        ("causal", "vllm"),
    ]


def test_validates_backend_specific_model_paths(tmp_path: Path) -> None:
    gguf = tmp_path / "chat.gguf"
    gguf.write_bytes(b"x")
    assert validate_model_path(str(gguf), "llama") == ""
    assert "directory" in validate_model_path(str(gguf), "vllm")


def test_model_root_is_fixed_and_paths_outside_it_are_rejected(
    tmp_path: Path,
) -> None:
    root = tmp_path / "base_models"
    root.mkdir()
    inside = root / "inside.gguf"
    inside.write_bytes(b"x")
    outside = tmp_path / "outside.gguf"
    outside.write_bytes(b"x")

    assert BASE_MODELS_DIR == "/home/ned/models/base_models"
    assert validate_model_path(str(inside), "llama", str(root)) == ""
    assert "must be stored under" in validate_model_path(
        str(outside),
        "llama",
        str(root),
    )


def test_sharded_gguf_is_discovered_once_and_sized_as_a_set(
    tmp_path: Path,
) -> None:
    first = tmp_path / "chat-00001-of-00002.gguf"
    second = tmp_path / "chat-00002-of-00002.gguf"
    for path in (first, second):
        with path.open("wb") as handle:
            handle.truncate(101 * 1024 * 1024)

    models = discover_local_llm_models(str(tmp_path))

    assert len(models) == 1
    assert models[0].path == str(first)
    assert model_size_bytes(str(first), "llama") == 202 * 1024 * 1024


def test_vram_estimate_includes_runtime_overhead(tmp_path: Path) -> None:
    gguf = tmp_path / "chat.gguf"
    gguf.write_bytes(b"x" * (1024 * 1024))

    assert estimate_model_vram_mb(str(gguf), "llama") == 4096


def test_larger_llama_context_increases_the_safety_estimate(tmp_path: Path) -> None:
    gguf = tmp_path / "chat.gguf"
    with gguf.open("wb") as handle:
        handle.truncate(8 * 1024**3)

    normal = estimate_model_vram_mb(str(gguf), "llama", context_size=32_768)
    large = estimate_model_vram_mb(str(gguf), "llama", context_size=131_072)

    assert large > normal


def test_selected_model_size_can_override_static_gatekeeper_estimate() -> None:
    plan = {
        "ok": True,
        "vram_math": {
            "effective_available_mb": 20_000,
            "runtime_required_mb": 12_000,
        },
    }

    denied = assess_gatekeeper_plan(plan, 21_000)
    allowed = assess_gatekeeper_plan(plan, 18_000)

    assert not denied.allowed
    assert denied.required_vram_mb == 21_000
    assert allowed.allowed
    assert allowed.required_vram_mb == 18_000


def test_gatekeeper_denial_is_never_overridden() -> None:
    assessment = assess_gatekeeper_plan(
        {
            "ok": False,
            "explanation": "Scanner reserve would be violated.",
            "vram_math": {
                "effective_available_mb": 30_000,
                "runtime_required_mb": 12_000,
            },
        },
        8_000,
    )

    assert not assessment.allowed
    assert "Scanner reserve" in assessment.explanation


def test_vllm_utilization_is_included_in_scanner_safety_math() -> None:
    plan = {
        "ok": True,
        "vram_math": {
            "gpu_total_mb": 32_000,
            "effective_available_mb": 25_000,
            "runtime_required_mb": 16_000,
        },
    }

    assessment = assess_gatekeeper_plan(
        plan,
        selected_model_vram_mb=18_000,
        requested_runtime_vram_mb=28_800,
    )

    assert not assessment.allowed
    assert assessment.required_vram_mb == 28_800
    assert "maximum safe GPU utilization" in assessment.explanation


def test_vllm_allocation_must_be_large_enough_for_model() -> None:
    assessment = assess_gatekeeper_plan(
        {
            "ok": True,
            "vram_math": {
                "gpu_total_mb": 32_000,
                "effective_available_mb": 25_000,
                "runtime_required_mb": 12_000,
            },
        },
        selected_model_vram_mb=18_000,
        requested_runtime_vram_mb=16_000,
    )

    assert not assessment.allowed
    assert "Increase GPU memory utilization" in assessment.explanation


def test_vllm_control_card_round_trips_requested_parameters() -> None:
    settings = VllmRunSettings(
        host="0.0.0.0",
        port=30_001,
        gpu_memory_utilization=0.90,
        max_model_len=32_768,
        extra_args="--enable-auto-tool-choice --tool-call-parser hermes",
    )
    command = build_vllm_command(
        "/home/ned/vllm_stack/bin/vllm",
        "openai/gpt-oss-20b",
        settings,
    )

    assert "serve openai/gpt-oss-20b" in command
    assert "--port 30001" in command
    assert "--gpu-memory-utilization 0.9" in command
    assert "--max-model-len 32768" in command
    assert parse_vllm_command(command) == settings


def test_extra_args_cannot_override_gatekeeper_checked_controls() -> None:
    assert "--gpu-memory-utilization" in validate_extra_args(
        "--gpu-memory-utilization=0.95",
        "vllm",
    )
    assert "--port" in validate_extra_args("--port 12345", "llama")
    assert validate_extra_args(
        "--enable-auto-tool-choice --tool-call-parser hermes",
        "vllm",
    ) == ""


def test_downloads_get_a_repo_specific_directory(tmp_path: Path) -> None:
    destination = recommended_download_dir(
        str(tmp_path),
        "TheBloke/Model Name-GGUF",
    )

    assert destination == str(tmp_path / "TheBloke__Model-Name-GGUF")
