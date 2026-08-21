"""
gpu_model_manager/core/registry_validator.py

Validate runtime registry entries and produce structured results.
Does not raise on bad config — returns clean errors/warnings.
"""
from __future__ import annotations

from urllib.parse import urlparse

from .config import RUNTIMES_CONFIG_FILE
from .logging_config import get_logger
from .schemas import RegistryValidationResult

log = get_logger("registry_validator")

_VALID_KIND = {"whisper", "llm"}
_VALID_PRIORITY = {"critical", "interactive", "batch"}
_VALID_WARM = {True, False, "optional"}
_VALID_STOP_POLICY = {"never_auto", "allowed"}
_VALID_RESTART_POLICY = {"manual_confirm", "allowed"}


def _is_valid_url(url: str) -> bool:
    try:
        p = urlparse(url)
        return p.scheme in ("http", "https") and bool(p.netloc)
    except Exception:
        return False


def validate_runtime_registry() -> RegistryValidationResult:
    """
    Validate every entry in config/runtimes.json.
    Returns structured result with errors, warnings, and counts.
    """
    import json

    errors: list[str] = []
    warnings: list[str] = []
    seen_keys: set[str] = set()
    protected_count = 0

    if not RUNTIMES_CONFIG_FILE.exists():
        return RegistryValidationResult(
            ok=False,
            runtime_count=0,
            protected_count=0,
            errors=[f"Registry file not found: {RUNTIMES_CONFIG_FILE}"],
            warnings=[],
        )

    try:
        raw: list[dict] = json.loads(RUNTIMES_CONFIG_FILE.read_text(encoding="utf-8"))
    except Exception as exc:
        return RegistryValidationResult(
            ok=False,
            runtime_count=0,
            protected_count=0,
            errors=[f"Failed to parse runtimes.json: {exc}"],
            warnings=[],
        )

    if not isinstance(raw, list):
        return RegistryValidationResult(
            ok=False,
            runtime_count=0,
            protected_count=0,
            errors=["runtimes.json must be a JSON array at the top level"],
            warnings=[],
        )

    for i, entry in enumerate(raw):
        prefix = f"[entry {i}]"

        # --- key ---
        key = entry.get("key")
        if not key:
            errors.append(f"{prefix} missing required field 'key'")
            key = f"<entry_{i}>"
        else:
            prefix = f"[{key}]"
            if key in seen_keys:
                errors.append(f"{prefix} duplicate key")
            seen_keys.add(key)

        # --- required string fields ---
        for field in ("display_name", "kind", "owner", "description"):
            if not entry.get(field):
                errors.append(f"{prefix} missing required field '{field}'")

        # --- kind ---
        kind = entry.get("kind", "")
        if kind and kind not in _VALID_KIND:
            errors.append(f"{prefix} kind={kind!r} must be one of {sorted(_VALID_KIND)}")

        # --- estimated_vram_mb ---
        vram = entry.get("estimated_vram_mb")
        if vram is None:
            errors.append(f"{prefix} missing required field 'estimated_vram_mb'")
        elif not isinstance(vram, (int, float)) or vram <= 0:
            errors.append(f"{prefix} estimated_vram_mb must be a positive number, got {vram!r}")

        # --- priority ---
        prio = entry.get("priority", "")
        if prio not in _VALID_PRIORITY:
            errors.append(f"{prefix} priority={prio!r} must be one of {sorted(_VALID_PRIORITY)}")

        # --- protected ---
        protected = entry.get("protected", False)
        if not isinstance(protected, bool):
            errors.append(f"{prefix} protected must be boolean, got {protected!r}")
        if protected:
            protected_count += 1

        # --- warm ---
        warm = entry.get("warm", False)
        if warm not in _VALID_WARM:
            errors.append(f"{prefix} warm={warm!r} must be true, false, or 'optional'")

        # --- stop_policy ---
        stop_policy = entry.get("stop_policy", "")
        if stop_policy not in _VALID_STOP_POLICY:
            errors.append(f"{prefix} stop_policy={stop_policy!r} must be one of {sorted(_VALID_STOP_POLICY)}")

        # --- restart_policy ---
        restart_policy = entry.get("restart_policy", "")
        if restart_policy not in _VALID_RESTART_POLICY:
            errors.append(f"{prefix} restart_policy={restart_policy!r} must be one of {sorted(_VALID_RESTART_POLICY)}")

        # --- protected consistency ---
        if protected:
            if stop_policy and stop_policy != "never_auto":
                errors.append(
                    f"{prefix} protected=true but stop_policy={stop_policy!r}; "
                    "protected runtimes should use stop_policy='never_auto'"
                )
            if restart_policy and restart_policy != "manual_confirm":
                warnings.append(
                    f"{prefix} protected=true but restart_policy={restart_policy!r}; "
                    "consider restart_policy='manual_confirm' for protected runtimes"
                )

        # --- service_name ---
        svc = entry.get("service_name")
        if svc is not None:
            if not isinstance(svc, str) or not svc.endswith(".service"):
                errors.append(f"{prefix} service_name={svc!r} must be a string ending in '.service'")

        # --- endpoint ---
        ep = entry.get("endpoint")
        if ep is not None:
            if not _is_valid_url(ep):
                errors.append(
                    f"{prefix} endpoint={ep!r} is not a valid http/https URL"
                )

        # --- warnings for incomplete entries ---
        if not svc and not ep:
            warnings.append(
                f"{prefix} no service_name or endpoint configured; "
                "manager cannot start/stop or check reachability"
            )

    ok = len(errors) == 0
    if errors:
        log.warning("Registry validation found %d error(s)", len(errors))
    if warnings:
        log.debug("Registry validation found %d warning(s)", len(warnings))

    return RegistryValidationResult(
        ok=ok,
        runtime_count=len(raw),
        protected_count=protected_count,
        errors=errors,
        warnings=warnings,
    )
