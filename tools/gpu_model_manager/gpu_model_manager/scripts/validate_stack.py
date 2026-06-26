"""
validate_stack.py — smoke-test the GPU Model Manager stack.

Run with:
    python -m gpu_model_manager.scripts.validate_stack
"""
from __future__ import annotations

import argparse
import json
import sys
from typing import Any


def _header(title: str):
    print(f"\n{'=' * 60}")
    print(f"  {title}")
    print("=" * 60)


def _safe(obj: Any):
    try:
        return json.loads(json.dumps(obj, default=str))
    except Exception:
        return str(obj)


def validate(output_json: bool = False):
    errors: list[str] = []
    report: dict = {"gpu": None, "runtimes": [], "services": [], "endpoints": {}, "policy": None, "leases": []}

    _header("GPU Inventory")
    try:
        from ..core.gpu_inventory import get_gpu_status

        gpu = get_gpu_status()
        print(f"  Available : {bool(gpu.available)}")
        print(f"  Name      : {gpu.name}")
        print(f"  Driver    : {gpu.driver_version}")
        print(f"  Total VRAM: {gpu.memory_total_mb:.0f} MB")
        print(f"  Used VRAM : {gpu.memory_used_mb:.0f} MB")
        print(f"  Free VRAM : {gpu.memory_free_mb:.0f} MB")
        print(f"  Utilization: {gpu.utilization_pct:.0f}%")
        print(f"  Temp      : {gpu.temperature_c:.0f} °C")
        if not gpu.available:
            print(f"  WARNING: GPU unavailable: {gpu.error}")
        report["gpu"] = _safe(gpu.model_dump() if hasattr(gpu, 'model_dump') else gpu.__dict__)
    except Exception as e:
        errors.append(f"gpu_inventory error: {e}")
        print(f"  ERROR: {e}")

    _header("Runtime Registry")
    try:
        from ..core.runtime_registry import get_runtime_registry

        runtimes = list(get_runtime_registry().values())
        for r in runtimes:
            prot = "[PROTECTED]" if r.protected else ""
            print(f"  {r.key:<28} {r.estimated_vram_mb:>6} MB  {prot}")
            report["runtimes"].append(_safe(r.model_dump() if hasattr(r, 'model_dump') else r.__dict__))
    except Exception as e:
        errors.append(f"runtime_registry error: {e}")
        print(f"  ERROR: {e}")

    _header("Service Status")
    try:
        from ..core.service_controller import discover_model_services

        services = discover_model_services()
        if not services:
            print("  (no model services discovered)")
        for svc in services:
            print(f"  {svc.name:<40} active={svc.is_active}")
            report["services"].append(_safe(svc.model_dump() if hasattr(svc, 'model_dump') else svc.__dict__))
    except Exception as e:
        errors.append(f"service_controller error: {e}")
        print(f"  ERROR: {e}")

    _header("Endpoint Health")
    try:
        from ..core.endpoint_monitor import check_all_runtime_endpoints

        endpoints = check_all_runtime_endpoints()
        for key, ep in endpoints.items():
            print(f"  {key:<28} reachable={ep.reachable}  latency={ep.response_ms or 0:.0f}ms")
            report["endpoints"][key] = _safe(ep.model_dump() if hasattr(ep, 'model_dump') else ep.__dict__)
    except Exception as e:
        errors.append(f"endpoint_monitor error: {e}")
        print(f"  ERROR: {e}")

    _header("Policy Calculation")
    try:
        from ..core.policy_engine import get_policy_status

        policy = get_policy_status()
        print(f"  free_vram          : {policy.free_vram_mb:.0f} MB")
        print(f"  safety_margin (−)  : {policy.gpu_safety_margin_mb} MB")
        print(f"  scanner_reserve (−): {policy.scanner_whisper_reserved_mb} MB")
        print(f"  effective_available: {policy.effective_available_mb:.0f} MB")
        report["policy"] = _safe(policy.model_dump() if hasattr(policy, 'model_dump') else policy.__dict__)
    except Exception as e:
        errors.append(f"policy_engine error: {e}")
        print(f"  ERROR: {e}")

    _header("Active Leases")
    try:
        from ..core.lease_manager import list_leases

        leases = list_leases()
        if not leases:
            print("  (no active leases)")
        for lease in leases:
            print(f"  {lease.lease_id}  {lease.runtime_key}  owner={lease.owner}")
            report["leases"].append(_safe(lease.model_dump() if hasattr(lease, 'model_dump') else lease.__dict__))
    except Exception as e:
        errors.append(f"lease_manager error: {e}")
        print(f"  ERROR: {e}")

    _header("Summary")
    if errors:
        for err in errors:
            print(f"  [FAIL] {err}")
        print(f"\n  {len(errors)} error(s) found.")
        if output_json:
            out = {"ok": False, "errors": errors, "report": report}
            print(json.dumps(out, indent=2, default=str))
        sys.exit(1)
    else:
        print("  All checks passed.")
        if output_json:
            out = {"ok": True, "errors": [], "report": report}
            print(json.dumps(out, indent=2, default=str))
        sys.exit(0)


def main(argv=None):
    parser = argparse.ArgumentParser(description="Smoke-test the GPU Model Manager stack")
    parser.add_argument("--json", "-j", action="store_true", dest="json", help="Emit structured JSON report to stdout")
    args = parser.parse_args(argv)
    validate(output_json=bool(args.json))


if __name__ == "__main__":
    main()
