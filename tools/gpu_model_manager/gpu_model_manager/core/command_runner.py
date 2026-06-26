"""
gpu_model_manager/core/command_runner.py

Safe subprocess execution. Never crashes the app on command failure.
shell=True is never used.
"""
from __future__ import annotations

import subprocess

from .logging_config import get_logger
from .schemas import CommandResult

log = get_logger("command_runner")


def run_command(args: list[str], timeout: int = 10) -> CommandResult:
    """Run a subprocess, capture stdout/stderr, return CommandResult."""
    try:
        result = subprocess.run(
            args,
            capture_output=True,
            text=True,
            timeout=timeout,
        )
        return CommandResult(
            args=args,
            returncode=result.returncode,
            stdout=result.stdout.strip(),
            stderr=result.stderr.strip(),
        )
    except subprocess.TimeoutExpired:
        log.warning("Command timed out after %ds: %s", timeout, args)
        return CommandResult(
            args=args,
            returncode=-1,
            stdout="",
            stderr="",
            timed_out=True,
            error=f"timed_out_after_{timeout}s",
        )
    except FileNotFoundError:
        log.warning("Command not found: %s", args[0] if args else "?")
        return CommandResult(
            args=args,
            returncode=-1,
            stdout="",
            stderr="",
            error=f"command_not_found: {args[0] if args else ''}",
        )
    except Exception as exc:
        log.error("Unexpected error running %s: %s", args, exc)
        return CommandResult(
            args=args,
            returncode=-1,
            stdout="",
            stderr="",
            error=str(exc),
        )


def run_systemctl_user(args: list[str], timeout: int = 10) -> CommandResult:
    """Run systemctl --user <args>."""
    return run_command(["systemctl", "--user"] + args, timeout=timeout)
