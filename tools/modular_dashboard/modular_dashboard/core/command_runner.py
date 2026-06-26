"""
core/command_runner.py — Centralised subprocess execution wrapper.

All systemctl and other shell commands go through this module.
Never crashes the caller on subprocess failure.
"""
from __future__ import annotations

import subprocess
from typing import Optional

from modular_dashboard.core.logging_config import get_logger
from modular_dashboard.core.schemas import CommandResult

log = get_logger("command_runner")


def run_command(
    args: list[str],
    timeout: int = 10,
    extra_env: Optional[dict] = None,
) -> CommandResult:
    """Run a command and return a CommandResult.

    Parameters
    ----------
    args:
        Command and arguments as a list (never shell=True).
    timeout:
        Seconds before the command is killed and timed_out is set.
    extra_env:
        Additional environment variables merged on top of the current env.
    """
    import os
    import copy

    env = None
    if extra_env:
        env = copy.copy(os.environ)
        env.update(extra_env)

    log.debug("run_command: %s", args)
    try:
        result = subprocess.run(
            args,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            timeout=timeout,
            env=env,
        )
        cmd_result = CommandResult(
            args=args,
            returncode=result.returncode,
            stdout=(result.stdout or "").rstrip(),
            stderr=(result.stderr or "").rstrip(),
        )
        if result.returncode != 0:
            log.debug(
                "command exited %d: %s | stderr: %s",
                result.returncode,
                args,
                cmd_result.stderr[:200],
            )
        return cmd_result

    except subprocess.TimeoutExpired:
        log.warning("command timed out after %ds: %s", timeout, args)
        return CommandResult(
            args=args,
            returncode=-1,
            stdout="",
            stderr="",
            timed_out=True,
            error=f"Command timed out after {timeout}s",
        )
    except FileNotFoundError as exc:
        log.warning("command not found: %s — %s", args[0], exc)
        return CommandResult(
            args=args,
            returncode=127,
            stdout="",
            stderr=str(exc),
            error=f"Command not found: {args[0]}",
        )
    except Exception as exc:  # noqa: BLE001
        log.error("unexpected error running %s: %s", args, exc)
        return CommandResult(
            args=args,
            returncode=-1,
            stdout="",
            stderr=str(exc),
            error=str(exc),
        )


def run_systemctl_user(args: list[str], timeout: int = 10) -> CommandResult:
    """Run a `systemctl --user <args>` command.

    Prepends ``systemctl --user`` so callers only pass the sub-command.

    Example::

        run_systemctl_user(["status", "scanner-mcp.service"])
    """
    return run_command(["systemctl", "--user"] + args, timeout=timeout)
