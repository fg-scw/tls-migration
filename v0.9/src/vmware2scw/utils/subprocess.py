"""Subprocess wrapper with logging and progress tracking."""

from __future__ import annotations

import os
import re
import subprocess
from typing import Callable, Optional

from vmware2scw.utils.logging import get_logger

logger = get_logger(__name__)


class CommandResult:
    """Result of a subprocess execution."""

    def __init__(self, returncode: int, stdout: str = "", stderr: str = ""):
        self.returncode = returncode
        self.stdout = stdout
        self.stderr = stderr

    @property
    def success(self) -> bool:
        return self.returncode == 0


def run_command(
    cmd: list[str],
    capture_output: bool = False,
    check: bool = True,
    timeout: int | None = None,
    env: dict[str, str] | None = None,
    cwd: str | None = None,
    progress_pattern: str | None = None,
    progress_callback: Optional[Callable[[float], None]] = None,
) -> CommandResult:
    """Run a system command with logging and optional progress tracking.

    Args:
        cmd: Command and arguments as list
        capture_output: Capture stdout/stderr instead of streaming
        check: Raise on non-zero exit code
        timeout: Command timeout in seconds
        env: Additional environment variables (merged with current env)
        cwd: Working directory
        progress_pattern: Regex pattern to extract progress percentage from stderr
        progress_callback: Callback function receiving progress (0.0 - 100.0)

    Returns:
        CommandResult with returncode, stdout, stderr

    Raises:
        RuntimeError: If check=True and command fails
        TimeoutError: If command exceeds timeout
    """
    # Merge environment
    full_env = os.environ.copy()
    if env:
        full_env.update(env)

    # Redact passwords in log output
    safe_cmd = _redact_sensitive(cmd)
    logger.debug(f"Running: {' '.join(safe_cmd)}")

    try:
        if capture_output:
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=timeout,
                env=full_env,
                cwd=cwd,
            )
            cmd_result = CommandResult(result.returncode, result.stdout, result.stderr)

        elif progress_pattern and progress_callback:
            # Stream stderr for progress tracking
            proc = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                env=full_env,
                cwd=cwd,
            )

            stderr_lines = []
            pattern = re.compile(progress_pattern)

            for line in iter(proc.stderr.readline, ""):
                stderr_lines.append(line)
                match = pattern.search(line)
                if match:
                    try:
                        pct = float(match.group(1))
                        progress_callback(pct)
                    except (ValueError, IndexError):
                        pass

            proc.wait(timeout=timeout)
            stdout = proc.stdout.read() if proc.stdout else ""
            cmd_result = CommandResult(proc.returncode, stdout, "".join(stderr_lines))

        else:
            result = subprocess.run(
                cmd,
                text=True,
                timeout=timeout,
                env=full_env,
                cwd=cwd,
            )
            cmd_result = CommandResult(result.returncode)

    except subprocess.TimeoutExpired:
        raise TimeoutError(f"Command timed out after {timeout}s: {' '.join(safe_cmd)}")
    except FileNotFoundError:
        raise RuntimeError(f"Command not found: {cmd[0]}")

    if check and not cmd_result.success:
        error_msg = cmd_result.stderr.strip() if cmd_result.stderr else f"exit code {cmd_result.returncode}"
        raise RuntimeError(f"Command failed ({' '.join(safe_cmd)}): {error_msg}")

    return cmd_result


def check_tool_available(tool: str) -> bool:
    """Check if a system tool is available in PATH."""
    import shutil
    return shutil.which(tool) is not None


def verify_required_tools() -> dict[str, bool]:
    """Verify all required system tools are available.

    Returns dict of {tool_name: is_available}.
    """
    tools = {
        "qemu-img": "Disk conversion (VMDK → qcow2)",
        "virt-customize": "Guest OS modification",
        "virt-v2v": "VM conversion (optional, for in-place conversion)",
        "guestfish": "Guest filesystem access",
        "guestmount": "Guest filesystem mounting",
        "nbdkit": "NBD server (optional, for streaming export)",
    }

    results = {}
    for tool, description in tools.items():
        available = check_tool_available(tool)
        results[tool] = available
        status = "✅" if available else "❌"
        logger.info(f"  {status} {tool}: {description}")

    return results


def _redact_sensitive(cmd: list[str]) -> list[str]:
    """Redact passwords and secrets from command args for logging."""
    sensitive_keys = {"password", "pwd", "secret", "token", "key"}
    redacted = []
    skip_next = False

    for i, arg in enumerate(cmd):
        if skip_next:
            redacted.append("[REDACTED]")
            skip_next = False
            continue

        lower = arg.lower()
        if any(k in lower for k in sensitive_keys) and "=" in arg:
            key, _ = arg.split("=", 1)
            redacted.append(f"{key}=[REDACTED]")
        elif any(k in lower for k in sensitive_keys) and i + 1 < len(cmd):
            redacted.append(arg)
            skip_next = True
        else:
            redacted.append(arg)

    return redacted
