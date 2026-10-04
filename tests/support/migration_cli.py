"""
Run the migration tool as a real CLI process (Phase 34 / MIGR-03).

Every invocation here is a fresh interpreter. That is the point: a migration that
"resumes after a crash" must survive the loss of all in-memory state, which an
in-process call to `main()` cannot demonstrate.
"""

from __future__ import annotations

import os
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Sequence

PROJECT_ROOT = Path(__file__).resolve().parents[2]
TOOL = PROJECT_ROOT / "tools" / "migrate_streaming_to_parquet.py"
FAULT_RUNNER = "tests.support.migration_fault_runner"


@dataclass
class CliResult:
    returncode: int
    stdout: str
    stderr: str

    @property
    def ok(self) -> bool:
        return self.returncode == 0

    def describe(self) -> str:
        return (
            f"exit={self.returncode}\n--- stdout ---\n{self.stdout}\n"
            f"--- stderr ---\n{self.stderr}"
        )


def _env() -> dict:
    env = dict(os.environ)
    existing = env.get("PYTHONPATH", "")
    env["PYTHONPATH"] = str(PROJECT_ROOT) + (os.pathsep + existing if existing else "")
    return env


def run_cli(args: Sequence[str], *, timeout: float = 600.0) -> CliResult:
    """Run the migration tool in a fresh process and capture its output."""
    completed = subprocess.run(
        [sys.executable, str(TOOL), *args],
        cwd=PROJECT_ROOT,
        env=_env(),
        capture_output=True,
        text=True,
        timeout=timeout,
    )
    return CliResult(completed.returncode, completed.stdout, completed.stderr)


def run_cli_with_fault(
    args: Sequence[str],
    *,
    fault: str = "replace",
    failures: int = 1,
    match: str = "parquet",
    timeout: float = 600.0,
) -> CliResult:
    """Run the migration tool in a fresh process with an injected crash point."""
    completed = subprocess.run(
        [
            sys.executable, "-m", FAULT_RUNNER,
            "--fault", fault,
            "--failures", str(failures),
            "--match", match,
            "--", *args,
        ],
        cwd=PROJECT_ROOT,
        env=_env(),
        capture_output=True,
        text=True,
        timeout=timeout,
    )
    return CliResult(completed.returncode, completed.stdout, completed.stderr)


def start_cli(args: Sequence[str]) -> subprocess.Popen:
    """Launch the migration tool without waiting, so it can be killed mid-stage."""
    return subprocess.Popen(
        [sys.executable, str(TOOL), *args],
        cwd=PROJECT_ROOT,
        env=_env(),
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )
