"""
Crash-point runner for the migration CLI (Phase 34 / MIGR-03).

Migration stages run as separate CLI invocations, so the honest way to model "a
crash between export and verify" is to run export and then never run verify. But
"a crash *during* publish, after the data file is promoted but before the receipt
lands" cannot be produced by stopping early — it has to be injected at the
system call that decides durability.

This module is executed as a fresh process (it calls the tool's real `main()`),
so a restart after a crash is a genuine restart in a new interpreter with no
in-memory state carried over. Faults are injected with the same helper the
durability tests use, scoped by path predicate so unrelated file I/O is
untouched.

Usage:
    python -m tests.support.migration_fault_runner \
        --fault replace --failures 1 --match parquet -- <migration CLI args...>
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


def _build_predicate(match: str):
    """Scope the fault so it cannot hit unrelated file operations."""
    separators = os.sep

    def _promotion(_src, dst) -> bool:
        name = str(dst)
        if match == "parquet":
            return name.endswith(".parquet") and f"{separators}ticks{separators}" in name
        if match == "receipt":
            return "receipts" in name
        if match == "intent":
            return "intent" in name
        if match == "state":
            return name.endswith("state.json") or name.endswith("plan.json")
        raise SystemExit(f"unknown fault match: {match}")

    return _promotion


def _install(fault: str, failures: int, match: str) -> None:
    if fault in (None, "", "none"):
        return

    from tests.support.faults import inject_failure

    class _Shim:
        """Minimal monkeypatch stand-in: these faults must outlive the call."""

        def setattr(self, target, name, value):
            setattr(target, name, value)

    predicate = _build_predicate(match)

    if fault == "link":
        # Migration publishes by hard link (tools/migrate_streaming_to_parquet.py
        # `_copy_verified_file_no_replace` uses os.link, not os.replace), so the
        # "data file became visible" boundary for migration is os.link.
        inject_failure(
            _Shim(), os, "link", predicate=predicate, failures=failures,
            error=OSError("injected migration crash: promotion link failed"),
        )
    elif fault == "replace":
        inject_failure(
            _Shim(), os, "replace", predicate=predicate, failures=failures,
            error=OSError("injected migration crash: promotion failed"),
        )
    elif fault == "fsync":
        inject_failure(
            _Shim(), os, "fsync", predicate=None, failures=failures,
            error=OSError("injected migration crash: fsync failed"),
        )
    else:
        raise SystemExit(f"unknown fault: {fault}")


def main(argv: list[str]) -> int:
    fault = "none"
    failures = 1
    match = "parquet"
    rest: list[str] = []

    index = 0
    while index < len(argv):
        arg = argv[index]
        if arg == "--fault":
            fault = argv[index + 1]
            index += 2
        elif arg == "--failures":
            failures = int(argv[index + 1])
            index += 2
        elif arg == "--match":
            match = argv[index + 1]
            index += 2
        elif arg == "--":
            rest = argv[index + 1:]
            break
        else:
            rest = argv[index:]
            break

    _install(fault, failures, match)

    from tools.migrate_streaming_to_parquet import main as migration_main

    return migration_main(rest)


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
