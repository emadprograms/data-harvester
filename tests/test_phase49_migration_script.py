"""Phase 49 — the owner's one-command migration gate.

`tools/mac/run_phase49_migration.sh` is the only thing standing between the
owner's real database and a bad deletion, so its refusals are pinned here:

- a missing or empty source is refused (exit 2) and **nothing** is created;
- a live streamer is refused (exit 3) with the stop instruction;
- the happy path migrates, verifies, audits, re-runs without duplicating,
  prints the CO-03 table, exits 0 — and leaves the source database untouched.

The script is never allowed to delete anything: that is asserted by checksum.
"""

from __future__ import annotations

import hashlib
import json
import os
import subprocess
import sys
from datetime import datetime, timedelta
from pathlib import Path

import duckdb
import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
SCRIPT = REPO_ROOT / "tools" / "mac" / "run_phase49_migration.sh"

LEGACY_COLUMNS = (
    "timestamp TIMESTAMP, symbol VARCHAR, price DOUBLE, volume DOUBLE, "
    "bid DOUBLE, ask DOUBLE, source VARCHAR, session VARCHAR"
)


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with open(path, "rb") as handle:
        for chunk in iter(lambda: handle.read(65536), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _legacy_source(path: Path, rows: int = 30) -> Path:
    """A miniature version of the owner's streaming.duckdb: two symbols, two dates."""
    path.parent.mkdir(parents=True, exist_ok=True)
    connection = duckdb.connect(str(path))
    connection.execute(f"CREATE TABLE tick_data ({LEGACY_COLUMNS})")
    start = datetime(2026, 9, 21, 13, 30)
    payload = []
    for symbol, offset in (("AAPL", 0.0), ("MSFT", 5.0)):
        for day in (0, 1):
            for index in range(rows // 4):
                timestamp = start + timedelta(days=day, seconds=index * 9)
                price = round(200.0 + offset + index * 0.05, 4)
                payload.append(
                    (timestamp, symbol, price, 10.0, price - 0.02, price + 0.02, "CAPITAL", "REG")
                )
    connection.executemany("INSERT INTO tick_data VALUES (?,?,?,?,?,?,?,?)", payload)
    connection.close()
    return path


def _run(source: Path, lake: Path, *extra: str, **env_overrides: str) -> subprocess.CompletedProcess:
    env = dict(os.environ)
    env.pop("TICK_LAKE_ROOT", None)
    env.pop("DATA_DIR", None)
    env.pop("MICRON_DATA_DIR", None)
    env.update(
        {
            "PYTHON_BIN": sys.executable,
            "TICK_LAKE_ROOT": str(lake),
        }
    )
    env.update(env_overrides)
    return subprocess.run(
        [str(SCRIPT), "--source-db", str(source), *extra],
        cwd=str(REPO_ROOT),
        env=env,
        capture_output=True,
        text=True,
        timeout=600,
    )


def test_a_missing_source_is_refused_before_anything_is_created(tmp_path) -> None:
    absent = tmp_path / "data" / "streaming.duckdb"
    lake = tmp_path / "lake"

    result = _run(absent, lake)

    output = result.stdout + result.stderr
    assert result.returncode == 2, output
    assert "nothing was migrated, nothing was deleted" in output
    assert not lake.exists(), "the script wrote to the lake before finding the source"


def test_an_empty_source_is_refused(tmp_path) -> None:
    empty = tmp_path / "data" / "streaming.duckdb"
    empty.parent.mkdir(parents=True)
    empty.touch()
    lake = tmp_path / "lake"

    result = _run(empty, lake)

    output = result.stdout + result.stderr
    assert result.returncode == 2, output
    assert "is empty" in output
    assert not lake.exists()


def test_a_live_streamer_is_refused_with_the_stop_instruction(tmp_path) -> None:
    source = _legacy_source(tmp_path / "data" / "streaming.duckdb")
    lake = tmp_path / "lake"
    control = lake / "_control"
    control.mkdir(parents=True)
    (control / "writer_status.json").write_text(
        json.dumps({"status": "RUNNING", "pid": os.getpid()}), encoding="utf-8"
    )

    result = _run(source, lake)

    output = result.stdout + result.stderr
    assert result.returncode == 3, output
    assert "looks alive" in output
    assert "./STOP_SERVICES.sh" in output
    assert not (lake / "ticks").exists(), "a live writer must stop the run before any export"


def test_the_gate_migrates_verifies_re_runs_and_never_deletes(tmp_path) -> None:
    source = _legacy_source(tmp_path / "data" / "streaming.duckdb")
    source_sha = _sha256(source)
    lake = tmp_path / "lake"

    result = _run(source, lake)

    assert result.returncode == 0, result.stdout + result.stderr
    assert "GATE PASSED" in result.stdout

    # MIG-01: the verification is bound to the source and reports zero differences.
    verification = json.loads((lake / "_migration" / "verification.json").read_text(encoding="utf-8"))
    assert verification["status"] == "PASSED"
    assert verification["discrepancies"] == []
    assert verification["total_source_rows"] == verification["total_parquet_rows"] > 0
    assert len(verification["files"]) == 4  # two symbols x two dates

    plan = json.loads((lake / "_migration" / "plan.json").read_text(encoding="utf-8"))
    assert plan["total_rows"] == verification["total_source_rows"]

    # MIG-03: the re-run is visible in the report and published nothing new.
    report_line = next(
        line for line in result.stdout.splitlines() if "MIG-03 re-run rows before / after" in line
    )
    before, after = [part.strip() for part in report_line.split("|")[2].split("/")]
    assert before == after, result.stdout

    # The lake holds exactly the source rows, and nothing was deleted.
    published = sorted((lake / "ticks").glob("symbol=*/date=*/*.parquet"))
    assert len(published) == 4
    connection = duckdb.connect()
    total = connection.execute(
        "SELECT count(*) FROM read_parquet(?)", [list(map(str, published))]
    ).fetchone()[0]
    connection.close()
    assert total == verification["total_source_rows"]
    assert source.is_file() and _sha256(source) == source_sha, "the script touched the owner's database"
    assert "Nothing has been deleted yet" in result.stdout
    assert "rm -f data/streaming.duckdb data/historical.duckdb" in result.stdout


def test_the_dry_run_plans_without_writing(tmp_path) -> None:
    source = _legacy_source(tmp_path / "data" / "streaming.duckdb")
    lake = tmp_path / "lake"

    result = _run(source, lake, "--dry-run")

    assert result.returncode == 0, result.stdout + result.stderr
    assert "Dry run OK" in result.stdout
    assert not lake.exists(), "the dry run must not create the lake"
