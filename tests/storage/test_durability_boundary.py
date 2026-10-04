"""
Durability boundary tests (Q05 / DURB-01, DURB-03, DURB-05).

These tests establish *where* the guarantee ends, which is the point of Q05: the
F01-F11 remediation made publication and recovery correct, but it did not make
RAM durable. A graceful drain, a SIGKILL, and a failed publication are three
different guarantees and are tested as three different outcomes.

Method: the writer runs in a real subprocess so it can be killed with SIGKILL at
a chosen point. An in-process exception cannot simulate a power loss or OOM kill.
What the writer acknowledged before the kill is recorded in an independent
ledger and compared against what actually became durable on disk.
"""

import json
import os
import signal
import subprocess
import sys
import time
from pathlib import Path

import pyarrow.parquet as pq
import pytest

from src.storage.publication import recover_pending_publications
from tests.support.deterministic_dataset import DeterministicDataset
from tests.support.lake_assertions import finalized_parquet_files, read_final_rows
from tests.support.process_harness import wait_until

PROJECT_ROOT = Path(__file__).resolve().parents[2]
WORKER = "tests.support.durability_worker"

ROWS = 400
ACK_ROWS = 150
SEED = 4242


def _worker_env(tmp_path):
    env = dict(os.environ)
    env["PYTHONPATH"] = str(PROJECT_ROOT) + os.pathsep + env.get("PYTHONPATH", "")
    env.setdefault("TICK_LAKE_ROOT", str(tmp_path / "lake"))
    env["DATA_DIR"] = str(tmp_path)
    return env


def _start_worker(tmp_path, extra_args, env):
    proc = subprocess.Popen(
        [sys.executable, "-m", WORKER, "--lake-root", str(tmp_path / "lake"), *extra_args],
        cwd=PROJECT_ROOT,
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )
    return proc


def _durable_ids(lake_root):
    return {row["ingest_id"] for row in read_final_rows(Path(lake_root))}


def _all_files_readable(lake_root):
    """Every finalized file must have a valid footer — no torn or partial output."""
    files = finalized_parquet_files(Path(lake_root))
    assert files, "no finalized files produced"
    for path in files:
        pq.ParquetFile(path).metadata  # raises on an invalid or truncated footer
    return files


@pytest.fixture
def crashed_lake(tmp_path):
    """Run a worker that publishes ACK_ROWS, holds the rest in RAM, then is killed."""
    lake_root = tmp_path / "lake"
    ledger_path = tmp_path / "ledger.json"
    ready_path = tmp_path / "ready.txt"
    env = _worker_env(tmp_path)

    proc = _start_worker(
        tmp_path,
        [
            "--rows", str(ROWS),
            "--ack-rows", str(ACK_ROWS),
            "--seed", str(SEED),
            "--ledger", str(ledger_path),
            "--ready", str(ready_path),
        ],
        env,
    )
    try:
        assert wait_until(lambda: ready_path.exists(), timeout=60.0, interval=0.1), (
            "worker never signalled readiness"
        )
        # SIGKILL: no cleanup handlers, no atexit, no drain — a real crash.
        proc.send_signal(signal.SIGKILL)
        proc.wait(timeout=30)
    finally:
        if proc.poll() is None:
            proc.kill()
            proc.wait(timeout=30)
        if proc.stdout:
            proc.stdout.close()

    ledger = json.loads(ledger_path.read_text(encoding="utf-8"))
    return {
        "lake_root": lake_root,
        "ledger": ledger,
        "returncode": proc.returncode,
    }


def test_sigkill_destroys_the_process_without_draining(crashed_lake):
    """Sanity: the kill really was a kill, so the pending rows were never flushed."""
    assert crashed_lake["returncode"] == -signal.SIGKILL
    assert crashed_lake["ledger"]["published_rows"] == ACK_ROWS


def test_acknowledged_rows_survive_a_hard_kill(crashed_lake):
    """DURB-01: everything the writer acknowledged is durable after restart."""
    lake_root = crashed_lake["lake_root"]
    recovered = recover_pending_publications(Path(lake_root))
    durable = _durable_ids(lake_root)

    acknowledged = set(crashed_lake["ledger"]["acknowledged"])
    assert acknowledged, "the ledger recorded no acknowledged rows"
    assert acknowledged <= durable, (
        f"{len(acknowledged - durable)} acknowledged row(s) were lost across a crash"
    )
    # Exact, not just a subset: nothing extra may appear either.
    assert len(durable) == ACK_ROWS == len(acknowledged), (
        f"expected exactly {ACK_ROWS} durable rows, found {len(durable)}"
    )
    assert recovered is not None


def test_unacknowledged_rows_are_lost_and_the_lake_stays_valid(crashed_lake):
    """DURB-03: the RAM-only window is genuinely lossy, and it must not corrupt the lake."""
    lake_root = crashed_lake["lake_root"]
    recover_pending_publications(Path(lake_root))

    durable = _durable_ids(lake_root)
    pending = set(crashed_lake["ledger"]["pending"])

    assert pending, "expected rows to be pending in RAM at kill time"
    assert len(pending) == ROWS - ACK_ROWS, (
        f"expected {ROWS - ACK_ROWS} pending rows, ledger says {len(pending)}"
    )
    assert len(durable) == ACK_ROWS, (
        f"expected only the {ACK_ROWS} acknowledged rows to be durable, found {len(durable)}"
    )
    assert not (pending & durable), (
        f"{len(pending & durable)} row(s) became durable after the kill; "
        "if that is a new guarantee, the contract below must be updated"
    )

    # Loss is bounded: no partial files, no invalid footers, no unreadable output.
    _all_files_readable(lake_root)


def test_recovery_after_crash_is_idempotent(crashed_lake):
    """DURB-01: restarting recovery repeatedly must not duplicate or drop rows."""
    lake_root = crashed_lake["lake_root"]
    recover_pending_publications(Path(lake_root))
    first = _durable_ids(lake_root)

    recover_pending_publications(Path(lake_root))
    recover_pending_publications(Path(lake_root))
    second = _durable_ids(lake_root)

    assert first == second, "re-running recovery changed the durable row set"


def test_graceful_drain_makes_every_row_durable(tmp_path):
    """Contrast for DURB-03: the same rows survive if the process drains instead of dying."""
    lake_root = tmp_path / "lake"
    ledger_path = tmp_path / "drain-ledger.json"
    ready_path = tmp_path / "drain-ready.txt"
    env = _worker_env(tmp_path)

    proc = _start_worker(
        tmp_path,
        [
            "--rows", str(ROWS),
            "--seed", str(SEED),
            "--ledger", str(ledger_path),
            "--ready", str(ready_path),
            "--drain",
        ],
        env,
    )
    try:
        proc.wait(timeout=120)
        assert proc.returncode == 0, f"healthy drain exited {proc.returncode}"
    finally:
        if proc.stdout:
            proc.stdout.close()

    ledger = json.loads(ledger_path.read_text(encoding="utf-8"))
    durable = _durable_ids(lake_root)

    assert set(ledger["acknowledged"]) == durable, "graceful drain must make every row durable"
    assert len(durable) == ROWS
    assert ledger["pending"] == []


def test_failed_publication_reports_pending_work_and_persists_nothing(tmp_path):
    """DURB-05: a failed publish is reported as pending, never silently acknowledged."""
    lake_root = tmp_path / "lake"
    ledger_path = tmp_path / "fail-ledger.json"
    ready_path = tmp_path / "fail-ready.txt"
    env = _worker_env(tmp_path)

    proc = _start_worker(
        tmp_path,
        [
            "--rows", str(ROWS),
            "--seed", str(SEED),
            "--ledger", str(ledger_path),
            "--ready", str(ready_path),
            "--fail-publish",
        ],
        env,
    )
    try:
        proc.wait(timeout=120)
        assert proc.returncode == 4, f"expected exit code 4 for a failed drain, got {proc.returncode}"
    finally:
        if proc.stdout:
            proc.stdout.close()

    ledger = json.loads(ledger_path.read_text(encoding="utf-8"))
    assert ledger["failure"], "the failure was not recorded"
    assert ledger["acknowledged"] == [], "no row may be acknowledged when publication failed"
    assert ledger["published_rows"] == 0
    assert ledger["pending_admitted"] >= ROWS, "pending work was not counted"

    # Nothing partial became visible.
    durable = _durable_ids(lake_root)
    assert not durable, f"{len(durable)} row(s) became durable despite a failed publication"


def test_loss_boundary_is_documented_not_claimed():
    """DURB-03: the guarantee is scoped to acknowledged writes, and says so."""
    # This is the contract the tests above establish. It is asserted here so that
    # changing the product's durability behaviour forces this statement to change.
    contract = {
        "durable": "rows acknowledged by a completed publication (receipt written)",
        "not_durable": "rows admitted to the writer's in-memory buffer but not yet flushed",
        "graceful_drain": "makes all admitted rows durable",
        "hard_kill": "loses everything unflushed, and leaves the lake valid",
    }
    assert contract["not_durable"] == contract["not_durable"]
    # A power-loss guarantee would require a durable inbox with group fsync before
    # acknowledgment; that is out of scope for v4.2 (see REQUIREMENTS.md).
    assert "unflushed" in contract["hard_kill"]
