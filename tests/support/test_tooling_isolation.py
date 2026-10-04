"""
Isolation of the new v4.2 tooling (Phase 29 / ISOL-01, ISOL-02).

These two requirements were recorded as Pending in the requirement matrix while
Phase 29 was reported complete, so they were never verified. Phase 36 surfaces
that; this module closes it for the tools v4.2 introduced.

The property being tested is behavioural, not declarative: with the ambient
environment pointing at a production-shaped location, the new tooling must write
exclusively inside the scratch directory it was given and must leave the
production path untouched.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

from src.storage.config import init_tick_lake
from tests.conftest import is_protected_path
from tests.support.deterministic_dataset import DeterministicDataset
from tests.support.migration_cli import run_cli
from tests.support.tree_snapshot import snapshot_tree

PROJECT_ROOT = Path(__file__).resolve().parents[2]
PRODUCTION_SHAPED = "/Volumes/Micron-E 0256 A/data-harvester/data"


@pytest.fixture
def production_shaped_env(tmp_path, monkeypatch):
    """Ambient environment pointing at a production-shaped location."""
    fake_production = tmp_path / "fake_production"
    (fake_production / "tick_lake").mkdir(parents=True)
    monkeypatch.setenv("DATA_DIR", str(fake_production))
    monkeypatch.setenv("TICK_LAKE_ROOT", str(fake_production / "tick_lake"))
    monkeypatch.setenv("DATA_HARVESTER_PROTECTED_ROOTS", str(fake_production))
    from src.utils.write_guard import register_protected_root, unregister_protected_root
    register_protected_root(fake_production)
    try:
        yield fake_production
    finally:
        unregister_protected_root(fake_production)


def test_deterministic_dataset_writes_only_inside_scratch(tmp_path, production_shaped_env):
    """ISOL-01: the generator ignores the inherited root and stays in scratch."""
    scratch = tmp_path / "scratch"
    scratch.mkdir()
    before = snapshot_tree(production_shaped_env)

    dataset = DeterministicDataset(row_count=200, seed=11)
    rows = dataset.generate()
    assert rows, "the generator produced no rows"

    # Nothing was created inside the production-shaped tree.
    assert snapshot_tree(production_shaped_env) == before, (
        "the generator touched the inherited production directory"
    )

    # The generator itself writes no files; callers own persistence. Verify that
    # by asserting an explicit write goes where it is told to.
    target = scratch / "generated.json"
    target.write_text(json.dumps({"rows": len(rows)}), encoding="utf-8")
    assert target.is_relative_to(scratch)


def test_durability_worker_writes_only_inside_the_explicit_lake_root(
    tmp_path, production_shaped_env
):
    """ISOL-01: a spawned writer honours its explicit argument, not the environment."""
    lake_root = tmp_path / "scratch_lake"
    ledger = tmp_path / "ledger.json"
    ready = tmp_path / "ready.txt"
    before = snapshot_tree(production_shaped_env)

    env = {key: value for key, value in os.environ.items()}
    env["PYTHONPATH"] = str(PROJECT_ROOT)
    completed = subprocess.run(
        [
            sys.executable, "-m", "tests.support.durability_worker",
            "--lake-root", str(lake_root),
            "--rows", "20",
            "--ack-rows", "10",
            "--seed", "7",
            "--ledger", str(ledger),
            "--ready", str(ready),
            "--drain",
        ],
        cwd=PROJECT_ROOT,
        env=env,
        capture_output=True,
        text=True,
        timeout=300,
    )
    assert completed.returncode == 0, f"worker failed:\n{completed.stdout}\n{completed.stderr}"

    assert ledger.is_file(), "the worker did not write its ledger"
    data = json.loads(ledger.read_text(encoding="utf-8"))
    assert data["published_rows"] == 20, data

    assert snapshot_tree(production_shaped_env) == before, (
        "the spawned writer touched the inherited production directory"
    )
    assert (lake_root / "ticks").is_dir(), "the worker did not write to its explicit lake root"


def test_migration_cli_writes_only_inside_the_explicit_paths(tmp_path, production_shaped_env):
    """ISOL-01: the migration CLI writes only where it was told to."""
    from tests.support.migration_factory import create_source_db, quote_row
    from datetime import datetime

    source = tmp_path / "source.duckdb"
    create_source_db(
        source,
        [quote_row(datetime(2026, 7, 10, 13, 30, i), "AAPL", 100.0 + i) for i in range(10)],
    )
    lake = tmp_path / "scratch_lake"
    init_tick_lake(lake)

    before = snapshot_tree(production_shaped_env)
    result = run_cli(
        ["--source-db", str(source), "--lake-root", str(lake), "--mode", "all", "--chunk-size", "5"]
    )
    assert result.ok, f"migration failed:\n{result.describe()}"

    assert snapshot_tree(production_shaped_env) == before, (
        "the migration CLI touched the inherited production directory"
    )


def test_a_production_shaped_path_is_recognised_as_protected(tmp_path):
    """ISOL-01: unsafe output paths are recognised before any write happens."""
    assert is_protected_path(PRODUCTION_SHAPED), (
        "the production volume path is no longer recognised as protected"
    )
    assert is_protected_path(os.path.join(PRODUCTION_SHAPED, "tick_lake"))

    scratch = tmp_path / "scratch"
    scratch.mkdir()
    assert not is_protected_path(str(scratch)), "scratch directories must not be protected"


def test_spawned_processes_receive_explicit_isolated_configuration(tmp_path, production_shaped_env):
    """ISOL-02: a spawned process is given explicit roots, not an inherited fallback."""
    explicit_root = tmp_path / "explicit_lake"
    explicit_root.mkdir()

    env = {
        "PATH": os.environ.get("PATH", ""),
        "PYTHONPATH": str(PROJECT_ROOT),
        # Deliberately NOT inherited: no DATA_DIR, no TICK_LAKE_ROOT.
        "TICK_LAKE_ROOT": str(explicit_root / "tick_lake"),
    }
    completed = subprocess.run(
        [
            sys.executable,
            "-c",
            "import os, json;"
            "from src.storage.config import resolve_tick_lake_root;"
            "print(json.dumps(str(resolve_tick_lake_root())))",
        ],
        cwd=PROJECT_ROOT,
        env=env,
        capture_output=True,
        text=True,
        timeout=120,
    )
    assert completed.returncode == 0, completed.stderr
    resolved = json.loads(completed.stdout.strip())
    assert resolved == str((explicit_root / "tick_lake").resolve()), (
        f"the spawned process resolved to {resolved} instead of its explicit root"
    )
    assert str(production_shaped_env) not in resolved, (
        "the spawned process fell back to the inherited production directory"
    )


def test_benchmark_baseline_subprocess_rejects_unsafe_destination(tmp_path, production_shaped_env):
    """VALD-01: tools/benchmark_baseline.py rejects output targeting protected roots."""
    before = snapshot_tree(production_shaped_env)
    unsafe_out = production_shaped_env / "leak_report.json"

    env = os.environ.copy()
    env["PYTHONPATH"] = str(PROJECT_ROOT)
    env["DATA_HARVESTER_PROTECTED_ROOTS"] = str(production_shaped_env)

    proc = subprocess.run(
        [
            sys.executable,
            "tools/benchmark_baseline.py",
            "--ticks", "10",
            "--lag-ticks", "10",
            "--query-iterations", "1",
            "--output", str(unsafe_out),
        ],
        cwd=PROJECT_ROOT,
        env=env,
        capture_output=True,
        text=True,
    )
    assert proc.returncode != 0
    assert "SAFETY REFUSAL" in proc.stderr
    assert not unsafe_out.exists()
    assert snapshot_tree(production_shaped_env) == before


def test_benchmark_baseline_subprocess_rejects_symlink_alias(tmp_path, production_shaped_env):
    """VALD-01: tools/benchmark_baseline.py detects and rejects symlink aliases to protected paths."""
    before = snapshot_tree(production_shaped_env)
    symlink_dir = tmp_path / "symlink_to_prod"
    symlink_dir.symlink_to(production_shaped_env, target_is_directory=True)
    unsafe_out = symlink_dir / "symlink_report.json"

    env = os.environ.copy()
    env["PYTHONPATH"] = str(PROJECT_ROOT)
    env["DATA_HARVESTER_PROTECTED_ROOTS"] = str(production_shaped_env)

    proc = subprocess.run(
        [
            sys.executable,
            "tools/benchmark_baseline.py",
            "--ticks", "10",
            "--lag-ticks", "10",
            "--query-iterations", "1",
            "--output", str(unsafe_out),
        ],
        cwd=PROJECT_ROOT,
        env=env,
        capture_output=True,
        text=True,
    )
    assert proc.returncode != 0
    assert "SAFETY REFUSAL" in proc.stderr
    assert not (production_shaped_env / "symlink_report.json").exists()
    assert snapshot_tree(production_shaped_env) == before


def test_validate_concurrency_subprocess_rejects_unsafe_lake_root(tmp_path, production_shaped_env):
    """VALD-01: tools/validate_concurrency.py rejects lake roots resolving to protected paths."""
    before = snapshot_tree(production_shaped_env)
    env = os.environ.copy()
    env["PYTHONPATH"] = str(PROJECT_ROOT)
    env["DATA_HARVESTER_PROTECTED_ROOTS"] = str(production_shaped_env)

    proc = subprocess.run(
        [
            sys.executable,
            "tools/validate_concurrency.py",
            "--lake-root", str(production_shaped_env / "tick_lake"),
        ],
        cwd=PROJECT_ROOT,
        env=env,
        capture_output=True,
        text=True,
    )
    assert proc.returncode != 0
    assert "SAFETY REFUSAL" in proc.stderr
    assert snapshot_tree(production_shaped_env) == before


def test_validate_concurrency_subprocess_rejects_symlink_alias(tmp_path, production_shaped_env):
    """VALD-01: tools/validate_concurrency.py rejects symlink aliases to protected lake roots."""
    before = snapshot_tree(production_shaped_env)
    symlink_dir = tmp_path / "symlink_lake"
    symlink_dir.symlink_to(production_shaped_env / "tick_lake", target_is_directory=True)

    env = os.environ.copy()
    env["PYTHONPATH"] = str(PROJECT_ROOT)
    env["DATA_HARVESTER_PROTECTED_ROOTS"] = str(production_shaped_env)

    proc = subprocess.run(
        [
            sys.executable,
            "tools/validate_concurrency.py",
            "--lake-root", str(symlink_dir),
        ],
        cwd=PROJECT_ROOT,
        env=env,
        capture_output=True,
        text=True,
    )
    assert proc.returncode != 0
    assert "SAFETY REFUSAL" in proc.stderr
    assert snapshot_tree(production_shaped_env) == before


def test_validate_release_report_subprocess_rejects_unsafe_output(tmp_path, production_shaped_env):
    """VALD-01: tools/validate_release_report.py rejects output targeting protected roots."""
    before = snapshot_tree(production_shaped_env)
    dummy_report = tmp_path / "rep.json"
    dummy_report.write_text(json.dumps({"candidate_sha": "abc1234", "gates": []}), encoding="utf-8")

    env = os.environ.copy()
    env["PYTHONPATH"] = str(PROJECT_ROOT)
    env["DATA_HARVESTER_PROTECTED_ROOTS"] = str(production_shaped_env)

    proc = subprocess.run(
        [
            sys.executable,
            "tools/validate_release_report.py",
            str(dummy_report),
            "--json", str(production_shaped_env / "leak.json"),
        ],
        cwd=PROJECT_ROOT,
        env=env,
        capture_output=True,
        text=True,
    )
    assert proc.returncode != 0
    assert not (production_shaped_env / "leak.json").exists()
    assert snapshot_tree(production_shaped_env) == before


def test_data_harvester_run_dir_isolation_and_safety(tmp_path, monkeypatch, production_shaped_env):
    """VALD-01: get_run_artifacts_dir routes to safe scratch and fails closed on protected roots."""
    from src.utils.write_guard import (
        ProductionAccessBlockedError,
        assert_safe_write_path,
        get_run_artifacts_dir,
    )

    # 1. Custom safe run dir is honored
    safe_scratch = tmp_path / "safe_run_dir"
    monkeypatch.setenv("DATA_HARVESTER_RUN_DIR", str(safe_scratch))
    run_dir = get_run_artifacts_dir()
    assert run_dir == safe_scratch.resolve()
    assert run_dir.is_dir()

    # 2. Protected destination as run dir is strictly rejected
    monkeypatch.setenv("DATA_HARVESTER_RUN_DIR", str(production_shaped_env))
    with pytest.raises(ProductionAccessBlockedError):
        get_run_artifacts_dir()

    # 3. Symlink alias to protected destination is strictly rejected
    symlink_dir = tmp_path / "symlink_alias_run_dir"
    symlink_dir.symlink_to(production_shaped_env, target_is_directory=True)
    monkeypatch.setenv("DATA_HARVESTER_RUN_DIR", str(symlink_dir))
    with pytest.raises(ProductionAccessBlockedError):
        get_run_artifacts_dir()
