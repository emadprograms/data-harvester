"""Phase 46 STOR-01 / STOR-02 — startup regressions against disk-backed DuckDB.

v5.0 removes DuckDB as *storage*.  These tests are the guard that fails if the
legacy backend is ever reintroduced into the streaming runtime: no code path may
open, create, or even import the disk-database layer.

They are deliberately written against behaviour the implementation does not yet
have (TDD), so every test in this module is expected to fail before Phase 46 is
implemented.
"""
from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]

# Symbols that only ever existed to reach a disk-backed DuckDB database.
LEGACY_RUNNER_SYMBOLS = (
    "DEFAULT_STREAMING_DB_PATH",
    "save_ticks_to_storage",
    "init_streaming_db",
    "get_streaming_db_connection",
    "get_streaming_database_symbols_from_db",
    "get_streaming_symbol_map_from_db",
)


def _clear_backend_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """Remove every environment variable that could select a backend."""
    monkeypatch.delenv("TICK_LAKE_ROOT", raising=False)
    monkeypatch.delenv("DATA_DIR", raising=False)


def test_engine_without_a_lake_root_is_rejected(monkeypatch: pytest.MonkeyPatch) -> None:
    """No resolvable lake root => hard failure, never a silent disk DB.

    Lake-root resolution is stubbed to fail so the test is deterministic on every
    host (the owner's machine has a hardware mount that resolution would find).
    """
    _clear_backend_env(monkeypatch)
    from src.storage import config as storage_config
    from src.stream.runner import StreamingEngine

    def no_root_available(*args, **kwargs):
        raise storage_config.StorageConfigError("no lake root on this host")

    monkeypatch.setattr(storage_config, "resolve_tick_lake_root", no_root_available)

    with pytest.raises((TypeError, ValueError, RuntimeError)) as excinfo:
        StreamingEngine()
    assert "lake root" in str(excinfo.value).lower()


def test_engine_rejects_the_legacy_db_path_argument(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """The `db_path` backend is removed, not merely disabled."""
    _clear_backend_env(monkeypatch)
    from src.stream.runner import StreamingEngine

    legacy_db = tmp_path / "legacy" / "streaming.duckdb"
    with pytest.raises(TypeError):
        StreamingEngine(db_path=str(legacy_db))
    assert not legacy_db.exists(), "constructing the engine must not touch a disk database"


def test_engine_construction_creates_no_duckdb_file(tmp_path: Path) -> None:
    """A lake-rooted engine writes Parquet and nothing else."""
    from src.stream.runner import StreamingEngine

    lake_root = tmp_path / "tick_lake"
    engine = StreamingEngine(lake_root=lake_root)
    try:
        assert engine.lake_root == lake_root.resolve()
        assert not list(tmp_path.rglob("*.duckdb")), "no .duckdb artifact may be created"
    finally:
        engine.stop()


def test_importing_the_runner_loads_no_disk_database_module() -> None:
    """Startup regression: importing the runner must not pull in the disk-DB layer."""
    script = (
        "import sys; import src.stream.runner; "
        "loaded = sorted(m for m in sys.modules if m == 'src.database' or m.startswith('src.database.')); "
        "print('|'.join(loaded)); "
        "raise SystemExit(1 if loaded else 0)"
    )
    env = dict(os.environ)
    env.pop("TICK_LAKE_ROOT", None)
    env["TICK_LAKE_ROOT"] = str(REPO_ROOT / ".nonexistent-lake-for-import-check")

    result = subprocess.run(
        [sys.executable, "-c", script],
        cwd=str(REPO_ROOT),
        capture_output=True,
        text=True,
        env=env,
        timeout=180,
    )
    assert result.returncode == 0, (
        "disk-database modules were imported: "
        f"{result.stdout.strip()} | {result.stderr.strip()[-400:]}"
    )


def test_runner_source_retains_no_legacy_backend_reference() -> None:
    """No symbol that can open a disk-backed DuckDB database survives in the runner."""
    source = (REPO_ROOT / "src" / "stream" / "runner.py").read_text(encoding="utf-8")
    remaining = [name for name in LEGACY_RUNNER_SYMBOLS if name in source]
    assert not remaining, f"legacy backend references remain in runner.py: {remaining}"


# --- STOR-01: the last reachable disk-database fallbacks ----------------------
# src/dashboard/analytics.py used to end every lake-first branch with
# `client = get_streaming_db_connection(read_only=True)`, so an unavailable lake
# silently opened a disk database instead of reporting the problem.

ANALYTICS_MODULE = REPO_ROOT / "src" / "dashboard" / "analytics.py"

LEGACY_ANALYTICS_SYMBOLS = (
    "src.database",
    "get_streaming_db_connection",
    "get_historical_db_connection",
    "DEFAULT_STREAMING_DB_PATH",
    "DEFAULT_HISTORICAL_DB_PATH",
)


def test_dashboard_analytics_retains_no_disk_database_reference() -> None:
    source = ANALYTICS_MODULE.read_text(encoding="utf-8")
    for needle in LEGACY_ANALYTICS_SYMBOLS:
        assert needle not in source, f"disk-database reference remains in analytics.py: {needle}"


def test_analytics_report_a_missing_lake_instead_of_a_disk_fallback(tmp_path, monkeypatch) -> None:
    """An explicitly selected but absent lake must fail loudly."""
    from src.dashboard import analytics

    monkeypatch.setenv("TICK_LAKE_ROOT", str(tmp_path / "absent_lake"))
    monkeypatch.setenv("DATA_DIR", str(tmp_path))

    with pytest.raises(Exception) as excinfo:
        analytics.get_stream_tape(limit=5)

    message = str(excinfo.value).lower()
    assert "lake" in message, f"unexpected failure for a missing lake: {excinfo.value}"


def test_no_analytics_function_opens_a_default_disk_database(monkeypatch) -> None:
    """With no lake selected at all, the functions must not reach a disk database."""
    _clear_backend_env(monkeypatch)
    from src.dashboard import analytics

    for call in (
        lambda: analytics.get_stream_tape(limit=5),
        lambda: analytics.get_stream_status(),
        lambda: analytics.discover_available_weeks(),
    ):
        try:
            result = call()
        except Exception:
            continue
        if isinstance(result, dict):
            assert result.get("database") != "streaming" or not result.get("ticks")
