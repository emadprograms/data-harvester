"""INCIDENT-2026-10-06 — the migration must carry the symbol set with the data.

The Phase 49 gate exported tick rows into Parquet and left the lake registry
empty, so the streamer had nothing to subscribe to and idled while every status
light stayed green. A migrated lake must come out of the gate with its symbols
registered, otherwise the streamer refuses to start (that is the corrected,
loud behavior) — this test pins the seeding half.
"""
from __future__ import annotations

from datetime import datetime
from pathlib import Path

import duckdb
import pytest

from src.storage.config import init_tick_lake, resolve_tick_lake_root
from src.storage.registry import SymbolRegistry
from tools.migrate_streaming_to_parquet import main as migration_main


@pytest.fixture()
def legacy_source(tmp_path: Path) -> Path:
    """A stand-in for the retired DuckDB streaming database."""
    source_db = tmp_path / "streaming.duckdb"
    con = duckdb.connect(str(source_db))
    con.execute(
        """
        CREATE TABLE tick_data (
            timestamp TIMESTAMP NOT NULL,
            symbol VARCHAR NOT NULL,
            price DOUBLE NOT NULL,
            volume DOUBLE,
            bid DOUBLE,
            ask DOUBLE,
            source VARCHAR,
            session VARCHAR DEFAULT 'REG'
        )
        """
    )
    rows = []
    for symbol in ("AAPL", "MSFT", "NVDA"):
        base = datetime(2026, 7, 10, 14, 30, 0)
        for i in range(6):
            rows.append((base.replace(second=i), symbol, 100.0 + i, 1.0, 99.9, 100.1, "CAPITAL", "REG"))
    con.executemany("INSERT INTO tick_data VALUES (?, ?, ?, ?, ?, ?, ?, ?)", rows)
    con.close()
    return source_db


def test_mode_all_leaves_the_registry_holding_the_migrated_symbols(tmp_path, legacy_source, monkeypatch):
    lake_root = tmp_path / "lake"
    init_tick_lake(lake_root)
    monkeypatch.setenv("TICK_LAKE_ROOT", str(lake_root))

    # The post-migration state that caused the outage: a registry listing nothing.
    from src.storage.registry import init_registry
    init_registry(lake_root)
    assert SymbolRegistry(root=lake_root).get_active_symbols() == []

    exit_code = migration_main([
        "--source-db", str(legacy_source),
        "--lake-root", str(lake_root),
        "--mode", "all",
        "--chunk-size", "50",
    ])
    assert exit_code == 0

    symbols = sorted(entry.symbol for entry in SymbolRegistry(root=lake_root).get_active_symbols())
    assert symbols == ["AAPL", "MSFT", "NVDA"], (
        "migrated symbols must land in the registry, or the streamer subscribes to nothing"
    )
    # And the migrated rows are on disk.
    assert list((lake_root / "ticks").glob("symbol=*/date=*/*.parquet"))


def test_no_seed_registry_opt_out_is_honored(tmp_path, legacy_source, monkeypatch):
    lake_root = tmp_path / "lake_opt_out"
    init_tick_lake(lake_root)
    monkeypatch.setenv("TICK_LAKE_ROOT", str(lake_root))

    exit_code = migration_main([
        "--source-db", str(legacy_source),
        "--lake-root", str(lake_root),
        "--mode", "all",
        "--chunk-size", "50",
        "--no-seed-registry",
    ])
    assert exit_code == 0
    # No registry file was created by the migration in this mode.
    assert not SymbolRegistry(root=lake_root).path.exists()


def test_seeded_lake_lets_the_registry_resolve_as_the_authority(tmp_path, legacy_source, monkeypatch):
    """After seeding, the symbols resolve through the lake root the readers use."""
    lake_root = tmp_path / "lake_resolve"
    init_tick_lake(lake_root)
    monkeypatch.setenv("TICK_LAKE_ROOT", str(lake_root))

    assert migration_main([
        "--source-db", str(legacy_source),
        "--lake-root", str(lake_root),
        "--mode", "all",
        "--chunk-size", "50",
    ]) == 0

    assert resolve_tick_lake_root() == lake_root.resolve()
    assert len(SymbolRegistry(root=resolve_tick_lake_root()).get_active_symbols()) == 3
