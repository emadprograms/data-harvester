"""
Comprehensive stress, schema-drift, crash-interruption, and two-way EXCEPT ALL
fuzz testing suite for historical DuckDB to Parquet migration tooling:
tools/migrate_streaming_to_parquet.py

Milestone v4.1 - Phase 26 (Migration Tooling Rehearsal & Fuzz Tests).

Fulfills requirements:
- TEST-P26-01: Migration of corrupt / partial legacy DuckDB tables and schema drift.
- TEST-P26-02: Simulated crash interruption across all migration modes (plan, export, verify, publish).
- TEST-P26-03: Two-way EXCEPT ALL fuzz testing with synthetic data corruption and precision mismatch detection.
"""
from datetime import datetime, timedelta
import json
import os
from pathlib import Path
import sys
from typing import Any, Callable, Dict, List, Optional
import uuid

import duckdb
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
import pytest

from src.storage.config import init_tick_lake
from src.storage.publication import LakeOwnershipError, LakePublisherLock
from src.storage.reader import TickLakeReader
from src.storage.schema import SchemaValidationError
from tools.migrate_streaming_to_parquet import (
    MigrationConfig,
    MigrationOrchestrator,
    MigrationPlan,
    MigrationState,
    VerificationResult,
    _atomic_save_json,
    main,
)


# ============================================================================
# Helpers and Fixtures
# ============================================================================

def _populate_duckdb(
    db_path: Path,
    table_name: str = "tick_data",
    rows: Optional[List[tuple]] = None,
    create_sql: Optional[str] = None,
) -> None:
    """Helper to populate a test DuckDB file with specified schema and rows."""
    con = duckdb.connect(str(db_path))
    try:
        if create_sql:
            con.execute(create_sql)
        else:
            con.execute(f"""
                CREATE TABLE {table_name} (
                    timestamp TIMESTAMP NOT NULL,
                    symbol VARCHAR NOT NULL,
                    price DOUBLE NOT NULL,
                    volume DOUBLE,
                    bid DOUBLE,
                    ask DOUBLE,
                    source VARCHAR,
                    session VARCHAR DEFAULT 'REG'
                )
            """)

        if rows:
            placeholders = ",".join(["?"] * len(rows[0]))
            con.executemany(f"INSERT INTO {table_name} VALUES ({placeholders})", rows)
    finally:
        con.close()


def _mutate_parquet_file(chunk_path: Path, mutate_fn: Callable[[pa.Table], pa.Table]) -> None:
    """Read a Parquet chunk table, apply a mutation function, and re-write to disk."""
    table = pq.read_table(chunk_path)
    mutated = mutate_fn(table)
    pq.write_table(mutated, chunk_path, compression="snappy")


# ============================================================================
# Group 1: TestMigrationCorruptAndSchemaDrift (TEST-P26-01)
# ============================================================================

class TestMigrationCorruptAndSchemaDrift:
    """Validates resilience against corrupt/partial legacy tables, aliases, and schema drift."""

    def test_table_autodetection_candidates(self, tmp_path: Path):
        """Verify precedence ordering and auto-detection across tables and --source-table override."""
        # Case a: DB with only streaming_ticks table
        db_single = tmp_path / "streaming_ticks.duckdb"
        t0 = datetime(2026, 7, 10, 14, 30, 0)
        rows_a = [(t0 + timedelta(seconds=i), "AAPL", 150.0 + i, 10.0, 149.9, 150.1, "CAPITAL", "REG") for i in range(5)]
        _populate_duckdb(db_single, table_name="streaming_ticks", rows=rows_a)

        lake_a = tmp_path / "lake_a"
        init_tick_lake(lake_a)
        orch_a = MigrationOrchestrator(MigrationConfig(source_db=db_single, lake_root=lake_a, mode="plan"))
        plan_a = orch_a.plan()
        assert len(plan_a.partitions) == 1
        assert plan_a.partitions[0]["symbol"] == "AAPL"
        assert plan_a.total_rows == 5

        # Case b: DB with both streaming_ticks and ticks (ticks takes precedence over streaming_ticks)
        db_multi = tmp_path / "multi_tables.duckdb"
        con = duckdb.connect(str(db_multi))
        con.execute("CREATE TABLE streaming_ticks (timestamp TIMESTAMP, symbol VARCHAR, price DOUBLE)")
        con.execute("CREATE TABLE ticks (timestamp TIMESTAMP, symbol VARCHAR, price DOUBLE)")
        for i in range(10):
            con.execute("INSERT INTO streaming_ticks VALUES (?, 'MSFT', ?)", [t0 + timedelta(seconds=i), 400.0 + i])
            con.execute("INSERT INTO ticks VALUES (?, 'NVDA', ?)", [t0 + timedelta(seconds=i), 120.0 + i])
        con.close()

        lake_b = tmp_path / "lake_b"
        init_tick_lake(lake_b)
        orch_b = MigrationOrchestrator(MigrationConfig(source_db=db_multi, lake_root=lake_b, mode="plan"))
        plan_b = orch_b.plan()
        # In precedence ["tick_data", "ticks", "streaming_ticks"], "ticks" wins
        assert len(plan_b.partitions) == 1
        assert plan_b.partitions[0]["symbol"] == "NVDA"

        # Case c: Custom table with explicit --source-table override
        db_custom = tmp_path / "custom.duckdb"
        _populate_duckdb(
            db_custom,
            table_name="custom_quotes",
            rows=[(t0 + timedelta(seconds=i), "TSLA", 250.0 + i, 10.0, 249.0, 251.0, "CAPITAL", "REG") for i in range(8)],
        )
        lake_c = tmp_path / "lake_c"
        init_tick_lake(lake_c)
        orch_c = MigrationOrchestrator(MigrationConfig(
            source_db=db_custom,
            lake_root=lake_c,
            source_table="custom_quotes",
            mode="plan",
        ))
        plan_c = orch_c.plan()
        assert len(plan_c.partitions) == 1
        assert plan_c.partitions[0]["symbol"] == "TSLA"
        assert plan_c.total_rows == 8

    def test_schema_drift_column_aliases(self, tmp_path: Path):
        """Migrate legacy table with column name drift: ts, sym, last, vol."""
        db_path = tmp_path / "alias_drift.duckdb"
        create_sql = "CREATE TABLE streaming_ticks (ts TIMESTAMP, sym VARCHAR, last DOUBLE, vol DOUBLE)"
        t0 = datetime(2026, 7, 10, 14, 30, 0)
        rows = [(t0 + timedelta(seconds=i), "AAPL", 150.0 + (i * 0.1), 100.0) for i in range(60)]
        _populate_duckdb(db_path, table_name="streaming_ticks", rows=rows, create_sql=create_sql)

        lake_root = tmp_path / "lake_alias"
        init_tick_lake(lake_root)

        config = MigrationConfig(
            source_db=db_path,
            lake_root=lake_root,
            chunk_size=50,
            mode="all",
        )
        orch = MigrationOrchestrator(config)
        ret = orch.run()
        assert ret == 0

        # Verify lake reader queryability on published partition
        reader = TickLakeReader(lake_root)
        ticks = reader.query_ticks(symbol="AAPL")
        assert len(ticks) == 60
        assert ticks[0]["price"] == 150.0
        assert ticks[-1]["price"] == 150.0 + (59 * 0.1)

    def test_schema_drift_missing_optional_columns(self, tmp_path: Path):
        """Migrate legacy table lacking volume, bid, ask, source, session (only timestamp, symbol, price)."""
        db_path = tmp_path / "missing_optional.duckdb"
        create_sql = "CREATE TABLE ticks (timestamp TIMESTAMP, symbol VARCHAR, price DOUBLE)"
        t0 = datetime(2026, 7, 10, 14, 30, 0)
        rows = [(t0 + timedelta(seconds=i), "MSFT", 420.0 + (i * 0.2)) for i in range(50)]
        _populate_duckdb(db_path, table_name="ticks", rows=rows, create_sql=create_sql)

        lake_root = tmp_path / "lake_optional"
        init_tick_lake(lake_root)

        config = MigrationConfig(
            source_db=db_path,
            lake_root=lake_root,
            mode="all",
        )
        orch = MigrationOrchestrator(config)
        ret = orch.run()
        assert ret == 0

        # Verify published Parquet contains synthesized default columns
        chunk_files = list((lake_root / "ticks" / "symbol=MSFT" / "date=2026-07-10").glob("*.parquet"))
        assert len(chunk_files) >= 1
        table = pq.read_table(chunk_files[0])
        assert pc.all(pc.is_null(table["volume"])).as_py() is True
        assert pc.all(pc.is_null(table["bid"])).as_py() is True
        assert pc.all(pc.is_null(table["ask"])).as_py() is True
        assert pc.all(pc.equal(table["source"], "LEGACY")).as_py() is True
        assert pc.all(pc.equal(table["session"], "REG")).as_py() is True

    def test_schema_drift_varchar_timestamp(self, tmp_path: Path):
        """Source table has timestamp stored as ISO-8601 string (VARCHAR)."""
        db_path = tmp_path / "varchar_ts.duckdb"
        create_sql = "CREATE TABLE tick_data (timestamp VARCHAR, symbol VARCHAR, price DOUBLE)"
        t0 = datetime(2026, 7, 10, 14, 30, 0)
        rows = [((t0 + timedelta(seconds=i)).isoformat(), "AAPL", 150.0 + i) for i in range(30)]
        _populate_duckdb(db_path, table_name="tick_data", rows=rows, create_sql=create_sql)

        lake_root = tmp_path / "lake_varchar"
        init_tick_lake(lake_root)

        config = MigrationConfig(
            source_db=db_path,
            lake_root=lake_root,
            mode="all",
        )
        orch = MigrationOrchestrator(config)
        ret = orch.run()
        assert ret == 0

        # Check timestamp type in published Parquet
        chunk_files = list((lake_root / "ticks" / "symbol=AAPL" / "date=2026-07-10").glob("*.parquet"))
        assert len(chunk_files) >= 1
        table = pq.read_table(chunk_files[0])
        assert pa.types.is_timestamp(table.schema.field("timestamp").type)

    def test_schema_drift_extra_unrelated_columns(self, tmp_path: Path):
        """Source table has extra metadata columns (trade_id, routing_venue, flags)."""
        db_path = tmp_path / "extra_cols.duckdb"
        create_sql = """
            CREATE TABLE tick_data (
                timestamp TIMESTAMP,
                symbol VARCHAR,
                price DOUBLE,
                volume DOUBLE,
                bid DOUBLE,
                ask DOUBLE,
                source VARCHAR,
                session VARCHAR,
                trade_id BIGINT,
                routing_venue VARCHAR,
                flags VARCHAR
            )
        """
        t0 = datetime(2026, 7, 10, 14, 30, 0)
        rows = [
            (t0 + timedelta(seconds=i), "NVDA", 120.0 + i, 50.0, 119.9, 120.1, "CAPITAL", "REG", 1000 + i, "ARCA", "NORMAL")
            for i in range(25)
        ]
        _populate_duckdb(db_path, table_name="tick_data", rows=rows, create_sql=create_sql)

        lake_root = tmp_path / "lake_extra"
        init_tick_lake(lake_root)

        config = MigrationConfig(source_db=db_path, lake_root=lake_root, mode="all")
        orch = MigrationOrchestrator(config)
        ret = orch.run()
        assert ret == 0

        # Extra columns should be stripped, retaining canonical 9 columns (8 + ingest_id)
        chunk_files = list((lake_root / "ticks" / "symbol=NVDA" / "date=2026-07-10").glob("*.parquet"))
        table = pq.read_table(chunk_files[0])
        assert "trade_id" not in table.column_names
        assert "routing_venue" not in table.column_names
        assert "flags" not in table.column_names
        assert "ingest_id" in table.column_names

    def test_missing_required_column_raises_schema_validation_error(self, tmp_path: Path):
        """Source table is missing required price column."""
        db_path = tmp_path / "missing_price.duckdb"
        create_sql = "CREATE TABLE tick_data (timestamp TIMESTAMP, symbol VARCHAR)"
        _populate_duckdb(db_path, table_name="tick_data", rows=[(datetime(2026, 7, 10, 14, 30, 0), "AAPL")], create_sql=create_sql)

        lake_root = tmp_path / "lake_req"
        init_tick_lake(lake_root)
        orch = MigrationOrchestrator(MigrationConfig(source_db=db_path, lake_root=lake_root, mode="plan"))

        with pytest.raises(SchemaValidationError, match="Required column 'price' missing"):
            orch.plan()

    def test_corrupt_duckdb_binary_graceful_exit(self, tmp_path: Path):
        """Handle completely corrupted DuckDB binary file gracefully."""
        corrupt_db = tmp_path / "corrupt.duckdb"
        corrupt_db.write_bytes(os.urandom(512))

        lake_root = tmp_path / "lake_corrupt"
        init_tick_lake(lake_root)

        exit_code = main(["--source-db", str(corrupt_db), "--lake-root", str(lake_root), "--mode", "all"])
        assert exit_code == 1
        # Lake should have no published ticks
        assert not (lake_root / "ticks").exists() or len(list((lake_root / "ticks").iterdir())) == 0

    def test_zero_byte_duckdb_file_graceful_exit(self, tmp_path: Path):
        """Handle 0-byte DuckDB file gracefully."""
        zero_db = tmp_path / "zero.duckdb"
        zero_db.touch()

        lake_root = tmp_path / "lake_zero"
        init_tick_lake(lake_root)

        orch = MigrationOrchestrator(MigrationConfig(source_db=zero_db, lake_root=lake_root, mode="all"))
        assert orch.run() == 1

    def test_dirty_rows_null_timestamp_rejected(self, tmp_path: Path):
        """Prevent silent dropping of rows with timestamp IS NULL in plan stage."""
        db_path = tmp_path / "null_ts.duckdb"
        t0 = datetime(2026, 7, 10, 14, 30, 0)
        rows = [(t0 + timedelta(seconds=i), "AAPL", 150.0 + i, 10.0, 149.0, 151.0, "CAPITAL", "REG") for i in range(20)]
        rows.append((None, "AAPL", 170.0, 10.0, 169.0, 171.0, "CAPITAL", "REG"))
        _populate_duckdb(
            db_path,
            table_name="tick_data",
            rows=rows,
            create_sql="CREATE TABLE tick_data (timestamp TIMESTAMP, symbol VARCHAR, price DOUBLE, volume DOUBLE, bid DOUBLE, ask DOUBLE, source VARCHAR, session VARCHAR)",
        )

        lake_root = tmp_path / "lake_null_ts"
        init_tick_lake(lake_root)
        orch = MigrationOrchestrator(MigrationConfig(source_db=db_path, lake_root=lake_root, mode="plan"))

        with pytest.raises(SchemaValidationError, match="dirty rows with NULL timestamp or empty symbol"):
            orch.plan()

    def test_dirty_rows_empty_symbol_rejected(self, tmp_path: Path):
        """Reject rows with symbol = '' or whitespace in plan stage."""
        db_path = tmp_path / "empty_sym.duckdb"
        t0 = datetime(2026, 7, 10, 14, 30, 0)
        rows = [
            (t0, "AAPL", 150.0, 10.0, 149.0, 151.0, "CAPITAL", "REG"),
            (t0 + timedelta(seconds=1), "", 150.1, 10.0, 149.0, 151.0, "CAPITAL", "REG"),
            (t0 + timedelta(seconds=2), "   ", 150.2, 10.0, 149.0, 151.0, "CAPITAL", "REG"),
        ]
        _populate_duckdb(db_path, table_name="tick_data", rows=rows)

        lake_root = tmp_path / "lake_empty_sym"
        init_tick_lake(lake_root)
        orch = MigrationOrchestrator(MigrationConfig(source_db=db_path, lake_root=lake_root, mode="plan"))

        with pytest.raises(SchemaValidationError, match="dirty rows with NULL timestamp or empty symbol"):
            orch.plan()

    def test_dirty_rows_negative_and_nan_prices_rejected(self, tmp_path: Path):
        """Reject rows with negative or NaN prices during export."""
        db_path = tmp_path / "bad_prices.duckdb"
        t0 = datetime(2026, 7, 10, 14, 30, 0)
        rows = [
            (t0, "AAPL", 150.0, 10.0, 149.0, 151.0, "CAPITAL", "REG"),
            (t0 + timedelta(seconds=1), "AAPL", -50.0, 10.0, 149.0, 151.0, "CAPITAL", "REG"),
            (t0 + timedelta(seconds=2), "AAPL", float("nan"), 10.0, 149.0, 151.0, "CAPITAL", "REG"),
        ]
        _populate_duckdb(db_path, table_name="tick_data", rows=rows)

        lake_root = tmp_path / "lake_bad_prices"
        init_tick_lake(lake_root)
        orch = MigrationOrchestrator(MigrationConfig(source_db=db_path, lake_root=lake_root, mode="export"))

        with pytest.raises(SchemaValidationError):
            orch.export()

    def test_out_of_order_source_timestamps_monotonically_ordered(self, tmp_path: Path):
        """Source table has timestamps out of order; exported Parquet must be monotonically ordered."""
        db_path = tmp_path / "out_of_order.duckdb"
        t0 = datetime(2026, 7, 10, 14, 30, 0)
        # Insert in reverse chronological order
        rows = [
            (t0 + timedelta(seconds=99 - i), "AAPL", 150.0 + (99 - i) * 0.1, 10.0, 149.9, 150.1, "CAPITAL", "REG")
            for i in range(100)
        ]
        _populate_duckdb(db_path, table_name="tick_data", rows=rows)

        lake_root = tmp_path / "lake_order"
        init_tick_lake(lake_root)

        config = MigrationConfig(source_db=db_path, lake_root=lake_root, chunk_size=50, mode="all")
        orch = MigrationOrchestrator(config)
        assert orch.run() == 0

        # Read back chunks and verify timestamps are strictly non-decreasing
        chunk_files = sorted((lake_root / "ticks" / "symbol=AAPL" / "date=2026-07-10").glob("*.parquet"))
        prev_ts = None
        total_ticks = 0
        for cf in chunk_files:
            table = pq.read_table(cf)
            ts_col = table["timestamp"].to_pylist()
            total_ticks += len(ts_col)
            for curr_ts in ts_col:
                if prev_ts is not None:
                    assert curr_ts >= prev_ts
                prev_ts = curr_ts
        assert total_ticks == 100


# ============================================================================
# Group 2: TestMigrationCrashInterruptionAndResume (TEST-P26-02)
# ============================================================================

class TestMigrationCrashInterruptionAndResume:
    """Validates crash recovery and consistency across plan, export, verify, and publish."""

    def test_plan_mode_interrupted_leaves_disk_consistent(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
        """Plan mode interrupted mid-execution does not leave torn plan.json."""
        db_path = tmp_path / "plan_crash.duckdb"
        t0 = datetime(2026, 7, 10, 14, 30, 0)
        _populate_duckdb(db_path, rows=[(t0 + timedelta(seconds=i), "AAPL", 150.0 + i, 10.0, 149.0, 151.0, "CAPITAL", "REG") for i in range(20)])

        lake_root = tmp_path / "lake_plan_crash"
        init_tick_lake(lake_root)
        orch = MigrationOrchestrator(MigrationConfig(source_db=db_path, lake_root=lake_root, mode="plan"))

        # Invalidate/fail during json save
        original_atomic_save = _atomic_save_json

        def faulty_save(data, path):
            if Path(path) == orch.plan_file:
                raise KeyboardInterrupt("Simulated user interrupt during plan.json write")
            original_atomic_save(data, path)

        monkeypatch.setattr("tools.migrate_streaming_to_parquet._atomic_save_json", faulty_save)

        with pytest.raises(KeyboardInterrupt):
            orch.plan()

        assert not orch.plan_file.exists()

        # Resumed execution without monkeypatch completes cleanly
        monkeypatch.undo()
        plan = orch.plan()
        assert orch.plan_file.is_file()
        assert plan.total_rows == 20

    def test_export_crash_mid_partition_chunks_and_resume(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
        """Partition with 3 chunks (120 rows, chunk_size=50) crashes on chunk 2 and resumes cleanly."""
        db_path = tmp_path / "export_crash.duckdb"
        t0 = datetime(2026, 7, 10, 14, 30, 0)
        rows = [(t0 + timedelta(seconds=i), "AAPL", 150.0 + (i * 0.05), 10.0, 149.9, 150.1, "CAPITAL", "REG") for i in range(120)]
        _populate_duckdb(db_path, rows=rows)

        lake_root = tmp_path / "lake_export_crash"
        init_tick_lake(lake_root)

        config = MigrationConfig(source_db=db_path, lake_root=lake_root, chunk_size=50, mode="export")
        orch = MigrationOrchestrator(config)

        # Crash after chunk 1 is written
        orig_write_table = pq.write_table
        chunk_count = 0

        def crash_on_chunk_2(table, where, **kwargs):
            nonlocal chunk_count
            chunk_count += 1
            if chunk_count == 2:
                raise RuntimeError("Simulated crash on chunk 2 write")
            orig_write_table(table, where, **kwargs)

        monkeypatch.setattr(pq, "write_table", crash_on_chunk_2)

        with pytest.raises(RuntimeError, match="Simulated crash on chunk 2 write"):
            orch.export()

        # Verify staging has chunk 1, but state does NOT mark partition completed
        staging_dir = lake_root / "_migration" / "staging" / "ticks" / "symbol=AAPL" / "date=2026-07-10"
        assert (staging_dir / "chunk_000001.parquet").exists()
        state = MigrationState.load(orch.state_file)
        part_key = "symbol=AAPL/date=2026-07-10"
        assert part_key not in state.partitions or state.partitions[part_key].get("status") != "COMPLETED"

        # Resume export: uncompleted partition is re-exported completely
        monkeypatch.undo()
        config_resume = MigrationConfig(source_db=db_path, lake_root=lake_root, chunk_size=50, mode="export", resume=True)
        orch_resume = MigrationOrchestrator(config_resume)
        state_resumed = orch_resume.export()

        assert state_resumed.partitions[part_key]["status"] == "COMPLETED"
        assert len(state_resumed.partitions[part_key]["chunks"]) == 3
        assert state_resumed.partitions[part_key]["row_count"] == 120

        # Verify exact 3 chunks exist with row counts 50, 50, 20
        c1_rows = pq.ParquetFile(staging_dir / "chunk_000001.parquet").metadata.num_rows
        c2_rows = pq.ParquetFile(staging_dir / "chunk_000002.parquet").metadata.num_rows
        c3_rows = pq.ParquetFile(staging_dir / "chunk_000003.parquet").metadata.num_rows
        assert c1_rows == 50
        assert c2_rows == 50
        assert c3_rows == 20

    def test_export_crash_multi_partition_resume_skips_completed(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
        """Multi-partition export crash resumes by skipping completed partitions."""
        db_path = tmp_path / "multi_part_resume.duckdb"
        t_d1 = datetime(2026, 7, 10, 14, 30, 0)
        t_d2 = datetime(2026, 7, 11, 14, 30, 0)
        rows = []
        # AAPL 2026-07-10: 60 ticks
        rows.extend([(t_d1 + timedelta(seconds=i), "AAPL", 150.0 + i, 10.0, 149.0, 151.0, "CAPITAL", "REG") for i in range(60)])
        # AAPL 2026-07-11: 20 ticks
        rows.extend([(t_d2 + timedelta(seconds=i), "AAPL", 155.0 + i, 10.0, 154.0, 156.0, "CAPITAL", "REG") for i in range(20)])
        # MSFT 2026-07-10: 30 ticks
        rows.extend([(t_d1 + timedelta(seconds=i), "MSFT", 400.0 + i, 10.0, 399.0, 401.0, "CAPITAL", "REG") for i in range(30)])
        _populate_duckdb(db_path, rows=rows)

        lake_root = tmp_path / "lake_multi_resume"
        init_tick_lake(lake_root)

        # 1. Start export of all partitions with crash injected on partition 2 (2026-07-11)
        cfg_all = MigrationConfig(
            source_db=db_path,
            lake_root=lake_root,
            chunk_size=100,
            mode="export",
        )
        orch = MigrationOrchestrator(cfg_all)
        orch.plan()

        orig_write_table = pq.write_table

        def crash_on_partition_2(table, where, **kwargs):
            if "2026-07-11" in str(where):
                raise RuntimeError("Injected failure at start of AAPL 2026-07-11")
            orig_write_table(table, where, **kwargs)

        monkeypatch.setattr(pq, "write_table", crash_on_partition_2)

        with pytest.raises(RuntimeError, match="Injected failure at start of AAPL 2026-07-11"):
            orch.export()

        state1 = MigrationState.load(orch.state_file)
        assert "symbol=AAPL/date=2026-07-10" in state1.partitions
        assert state1.partitions["symbol=AAPL/date=2026-07-10"]["status"] == "COMPLETED"

        aapl_d1_chunk = (lake_root / "_migration" / "staging" / "ticks" / "symbol=AAPL" / "date=2026-07-10" / "chunk_000001.parquet")
        assert aapl_d1_chunk.is_file()
        mtime_before = aapl_d1_chunk.stat().st_mtime_ns

        # 2. Resume export with all symbols and dates
        monkeypatch.undo()
        cfg_resume = MigrationConfig(
            source_db=db_path,
            lake_root=lake_root,
            chunk_size=100,
            mode="export",
            resume=True,
        )
        orch_resume = MigrationOrchestrator(cfg_resume)
        state_all = orch_resume.export()

        # AAPL 2026-07-10 chunk must NOT have been modified
        mtime_after = aapl_d1_chunk.stat().st_mtime_ns
        assert mtime_before == mtime_after

        # The other two partitions must now be completed
        assert state_all.partitions["symbol=AAPL/date=2026-07-11"]["status"] == "COMPLETED"
        assert state_all.partitions["symbol=MSFT/date=2026-07-10"]["status"] == "COMPLETED"

    def test_export_torn_chunk_file_repair_on_resume(self, tmp_path: Path):
        """Simulate torn 40-byte truncated chunk file repair on export resumption."""
        db_path = tmp_path / "torn_chunk.duckdb"
        t0 = datetime(2026, 7, 10, 14, 30, 0)
        rows = [(t0 + timedelta(seconds=i), "AAPL", 150.0 + (i * 0.05), 10.0, 149.9, 150.1, "CAPITAL", "REG") for i in range(120)]
        _populate_duckdb(db_path, rows=rows)

        lake_root = tmp_path / "lake_torn"
        init_tick_lake(lake_root)

        config = MigrationConfig(source_db=db_path, lake_root=lake_root, chunk_size=50, mode="export")
        orch = MigrationOrchestrator(config)
        orch.plan()

        # Plant a torn 40-byte file in staging for uncompleted partition
        staging_dir = lake_root / "_migration" / "staging" / "ticks" / "symbol=AAPL" / "date=2026-07-10"
        staging_dir.mkdir(parents=True, exist_ok=True)
        torn_file = staging_dir / "chunk_000002.parquet"
        torn_file.write_bytes(b"PAR1" + b"X" * 36)

        # Run export with resume=True: uncompleted partition purges torn chunks before export
        orch.export()

        # Verify all 3 chunks are valid Parquet files
        chunks = sorted(staging_dir.glob("*.parquet"))
        assert len(chunks) == 3
        for c in chunks:
            pf = pq.ParquetFile(c)
            assert pf.metadata.num_rows > 0

    def test_export_invalidates_stale_verification_receipt(self, tmp_path: Path):
        """Re-exporting invalidates any existing verification.json."""
        db_path = tmp_path / "invalidation.duckdb"
        t0 = datetime(2026, 7, 10, 14, 30, 0)
        _populate_duckdb(db_path, rows=[(t0 + timedelta(seconds=i), "AAPL", 150.0 + i, 10.0, 149.0, 151.0, "CAPITAL", "REG") for i in range(20)])

        lake_root = tmp_path / "lake_invalidation"
        init_tick_lake(lake_root)

        orch = MigrationOrchestrator(MigrationConfig(source_db=db_path, lake_root=lake_root, mode="all"))
        orch.plan()
        orch.export()
        vres = orch.verify()
        assert vres.status == "PASSED"
        assert orch.verification_file.is_file()

        # Running export() again must invalidate and delete verification.json
        orch.export()
        assert not orch.verification_file.exists()

    def test_verify_interrupted_blocks_publish(self, tmp_path: Path):
        """Interrupted verify or missing verification.json blocks publish completely."""
        db_path = tmp_path / "verify_block.duckdb"
        t0 = datetime(2026, 7, 10, 14, 30, 0)
        _populate_duckdb(db_path, rows=[(t0 + timedelta(seconds=i), "AAPL", 150.0 + i, 10.0, 149.0, 151.0, "CAPITAL", "REG") for i in range(20)])

        lake_root = tmp_path / "lake_verify_block"
        init_tick_lake(lake_root)

        orch = MigrationOrchestrator(MigrationConfig(source_db=db_path, lake_root=lake_root, mode="all"))
        orch.plan()
        orch.export()

        # Ensure verification.json is missing
        if orch.verification_file.is_file():
            orch.verification_file.unlink()

        with pytest.raises(RuntimeError, match="Publish aborted: verification must pass before publishing."):
            orch.publish()

        # Ensure zero files promoted to ticks/
        published_chunks = list((lake_root / "ticks").glob("**/*.parquet"))
        assert len(published_chunks) == 0

    def test_publish_crash_mid_chunk_promotion_resumption(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
        """Publish crashes after chunk 1 is promoted; resumption promotes chunks 2-3 and writes accurate receipt."""
        db_path = tmp_path / "publish_crash.duckdb"
        t0 = datetime(2026, 7, 10, 14, 30, 0)
        rows = [(t0 + timedelta(seconds=i), "AAPL", 150.0 + (i * 0.05), 10.0, 149.9, 150.1, "CAPITAL", "REG") for i in range(120)]
        _populate_duckdb(db_path, rows=rows)

        lake_root = tmp_path / "lake_pub_crash"
        init_tick_lake(lake_root)

        config = MigrationConfig(source_db=db_path, lake_root=lake_root, chunk_size=50, mode="all")
        orch = MigrationOrchestrator(config)
        orch.plan()
        orch.export()
        vres = orch.verify()
        assert vres.status == "PASSED"

        # Fail the second no-clobber promotion after the publish journal is durable.
        orig_link = os.link
        link_count = 0

        def crash_on_second_promotion(src, dst, *args, **kwargs):
            nonlocal link_count
            if str(dst).endswith(".parquet"):
                link_count += 1
                if link_count == 2:
                    raise OSError("Simulated disk error during chunk 2 promotion")
            return orig_link(src, dst, *args, **kwargs)

        monkeypatch.setattr(os, "link", crash_on_second_promotion)

        with pytest.raises(OSError, match="Simulated disk error"):
            orch.publish()

        target_dir = lake_root / "ticks" / "symbol=AAPL" / "date=2026-07-10"
        staging_dir = lake_root / "_migration" / "staging" / "ticks" / "symbol=AAPL" / "date=2026-07-10"
        journal = json.loads(orch.publish_journal_file.read_text(encoding="utf-8"))
        first_target = lake_root / journal["files"][0]["final_path"]

        # One immutable target is visible; the complete staged set remains until receipt durability.
        assert first_target.is_file()
        assert len(list(staging_dir.glob("*.parquet"))) == 3

        # Resume publish
        monkeypatch.undo()
        receipts = orch.publish()

        assert len(receipts) == 1
        receipt = receipts[0]
        assert receipt.row_count == 120
        assert len(receipt.file_paths) == 3

        # Target directory contains all 3 chunks
        assert len(list(target_dir.glob("*.parquet"))) == 3
        # Staging directory is cleaned up
        assert not staging_dir.exists()

    def test_publish_idempotent_after_full_success(self, tmp_path: Path):
        """Re-running publish after complete publication is safe and idempotent."""
        db_path = tmp_path / "idempotent.duckdb"
        t0 = datetime(2026, 7, 10, 14, 30, 0)
        _populate_duckdb(db_path, rows=[(t0 + timedelta(seconds=i), "AAPL", 150.0 + i, 10.0, 149.0, 151.0, "CAPITAL", "REG") for i in range(20)])

        lake_root = tmp_path / "lake_idempotent"
        init_tick_lake(lake_root)

        orch = MigrationOrchestrator(MigrationConfig(source_db=db_path, lake_root=lake_root, mode="all"))
        assert orch.run() == 0

        # Run publish() again
        receipts2 = orch.publish()
        assert receipts2 == []

        reader = TickLakeReader(lake_root)
        ticks = reader.query_ticks(symbol="AAPL")
        assert len(ticks) == 20

    def test_publish_lock_contention_aborts_without_modification(self, tmp_path: Path):
        """Lock contention aborts publication with LakeOwnershipError without modifying staging or prod."""
        db_path = tmp_path / "lock_contention.duckdb"
        t0 = datetime(2026, 7, 10, 14, 30, 0)
        _populate_duckdb(db_path, rows=[(t0 + timedelta(seconds=i), "AAPL", 150.0 + i, 10.0, 149.0, 151.0, "CAPITAL", "REG") for i in range(20)])

        lake_root = tmp_path / "lake_lock"
        init_tick_lake(lake_root)

        orch = MigrationOrchestrator(MigrationConfig(source_db=db_path, lake_root=lake_root, mode="all"))
        orch.plan()
        orch.export()
        vres = orch.verify()
        assert vres.status == "PASSED"

        # External writer acquires the lake publisher lock
        external_lock = LakePublisherLock(lake_root, writer_id="external_writer")
        external_lock.acquire()

        try:
            with pytest.raises(LakeOwnershipError):
                orch.publish()
        finally:
            external_lock.release()

        # Staging chunks remain intact; production ticks/ has no promoted files
        staging_chunks = list((lake_root / "_migration" / "staging").glob("**/*.parquet"))
        assert len(staging_chunks) > 0
        prod_chunks = list((lake_root / "ticks").glob("**/*.parquet"))
        assert len(prod_chunks) == 0


# ============================================================================
# Group 3: TestMigrationExceptAllFuzzing (TEST-P26-03)
# ============================================================================

class TestMigrationExceptAllFuzzing:
    """Validates two-way EXCEPT ALL reconciliation under synthetic fuzz perturbations."""

    def test_fuzz_satoshi_scale_price_perturbation(self, tmp_path: Path):
        """Inject satoshi-scale (+1e-8) price perturbation in staged Parquet; verify fails."""
        db_path = tmp_path / "fuzz_satoshi.duckdb"
        t0 = datetime(2026, 7, 10, 14, 30, 0)
        rows = [(t0 + timedelta(seconds=i), "AAPL", round(150.12345678 + i * 0.01, 8), 10.0, 149.0, 151.0, "CAPITAL", "REG") for i in range(100)]
        _populate_duckdb(db_path, rows=rows)

        lake_root = tmp_path / "lake_satoshi"
        init_tick_lake(lake_root)

        orch = MigrationOrchestrator(MigrationConfig(source_db=db_path, lake_root=lake_root, chunk_size=100, mode="all"))
        orch.plan()
        orch.export()

        # Mutate row 42 price in staged Parquet by +1e-8
        chunk_file = lake_root / "_migration" / "staging" / "ticks" / "symbol=AAPL" / "date=2026-07-10" / "chunk_000001.parquet"

        def perturb_satoshi(table: pa.Table) -> pa.Table:
            prices = table["price"].to_pylist()
            prices[42] = round(prices[42] + 1e-8, 8)
            idx = table.schema.get_field_index("price")
            return table.set_column(idx, pa.field("price", pa.float64(), nullable=False), pa.array(prices, type=pa.float64()))

        _mutate_parquet_file(chunk_file, perturb_satoshi)

        vres = orch.verify()
        assert vres.status == "FAILED"
        assert any(d.get("type") == "source_except_parquet_discrepancy" and d.get("missing_in_parquet") == 1 for d in vres.discrepancies)
        assert any(d.get("type") == "parquet_except_source_discrepancy" and d.get("missing_in_source") == 1 for d in vres.discrepancies)

    def test_fuzz_sub_satoshi_price_perturbation(self, tmp_path: Path):
        """Inject sub-satoshi (+1e-9) price perturbation in staged Parquet; verify fails."""
        db_path = tmp_path / "fuzz_subsatoshi.duckdb"
        t0 = datetime(2026, 7, 10, 14, 30, 0)
        rows = [(t0 + timedelta(seconds=i), "AAPL", 150.12345678, 10.0, 149.0, 151.0, "CAPITAL", "REG") for i in range(100)]
        _populate_duckdb(db_path, rows=rows)

        lake_root = tmp_path / "lake_subsatoshi"
        init_tick_lake(lake_root)

        orch = MigrationOrchestrator(MigrationConfig(source_db=db_path, lake_root=lake_root, chunk_size=100, mode="all"))
        orch.plan()
        orch.export()

        chunk_file = lake_root / "_migration" / "staging" / "ticks" / "symbol=AAPL" / "date=2026-07-10" / "chunk_000001.parquet"

        def perturb_subsatoshi(table: pa.Table) -> pa.Table:
            prices = table["price"].to_pylist()
            prices[42] = prices[42] + 1e-9
            idx = table.schema.get_field_index("price")
            return table.set_column(idx, pa.field("price", pa.float64(), nullable=False), pa.array(prices, type=pa.float64()))

        _mutate_parquet_file(chunk_file, perturb_subsatoshi)

        vres = orch.verify()
        assert vres.status == "FAILED"

    def test_fuzz_single_microsecond_timestamp_perturbation(self, tmp_path: Path):
        """Perturb timestamp of 1 row by exactly +1 microsecond in staged Parquet; verify fails."""
        db_path = tmp_path / "fuzz_ts.duckdb"
        t0 = datetime(2026, 7, 10, 14, 30, 0, 100000)
        rows = [(t0 + timedelta(seconds=i), "AAPL", 150.0 + i, 10.0, 149.0, 151.0, "CAPITAL", "REG") for i in range(100)]
        _populate_duckdb(db_path, rows=rows)

        lake_root = tmp_path / "lake_ts"
        init_tick_lake(lake_root)

        orch = MigrationOrchestrator(MigrationConfig(source_db=db_path, lake_root=lake_root, chunk_size=100, mode="all"))
        orch.plan()
        orch.export()

        chunk_file = lake_root / "_migration" / "staging" / "ticks" / "symbol=AAPL" / "date=2026-07-10" / "chunk_000001.parquet"

        def perturb_ts(table: pa.Table) -> pa.Table:
            timestamps = table["timestamp"].to_pylist()
            timestamps[42] = timestamps[42] + timedelta(microseconds=1)
            idx = table.schema.get_field_index("timestamp")
            return table.set_column(idx, pa.field("timestamp", pa.timestamp("us"), nullable=False), pa.array(timestamps, type=pa.timestamp("us")))

        _mutate_parquet_file(chunk_file, perturb_ts)

        vres = orch.verify()
        assert vres.status == "FAILED"

    def test_fuzz_dropped_row_in_parquet(self, tmp_path: Path):
        """Delete 1 row from staged Parquet; verify row count mismatch and EXCEPT ALL failure."""
        db_path = tmp_path / "fuzz_drop.duckdb"
        t0 = datetime(2026, 7, 10, 14, 30, 0)
        rows = [(t0 + timedelta(seconds=i), "AAPL", 150.0 + i, 10.0, 149.0, 151.0, "CAPITAL", "REG") for i in range(100)]
        _populate_duckdb(db_path, rows=rows)

        lake_root = tmp_path / "lake_drop"
        init_tick_lake(lake_root)

        orch = MigrationOrchestrator(MigrationConfig(source_db=db_path, lake_root=lake_root, chunk_size=100, mode="all"))
        orch.plan()
        orch.export()

        chunk_file = lake_root / "_migration" / "staging" / "ticks" / "symbol=AAPL" / "date=2026-07-10" / "chunk_000001.parquet"

        def drop_one_row(table: pa.Table) -> pa.Table:
            # Drop row 50
            part1 = table.slice(0, 50)
            part2 = table.slice(51)
            return pa.concat_tables([part1, part2])

        _mutate_parquet_file(chunk_file, drop_one_row)

        vres = orch.verify()
        assert vres.status == "FAILED"
        assert any(d.get("type") == "row_count_mismatch" for d in vres.discrepancies)
        assert any(d.get("type") == "source_except_parquet_discrepancy" and d.get("missing_in_parquet") == 1 for d in vres.discrepancies)

    def test_fuzz_phantom_row_in_parquet(self, tmp_path: Path):
        """Inject 1 extra synthetic row into staged Parquet; verify failure."""
        db_path = tmp_path / "fuzz_phantom.duckdb"
        t0 = datetime(2026, 7, 10, 14, 30, 0)
        rows = [(t0 + timedelta(seconds=i), "AAPL", 150.0 + i, 10.0, 149.0, 151.0, "CAPITAL", "REG") for i in range(100)]
        _populate_duckdb(db_path, rows=rows)

        lake_root = tmp_path / "lake_phantom"
        init_tick_lake(lake_root)

        orch = MigrationOrchestrator(MigrationConfig(source_db=db_path, lake_root=lake_root, chunk_size=100, mode="all"))
        orch.plan()
        orch.export()

        chunk_file = lake_root / "_migration" / "staging" / "ticks" / "symbol=AAPL" / "date=2026-07-10" / "chunk_000001.parquet"

        def add_phantom_row(table: pa.Table) -> pa.Table:
            # Duplicate the last row as row 101
            last_row = table.slice(99, 1)
            return pa.concat_tables([table, last_row])

        _mutate_parquet_file(chunk_file, add_phantom_row)

        vres = orch.verify()
        assert vres.status == "FAILED"
        assert any(d.get("type") == "row_count_mismatch" for d in vres.discrepancies)
        assert any(d.get("type") == "parquet_except_source_discrepancy" and d.get("missing_in_source") == 1 for d in vres.discrepancies)

    def test_fuzz_duplicate_multiplicity_loss(self, tmp_path: Path):
        """Source contains 3 identical ticks; Parquet mutated to contain only 2 copies; verify failure."""
        db_path = tmp_path / "fuzz_dup.duckdb"
        t0 = datetime(2026, 7, 10, 14, 30, 0)
        rows = [
            (t0, "AAPL", 150.0, 10.0, 149.0, 151.0, "CAPITAL", "REG"),
            (t0, "AAPL", 150.0, 10.0, 149.0, 151.0, "CAPITAL", "REG"),
            (t0, "AAPL", 150.0, 10.0, 149.0, 151.0, "CAPITAL", "REG"),
            (t0 + timedelta(seconds=1), "AAPL", 151.0, 10.0, 150.0, 152.0, "CAPITAL", "REG"),
        ]
        _populate_duckdb(db_path, rows=rows)

        lake_root = tmp_path / "lake_dup"
        init_tick_lake(lake_root)

        orch = MigrationOrchestrator(MigrationConfig(source_db=db_path, lake_root=lake_root, mode="all"))
        orch.plan()
        orch.export()

        chunk_file = lake_root / "_migration" / "staging" / "ticks" / "symbol=AAPL" / "date=2026-07-10" / "chunk_000001.parquet"

        def drop_one_duplicate(table: pa.Table) -> pa.Table:
            # Drop row index 1 (one of the duplicates)
            p1 = table.slice(0, 1)
            p2 = table.slice(2)
            return pa.concat_tables([p1, p2])

        _mutate_parquet_file(chunk_file, drop_one_duplicate)

        vres = orch.verify()
        assert vres.status == "FAILED"
        assert any(d.get("type") == "source_except_parquet_discrepancy" and d.get("missing_in_parquet") == 1 for d in vres.discrepancies)

    def test_fuzz_compensating_row_swap_equal_row_count(self, tmp_path: Path):
        """Swap 1 row so total row count is equal, but EXCEPT ALL catches compensating swap."""
        db_path = tmp_path / "fuzz_swap.duckdb"
        t0 = datetime(2026, 7, 10, 14, 30, 0)
        rows = [(t0 + timedelta(seconds=i), "AAPL", 150.0 + i, 10.0, 149.0, 151.0, "CAPITAL", "REG") for i in range(100)]
        _populate_duckdb(db_path, rows=rows)

        lake_root = tmp_path / "lake_swap"
        init_tick_lake(lake_root)

        orch = MigrationOrchestrator(MigrationConfig(source_db=db_path, lake_root=lake_root, chunk_size=100, mode="all"))
        orch.plan()
        orch.export()

        chunk_file = lake_root / "_migration" / "staging" / "ticks" / "symbol=AAPL" / "date=2026-07-10" / "chunk_000001.parquet"

        def swap_row(table: pa.Table) -> pa.Table:
            prices = table["price"].to_pylist()
            prices[50] = 99999.0
            idx = table.schema.get_field_index("price")
            return table.set_column(idx, pa.field("price", pa.float64(), nullable=False), pa.array(prices, type=pa.float64()))

        _mutate_parquet_file(chunk_file, swap_row)

        vres = orch.verify()
        # Row counts match (100 == 100), but EXCEPT ALL catches both directions!
        assert vres.status == "FAILED"
        assert vres.total_source_rows == 100
        assert vres.total_parquet_rows == 100
        assert not any(d.get("type") == "row_count_mismatch" for d in vres.discrepancies)
        assert any(d.get("type") == "source_except_parquet_discrepancy" and d.get("missing_in_parquet") == 1 for d in vres.discrepancies)
        assert any(d.get("type") == "parquet_except_source_discrepancy" and d.get("missing_in_source") == 1 for d in vres.discrepancies)

    def test_fuzz_null_vs_zero_volume_mutation(self, tmp_path: Path):
        """Mutate volume = NULL to volume = 0.0 in Parquet; EXCEPT ALL distinguishes NULL and 0.0."""
        db_path = tmp_path / "fuzz_null_zero.duckdb"
        t0 = datetime(2026, 7, 10, 14, 30, 0)
        rows = [
            (t0, "AAPL", 150.0, None, 149.0, 151.0, "CAPITAL", "REG"),
            (t0 + timedelta(seconds=1), "AAPL", 150.1, 10.0, 149.0, 151.0, "CAPITAL", "REG"),
        ]
        _populate_duckdb(db_path, rows=rows)

        lake_root = tmp_path / "lake_null_zero"
        init_tick_lake(lake_root)

        orch = MigrationOrchestrator(MigrationConfig(source_db=db_path, lake_root=lake_root, mode="all"))
        orch.plan()
        orch.export()

        chunk_file = lake_root / "_migration" / "staging" / "ticks" / "symbol=AAPL" / "date=2026-07-10" / "chunk_000001.parquet"

        def mutate_null_to_zero(table: pa.Table) -> pa.Table:
            vols = table["volume"].to_pylist()
            vols[0] = 0.0
            idx = table.schema.get_field_index("volume")
            return table.set_column(idx, pa.field("volume", pa.float64(), nullable=True), pa.array(vols, type=pa.float64()))

        _mutate_parquet_file(chunk_file, mutate_null_to_zero)

        vres = orch.verify()
        assert vres.status == "FAILED"
        assert any(d.get("type") == "source_except_parquet_discrepancy" for d in vres.discrepancies)

    def test_fuzz_symbol_and_session_corruption(self, tmp_path: Path):
        """Mutate symbol to 'NVDA' and session to 'POST' in staged Parquet; verify failure."""
        db_path = tmp_path / "fuzz_str_corrupt.duckdb"
        t0 = datetime(2026, 7, 10, 14, 30, 0)
        rows = [(t0 + timedelta(seconds=i), "AAPL", 150.0 + i, 10.0, 149.0, 151.0, "CAPITAL", "REG") for i in range(100)]
        _populate_duckdb(db_path, rows=rows)

        lake_root = tmp_path / "lake_str_corrupt"
        init_tick_lake(lake_root)

        orch = MigrationOrchestrator(MigrationConfig(source_db=db_path, lake_root=lake_root, chunk_size=100, mode="all"))
        orch.plan()
        orch.export()

        chunk_file = lake_root / "_migration" / "staging" / "ticks" / "symbol=AAPL" / "date=2026-07-10" / "chunk_000001.parquet"

        def corrupt_symbol_session(table: pa.Table) -> pa.Table:
            syms = table["symbol"].to_pylist()
            syms[10] = "NVDA"
            sym_arr = pc.dictionary_encode(pa.array(syms, type=pa.string()))
            s_idx = table.schema.get_field_index("symbol")
            t_mod = table.set_column(s_idx, pa.field("symbol", pa.dictionary(pa.int32(), pa.string()), nullable=False), sym_arr)

            sess = t_mod["session"].to_pylist()
            sess[20] = "POST"
            sess_idx = t_mod.schema.get_field_index("session")
            return t_mod.set_column(sess_idx, pa.field("session", pa.string(), nullable=True), pa.array(sess, type=pa.string()))

        _mutate_parquet_file(chunk_file, corrupt_symbol_session)

        vres = orch.verify()
        assert vres.status == "FAILED"

    def test_fuzz_corrupted_parquet_magic_bytes(self, tmp_path: Path):
        """Overwrite Parquet header and footer magic bytes; error trapping records parquet_read_error and blocks publish."""
        db_path = tmp_path / "fuzz_magic.duckdb"
        t0 = datetime(2026, 7, 10, 14, 30, 0)
        rows = [(t0 + timedelta(seconds=i), "AAPL", 150.0 + i, 10.0, 149.0, 151.0, "CAPITAL", "REG") for i in range(100)]
        _populate_duckdb(db_path, rows=rows)

        lake_root = tmp_path / "lake_magic"
        init_tick_lake(lake_root)

        orch = MigrationOrchestrator(MigrationConfig(source_db=db_path, lake_root=lake_root, chunk_size=100, mode="all"))
        orch.plan()
        orch.export()

        chunk_file = lake_root / "_migration" / "staging" / "ticks" / "symbol=AAPL" / "date=2026-07-10" / "chunk_000001.parquet"

        # Corrupt magic bytes at head and tail
        with open(chunk_file, "r+b") as f:
            f.seek(0)
            f.write(b"BAD1")
            f.seek(-4, 2)
            f.write(b"BAD1")

        vres = orch.verify()
        assert vres.status == "FAILED"
        assert any(d.get("type") == "parquet_read_error" for d in vres.discrepancies)

        # Verification file saved with FAILED status
        assert orch.verification_file.is_file()
        with open(orch.verification_file, "r", encoding="utf-8") as f:
            vdata = json.load(f)
        assert vdata["status"] == "FAILED"

        # Publish strictly blocked
        with pytest.raises(RuntimeError, match="Publish aborted: verification must pass before publishing."):
            orch.publish()

    def test_fuzz_extreme_numeric_float_limits_roundtrip(self, tmp_path: Path):
        """Float extremes (sys.float_info.min, max, subnormals, 1e+/-300) roundtrip with zero discrepancy."""
        db_path = tmp_path / "extreme_floats.duckdb"
        t0 = datetime(2026, 7, 10, 14, 30, 0)
        extreme_prices = [
            sys.float_info.min,
            sys.float_info.max,
            1e-300,
            1e+300,
            2.2250738585072014e-308,
        ]
        rows = [
            (t0 + timedelta(seconds=i), "AAPL", px, 100.0, px * 0.99 if px < 1e300 else px, px * 1.01 if px < 1e300 else px, "CAPITAL", "REG")
            for i, px in enumerate(extreme_prices)
        ]
        _populate_duckdb(db_path, rows=rows)

        lake_root = tmp_path / "lake_extreme"
        init_tick_lake(lake_root)

        orch = MigrationOrchestrator(MigrationConfig(source_db=db_path, lake_root=lake_root, mode="all"))
        ret = orch.run()
        assert ret == 0

        # Verification must pass with 0 discrepancies
        vres = orch.verify()
        assert vres.status == "PASSED"
        assert len(vres.discrepancies) == 0

        # Verify exact float64 representation across published Parquet
        chunk_files = list((lake_root / "ticks" / "symbol=AAPL" / "date=2026-07-10").glob("*.parquet"))
        assert len(chunk_files) >= 1
        published_table = pq.read_table(chunk_files[0])
        assert published_table["price"].to_pylist() == extreme_prices

        # Lake reader queryability
        reader = TickLakeReader(lake_root)
        ticks = reader.query_ticks(symbol="AAPL")
        assert len(ticks) == 5
