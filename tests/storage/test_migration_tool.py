"""
Comprehensive TDD test suite for historical migration tool:
tools/migrate_streaming_to_parquet.py

Milestone v4.0 - Phase 20 (P6: Zero-Loss Migration Tooling & Rehearsal).

Validates:
1. CLI flag parsing and configuration mapping.
2. Plan mode metadata generation, symbol/date filtering, and empty source handling.
3. Export mode chunking (50 rows/chunk), staging hierarchy, deterministic ingest ID synthesis,
   and resume skips on completed partitions.
4. Verify mode two-way mathematical reconciliation (EXCEPT ALL) ensuring zero data loss,
   exact duplicate multiplicity preservation, and corruption detection.
5. Publish mode fail-fast verification guard, atomic promotions to ticks/, receipt generation,
   and staging cleanup.
6. TickLakeReader queryability on published partitions.
7. Full end-to-end lifecycle (--mode all) and dry-run disk safety.
"""
from datetime import datetime, timedelta
import json
import os
from pathlib import Path
import re
from typing import Any, Dict, List

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from src.storage.config import init_tick_lake
from src.storage.reader import TickLakeReader
from tools.migrate_streaming_to_parquet import (
    MigrationConfig,
    MigrationOrchestrator,
    MigrationPlan,
    MigrationState,
    VerificationResult,
    main,
    parse_args,
)


@pytest.fixture
def migration_fixture(tmp_path: Path) -> Dict[str, Any]:
    """
    Initializes an isolated test environment with:
    1. A synthetic source DuckDB with `tick_data` table containing:
       - 120 rows for AAPL on 2026-07-10 (tests chunk_size=50 -> 3 chunk files)
       - 20 rows for AAPL on 2026-07-11
       - 30 rows for MSFT on 2026-07-10 (includes exact duplicates and NULLs in volume/bid/ask)
       - 15 rows for MSFT on 2026-07-11
       - 25 rows for NVDA on 2026-07-10
       - 10 rows for NVDA on 2026-07-11
       Total: 220 rows across 3 symbols and 2 dates.
    2. An initialized Tick Lake at lake_root.
    """
    source_db = tmp_path / "streaming.duckdb"
    con = duckdb.connect(str(source_db))
    con.execute("""
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
    """)

    rows = []

    # 1. AAPL 2026-07-10: 120 ticks (exactly 120 rows for chunk_size=50 test)
    t0_aapl_d1 = datetime(2026, 7, 10, 14, 30, 0)
    for i in range(120):
        rows.append((
            t0_aapl_d1 + timedelta(seconds=i),
            "AAPL",
            150.0 + (i * 0.05),
            100.0,
            149.95 + (i * 0.05),
            150.05 + (i * 0.05),
            "CAPITAL",
            "REG",
        ))

    # 2. AAPL 2026-07-11: 20 ticks
    t0_aapl_d2 = datetime(2026, 7, 11, 14, 30, 0)
    for i in range(20):
        rows.append((
            t0_aapl_d2 + timedelta(seconds=i),
            "AAPL",
            156.0 + (i * 0.05),
            100.0,
            155.95 + (i * 0.05),
            156.05 + (i * 0.05),
            "CAPITAL",
            "REG",
        ))

    # 3. MSFT 2026-07-10: 30 ticks total (includes duplicate ticks and nulls in volume/bid/ask)
    t0_msft_d1 = datetime(2026, 7, 10, 14, 30, 0)
    for i in range(26):
        vol = None if i % 5 == 0 else 50.0
        bid = None if i % 7 == 0 else (420.0 + i * 0.1)
        ask = None if i % 7 == 0 else (420.1 + i * 0.1)
        rows.append((
            t0_msft_d1 + timedelta(seconds=i),
            "MSFT",
            420.0 + (i * 0.1),
            vol,
            bid,
            ask,
            "CAPITAL",
            "REG",
        ))
    # Add exact duplicates (identical row values to test duplicate multiplicity reconciliation)
    # Duplicate of index 0
    rows.append((
        t0_msft_d1,
        "MSFT",
        420.0,
        None,
        420.0,
        420.1,
        "CAPITAL",
        "REG",
    ))
    rows.append((
        t0_msft_d1,
        "MSFT",
        420.0,
        None,
        420.0,
        420.1,
        "CAPITAL",
        "REG",
    ))
    # Duplicate of index 1
    rows.append((
        t0_msft_d1 + timedelta(seconds=1),
        "MSFT",
        420.1,
        50.0,
        420.1,
        420.2,
        "CAPITAL",
        "REG",
    ))
    rows.append((
        t0_msft_d1 + timedelta(seconds=1),
        "MSFT",
        420.1,
        50.0,
        420.1,
        420.2,
        "CAPITAL",
        "REG",
    ))

    # 4. MSFT 2026-07-11: 15 ticks
    t0_msft_d2 = datetime(2026, 7, 11, 14, 30, 0)
    for i in range(15):
        rows.append((
            t0_msft_d2 + timedelta(seconds=i),
            "MSFT",
            425.0 + (i * 0.1),
            50.0,
            424.95 + (i * 0.1),
            425.05 + (i * 0.1),
            "CAPITAL",
            "REG",
        ))

    # 5. NVDA 2026-07-10: 25 ticks
    t0_nvda_d1 = datetime(2026, 7, 10, 14, 30, 0)
    for i in range(25):
        rows.append((
            t0_nvda_d1 + timedelta(seconds=i),
            "NVDA",
            120.0 + (i * 0.2),
            200.0,
            119.9 + (i * 0.2),
            120.1 + (i * 0.2),
            "CAPITAL",
            "REG",
        ))

    # 6. NVDA 2026-07-11: 10 ticks
    t0_nvda_d2 = datetime(2026, 7, 11, 14, 30, 0)
    for i in range(10):
        rows.append((
            t0_nvda_d2 + timedelta(seconds=i),
            "NVDA",
            125.0 + (i * 0.2),
            200.0,
            124.9 + (i * 0.2),
            125.1 + (i * 0.2),
            "CAPITAL",
            "REG",
        ))

    con.executemany("INSERT INTO tick_data VALUES (?, ?, ?, ?, ?, ?, ?, ?)", rows)
    con.close()

    lake_root = tmp_path / "tick_lake"
    init_tick_lake(lake_root)

    return {
        "tmp_path": tmp_path,
        "source_db": source_db,
        "lake_root": lake_root,
        "total_rows": 220,
        "aapl_d1_rows": 120,
    }


@pytest.fixture
def empty_source_fixture(tmp_path: Path) -> Dict[str, Any]:
    """Initializes empty DuckDB table and lake for empty source edge cases."""
    empty_db = tmp_path / "empty_streaming.duckdb"
    con = duckdb.connect(str(empty_db))
    con.execute("""
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
    """)
    con.close()

    lake_root = tmp_path / "empty_tick_lake"
    init_tick_lake(lake_root)

    return {
        "source_db": empty_db,
        "lake_root": lake_root,
    }


# ============================================================================
# 1. CLI Argument Parsing
# ============================================================================

def test_cli_argument_parsing(tmp_path: Path):
    """
    Tests command line flag parsing:
    --source-db, --lake-root, --mode, --chunk-size, --symbols,
    --date-start, --date-end, --dry-run, --resume, --force.
    """
    src = tmp_path / "src.duckdb"
    dst = tmp_path / "lake"
    argv = [
        "--source-db", str(src),
        "--lake-root", str(dst),
        "--mode", "export",
        "--chunk-size", "50",
        "--symbols", "AAPL,MSFT",
        "--date-start", "2026-07-10",
        "--date-end", "2026-07-11",
        "--dry-run",
        "--resume",
        "--force",
    ]

    config = parse_args(argv)
    assert Path(config.source_db).resolve() == src.resolve()
    assert Path(config.lake_root).resolve() == dst.resolve()
    assert config.mode == "export"
    assert config.chunk_size == 50
    assert config.symbols == ["AAPL", "MSFT"]
    assert config.date_start == "2026-07-10"
    assert config.date_end == "2026-07-11"
    assert config.dry_run is True
    assert config.resume is True
    assert config.force is True


# ============================================================================
# 2. Plan Mode Tests
# ============================================================================

def test_plan_mode_generates_plan_json(migration_fixture: Dict[str, Any]):
    """
    Runs plan mode; asserts <lake_root>/_migration/plan.json is created
    with partition counts, dates, and min/max timestamps.
    """
    config = MigrationConfig(
        source_db=migration_fixture["source_db"],
        lake_root=migration_fixture["lake_root"],
        mode="plan",
    )
    orchestrator = MigrationOrchestrator(config)
    plan = orchestrator.plan()

    plan_file = migration_fixture["lake_root"] / "_migration" / "plan.json"
    assert plan_file.is_file(), f"Expected plan file at {plan_file}"

    with open(plan_file, "r", encoding="utf-8") as f:
        plan_data = json.load(f)

    assert plan_data["total_rows"] == migration_fixture["total_rows"]
    partitions = plan_data["partitions"]
    assert len(partitions) >= 6

    for p in partitions:
        assert "symbol" in p
        assert "date" in p
        assert "row_count" in p
        assert p["row_count"] > 0
        assert "min_timestamp" in p
        assert "max_timestamp" in p
        assert p["min_timestamp"] <= p["max_timestamp"]


def test_plan_mode_filters_symbols_and_dates(migration_fixture: Dict[str, Any]):
    """
    Runs plan mode with --symbols AAPL and date filters;
    asserts only AAPL partitions within the filtered date range are planned.
    """
    config = MigrationConfig(
        source_db=migration_fixture["source_db"],
        lake_root=migration_fixture["lake_root"],
        mode="plan",
        symbols=["AAPL"],
        date_start="2026-07-10",
        date_end="2026-07-10",
    )
    orchestrator = MigrationOrchestrator(config)
    plan = orchestrator.plan()

    plan_file = migration_fixture["lake_root"] / "_migration" / "plan.json"
    assert plan_file.is_file()

    with open(plan_file, "r", encoding="utf-8") as f:
        plan_data = json.load(f)

    assert plan_data["total_rows"] == 120
    assert len(plan_data["partitions"]) == 1
    part = plan_data["partitions"][0]
    assert part["symbol"] == "AAPL"
    assert part["date"] == "2026-07-10"
    assert part["row_count"] == 120


def test_plan_mode_empty_source(empty_source_fixture: Dict[str, Any]):
    """
    Runs plan mode on an empty DuckDB table; verifies clean exit and empty plan.
    """
    config = MigrationConfig(
        source_db=empty_source_fixture["source_db"],
        lake_root=empty_source_fixture["lake_root"],
        mode="plan",
    )
    orchestrator = MigrationOrchestrator(config)
    plan = orchestrator.plan()

    plan_file = empty_source_fixture["lake_root"] / "_migration" / "plan.json"
    assert plan_file.is_file()

    with open(plan_file, "r", encoding="utf-8") as f:
        plan_data = json.load(f)

    assert plan_data["total_rows"] == 0
    assert plan_data["partitions"] == []


# ============================================================================
# 3. Export Mode Tests
# ============================================================================

def test_export_mode_creates_staged_parquet_and_state(migration_fixture: Dict[str, Any]):
    """
    Runs export mode with --chunk-size 50 on 120 rows; asserts 3 chunk files created
    under _migration/staging/ticks/symbol=.../date=.../chunk_000001.parquet etc.,
    and _migration/state.json is written.
    """
    config = MigrationConfig(
        source_db=migration_fixture["source_db"],
        lake_root=migration_fixture["lake_root"],
        mode="export",
        chunk_size=50,
        symbols=["AAPL"],
        date_start="2026-07-10",
        date_end="2026-07-10",
    )
    orchestrator = MigrationOrchestrator(config)
    state = orchestrator.export()

    state_file = migration_fixture["lake_root"] / "_migration" / "state.json"
    assert state_file.is_file(), f"Expected state file at {state_file}"

    staging_part_dir = (
        migration_fixture["lake_root"]
        / "_migration"
        / "staging"
        / "ticks"
        / "symbol=AAPL"
        / "date=2026-07-10"
    )
    assert staging_part_dir.is_dir(), f"Expected staging directory at {staging_part_dir}"

    chunk_files = sorted(staging_part_dir.glob("*.parquet"))
    assert len(chunk_files) == 3, f"Expected 3 chunk files, found {len(chunk_files)}"

    assert chunk_files[0].name == "chunk_000001.parquet"
    assert chunk_files[1].name == "chunk_000002.parquet"
    assert chunk_files[2].name == "chunk_000003.parquet"

    # Verify chunk row sizes: 50, 50, 20
    assert pq.ParquetFile(chunk_files[0]).metadata.num_rows == 50
    assert pq.ParquetFile(chunk_files[1]).metadata.num_rows == 50
    assert pq.ParquetFile(chunk_files[2]).metadata.num_rows == 20


def test_export_mode_synthesizes_deterministic_ingest_id(migration_fixture: Dict[str, Any]):
    """
    Inspects exported Parquet chunks; asserts ingest_id matches
    mig_{symbol}_{date}_{idx:08d} and sorts monotonically with timestamp ASC.
    """
    config = MigrationConfig(
        source_db=migration_fixture["source_db"],
        lake_root=migration_fixture["lake_root"],
        mode="export",
        chunk_size=50,
        symbols=["AAPL"],
        date_start="2026-07-10",
        date_end="2026-07-10",
    )
    orchestrator = MigrationOrchestrator(config)
    orchestrator.export()

    staging_part_dir = (
        migration_fixture["lake_root"]
        / "_migration"
        / "staging"
        / "ticks"
        / "symbol=AAPL"
        / "date=2026-07-10"
    )
    chunk_files = sorted(staging_part_dir.glob("*.parquet"))

    id_regex = re.compile(r"^mig_[0-9a-f]{32}_\d{16}$")
    timestamps = []
    ingest_ids = []
    source_rows = []

    for cfile in chunk_files:
        tbl = pq.read_table(cfile)
        t_col = tbl["timestamp"].to_pylist()
        i_col = tbl["ingest_id"].to_pylist()
        columns = [tbl[name].to_pylist() for name in (
            "timestamp", "symbol", "price", "volume", "bid", "ask", "source", "session", "ingest_id"
        )]
        for ts, iid in zip(t_col, i_col):
            assert id_regex.match(iid), f"Ingest ID '{iid}' does not contain a migration namespace"
            timestamps.append(ts)
            ingest_ids.append(iid)
        source_rows.extend(zip(*columns))

    assert len(timestamps) == 120
    assert timestamps == sorted(timestamps), "Timestamps must be sorted in non-descending order"
    assert ingest_ids == sorted(ingest_ids), "Stable source ordinals must sort deterministically"
    assert len(set(ingest_ids)) == 120, "Migration-scoped ingest IDs must be strictly unique"

    # Chunk size is not part of source identity: rebuilding under the same
    # migration UUID must preserve the exact ID-to-row mapping.
    expected_mapping = sorted(source_rows, key=lambda row: row[-1])
    resized = MigrationOrchestrator(MigrationConfig(
        source_db=migration_fixture["source_db"],
        lake_root=migration_fixture["lake_root"],
        chunk_size=17,
        mode="export",
        symbols=["AAPL"],
        date_start="2026-07-10",
        date_end="2026-07-10",
    ))
    resized.export()
    rebuilt_rows = []
    rebuilt_dir = (
        migration_fixture["lake_root"] / "_migration" / "staging" / "ticks"
        / "symbol=AAPL" / "date=2026-07-10"
    )
    for cfile in sorted(rebuilt_dir.glob("*.parquet")):
        table = pq.read_table(cfile)
        rebuilt_rows.extend(zip(*[table[name].to_pylist() for name in (
            "timestamp", "symbol", "price", "volume", "bid", "ask", "source", "session", "ingest_id"
        )]))
    assert sorted(rebuilt_rows, key=lambda row: row[-1]) == expected_mapping


def test_export_mode_resume_skips_verified_completed_chunks(migration_fixture: Dict[str, Any]):
    """A valid checkpoint with intact hashes is reused without rewriting its files."""
    lake_root = migration_fixture["lake_root"]
    source_db = migration_fixture["source_db"]
    first = MigrationOrchestrator(MigrationConfig(
        source_db=source_db,
        lake_root=lake_root,
        mode="export",
        chunk_size=50,
    ))
    state = first.export()
    assert state.partitions
    files = sorted((lake_root / "_migration" / "staging").glob("ticks/**/*.parquet"))
    before = {path: (path.stat().st_mtime_ns, path.read_bytes()) for path in files}
    assert files

    resumed = MigrationOrchestrator(MigrationConfig(
        source_db=source_db,
        lake_root=lake_root,
        mode="export",
        chunk_size=50,
        resume=True,
    ))
    resumed_state = resumed.export()
    assert resumed_state.migration_id == state.migration_id
    assert all(part["status"] == "COMPLETED" for part in resumed_state.partitions.values())
    assert {path: (path.stat().st_mtime_ns, path.read_bytes()) for path in files} == before


# ============================================================================
# 4. Verify Mode Tests (EXCEPT ALL Reconciliation)
# ============================================================================

def test_verify_mode_passes_on_identical_data(migration_fixture: Dict[str, Any]):
    """
    Runs verify mode on faithfully exported data; asserts <lake_root>/_migration/verification.json
    has status: "PASSED", zero discrepancies, and returns exit code 0.
    """
    config = MigrationConfig(
        source_db=migration_fixture["source_db"],
        lake_root=migration_fixture["lake_root"],
        mode="all",
        chunk_size=50,
        symbols=["AAPL"],
        date_start="2026-07-10",
        date_end="2026-07-10",
    )
    orchestrator = MigrationOrchestrator(config)
    orchestrator.export()
    res = orchestrator.verify()

    v_file = migration_fixture["lake_root"] / "_migration" / "verification.json"
    assert v_file.is_file(), f"Expected verification file at {v_file}"

    with open(v_file, "r", encoding="utf-8") as f:
        v_data = json.load(f)

    assert v_data["status"] == "PASSED"
    assert v_data["discrepancies"] == []
    assert v_data["total_source_rows"] == 120
    assert v_data["total_parquet_rows"] == 120


def test_verify_mode_preserves_duplicate_multiplicity(migration_fixture: Dict[str, Any]):
    """
    Verifies that duplicate rows in source DuckDB match identical duplicate rows
    in Parquet via two-way EXCEPT ALL mathematical reconciliation.
    """
    config = MigrationConfig(
        source_db=migration_fixture["source_db"],
        lake_root=migration_fixture["lake_root"],
        mode="all",
        chunk_size=50,
        symbols=["MSFT"],
        date_start="2026-07-10",
        date_end="2026-07-10",
    )
    orchestrator = MigrationOrchestrator(config)
    orchestrator.export()
    v_res = orchestrator.verify()

    v_file = migration_fixture["lake_root"] / "_migration" / "verification.json"
    assert v_file.is_file()

    with open(v_file, "r", encoding="utf-8") as f:
        v_data = json.load(f)

    assert v_data["status"] == "PASSED"
    assert v_data["discrepancies"] == []
    assert v_data["total_source_rows"] == 30
    assert v_data["total_parquet_rows"] == 30


def test_verify_mode_detects_corrupted_or_missing_rows(migration_fixture: Dict[str, Any]):
    """
    Injects a modified price into a staged Parquet file (or removes a row);
    runs verify mode; asserts verification.json records status: "FAILED",
    lists discrepancies, and returns failure.
    """
    config = MigrationConfig(
        source_db=migration_fixture["source_db"],
        lake_root=migration_fixture["lake_root"],
        mode="all",
        chunk_size=50,
        symbols=["AAPL"],
        date_start="2026-07-10",
        date_end="2026-07-10",
    )
    orchestrator = MigrationOrchestrator(config)
    orchestrator.export()

    # Corrupt chunk_000001.parquet: modify the price of the first row
    chunk1_path = (
        migration_fixture["lake_root"]
        / "_migration"
        / "staging"
        / "ticks"
        / "symbol=AAPL"
        / "date=2026-07-10"
        / "chunk_000001.parquet"
    )
    table = pq.read_table(chunk1_path)
    p_dict = table.to_pydict()
    p_dict["price"][0] = 99999.99  # Corrupted price
    corrupted_table = pa.Table.from_pydict(p_dict, schema=table.schema)
    pq.write_table(corrupted_table, chunk1_path)

    v_res = orchestrator.verify()

    v_file = migration_fixture["lake_root"] / "_migration" / "verification.json"
    assert v_file.is_file()

    with open(v_file, "r", encoding="utf-8") as f:
        v_data = json.load(f)

    assert v_data["status"] == "FAILED"
    assert len(v_data["discrepancies"]) > 0


# ============================================================================
# 5. Publish Mode Tests
# ============================================================================

def test_publish_mode_refuses_without_passed_verification(migration_fixture: Dict[str, Any]):
    """
    Attempts publish when verification.json is missing or FAILED;
    asserts it aborts with error without moving files to ticks/.
    """
    lake_root = migration_fixture["lake_root"]
    staging_part = (
        lake_root / "_migration" / "staging" / "ticks" / "symbol=AAPL" / "date=2026-07-10"
    )
    staging_part.mkdir(parents=True, exist_ok=True)
    (staging_part / "chunk_000001.parquet").write_bytes(b"staged_data")

    # Case 1: verification.json is missing
    config = MigrationConfig(
        source_db=migration_fixture["source_db"],
        lake_root=lake_root,
        mode="publish",
    )
    orchestrator = MigrationOrchestrator(config)
    try:
        orchestrator.publish()
        pytest.fail("Expected orchestrator.publish() to raise error when verification.json is missing")
    except NotImplementedError:
        raise
    except Exception:
        pass

    # Staged file must still be in staging, and ticks/ must be untouched
    assert (staging_part / "chunk_000001.parquet").is_file()
    assert not (lake_root / "ticks" / "symbol=AAPL" / "date=2026-07-10").exists()

    # Case 2: verification.json has status FAILED
    v_file = lake_root / "_migration" / "verification.json"
    with open(v_file, "w", encoding="utf-8") as f:
        json.dump({"status": "FAILED", "discrepancies": [{"error": "mismatch"}]}, f)

    try:
        orchestrator.publish()
        pytest.fail("Expected orchestrator.publish() to raise error when verification status is FAILED")
    except NotImplementedError:
        raise
    except Exception:
        pass

    assert (staging_part / "chunk_000001.parquet").is_file()
    assert not (lake_root / "ticks" / "symbol=AAPL" / "date=2026-07-10").exists()


def test_publish_mode_promotes_staged_files_and_writes_receipts(migration_fixture: Dict[str, Any]):
    """
    Runs publish when verification passed; asserts files atomically moved to
    ticks/symbol=.../date=.../, immutable receipts written to _control/receipts/,
    and staging directory cleaned up.
    """
    config = MigrationConfig(
        source_db=migration_fixture["source_db"],
        lake_root=migration_fixture["lake_root"],
        mode="all",
        chunk_size=50,
        symbols=["AAPL"],
        date_start="2026-07-10",
        date_end="2026-07-10",
    )
    orchestrator = MigrationOrchestrator(config)
    orchestrator.export()
    orchestrator.verify()
    receipts = orchestrator.publish()

    lake_root = migration_fixture["lake_root"]

    # 1. Published files exist in production lake
    prod_part = lake_root / "ticks" / "symbol=AAPL" / "date=2026-07-10"
    assert prod_part.is_dir()
    published_files = sorted(prod_part.glob("*.parquet"))
    assert len(published_files) == 3

    # 2. Immutable receipts written to _control/receipts/
    receipts_dir = lake_root / "_control" / "receipts"
    assert receipts_dir.is_dir()
    receipt_files = list(receipts_dir.glob("*.json"))
    assert len(receipt_files) >= 1

    # 3. Staging directory cleaned up
    staging_part = (
        lake_root / "_migration" / "staging" / "ticks" / "symbol=AAPL" / "date=2026-07-10"
    )
    assert not staging_part.exists() or len(list(staging_part.glob("*.parquet"))) == 0


def test_published_partitions_queryable_by_lake_reader(migration_fixture: Dict[str, Any]):
    """
    Queries published partitions using TickLakeReader.get_candles() and
    TickLakeReader.get_tape(); verifies query results match source data.
    """
    config = MigrationConfig(
        source_db=migration_fixture["source_db"],
        lake_root=migration_fixture["lake_root"],
        mode="all",
        chunk_size=50,
        symbols=["AAPL"],
        date_start="2026-07-10",
        date_end="2026-07-10",
    )
    orchestrator = MigrationOrchestrator(config)
    orchestrator.run()

    reader = TickLakeReader(root=migration_fixture["lake_root"])

    # Query candles
    candle_resp = reader.get_candles(symbol="AAPL", date="2026-07-10")
    assert candle_resp["symbol"] == "AAPL"
    assert candle_resp["count"] > 0
    assert len(candle_resp["candles"]) > 0

    # Query tape
    tape_resp = reader.get_tape(symbol="AAPL", limit=20)
    assert tape_resp["symbol"] == "AAPL"
    assert tape_resp["count"] > 0
    assert len(tape_resp["ticks"]) > 0


# ============================================================================
# 6. End-to-End Lifecycle & Dry-Run Tests
# ============================================================================

def test_mode_all_end_to_end_lifecycle(migration_fixture: Dict[str, Any]):
    """
    Executes --mode all in a single run; asserts plan -> export -> verify -> publish
    completes with exit code 0 and published lake.
    """
    lake_root = migration_fixture["lake_root"]
    source_db = migration_fixture["source_db"]

    exit_code = main([
        "--source-db", str(source_db),
        "--lake-root", str(lake_root),
        "--mode", "all",
        "--chunk-size", "50",
    ])
    assert exit_code == 0

    # Verify plan.json, state.json, verification.json all exist
    migration_dir = lake_root / "_migration"
    assert (migration_dir / "plan.json").is_file()
    assert (migration_dir / "state.json").is_file()
    assert (migration_dir / "verification.json").is_file()

    with open(migration_dir / "verification.json", "r", encoding="utf-8") as f:
        v_data = json.load(f)
    assert v_data["status"] == "PASSED"

    # Verify published parquet files in ticks/
    ticks_dir = lake_root / "ticks"
    published_files = list(ticks_dir.glob("*/*/*.parquet"))
    assert len(published_files) > 0


def test_dry_run_leaves_disk_unmodified(migration_fixture: Dict[str, Any]):
    """
    Executes --mode all --dry-run; asserts no Parquet files or migration state
    are written to disk.
    """
    lake_root = migration_fixture["lake_root"]
    source_db = migration_fixture["source_db"]

    exit_code = main([
        "--source-db", str(source_db),
        "--lake-root", str(lake_root),
        "--mode", "all",
        "--chunk-size", "50",
        "--dry-run",
    ])
    assert exit_code == 0

    # Ticks directory must contain no parquet files
    ticks_dir = lake_root / "ticks"
    assert len(list(ticks_dir.glob("*/*/*.parquet"))) == 0

    # _migration directory must contain no staging parquet files or state json
    migration_dir = lake_root / "_migration"
    if migration_dir.exists():
        assert not (migration_dir / "state.json").exists()
        assert not (migration_dir / "verification.json").exists()
        assert len(list(migration_dir.glob("staging/*/*/*/*.parquet"))) == 0
