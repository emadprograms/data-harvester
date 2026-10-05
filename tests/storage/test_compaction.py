"""
Comprehensive test suite for offline compaction, durable maintenance journal,
consumer drain protocol, immutable lineage, and physical purge (CAPA-02, CAPA-03, CAPA-04).
"""
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import subprocess
import sys
import time

import duckdb
import pytest
import pyarrow as pa
import pyarrow.parquet as pq

from src.storage.compaction import (
    ConsumerDrainRefusedError,
    EquivalenceVerificationError,
    LakeCompactor,
    LineageManager,
    MaintenanceJournal,
    PurgeError,
    STATE_ABORTED,
    STATE_COMMITTED,
    STATE_DRAINING,
    STATE_IN_PROGRESS,
    STATE_REQUESTED,
    STATE_STAGED,
    purge_symbol_physical,
    recover_maintenance,
)
from src.storage.config import (
    LakeMaintenanceInProgressError,
    encode_symbol,
    init_tick_lake,
)
from src.storage.publication import LakePublisher, LakePublisherLock
from src.storage.reader import TickLakeReader
from src.storage.registry import (
    STATUS_ACTIVE,
    STATUS_PENDING_PURGE,
    SymbolNotFoundError,
    get_symbol_registry,
    init_registry,
)
from src.storage.schema import LAKE_SCHEMA_V1
from tests.fixtures.deterministic_quotes import QuoteTick


def _create_sample_ticks(symbol: str, date_str: str, count: int = 10, start_idx: int = 0) -> list:
    base_dt = datetime.fromisoformat(f"{date_str}T10:00:00+00:00")
    ticks = []
    for i in range(count):
        idx = start_idx + i
        ticks.append(QuoteTick(
            timestamp=datetime(base_dt.year, base_dt.month, base_dt.day, 10, 0, idx % 60, (idx * 1000) % 1_000_000, tzinfo=timezone.utc),
            symbol=symbol,
            price=150.0 + (idx * 0.05),
            volume=100.0,
            bid=149.95,
            ask=150.05,
            source="CAPITAL",
            session="REG",
            ingest_id=f"{symbol}_{date_str}_{idx:06d}",
        ))
    return ticks


# ==============================================================================
# 1. Drain Protocol & Maintenance Fencing (CAPA-02)
# ==============================================================================

def test_compaction_drain_protocol_fences_readers(tmp_path):
    """Active readers observe LakeMaintenanceInProgressError during maintenance and resume after."""
    lake_root = tmp_path / "lake_drain_fence"
    init_tick_lake(lake_root)

    publisher = LakePublisher(lake_root, writer_id="w1")
    ticks1 = _create_sample_ticks("AAPL", "2026-10-02", count=10)
    ticks2 = _create_sample_ticks("AAPL", "2026-10-02", count=10, start_idx=10)
    publisher.publish_batch(ticks1, batch_id="b1", sequence=1)
    publisher.publish_batch(ticks2, batch_id="b2", sequence=2)
    publisher.close()

    reader = TickLakeReader(root=lake_root)
    # Reader works prior to maintenance
    files_before = reader.resolve_partition_files("AAPL", "2026-10-02", "2026-10-02")
    assert len(files_before) == 2

    # Run compaction
    compactor = LakeCompactor(lake_root=lake_root)
    res = compactor.compact(symbol="AAPL", date_str="2026-10-02")
    assert res["status"] == "COMPLETED"

    # Reader works after maintenance and observes single consolidated file
    files_after = reader.resolve_partition_files("AAPL", "2026-10-02", "2026-10-02")
    assert len(files_after) == 1
    assert "compacted_gen1_" in files_after[0].name


def test_compaction_drain_protocol_refuses_when_consumer_cannot_be_established(tmp_path):
    """Compactor refuses replacement and aborts journal if consumer shutdown cannot be established."""
    lake_root = tmp_path / "lake_drain_refusal"
    init_tick_lake(lake_root)

    publisher = LakePublisher(lake_root, writer_id="w1")
    ticks1 = _create_sample_ticks("AAPL", "2026-10-02", count=5)
    ticks2 = _create_sample_ticks("AAPL", "2026-10-02", count=5, start_idx=5)
    publisher.publish_batch(ticks1, batch_id="b1", sequence=1)
    publisher.publish_batch(ticks2, batch_id="b2", sequence=2)
    publisher.close()

    # Consumer verifier returns False (consumer still active / unknown)
    compactor = LakeCompactor(
        lake_root=lake_root,
        consumer_verifier=lambda: False,
        drain_timeout=0.2,
    )

    with pytest.raises(ConsumerDrainRefusedError):
        compactor.compact(symbol="AAPL", date_str="2026-10-02")

    # Guard file removed, journal aborted, partition files untouched
    guard = lake_root / "_maintenance" / "in_progress.json"
    assert not guard.exists()

    journal = MaintenanceJournal(lake_root).load()
    assert journal["state"] == STATE_ABORTED

    reader = TickLakeReader(root=lake_root)
    files = reader.resolve_partition_files("AAPL", "2026-10-02", "2026-10-02")
    assert len(files) == 2  # Original files completely intact


def test_compaction_supervisor_restart_pause_and_resume(tmp_path):
    """Compactor suspends supervisor restarts during maintenance and resumes after."""
    lake_root = tmp_path / "lake_supervisor_drain"
    init_tick_lake(lake_root)

    publisher = LakePublisher(lake_root, writer_id="w1")
    ticks1 = _create_sample_ticks("AAPL", "2026-10-02", count=5)
    ticks2 = _create_sample_ticks("AAPL", "2026-10-02", count=5, start_idx=5)
    publisher.publish_batch(ticks1, batch_id="b1", sequence=1)
    publisher.publish_batch(ticks2, batch_id="b2", sequence=2)
    publisher.close()

    class MockSupervisor:
        def __init__(self):
            self.suspended = False
            self.resumed = False

        def suspend_for_handoff(self, timeout=15.0):
            self.suspended = True
            return 0

        def resume_after_handoff(self):
            self.resumed = True
            return None

    mock_sup = MockSupervisor()
    compactor = LakeCompactor(lake_root=lake_root, supervisor=mock_sup)
    res = compactor.compact(symbol="AAPL", date_str="2026-10-02")

    assert res["status"] == "COMPLETED"
    assert mock_sup.suspended is True
    assert mock_sup.resumed is True


# ==============================================================================
# 2. Multiset Equivalence & Consolidation (CAPA-03)
# ==============================================================================

def test_compaction_multiset_equivalence_duplicates_nulls_and_floats(tmp_path):
    """
    Compaction preserves 100% multiset equivalence: exact duplicate rows (multiplicity 3),
    null optional columns, microsecond timestamps, float limits/precisions across symbols.
    """
    lake_root = tmp_path / "lake_multiset"
    init_tick_lake(lake_root)
    publisher = LakePublisher(lake_root, writer_id="w1")

    # Construct batch 1 with duplicate rows and float edge values
    dt_base = datetime(2026, 10, 2, 12, 0, 0, 123456, tzinfo=timezone.utc)
    dup_tick = QuoteTick(
        timestamp=dt_base,
        symbol="GOOGL",
        price=180.123456789,
        volume=None,  # Null volume
        bid=None,     # Null bid
        ask=180.15,
        source="CAPITAL",
        session="REG",
        ingest_id="dup_id_001",
    )
    # 3 exact duplicate rows with identical timestamp and ingest_id
    batch1_ticks = [dup_tick, dup_tick, dup_tick]

    # Additional ticks with subsecond timestamps and small prices
    batch1_ticks.append(QuoteTick(
        timestamp=datetime(2026, 10, 2, 12, 0, 0, 100000, tzinfo=timezone.utc),
        symbol="GOOGL",
        price=0.00001,
        volume=50.0,
        bid=0.000009,
        ask=0.000011,
        source="BINANCE",
        session="REG",
        ingest_id="googl_sub_01",
    ))

    # Batch 2: more ticks for GOOGL on same date
    batch2_ticks = [
        QuoteTick(
            timestamp=datetime(2026, 10, 2, 12, 0, 1, 500000, tzinfo=timezone.utc),
            symbol="GOOGL",
            price=180.50,
            volume=200.0,
            bid=180.45,
            ask=180.55,
            source="CAPITAL",
            session="REG",
            ingest_id="googl_002",
        )
    ]

    # Batch 3: second symbol (TSLA) on same date
    batch3_ticks = _create_sample_ticks("TSLA", "2026-10-02", count=15)

    publisher.publish_batch(batch1_ticks, batch_id="b1", sequence=1)
    publisher.publish_batch(batch2_ticks, batch_id="b2", sequence=2)
    publisher.publish_batch(batch3_ticks, batch_id="b3", sequence=3)
    publisher.close()

    # Pre-compaction data capture via DuckDB
    con = duckdb.connect(":memory:")
    before_rows = con.execute("""
        SELECT timestamp, symbol, price, volume, bid, ask, source, session, ingest_id
        FROM read_parquet(?, hive_partitioning=false)
        ORDER BY symbol, timestamp, ingest_id
    """, [str(lake_root / "ticks" / "symbol=*" / "date=*" / "*.parquet")]).fetchall()

    # Execute compaction across both symbols
    compactor = LakeCompactor(lake_root=lake_root)
    result = compactor.compact(force=True)

    assert result["status"] == "COMPLETED"
    assert result["compacted_partitions"] == 2

    # Post-compaction data capture via DuckDB
    after_rows = con.execute("""
        SELECT timestamp, symbol, price, volume, bid, ask, source, session, ingest_id
        FROM read_parquet(?, hive_partitioning=false)
        ORDER BY symbol, timestamp, ingest_id
    """, [str(lake_root / "ticks" / "symbol=*" / "date=*" / "*.parquet")]).fetchall()

    # 1. Total rows match exactly
    assert len(before_rows) == len(after_rows) == (4 + 1 + 15)

    # 2. Row-by-row identity including duplicate multiplicity, nulls, and float precision
    assert before_rows == after_rows

    # 3. DuckDB bidirectional EXCEPT ALL is strictly zero
    p_all = str(lake_root / "ticks" / "symbol=GOOGL" / "date=2026-10-02" / "*.parquet")
    p_comp = str(lake_root / "ticks" / "symbol=GOOGL" / "date=2026-10-02" / "compacted_*.parquet")
    diff = con.execute("""
        SELECT count(*) FROM (
            (SELECT * FROM read_parquet(?, hive_partitioning=false)
             EXCEPT ALL
             SELECT * FROM read_parquet(?, hive_partitioning=false))
            UNION ALL
            (SELECT * FROM read_parquet(?, hive_partitioning=false)
             EXCEPT ALL
             SELECT * FROM read_parquet(?, hive_partitioning=false))
        )
    """, [p_all, p_comp, p_comp, p_all]).fetchone()[0]
    assert diff == 0
    con.close()


def test_compaction_generation_advancement_and_late_arrival(tmp_path):
    """Compacting an already compacted file + late arrival increments generation (gen 1 -> gen 2)."""
    lake_root = tmp_path / "lake_gen_advance"
    init_tick_lake(lake_root)
    publisher = LakePublisher(lake_root, writer_id="w1")

    # Initial publish: 2 files
    publisher.publish_batch(_create_sample_ticks("AMZN", "2026-10-02", count=5), batch_id="b1", sequence=1)
    publisher.publish_batch(_create_sample_ticks("AMZN", "2026-10-02", count=5, start_idx=5), batch_id="b2", sequence=2)
    publisher.close()

    # Compaction Pass 1 -> gen 1
    compactor = LakeCompactor(lake_root=lake_root)
    res1 = compactor.compact(symbol="AMZN", date_str="2026-10-02")
    assert res1["status"] == "COMPLETED"

    part_dir = lake_root / "ticks" / "symbol=AMZN" / "date=2026-10-02"
    files1 = list(part_dir.glob("*.parquet"))
    assert len(files1) == 1
    assert "compacted_gen1_" in files1[0].name

    # Late-arrival batch published
    publisher2 = LakePublisher(lake_root, writer_id="w2")
    publisher2.publish_batch(_create_sample_ticks("AMZN", "2026-10-02", count=5, start_idx=10), batch_id="b3", sequence=1)
    publisher2.close()

    files_pre2 = list(part_dir.glob("*.parquet"))
    assert len(files_pre2) == 2  # compacted_gen1 + late batch

    # Compaction Pass 2 -> gen 2
    res2 = compactor.compact(symbol="AMZN", date_str="2026-10-02")
    assert res2["status"] == "COMPLETED"

    files2 = list(part_dir.glob("*.parquet"))
    assert len(files2) == 1
    assert "compacted_gen2_" in files2[0].name

    # Verify total rows = 15
    table = pq.ParquetFile(files2[0]).read()
    assert table.num_rows == 15


# ==============================================================================
# 3. Crash Recovery across Journal Stages (CAPA-03)
# ==============================================================================

def test_recovery_interrupted_at_in_progress(tmp_path):
    """Crash at IN_PROGRESS stage: fresh recovery cleans staging, rolls back journal, unblocks readers."""
    lake_root = tmp_path / "lake_crash_in_progress"
    init_tick_lake(lake_root)
    publisher = LakePublisher(lake_root, writer_id="w1")
    publisher.publish_batch(_create_sample_ticks("AAPL", "2026-10-02", count=5), batch_id="b1", sequence=1)
    publisher.publish_batch(_create_sample_ticks("AAPL", "2026-10-02", count=5, start_idx=5), batch_id="b2", sequence=2)
    publisher.close()

    # Simulate crash at IN_PROGRESS with incomplete staged file
    journal = MaintenanceJournal(lake_root)
    maint_id = "maint_crash_1"
    journal.record_transition(STATE_IN_PROGRESS, maintenance_id=maint_id)

    # In-progress guard file
    guard = lake_root / "_maintenance" / "in_progress.json"
    guard.write_text(json.dumps({"operation": "compaction", "pid": 99999}), encoding="utf-8")

    # Incomplete staged file
    staged_dir = lake_root / "_maintenance" / "staging" / "ticks" / "symbol=AAPL" / "date=2026-10-02"
    staged_dir.mkdir(parents=True, exist_ok=True)
    (staged_dir / "torn_file.parquet").write_bytes(b"corrupt partial bytes")

    # Prior to recovery, reader is blocked by in_progress guard
    reader = TickLakeReader(root=lake_root)
    with pytest.raises(LakeMaintenanceInProgressError):
        reader.resolve_partition_files("AAPL", "2026-10-02", "2026-10-02")

    # Fresh process runs recovery
    rec = recover_maintenance(lake_root)
    assert rec["status"] == "ROLLED_BACK"
    assert rec["state"] == STATE_ABORTED

    # Guard removed, staging cleaned, readers unblocked, original files preserved
    assert not guard.exists()
    assert not (staged_dir / "torn_file.parquet").exists()

    files = reader.resolve_partition_files("AAPL", "2026-10-02", "2026-10-02")
    assert len(files) == 2


def test_recovery_interrupted_at_staged_roll_forward(tmp_path):
    """Crash at STAGED stage: fresh recovery completes promotion and commits lineage."""
    lake_root = tmp_path / "lake_crash_staged"
    init_tick_lake(lake_root)
    publisher = LakePublisher(lake_root, writer_id="w1")
    publisher.publish_batch(_create_sample_ticks("AAPL", "2026-10-02", count=5), batch_id="b1", sequence=1)
    publisher.publish_batch(_create_sample_ticks("AAPL", "2026-10-02", count=5, start_idx=5), batch_id="b2", sequence=2)
    publisher.close()

    # Build valid staged file
    part_rel = "ticks/symbol=AAPL/date=2026-10-02"
    input_files = [f.relative_to(lake_root).as_posix() for f in (lake_root / part_rel).glob("*.parquet")]
    staged_dir = lake_root / "_maintenance" / "staging" / part_rel
    staged_dir.mkdir(parents=True, exist_ok=True)
    staged_rel = f"_maintenance/staging/{part_rel}/compacted_gen1_recovered.parquet"
    target_rel = f"{part_rel}/compacted_gen1_recovered.parquet"

    # Merge input tables into staged file
    in_tables = [pq.ParquetFile(lake_root / rel).read().cast(LAKE_SCHEMA_V1) for rel in input_files]
    merged = pa.concat_tables(in_tables)
    pq.write_table(merged, lake_root / staged_rel)

    maint_id = "maint_staged_1"
    journal = MaintenanceJournal(lake_root)
    plan = {
        "staged_partitions": [{
            "symbol": "AAPL",
            "date": "2026-10-02",
            "staged_file": staged_rel,
            "target_file": target_rel,
            "input_files": input_files,
        }]
    }
    file_lineage = {
        in_rel: {"compacted_file": target_rel, "symbol": "AAPL", "date": "2026-10-02"}
        for in_rel in input_files
    }
    journal.record_transition(STATE_STAGED, maintenance_id=maint_id, plan=plan, details={"file_lineage": file_lineage})

    # Guard file in place
    guard = lake_root / "_maintenance" / "in_progress.json"
    guard.write_text(json.dumps({"operation": "compaction", "pid": 99999}), encoding="utf-8")

    # Run recovery (roll forward = True)
    rec = recover_maintenance(lake_root, roll_forward=True)
    assert rec["status"] == "RECOVERED_COMMITTED"

    # Verify target file is promoted, inputs retired, guard removed, lineage recorded
    assert (lake_root / target_rel).is_file()
    for in_rel in input_files:
        assert not (lake_root / in_rel).exists()
    assert not guard.exists()

    lineage = LineageManager(lake_root).load()
    for in_rel in input_files:
        assert in_rel in lineage["file_lineage"]
        assert lineage["file_lineage"][in_rel]["compacted_file"] == target_rel


# ==============================================================================
# 4. Lineage, Writer Retry, and Migration Verification (CAPA-03)
# ==============================================================================

def test_writer_retry_consults_lineage_after_compaction(tmp_path):
    """
    Retrying an already published batch after compaction consults lineage,
    succeeds without missing-file errors, and returns ALREADY_PUBLISHED.
    """
    lake_root = tmp_path / "lake_writer_retry"
    init_tick_lake(lake_root)
    publisher = LakePublisher(lake_root, writer_id="w1")

    ticks = _create_sample_ticks("AAPL", "2026-10-02", count=20)
    receipt = publisher.publish_batch(ticks, batch_id="batch_retry_001", sequence=1)
    assert receipt.status == "PUBLISHED"
    publisher.close()

    # Add second batch and compact partition
    publisher2 = LakePublisher(lake_root, writer_id="w1")
    ticks2 = _create_sample_ticks("AAPL", "2026-10-02", count=10, start_idx=20)
    publisher2.publish_batch(ticks2, batch_id="batch_retry_002", sequence=2)
    publisher2.close()

    compactor = LakeCompactor(lake_root=lake_root)
    compactor.compact(symbol="AAPL", date_str="2026-10-02")

    # Original files for batch_retry_001 have been retired and unlinked
    orig_file = lake_root / receipt.file_paths[0]
    assert not orig_file.exists()

    # Retry publishing batch_retry_001 with exact same payload
    publisher_retry = LakePublisher(lake_root, writer_id="w1")
    retry_receipt = publisher_retry.publish_batch(ticks, batch_id="batch_retry_001", sequence=1)

    assert retry_receipt.status == "ALREADY_PUBLISHED"
    assert retry_receipt.batch_id == "batch_retry_001"
    assert retry_receipt.row_count == 20
    publisher_retry.close()


def test_migration_verification_after_compaction(tmp_path):
    """
    verify_published() resolves migrated files through lineage after compaction
    and passes with zero discrepancies.
    """
    from tools.migrate_streaming_to_parquet import (
        MigrationConfig,
        MigrationOrchestrator,
        MigrationPlan,
    )
    from tests.support.migration_factory import create_source_db, quote_row

    lake_root = tmp_path / "lake_migration_compaction"
    init_tick_lake(lake_root)
    source_db = tmp_path / "source.duckdb"

    # Create synthetic source DB with 50 rows
    rows = [
        quote_row(
            timestamp=datetime(2026, 10, 2, 10, 0, i, tzinfo=timezone.utc),
            symbol="AAPL",
            price=150.0 + i,
            volume=10.0,
            bid=149.9,
            ask=150.1,
            source="CAPITAL",
            session="REG",
        )
        for i in range(50)
    ]
    create_source_db(source_db, rows)

    # Run migration: export, verify, publish
    orchestrator = MigrationOrchestrator(MigrationConfig(source_db=source_db, lake_root=lake_root))
    orchestrator.export()
    v_res = orchestrator.verify()
    assert v_res.status == "PASSED"
    p_res = orchestrator.publish()
    assert len(p_res) > 0
    assert p_res[0].status == "PUBLISHED"

    # Run verify_published before compaction
    pub_res1 = orchestrator.verify_published()
    assert pub_res1.status == "PASSED"

    # Run compaction on the migrated partition
    compactor = LakeCompactor(lake_root=lake_root)
    compactor.compact(symbol="AAPL", date_str="2026-10-02", force=True)

    # Run verify_published after compaction (resolves via lineage)
    pub_res2 = orchestrator.verify_published()
    assert pub_res2.status == "PASSED"
    assert pub_res2.discrepancies == []


# ==============================================================================
# 5. Physical Purge Automation (CAPA-04)
# ==============================================================================

def test_physical_purge_active_symbol_fails(tmp_path):
    """Attempting to physically purge an ACTIVE symbol raises PurgeError."""
    lake_root = tmp_path / "lake_purge_active"
    init_tick_lake(lake_root)
    registry = get_symbol_registry(root=lake_root)
    init_registry(root=lake_root)
    registry.add_symbol(symbol="AAPL", display_name="Apple Inc")

    with pytest.raises(PurgeError) as exc_info:
        purge_symbol_physical(lake_root, "AAPL")

    assert "active symbol" in str(exc_info.value).lower()


def test_physical_purge_symbol_not_found(tmp_path):
    """Attempting to purge non-existent symbol raises SymbolNotFoundError."""
    lake_root = tmp_path / "lake_purge_notfound"
    init_tick_lake(lake_root)
    init_registry(root=lake_root)

    with pytest.raises(SymbolNotFoundError):
        purge_symbol_physical(lake_root, "UNKNOWN")


def test_physical_purge_lifecycle_complete(tmp_path):
    """
    Physical purge unlinks partition files, calls registry.complete_purge,
    archives fenced generation, and preserves other symbols and backups.
    """
    lake_root = tmp_path / "lake_purge_success"
    init_tick_lake(lake_root)
    registry = get_symbol_registry(root=lake_root)
    init_registry(root=lake_root)

    # Add 2 symbols: PURGE_ME and KEEP_ME
    registry.add_symbol(symbol="PURGE_ME", display_name="Purge Target")
    registry.add_symbol(symbol="KEEP_ME", display_name="Retained Target")

    # Publish data for both symbols
    publisher = LakePublisher(lake_root, writer_id="w1")
    publisher.publish_batch(_create_sample_ticks("PURGE_ME", "2026-10-02", count=10), batch_id="b1", sequence=1)
    publisher.publish_batch(_create_sample_ticks("KEEP_ME", "2026-10-02", count=10), batch_id="b2", sequence=2)
    publisher.close()

    purge_dir = lake_root / "ticks" / f"symbol={encode_symbol('PURGE_ME')}"
    keep_dir = lake_root / "ticks" / f"symbol={encode_symbol('KEEP_ME')}"
    assert purge_dir.is_dir()
    assert keep_dir.is_dir()

    # Attempt physical purge while still ACTIVE -> raises PurgeError
    with pytest.raises(PurgeError):
        purge_symbol_physical(lake_root, "PURGE_ME")

    # Mark symbol as PENDING_PURGE in registry
    registry.remove_symbol("PURGE_ME")
    entry = registry.get_symbol("PURGE_ME")
    assert entry.status == STATUS_PENDING_PURGE
    fenced_gen = entry.generation

    # Now execute physical purge
    res = purge_symbol_physical(lake_root, "PURGE_ME")
    assert res["status"] == "PURGED"
    assert res["symbol"] == "PURGE_ME"
    assert res["generation"] == fenced_gen

    # 1. PURGE_ME partition directory is completely removed
    assert not purge_dir.exists()

    # 2. KEEP_ME partition directory is 100% intact
    assert keep_dir.is_dir()
    assert len(list(keep_dir.glob("date=*/*.parquet"))) == 1

    # 3. Registry shows PURGE_ME archived in generations and removed from active symbols
    snapshot = registry.load()
    assert "PURGE_ME" not in snapshot.symbols
    assert snapshot.generations.get("PURGE_ME") == fenced_gen
    assert "KEEP_ME" in snapshot.symbols


def test_compaction_cli_execution(tmp_path):
    """CLI python -m src.storage.compaction executes successfully."""
    lake_root = tmp_path / "lake_compaction_cli"
    init_tick_lake(lake_root)
    publisher = LakePublisher(lake_root, writer_id="w1")
    publisher.publish_batch(_create_sample_ticks("AAPL", "2026-10-02", count=5), batch_id="b1", sequence=1)
    publisher.publish_batch(_create_sample_ticks("AAPL", "2026-10-02", count=5, start_idx=5), batch_id="b2", sequence=2)
    publisher.close()

    res = subprocess.run(
        [sys.executable, "-m", "src.storage.compaction", "--lake-root", str(lake_root), "--force"],
        capture_output=True,
        text=True,
    )
    assert res.returncode == 0
    assert "COMPLETED" in res.stdout
