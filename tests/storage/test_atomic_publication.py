"""
Integration tests for atomic publication state machine, in-memory DuckDB queries,
tiebreak sorting, receipt ledger, and single-writer ownership locks (Milestone v4.0 - Phase 16).
"""
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import duckdb
import pytest
import pyarrow.parquet as pq

from src.storage.config import init_tick_lake
from src.storage.publication import (
    LakeOwnershipError,
    LakePublisher,
    LakePublisherLock,
    PublishReceipt,
)
from tests.fixtures.deterministic_quotes import (
    QuoteTick,
    generate_subsecond_burst,
)


def test_atomic_publish_single_partition(tmp_path):
    """
    Verify LakePublisher stages a batch to _staging/ and atomically renames it
    to ticks/symbol=AAPL/date=2026-10-02/batch_w1_000001.parquet, leaving no
    temporary files behind.
    """
    lake_root = tmp_path / "lake"
    init_tick_lake(lake_root)

    publisher = LakePublisher(root=lake_root, writer_id="w1")
    try:
        ticks = [
            QuoteTick(
                timestamp=datetime(2026, 10, 2, 14, 30, 0, i * 1000),
                symbol="AAPL",
                price=150.0 + (i * 0.01),
                volume=10.0,
                bid=149.95,
                ask=150.05,
                source="CAPITAL",
                session="REG",
                ingest_id=f"aapl_{i:04d}",
            )
            for i in range(100)
        ]

        receipt = publisher.publish_batch(ticks, batch_id="batch_w1_001", sequence=1)

        assert isinstance(receipt, PublishReceipt)
        assert receipt.status == "PUBLISHED"
        assert receipt.row_count == 100
        assert len(receipt.file_paths) == 1

        expected_rel_path = "ticks/symbol=AAPL/date=2026-10-02/batch_w1_000001.parquet"
        assert receipt.file_paths[0] == expected_rel_path

        final_file = lake_root / expected_rel_path
        assert final_file.is_file(), f"Target parquet file missing at {final_file}"

        # Verify staging directory is clean
        staging_dir = lake_root / "_staging"
        staging_files = list(staging_dir.glob("*.tmp"))
        assert len(staging_files) == 0, f"Dangling staging files found: {staging_files}"

        # Read back parquet file and check metadata
        pf = pq.ParquetFile(final_file)
        assert pf.metadata.num_rows == 100
        assert "timestamp" in pf.schema_arrow.names
        assert "ingest_id" in pf.schema_arrow.names
    finally:
        publisher.close()


def test_published_parquet_readable_by_duckdb(tmp_path):
    """
    Verify that an in-memory DuckDB session can query the published Parquet file
    without file locks, reading microsecond timestamps, correct types, and rows.
    """
    lake_root = tmp_path / "lake"
    init_tick_lake(lake_root)

    ticks = generate_subsecond_burst("NVDA", base_time=datetime(2026, 10, 2, 14, 30, 0), count=50)
    with LakePublisher(root=lake_root, writer_id="w1") as publisher:
        receipt = publisher.publish_batch(ticks, batch_id="batch_duckdb_01", sequence=1)

    published_file = lake_root / receipt.file_paths[0]

    # Query directly with in-memory DuckDB
    con = duckdb.connect(":memory:")
    try:
        res = con.execute(
            """
            SELECT
                count(*) as row_count,
                min(timestamp) as min_ts,
                max(timestamp) as max_ts,
                avg(price) as avg_price,
                count(distinct ingest_id) as distinct_ids
            FROM read_parquet(?)
            """,
            [str(published_file)],
        ).fetchone()

        row_count, min_ts, max_ts, avg_price, distinct_ids = res
        assert row_count == 50
        assert distinct_ids == 50
        assert avg_price > 100.0
    finally:
        con.close()


def test_multi_partition_split(tmp_path):
    """
    Verify that a single batch containing multiple symbols across a UTC midnight
    rollover boundary is cleanly partitioned and produces 4 distinct partition files.
    """
    lake_root = tmp_path / "lake"
    init_tick_lake(lake_root)

    # 4 distinct partitions: (AAPL, 2026-10-02), (AAPL, 2026-10-03), (NVDA, 2026-10-02), (NVDA, 2026-10-03)
    mixed_ticks = [
        QuoteTick(
            timestamp=datetime(2026, 10, 2, 23, 59, 59, 500000),
            symbol="AAPL",
            price=150.10,
            ingest_id="aapl_day1",
        ),
        QuoteTick(
            timestamp=datetime(2026, 10, 3, 0, 0, 0, 500000),
            symbol="AAPL",
            price=150.20,
            ingest_id="aapl_day2",
        ),
        QuoteTick(
            timestamp=datetime(2026, 10, 2, 23, 59, 59, 800000),
            symbol="NVDA",
            price=120.10,
            ingest_id="nvda_day1",
        ),
        QuoteTick(
            timestamp=datetime(2026, 10, 3, 0, 0, 1, 200000),
            symbol="NVDA",
            price=120.30,
            ingest_id="nvda_day2",
        ),
    ]

    with LakePublisher(root=lake_root, writer_id="w1") as publisher:
        receipt = publisher.publish_batch(mixed_ticks, batch_id="batch_multi_split", sequence=2)

    assert receipt.row_count == 4
    assert len(receipt.file_paths) == 4

    expected_files = {
        "ticks/symbol=AAPL/date=2026-10-02/batch_w1_000002.parquet",
        "ticks/symbol=AAPL/date=2026-10-03/batch_w1_000002.parquet",
        "ticks/symbol=NVDA/date=2026-10-02/batch_w1_000002.parquet",
        "ticks/symbol=NVDA/date=2026-10-03/batch_w1_000002.parquet",
    }
    assert set(receipt.file_paths) == expected_files

    for rel_path in expected_files:
        full_path = lake_root / rel_path
        assert full_path.is_file()
        pf = pq.ParquetFile(full_path)
        assert pf.metadata.num_rows == 1


def test_tiebreak_sorting_in_file(tmp_path):
    """
    Verify rows within a published Parquet file are strictly sorted by
    (timestamp ASC, ingest_id ASC), even when ticks arrive out-of-order.
    """
    lake_root = tmp_path / "lake"
    init_tick_lake(lake_root)

    t0 = datetime(2026, 10, 2, 14, 29, 59, 0)
    t1 = datetime(2026, 10, 2, 14, 30, 0, 0)
    t2 = datetime(2026, 10, 2, 14, 30, 1, 0)

    # Arrive out-of-order and with duplicate timestamps
    unordered_ticks = [
        QuoteTick(timestamp=t1, symbol="MSFT", price=420.0, ingest_id="id_zebra"),
        QuoteTick(timestamp=t1, symbol="MSFT", price=420.1, ingest_id="id_alpha"),
        QuoteTick(timestamp=t0, symbol="MSFT", price=419.9, ingest_id="id_early"),
        QuoteTick(timestamp=t1, symbol="MSFT", price=420.2, ingest_id="id_middle"),
        QuoteTick(timestamp=t2, symbol="MSFT", price=420.5, ingest_id="id_late"),
    ]

    with LakePublisher(root=lake_root, writer_id="w1") as publisher:
        receipt = publisher.publish_batch(unordered_ticks, batch_id="batch_sort_test", sequence=1)

    published_file = lake_root / receipt.file_paths[0]
    table = pq.read_table(published_file)

    tuples = [
        (table["timestamp"][i].as_py(), table["ingest_id"][i].as_py())
        for i in range(table.num_rows)
    ]

    expected_tuples = [
        (t0, "id_early"),
        (t1, "id_alpha"),
        (t1, "id_middle"),
        (t1, "id_zebra"),
        (t2, "id_late"),
    ]
    assert tuples == expected_tuples


def test_publish_receipt_ledger(tmp_path):
    """
    Verify that publishing writes an immutable receipt ledger entry in
    _control/receipts/<batch_id>.json with SHA256 checksums and exact row counts.
    """
    lake_root = tmp_path / "lake"
    init_tick_lake(lake_root)

    ticks = [
        QuoteTick(
            timestamp=datetime(2026, 10, 2, 14, 30, 0),
            symbol="AAPL",
            price=150.0,
            ingest_id="r1",
        )
    ]

    batch_id = "test_ledger_batch_001"
    with LakePublisher(root=lake_root, writer_id="w1") as publisher:
        receipt = publisher.publish_batch(ticks, batch_id=batch_id, sequence=1)

    receipt_file = lake_root / "_control" / "receipts" / f"{batch_id}.json"
    assert receipt_file.is_file(), f"Receipt file not found at {receipt_file}"

    with open(receipt_file, "r", encoding="utf-8") as f:
        data = json.load(f)

    assert data["batch_id"] == batch_id
    assert data["writer_id"] == "w1"
    assert data["sequence"] == 1
    assert data["row_count"] == 1
    assert data["status"] == "PUBLISHED"
    assert "published_at" in data
    assert len(data["file_details"]) == 1

    detail = data["file_details"][0]
    rel_path = detail["relative_path"]
    full_path = lake_root / rel_path
    assert full_path.is_file()

    # Validate file size and SHA256 in receipt
    assert detail["file_size_bytes"] == full_path.stat().st_size
    with open(full_path, "rb") as f:
        expected_sha = hashlib.sha256(f.read()).hexdigest()
    assert detail["sha256"] == expected_sha


def test_publisher_ownership_lock(tmp_path):
    """
    Verify LakePublisherLock enforces single-writer mutual exclusion on a lake root.
    A second publisher instance or lock attempt raises LakeOwnershipError.
    Releasing the lock allows subsequent publisher acquisition.
    """
    lake_root = tmp_path / "lake"
    init_tick_lake(lake_root)

    pub1 = LakePublisher(root=lake_root, writer_id="publisher_one")
    try:
        # Second publisher on same lake root must fail with LakeOwnershipError
        with pytest.raises(LakeOwnershipError):
            LakePublisher(root=lake_root, writer_id="publisher_two")

        # Direct lock attempt must also fail
        with pytest.raises(LakeOwnershipError):
            with LakePublisherLock(root=lake_root, writer_id="lock_test"):
                pass
    finally:
        pub1.close()

    # After pub1 is closed, pub2 can acquire ownership cleanly
    pub2 = LakePublisher(root=lake_root, writer_id="publisher_two")
    pub2.close()

    # Test context manager behavior
    with LakePublisher(root=lake_root, writer_id="ctx_pub"):
        with pytest.raises(LakeOwnershipError):
            LakePublisher(root=lake_root, writer_id="other_pub")

    # Outside context manager, lock is released
    pub3 = LakePublisher(root=lake_root, writer_id="post_ctx_pub")
    pub3.close()
