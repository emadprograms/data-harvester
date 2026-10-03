"""
Unit and integration tests for TickLakeWriter (Milestone v4.0 - Phase 17).

Verifies micro-batch flushing, monotonic clock timeouts, off-loop PyArrow worker
responsiveness (<20ms event loop lag), transient I/O retries, malformed tick
quarantine, and writer status lifecycle.
"""
import asyncio
from datetime import datetime, timedelta, timezone
import json
import os
from pathlib import Path
import time
import duckdb
import pytest

from src.storage.config import load_lake_metadata
from src.storage.publication import LakeOwnershipError, LakePublisherLock, LakePublisher
from src.storage.parquet_writer import TickLakeWriter, WriterMetrics
from tests.fixtures.deterministic_quotes import QuoteTick


def test_writer_initialization_and_lock(tmp_path):
    """
    Verifies TickLakeWriter initializes directory structure, creates lake.json,
    acquires _control/publisher.lock, and raises LakeOwnershipError if a second
    instance targets the same root.
    """
    lake_root = tmp_path / "lake"
    writer = TickLakeWriter(root=lake_root, writer_id="writer_test_1")

    # 1. Directory hierarchy and lake.json metadata
    assert (lake_root / "lake.json").is_file(), "lake.json metadata file was not created"
    meta = load_lake_metadata(lake_root)
    assert meta.schema_version == 1

    # 2. Advisory publisher lock acquired
    lock_file = lake_root / "_control" / "publisher.lock"
    assert lock_file.is_file(), "publisher.lock file was not created in _control"
    assert writer.status == "RUNNING"

    # 3. Second instance on same root must raise LakeOwnershipError
    with pytest.raises(LakeOwnershipError):
        TickLakeWriter(root=lake_root, writer_id="writer_test_2")

    # 4. Closing the first writer releases the lock
    writer.close()
    assert writer.status == "STOPPED"

    # Second instance can now acquire ownership
    writer2 = TickLakeWriter(root=lake_root, writer_id="writer_test_2")
    assert writer2.status == "RUNNING"
    writer2.close()


def test_batch_flush_on_max_rows(tmp_path):
    """
    With max_batch_rows=5 and flush_interval_seconds=60.0, publishing/enqueueing
    5 ticks immediately triggers a batch publication.
    """
    lake_root = tmp_path / "lake"
    writer = TickLakeWriter(root=lake_root, max_batch_rows=5, flush_interval_seconds=60.0)

    base_time = datetime(2026, 10, 2, 14, 30, 0, tzinfo=timezone.utc)
    ticks = [
        QuoteTick(
            timestamp=base_time + timedelta(seconds=i),
            symbol="AAPL",
            price=150.0 + i,
            volume=1.0,
            bid=149.9,
            ask=150.1,
            source="CAPITAL",
            session="REG",
            ingest_id=f"aapl_{i:04d}",
        )
        for i in range(5)
    ]

    # Enqueue 4 ticks (less than max_batch_rows=5)
    for t in ticks[:4]:
        writer.write_tick(t)

    assert writer.metrics.batches_published == 0
    assert writer.metrics.total_published == 0

    # 5th tick immediately satisfies max_batch_rows and triggers flush
    writer.write_tick(ticks[4])

    assert writer.metrics.batches_published == 1
    assert writer.metrics.total_published == 5

    # Verify published Parquet file
    parquet_files = list(lake_root.glob("ticks/symbol=AAPL/date=2026-10-02/*.parquet"))
    assert len(parquet_files) == 1

    con = duckdb.connect(":memory:")
    res = con.execute("SELECT count(*) FROM read_parquet(?)", [str(parquet_files[0])]).fetchone()
    assert res[0] == 5
    con.close()

    writer.close()


def test_batch_flush_on_monotonic_age(tmp_path):
    """
    With max_batch_rows=1000 and flush_interval_seconds=0.1, publishing ticks
    and waiting 0.15s triggers publication via monotonic clock check.
    """
    lake_root = tmp_path / "lake"
    writer = TickLakeWriter(root=lake_root, max_batch_rows=1000, flush_interval_seconds=0.1)

    base_time = datetime(2026, 10, 2, 14, 30, 0, tzinfo=timezone.utc)
    ticks = [
        QuoteTick(
            timestamp=base_time + timedelta(seconds=i),
            symbol="MSFT",
            price=300.0 + i,
            volume=1.0,
            bid=299.9,
            ask=300.1,
            source="CAPITAL",
            session="REG",
            ingest_id=f"msft_{i:04d}",
        )
        for i in range(2)
    ]

    for t in ticks:
        writer.write_tick(t)

    # Immediately after write, 2 ticks < 1000 rows -> not published yet
    assert writer.metrics.batches_published == 0

    # Wait 0.15s to exceed flush_interval_seconds=0.1
    time.sleep(0.15)

    # Wait up to 1.0s for background flush or trigger flush check
    deadline = time.monotonic() + 1.0
    while writer.metrics.batches_published == 0 and time.monotonic() < deadline:
        time.sleep(0.02)

    assert writer.metrics.batches_published == 1
    assert writer.metrics.total_published == 2

    parquet_files = list(lake_root.glob("ticks/symbol=MSFT/date=2026-10-02/*.parquet"))
    assert len(parquet_files) == 1
    writer.close()


def test_off_loop_event_loop_responsiveness(tmp_path):
    """
    Runs a high-frequency 5ms heartbeat task on the asyncio loop while calling
    publish_batch_async with 5,000 ticks. Asserts max event loop scheduling lag
    is strictly < 20.0ms.
    """
    async def _run():
        lake_root = tmp_path / "lake"
        writer = TickLakeWriter(root=lake_root)

        base_ts = datetime(2026, 10, 2, 14, 30, 0, tzinfo=timezone.utc)
        ticks = [
            QuoteTick(
                timestamp=base_ts + timedelta(milliseconds=i),
                symbol="NVDA",
                price=500.0 + (i * 0.005),
                volume=1.0,
                bid=499.9,
                ask=500.1,
                source="CAPITAL",
                session="REG",
                ingest_id=f"nvda_{i:05d}",
            )
            for i in range(5000)
        ]

        heartbeat_interval = 0.005  # 5ms
        max_lag = 0.0
        stop_heartbeat = asyncio.Event()

        async def heartbeat():
            nonlocal max_lag
            while not stop_heartbeat.is_set():
                t0 = time.monotonic()
                await asyncio.sleep(heartbeat_interval)
                elapsed = time.monotonic() - t0
                lag = elapsed - heartbeat_interval
                if lag > max_lag:
                    max_lag = lag

        hb_task = asyncio.create_task(heartbeat())
        try:
            receipt = await writer.publish_batch_async(ticks)
            assert receipt.status == "PUBLISHED"
            assert receipt.row_count == 5000
        finally:
            stop_heartbeat.set()
            await hb_task
            writer.close()

        assert max_lag < 0.020, (
            f"Event loop scheduling lag too high: {max_lag * 1000:.2f}ms >= 20.0ms threshold"
        )

    asyncio.run(_run())


def test_transient_io_retry_and_honest_counters(tmp_path, monkeypatch):
    """
    Injects transient OSError into publication for 2 attempts; verifies automatic
    backoff retry succeeds on 3rd attempt without data loss, with total_retrying
    updated during retries.
    """
    lake_root = tmp_path / "lake"
    writer = TickLakeWriter(
        root=lake_root,
        max_batch_rows=10,
        retry_attempts=3,
        retry_backoff_base=0.01,
    )

    base_time = datetime(2026, 10, 2, 14, 30, 0, tzinfo=timezone.utc)
    ticks = [
        QuoteTick(
            timestamp=base_time + timedelta(seconds=i),
            symbol="AAPL",
            price=150.0 + i,
            volume=1.0,
            bid=149.9,
            ask=150.1,
            source="CAPITAL",
            session="REG",
            ingest_id=f"retry_{i:04d}",
        )
        for i in range(10)
    ]

    attempts = 0
    orig_publish = LakePublisher.publish_batch

    def flaky_publish(self, *args, **kwargs):
        nonlocal attempts
        attempts += 1
        if attempts <= 2:
            raise OSError("Simulated transient disk I/O error (EIO)")
        return orig_publish(self, *args, **kwargs)

    monkeypatch.setattr(LakePublisher, "publish_batch", flaky_publish)

    for t in ticks:
        writer.write_tick(t)
    writer.flush()

    assert attempts == 3, f"Expected 3 publication attempts (2 failures + 1 success), got {attempts}"
    assert writer.metrics.total_retrying == 2
    assert writer.metrics.total_published == 10

    parquet_files = list(lake_root.glob("ticks/symbol=AAPL/date=2026-10-02/*.parquet"))
    assert len(parquet_files) == 1
    writer.close()


def test_quarantine_malformed_ticks(tmp_path):
    """
    Submits malformed ticks (e.g. price <= 0, NaN, traversal symbols); verifies
    they are quarantined/skipped, total_quarantined is incremented, and valid ticks
    publish cleanly.
    """
    lake_root = tmp_path / "lake"
    writer = TickLakeWriter(root=lake_root, max_batch_rows=100)

    ticks = [
        # Valid tick 1
        QuoteTick(
            timestamp=datetime(2026, 10, 2, 14, 30, 0, tzinfo=timezone.utc),
            symbol="AAPL",
            price=150.0,
            volume=1.0,
            bid=149.9,
            ask=150.1,
            source="CAPITAL",
            session="REG",
            ingest_id="valid_1",
        ),
        # Malformed 1: Negative price
        {
            "timestamp": datetime(2026, 10, 2, 14, 30, 1, tzinfo=timezone.utc),
            "symbol": "AAPL",
            "price": -5.0,
            "volume": 1.0,
            "bid": 0.0,
            "ask": 0.0,
            "ingest_id": "bad_neg_price",
        },
        # Malformed 2: Zero price
        {
            "timestamp": datetime(2026, 10, 2, 14, 30, 2, tzinfo=timezone.utc),
            "symbol": "AAPL",
            "price": 0.0,
            "volume": 1.0,
            "bid": 0.0,
            "ask": 0.0,
            "ingest_id": "bad_zero_price",
        },
        # Malformed 3: NaN price
        {
            "timestamp": datetime(2026, 10, 2, 14, 30, 3, tzinfo=timezone.utc),
            "symbol": "AAPL",
            "price": float("nan"),
            "volume": 1.0,
            "bid": 0.0,
            "ask": 0.0,
            "ingest_id": "bad_nan_price",
        },
        # Malformed 4: Path traversal symbol
        {
            "timestamp": datetime(2026, 10, 2, 14, 30, 4, tzinfo=timezone.utc),
            "symbol": "../../etc/shadow",
            "price": 100.0,
            "volume": 1.0,
            "bid": 99.0,
            "ask": 101.0,
            "ingest_id": "bad_traversal_sym",
        },
        # Valid tick 2
        QuoteTick(
            timestamp=datetime(2026, 10, 2, 14, 30, 5, tzinfo=timezone.utc),
            symbol="AAPL",
            price=151.0,
            volume=1.0,
            bid=150.9,
            ask=151.1,
            source="CAPITAL",
            session="REG",
            ingest_id="valid_2",
        ),
    ]

    writer.write_ticks(ticks)
    writer.flush()

    assert writer.metrics.total_quarantined == 4
    assert writer.metrics.total_published == 2

    # Verify published Parquet contents
    parquet_files = list(lake_root.glob("ticks/symbol=AAPL/date=2026-10-02/*.parquet"))
    assert len(parquet_files) == 1

    con = duckdb.connect(":memory:")
    df = con.execute("SELECT symbol, price, ingest_id FROM read_parquet(?)", [str(parquet_files[0])]).fetchdf()
    assert len(df) == 2
    assert set(df["ingest_id"].tolist()) == {"valid_1", "valid_2"}
    assert all(df["price"] > 0)
    con.close()

    writer.close()


def test_writer_status_file_updates(tmp_path):
    """
    Verifies _control/writer_status.json reflects RUNNING, row counts, and transitions
    to STOPPED upon close().
    """
    lake_root = tmp_path / "lake"
    status_file = lake_root / "_control" / "writer_status.json"

    writer = TickLakeWriter(root=lake_root, writer_id="w_stat_1", max_batch_rows=100)

    # 1. Created and in RUNNING state
    assert status_file.is_file(), "writer_status.json file was not created"
    status_data = json.loads(status_file.read_text(encoding="utf-8"))
    assert status_data["status"] == "RUNNING"
    assert status_data["writer_id"] == "w_stat_1"
    assert status_data["pid"] == os.getpid()
    assert status_data["total_rows_written"] == 0

    # 2. Update status file upon batch flush
    base_time = datetime(2026, 10, 2, 14, 30, 0, tzinfo=timezone.utc)
    ticks = [
        QuoteTick(
            timestamp=base_time + timedelta(seconds=i),
            symbol="AAPL",
            price=150.0 + i,
            volume=1.0,
            bid=149.9,
            ask=150.1,
            source="CAPITAL",
            session="REG",
            ingest_id=f"stat_{i:04d}",
        )
        for i in range(10)
    ]
    writer.write_ticks(ticks)
    writer.flush()

    status_data = json.loads(status_file.read_text(encoding="utf-8"))
    assert status_data["status"] == "RUNNING"
    assert status_data["total_rows_written"] == 10

    # 3. Transitions to STOPPED upon close()
    writer.close()
    status_data = json.loads(status_file.read_text(encoding="utf-8"))
    assert status_data["status"] == "STOPPED"
