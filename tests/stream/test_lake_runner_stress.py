"""
Phase 23: Streaming Writer & Runner Stress, Backpressure, and Lifecycle Test Suite.

Comprehensive stress, boundary condition, and lifecycle tests covering:
- TEST-P23-01: High-throughput micro-batching under memory pressure (100k+ ticks) and bounded queue backpressure.
- TEST-P23-02: Runner sudden shutdown mid-flush, graceful drain timeouts, and honest queue acknowledgments.
- TEST-P23-03: Transient disk full / I/O error exponential backoff and quarantine handling.
"""
import asyncio
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
import errno
import json
import math
import os
from pathlib import Path
import signal
import subprocess
import sys
import time
from typing import Any, List

import duckdb
import pyarrow as pa
import pytest

from src.storage.parquet_writer import TickLakeWriter
from src.storage.publication import LakePublisherLock, PublishError
from src.storage.schema import QuoteTick
from src.stream.runner import StreamingEngine


# =====================================================================
# Group 1: Micro-Batching, Surge, and Backpressure (TEST-P23-01)
# =====================================================================


def test_stress_multi_partition_surge_100k_ticks(tmp_path):
    """
    100,000 synthetic ticks across 50 symbols crossing UTC midnight (dates: 2026-10-02 and 2026-10-03).
    Verifies DuckDB read_parquet row count == 100,000, distinct ingest_id == 100,000,
    100 partitions, zero .tmp files left.
    """
    lake_root = tmp_path / "lake"
    writer = TickLakeWriter(root=lake_root, max_batch_rows=5000, auto_init=True)

    ticks = []
    # 50 symbols, 2 dates -> exactly 100 partitions
    for i in range(100_000):
        sym = f"SYM_{i % 50:02d}"
        day = 2 if (i // 50) % 2 == 0 else 3
        hour = 23 if day == 2 else 0
        minute = (i % 60)
        second = (i // 60) % 60
        ticks.append(QuoteTick(
            timestamp=datetime(2026, 10, day, hour, minute, second, tzinfo=timezone.utc).replace(tzinfo=None),
            symbol=sym,
            price=100.0 + (i % 100),
            volume=1.0,
            bid=99.9,
            ask=100.1,
            source="CAPITAL",
            session="REG",
            ingest_id=f"surge_100k_{i:07d}",
        ))

    # Ingest entire 100k ticks
    writer.publish_batch(ticks)
    writer.close()

    # 1. Zero .tmp files left anywhere in lake
    tmp_files = list(lake_root.glob("**/*.tmp"))
    assert len(tmp_files) == 0, f"Found unexpected tmp files: {tmp_files}"

    # 2. Exactly 100 partitions: 50 symbols * 2 dates
    partitions = list(lake_root.glob("ticks/symbol=*/date=*"))
    assert len(partitions) == 100, f"Expected 100 partitions, found {len(partitions)}"

    # 3. DuckDB verification of row count, distinct IDs, and partition integrity
    con = duckdb.connect()
    parquet_glob = str(lake_root / "ticks/*/*/*.parquet")
    res = con.execute(
        f"SELECT count(*), count(DISTINCT ingest_id), count(DISTINCT symbol), count(DISTINCT date_trunc('day', timestamp)) "
        f"FROM read_parquet('{parquet_glob}')"
    ).fetchone()

    total_rows = res[0]
    distinct_ids = res[1]
    distinct_symbols = res[2]
    distinct_days = res[3]

    assert total_rows == 100_000
    assert distinct_ids == 100_000
    assert distinct_symbols == 50
    assert distinct_days == 2


def test_tick_lake_writer_bounded_buffer_backpressure(tmp_path):
    """
    Slow flush mock (time.sleep(0.02)), burst of 1,000 ticks into writer with max_queue_size=200.
    Verifies buffer bounded, total_dropped > 0, and status file reflects drop count.
    """
    lake_root = tmp_path / "lake"
    writer = TickLakeWriter(root=lake_root, max_queue_size=200, max_batch_rows=1000)

    orig_publish = writer.publisher.publish_batch

    def slow_publish(*args, **kwargs):
        time.sleep(0.02)
        return orig_publish(*args, **kwargs)

    writer.publisher.publish_batch = slow_publish

    ticks = [
        ("2026-10-02 12:00:00", f"SYM_{i % 5}", 100.0 + (i % 100), 1.0, 99.9, 100.1, "CAPITAL", "REG")
        for i in range(1000)
    ]

    with ThreadPoolExecutor(max_workers=8) as ex:
        list(ex.map(writer.write_tick, ticks))

    # Buffer must remain bounded <= 200
    with writer._buffer_lock:
        assert len(writer._buffer) <= 200

    # Backpressure caused drops
    assert writer.metrics.total_dropped > 0

    # Flush to ensure rate-limited status file reflects drop count
    writer.flush()

    # Status file reflects drop count
    status_file = lake_root / "_control" / "writer_status.json"
    assert status_file.exists()
    status = json.loads(status_file.read_text())
    assert status["total_dropped"] == writer.metrics.total_dropped
    assert status["total_dropped"] > 0
    assert status["queue_depth"] <= 200

    writer.close()


def test_streaming_engine_concurrent_producers_surge(tmp_path):
    """
    10 concurrent async producers dumping 20,000+ ticks into StreamingEngine(max_queue_size=500).
    Verifies no uncaught QueueFull, write_queue.qsize() <= 500,
    conservation invariant ticks_received == ticks_committed + ticks_dropped,
    and committed ticks queryable via DuckDB.
    """
    async def _run():
        lake_root = tmp_path / "lake"
        engine = StreamingEngine(lake_root=lake_root, max_queue_size=500, flush_interval=0.05)
        engine.running = True

        worker_task = asyncio.create_task(engine._lake_writer_worker())

        async def producer(pid: int):
            for j in range(2500):
                tick = (
                    "2026-10-02 14:00:00.000000",
                    f"SYM_{pid}",
                    100.0 + j,
                    1.0,
                    99.9,
                    100.1,
                    "CAPITAL",
                    "REG",
                )
                engine._enqueue_tick(tick)
                if j % 100 == 0:
                    await asyncio.sleep(0.0005)

        # Run 10 concurrent producers (total 25,000 ticks)
        await asyncio.gather(*(producer(p) for p in range(10)))

        # Assert queue size never exceeded maxsize
        assert engine.write_queue.qsize() <= 500

        # Stop engine and wait for queue to drain
        engine.stop()
        await engine.shutdown()

        worker_task.cancel()
        try:
            await worker_task
        except asyncio.CancelledError:
            pass

        # Conservation invariant: ticks_received == ticks_committed + ticks_dropped
        assert engine.ticks_received == 25_000
        assert engine.ticks_received == engine.ticks_committed + engine.ticks_dropped
        assert engine.ticks_committed > 0

        # Committed ticks queryable via DuckDB
        con = duckdb.connect()
        parquet_glob = str(lake_root / "ticks/*/*/*.parquet")
        db_rows = con.execute(f"SELECT count(*) FROM read_parquet('{parquet_glob}')").fetchone()[0]
        assert db_rows == engine.ticks_committed

    asyncio.run(_run())


def test_micro_batch_boundary_conditions(tmp_path):
    """
    Tests flush triggers at max_batch_rows - 1, exactly max_batch_rows,
    empty buffer flush returning None, and 1 tick + flush_interval_seconds timer.
    """
    lake_root = tmp_path / "lake"
    writer = TickLakeWriter(root=lake_root, max_batch_rows=10, flush_interval_seconds=100.0)

    # 1. max_batch_rows - 1 (9 ticks): does not trigger flush
    for i in range(9):
        writer.write_tick(("2026-10-02 12:00:00", "AAPL", 150.0 + i, 1.0, 149.9, 150.1, "CAPITAL", "REG"))
    assert writer.metrics.batches_published == 0
    with writer._buffer_lock:
        assert len(writer._buffer) == 9

    # 2. Exactly max_batch_rows (10th tick): triggers synchronous flush
    writer.write_tick(("2026-10-02 12:00:00", "AAPL", 159.0, 1.0, 149.9, 150.1, "CAPITAL", "REG"))
    assert writer.metrics.batches_published == 1
    assert writer.metrics.total_published == 10
    with writer._buffer_lock:
        assert len(writer._buffer) == 0

    # 3. Empty buffer flush returns None
    receipt = writer.flush()
    assert receipt is None

    # 4. 1 tick + flush_interval_seconds timer triggers flush
    writer2 = TickLakeWriter(root=lake_root / "sub", max_batch_rows=100, flush_interval_seconds=0.05)
    writer2.write_tick(("2026-10-02 12:00:00", "AAPL", 150.0, 1.0, 149.9, 150.1, "CAPITAL", "REG"))
    assert writer2.metrics.batches_published == 0

    time.sleep(0.15)
    assert writer2.metrics.batches_published == 1
    assert writer2.metrics.total_published == 1

    writer.close()
    writer2.close()


def test_monotonic_clock_immunity_to_wall_clock_ntp_jumps(tmp_path, monkeypatch):
    """
    Verifies flusher uses time.monotonic() and is immune to datetime.now() jumps backward by 1 hour.
    """
    lake_root = tmp_path / "lake"
    writer = TickLakeWriter(root=lake_root, max_batch_rows=100, flush_interval_seconds=0.05)

    # Monkeypatch datetime.now to jump backward by 1 hour
    real_datetime = datetime
    class JumperDatetime(datetime):
        @classmethod
        def now(cls, tz=None):
            return real_datetime.now(tz) - timedelta(hours=1)

    monkeypatch.setattr("src.storage.parquet_writer.datetime", JumperDatetime)

    # Write 1 tick
    writer.write_tick(("2026-10-02 12:00:00", "AAPL", 150.0, 1.0, 149.9, 150.1, "CAPITAL", "REG"))
    assert writer.metrics.batches_published == 0

    # Wait for monotonic timer to expire (0.05s interval -> wait 0.15s)
    time.sleep(0.15)

    # Flusher must have flushed despite wall clock jumping backward
    assert writer.metrics.batches_published == 1
    assert writer.metrics.total_published == 1

    writer.close()


# =====================================================================
# Group 2: Shutdown, Drain Timeouts, and Lifecycle (TEST-P23-02)
# =====================================================================


def test_runner_shutdown_mid_flush_race(tmp_path):
    """
    In-flight flush barrier (asyncio.Event), stop() and shutdown() called while flush in flight.
    Verifies in-flight batch completes cleanly, all rows committed, lock cleanly released, 0 .tmp files.
    """
    async def _run():
        lake_root = tmp_path / "lake"
        engine = StreamingEngine(lake_root=lake_root, flush_interval=0.05)
        engine.running = True

        in_flush = asyncio.Event()
        release_flush = asyncio.Event()
        orig_publish = engine.writer.publish_batch_async

        async def hooked_publish(batch):
            in_flush.set()
            await release_flush.wait()
            return await orig_publish(batch)

        engine.writer.publish_batch_async = hooked_publish

        worker_task = asyncio.create_task(engine._lake_writer_worker())

        for i in range(10):
            engine._enqueue_tick(("2026-10-02 12:00:00", "AAPL", 100.0 + i, 1.0, 99.9, 100.1, "CAPITAL", "REG"))

        await asyncio.wait_for(in_flush.wait(), timeout=2.0)

        # While flush is in flight, stop() and shutdown() called
        engine.stop()
        shutdown_task = asyncio.create_task(engine.shutdown())

        # Ensure shutdown has started waiting on write_queue.join()
        await asyncio.sleep(0.05)

        # Release in-flight flush
        release_flush.set()
        await asyncio.wait_for(shutdown_task, timeout=3.0)

        assert engine.ticks_committed == 10
        assert engine.writer.status == "STOPPED"

        # Publisher lock cleanly released
        with LakePublisherLock(lake_root, "test_verify") as lock:
            assert lock._is_locked

        # Zero .tmp files left
        tmp_files = list(lake_root.glob("_staging/*.tmp"))
        assert len(tmp_files) == 0

        worker_task.cancel()
        try:
            await worker_task
        except asyncio.CancelledError:
            pass

    asyncio.run(_run())


def test_runner_drain_timeout_enforcement_and_honest_reporting(tmp_path):
    """
    Drain timeout (e.g. 0.2s) with slow flush mock.
    Verifies shutdown finishes without hanging, logs timeout, accounts undrained tasks in ticks_dropped.
    """
    async def _run():
        lake_root = tmp_path / "lake"
        engine = StreamingEngine(lake_root=lake_root, flush_interval=0.05)
        engine.running = True

        # Enqueue ticks without running worker so tasks remain unfinished
        for i in range(25):
            engine._enqueue_tick(("2026-10-02 12:00:00", "AAPL", 100.0 + i, 1.0, 99.9, 100.1, "CAPITAL", "REG"))

        assert engine.write_queue.unfinished_tasks == 25

        t0 = time.monotonic()
        # shutdown with short drain_timeout of 0.2s
        await engine.shutdown(drain_timeout=0.2)
        elapsed = time.monotonic() - t0

        assert elapsed < 0.8  # Terminated promptly around 0.2s without freezing
        assert engine.ticks_dropped >= 25  # Undrained tasks accounted in ticks_dropped

    asyncio.run(_run())


def test_honest_task_done_accounting_under_partial_flushes(tmp_path):
    """
    Verifies unfinished_tasks remains strictly > 0 while batch write is in flight,
    decrements to 0 only after fsync receipt.
    """
    async def _run():
        lake_root = tmp_path / "lake"
        engine = StreamingEngine(lake_root=lake_root, flush_interval=0.05)
        engine.running = True

        in_write = asyncio.Event()
        release_write = asyncio.Event()
        orig_publish = engine.writer.publish_batch_async

        async def gated_publish(batch):
            in_write.set()
            await release_write.wait()
            return await orig_publish(batch)

        engine.writer.publish_batch_async = gated_publish
        worker_task = asyncio.create_task(engine._lake_writer_worker())

        for i in range(5):
            engine._enqueue_tick(("2026-10-02 12:00:00", "MSFT", 300.0 + i, 1.0, 299.9, 300.1, "CAPITAL", "REG"))

        await asyncio.wait_for(in_write.wait(), timeout=2.0)

        # While write is in flight, unfinished_tasks must remain strictly > 0 (honest accounting)
        assert engine.write_queue.unfinished_tasks == 5

        # Release fsync/write receipt
        release_write.set()

        await asyncio.wait_for(engine.write_queue.join(), timeout=2.0)
        assert engine.write_queue.unfinished_tasks == 0
        assert engine.ticks_committed == 5

        engine.stop()
        worker_task.cancel()
        try:
            await worker_task
        except asyncio.CancelledError:
            pass
        await engine.shutdown()

    asyncio.run(_run())


def test_runner_sigint_sigterm_lifecycle(tmp_path):
    """
    Spawns real OS subprocess running python -m src.stream.runner --mock ...,
    tests real SIGINT and SIGTERM, verifies _handle_signal catches the signal,
    drains queued ticks to Parquet lake, updates status to STOPPED, and exits with returncode 0.
    """
    for sig in (signal.SIGINT, signal.SIGTERM):
        lake_root = tmp_path / f"lake_signal_{sig.name}"
        proc = subprocess.Popen(
            [
                sys.executable,
                "-m", "src.stream.runner",
                "--lake-root", str(lake_root),
                "--writer-id", f"writer_{sig.name.lower()}",
                "--flush-interval", "0.1",
                "--mock",
                "--ticks-per-sec", "100.0",
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            env={**os.environ, "PYTHONUNBUFFERED": "1"},
        )

        try:
            # Wait for runner to start and write at least one batch
            status_file = lake_root / "_control" / "writer_status.json"
            started = False
            for _ in range(50):
                time.sleep(0.1)
                if status_file.exists():
                    try:
                        data = json.loads(status_file.read_text(encoding="utf-8"))
                        if data.get("status") == "RUNNING" and data.get("total_rows_written", 0) > 0:
                            started = True
                            break
                    except Exception:
                        pass
            assert started, f"Runner failed to start and write ticks under {sig.name}"

            # Deliver real signal
            proc.send_signal(sig)

            # Wait for graceful drain and clean termination
            stdout, stderr = proc.communicate(timeout=6.0)
            assert proc.returncode == 0, (
                f"Expected returncode 0 on {sig.name}, got {proc.returncode}. "
                f"Stderr: {stderr.decode()}"
            )

            # Assert status file reflects STOPPED
            assert status_file.exists()
            final_status = json.loads(status_file.read_text(encoding="utf-8"))
            assert final_status.get("status") == "STOPPED"
            assert final_status.get("total_rows_written", 0) > 0

            # Verify Parquet row count > 0 via DuckDB
            parquet_files = [str(p) for p in lake_root.glob("ticks/symbol=*/date=*/*.parquet")]
            assert len(parquet_files) > 0, f"No Parquet files found after {sig.name} shutdown"
            con = duckdb.connect(":memory:")
            try:
                cnt = con.execute("SELECT count(*) FROM read_parquet(?, hive_partitioning=false)", [parquet_files]).fetchone()[0]
                assert cnt == final_status["total_rows_written"]
                assert cnt > 0
            finally:
                con.close()
        finally:
            if proc.poll() is None:
                proc.kill()
                proc.wait()


def test_lake_writer_worker_cancellation_shielding(tmp_path):
    """
    Worker task cancellation does not abort in-flight or buffered final flush.
    """
    async def _run():
        lake_root = tmp_path / "lake"
        engine = StreamingEngine(lake_root=lake_root, flush_interval=0.05)
        engine.running = True

        in_publish = asyncio.Event()
        release_publish = asyncio.Event()
        orig_publish = engine.writer.publish_batch_async

        async def shielded_publish(batch):
            in_publish.set()
            await release_publish.wait()
            return await orig_publish(batch)

        engine.writer.publish_batch_async = shielded_publish
        worker_task = asyncio.create_task(engine._lake_writer_worker())

        for i in range(5):
            engine._enqueue_tick(("2026-10-02 12:00:00", "AAPL", 150.0 + i, 1.0, 149.9, 150.1, "CAPITAL", "REG"))

        # Wait until publish starts
        await asyncio.wait_for(in_publish.wait(), timeout=2.0)

        # Cancel worker while publish is in flight
        worker_task.cancel()

        # Release publish
        release_publish.set()

        # Await worker task
        try:
            await worker_task
        except asyncio.CancelledError:
            pass

        assert engine.ticks_committed == 5
        assert engine.write_queue.unfinished_tasks == 0

        con = duckdb.connect()
        parquet_glob = str(lake_root / "ticks/*/*/*.parquet")
        count = con.execute(f"SELECT count(*) FROM read_parquet('{parquet_glob}')").fetchone()[0]
        assert count == 5

        await engine.shutdown()

    asyncio.run(_run())


# =====================================================================
# Group 3: Transient Faults, Retries, and Telemetry (TEST-P23-03)
# =====================================================================


def test_transient_enospc_disk_full_exponential_backoff(tmp_path):
    """
    Mock raises OSError(errno.ENOSPC) on attempts 1 and 2, succeeds on attempt 3.
    Verifies 3 attempts, total_retrying == 2, last_error recorded, write succeeds, 20 rows verified in Parquet.
    """
    lake_root = tmp_path / "lake"
    writer = TickLakeWriter(root=lake_root, retry_attempts=3, retry_backoff_base=0.01)

    attempts = 0
    orig_publish = writer.publisher.publish_batch

    def failing_publish(*args, **kwargs):
        nonlocal attempts
        attempts += 1
        if attempts < 3:
            raise OSError(errno.ENOSPC, "No space left on device")
        return orig_publish(*args, **kwargs)

    writer.publisher.publish_batch = failing_publish

    ticks = [
        ("2026-10-02 12:00:00", "AAPL", 150.0 + i, 1.0, 149.9, 150.1, "CAPITAL", "REG")
        for i in range(20)
    ]

    receipt = writer.publish_batch(ticks)
    assert attempts == 3
    assert writer.metrics.total_retrying == 2
    assert "No space left on device" in (writer.metrics.last_error or "")
    assert receipt.status == "PUBLISHED"
    assert receipt.row_count == 20

    con = duckdb.connect()
    parquet_glob = str(lake_root / "ticks/*/*/*.parquet")
    count = con.execute(f"SELECT count(*) FROM read_parquet('{parquet_glob}')").fetchone()[0]
    assert count == 20

    writer.close()


def test_exhausted_retries_io_error_telemetry(tmp_path):
    """
    Mock permanently raises OSError(errno.EIO).
    Verifies retries up to limit, total_retrying == 3, status file records error state and timestamp,
    staging temp files cleaned up.
    """
    lake_root = tmp_path / "lake"
    writer = TickLakeWriter(root=lake_root, retry_attempts=3, retry_backoff_base=0.01)

    def failing_publish(*args, **kwargs):
        raise OSError(errno.EIO, "Input/output error")

    writer.publisher.publish_batch = failing_publish

    ticks = [
        ("2026-10-02 12:00:00", "AAPL", 150.0 + i, 1.0, 149.9, 150.1, "CAPITAL", "REG")
        for i in range(10)
    ]

    with pytest.raises(OSError, match="Input/output error"):
        writer.publish_batch(ticks)

    assert writer.metrics.total_retrying == 3

    # Status file records error state and timestamp
    status_file = lake_root / "_control" / "writer_status.json"
    assert status_file.exists()
    status = json.loads(status_file.read_text())
    assert "Input/output error" in (status.get("last_error") or "")
    assert status.get("total_retrying") == 3
    assert status.get("last_error_time") is not None

    # Staging temp files cleaned up
    staging_tmps = list(lake_root.glob("_staging/*.tmp"))
    assert len(staging_tmps) == 0

    writer.close()


def test_disk_full_in_runner_pipeline_prevents_queue_hang(tmp_path):
    """
    Mock publish_batch_async in StreamingEngine to raise OSError(errno.ENOSPC).
    Verifies shutdown() does not freeze or time out, ticks_dropped incremented, unfinished_tasks cleared cleanly.
    """
    async def _run():
        lake_root = tmp_path / "lake"
        engine = StreamingEngine(lake_root=lake_root, flush_interval=0.05)
        engine.running = True

        async def enospc_publish(batch):
            raise OSError(errno.ENOSPC, "No space left on device")

        engine.writer.publish_batch_async = enospc_publish
        worker_task = asyncio.create_task(engine._lake_writer_worker())

        for i in range(10):
            engine._enqueue_tick(("2026-10-02 12:00:00", "NVDA", 450.0 + i, 1.0, 449.9, 450.1, "CAPITAL", "REG"))

        # When worker encounters error, it must task_done() so shutdown does not hang
        await engine.shutdown(drain_timeout=2.0)

        assert engine.ticks_dropped == 10
        assert engine.write_queue.unfinished_tasks == 0

        worker_task.cancel()
        try:
            await worker_task
        except asyncio.CancelledError:
            pass

    asyncio.run(_run())


def test_comprehensive_malformed_tick_quarantine(tmp_path):
    """
    50 valid ticks mixed with 50 poison ticks (NaN, +inf, negative price, zero price,
    negative volume, None symbol, integer symbol, path traversal symbol, null timestamp).
    Verifies total_quarantined == 50, total_published == 50, only valid ticks stored in Parquet.
    """
    lake_root = tmp_path / "lake"
    writer = TickLakeWriter(root=lake_root)

    # 50 poison ticks with various malformations
    poison_ticks = [
        {"symbol": "AAPL", "price": float("nan")},
        {"symbol": "AAPL", "price": float("inf")},
        {"symbol": "AAPL", "price": float("-inf")},
        {"symbol": "AAPL", "price": -50.0},
        {"symbol": "AAPL", "price": 0.0},
        {"symbol": "AAPL", "price": -0.0},
        {"symbol": "AAPL", "price": "not_a_number"},
        {"symbol": None, "price": 150.0},
        {"symbol": "", "price": 150.0},
        {"symbol": 12345, "price": 150.0},
        {"symbol": ["AAPL"], "price": 150.0},
        {"symbol": "../../../etc/passwd", "price": 150.0},
        {"symbol": "AAPL/../SECRET", "price": 150.0},
        {"symbol": "AAPL", "price": 150.0, "volume": -1.0},
        {"symbol": "AAPL", "price": 150.0, "volume": float("nan")},
        {"symbol": "AAPL", "price": 150.0, "volume": float("inf")},
        {"symbol": "AAPL", "price": 150.0, "bid": -1.0},
        {"symbol": "AAPL", "price": 150.0, "bid": float("nan")},
        {"symbol": "AAPL", "price": 150.0, "bid": float("inf")},
        {"symbol": "AAPL", "price": 150.0, "ask": -1.0},
        {"symbol": "AAPL", "price": 150.0, "ask": float("nan")},
        {"symbol": "AAPL", "price": 150.0, "ask": float("inf")},
        {"symbol": "AAPL", "price": 150.0, "timestamp": "invalid-datetime-format"},
        ("not_enough_tuple_elements", 123),
        None,
    ]
    # Pad to reach exactly 50 poison ticks
    for k in range(len(poison_ticks), 50):
        poison_ticks.append({"symbol": f"BAD_{k}", "price": -10.0 - k})

    assert len(poison_ticks) == 50

    # 50 valid ticks
    valid_ticks = [
        QuoteTick(
            timestamp=datetime(2026, 10, 2, 12, 0, 0, tzinfo=timezone.utc).replace(tzinfo=None),
            symbol=f"SYM_{i % 5}",
            price=100.0 + i,
            volume=1.0,
            bid=99.9,
            ask=100.1,
            source="CAPITAL",
            session="REG",
            ingest_id=f"val_{i:04d}",
        )
        for i in range(50)
    ]
    assert len(valid_ticks) == 50

    # Interleave valid and poison ticks
    mixed = []
    for v, p in zip(valid_ticks, poison_ticks):
        mixed.append(v)
        mixed.append(p)

    receipt = writer.publish_batch(mixed)

    assert writer.metrics.total_quarantined == 50
    assert writer.metrics.total_published == 50
    assert receipt.row_count == 50

    con = duckdb.connect()
    parquet_glob = str(lake_root / "ticks/*/*/*.parquet")
    rows = con.execute(f"SELECT count(*), min(price), max(price) FROM read_parquet('{parquet_glob}')").fetchone()
    assert rows[0] == 50
    assert rows[1] >= 100.0  # min price
    assert rows[2] <= 150.0  # max price

    writer.close()
