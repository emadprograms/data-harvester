"""
Concurrency and lock-freedom test suite for TickLakeReader (Milestone v4.0 - Phase 19).

Validates:
1. Lock-free reading: Reader never acquires a disk lock on streaming.duckdb.
   Simulates another process holding an exclusive write lock on streaming.duckdb.
   TickLakeReader queries execute without any lock collision because they use in-memory (:memory:) DuckDB.
2. Concurrent reader threads: 10 parallel threads executing get_candles and get_tape simultaneously
   without race conditions, thread safety errors, or crashes.
"""
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
import duckdb
import pytest

from src.storage.config import init_tick_lake
from src.storage.publication import LakePublisher
from src.storage.reader import TickLakeReader
from tests.fixtures.deterministic_quotes import QuoteTick


@pytest.fixture
def test_lake_with_data(tmp_path):
    """Initializes a tick lake with a batch of published quotes."""
    lake_root = tmp_path / "lake"
    init_tick_lake(lake_root)

    t_base = datetime(2026, 10, 2, 14, 30, 0)
    ticks = [
        QuoteTick(
            timestamp=t_base + timedelta(seconds=i),
            symbol="AAPL",
            price=150.0 + i * 0.1,
            volume=10.0,
            bid=149.95 + i * 0.1,
            ask=150.05 + i * 0.1,
            source="CAPITAL",
            session="REG",
            ingest_id=f"lockfree_{i:04d}",
        )
        for i in range(100)
    ]
    with LakePublisher(root=lake_root, writer_id="w1") as publisher:
        publisher.publish_batch(ticks, batch_id="batch_lockfree_01", sequence=1)

    return lake_root


import subprocess
import sys
import time

def test_lake_reader_never_acquires_disk_lock(test_lake_with_data, tmp_path):
    """
    Simulate another process holding an exclusive write lock on a legacy streaming.duckdb file.
    Execute TickLakeReader queries against the Parquet lake; verify they succeed without any
    duckdb.IOException or lock collisions because TickLakeReader uses isolated in-memory (:memory:) sessions.
    """
    # Simulate an external process holding an exclusive write lock on streaming.duckdb
    dummy_db_dir = tmp_path / "legacy_db"
    dummy_db_dir.mkdir(parents=True, exist_ok=True)
    streaming_db_path = dummy_db_dir / "streaming.duckdb"

    # Start external process holding exclusive write connection to streaming.duckdb
    proc = subprocess.Popen([
        sys.executable, "-c",
        f"import duckdb, time; c = duckdb.connect(r'{streaming_db_path}', read_only=False); c.execute('CREATE TABLE locks (id INT PRIMARY KEY, locked_at TIMESTAMP);'); c.execute('INSERT INTO locks VALUES (1, now());'); time.sleep(30)"
    ])
    time.sleep(0.6)

    try:
        # A second read-write connection to the same file from this process fails with duckdb.IOException
        with pytest.raises(duckdb.IOException):
            _ = duckdb.connect(str(streaming_db_path), read_only=False)

        # Now execute TickLakeReader queries on the lake
        reader = TickLakeReader(root=test_lake_with_data)

        # 1. query_candles
        candles = reader.query_candles(symbol="AAPL", timeframe="1m")
        assert len(candles) > 0

        # 2. get_candles
        dashboard_candles = reader.get_candles(symbol="AAPL", date="2026-10-02")
        assert dashboard_candles.get("count", 0) > 0

        # 3. get_tape
        tape = reader.get_tape(symbol="AAPL", limit=20)
        assert len(tape.get("ticks", [])) > 0

        # 4. get_latest_tick
        latest = reader.get_latest_tick(symbol="AAPL")
        assert latest is not None
        assert latest.get("symbol") == "AAPL"

    finally:
        proc.terminate()
        try:
            proc.wait(timeout=3)
        except Exception:
            proc.kill()


def test_concurrent_reader_threads(test_lake_with_data):
    """
    Run 10 concurrent threads executing get_candles and get_tape simultaneously on TickLakeReader.
    Verify all 10 threads complete without race conditions, shared connection leaks, or crashes.
    """
    reader = TickLakeReader(root=test_lake_with_data)

    def reader_worker(worker_id: int):
        # Execute multiple queries per worker
        res_candles = reader.get_candles(symbol="AAPL", date="2026-10-02", timeframe="1m")
        res_tape = reader.get_tape(symbol="AAPL", limit=10)
        res_query = reader.query_candles(symbol="AAPL", timeframe="1m")
        return {
            "worker_id": worker_id,
            "candles_count": res_candles.get("count", 0),
            "tape_count": len(res_tape.get("ticks", [])),
            "raw_candles_count": len(res_query),
        }

    num_workers = 10
    with ThreadPoolExecutor(max_workers=num_workers) as executor:
        futures = [executor.submit(reader_worker, i) for i in range(num_workers)]
        results = [f.result() for f in as_completed(futures)]

    assert len(results) == num_workers
    for r in results:
        assert r["candles_count"] > 0
        assert r["tape_count"] > 0
        assert r["raw_candles_count"] > 0
