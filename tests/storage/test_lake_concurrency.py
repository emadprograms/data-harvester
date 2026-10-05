"""Concurrency test suite for TickLakeReader.

v5.0 deleted the disk-database layer, so the old "a locked legacy database does
not block the reader" test has nothing left to simulate: no runtime path can
create or lock a ``.duckdb`` file any more (see
``tests/test_disk_database_layer_removed.py``). What remains is the property that
still matters — many concurrent readers over the same lake.

Validates: 10 parallel threads executing get_candles and get_tape simultaneously
without race conditions, thread safety errors, or crashes.
"""
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
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
