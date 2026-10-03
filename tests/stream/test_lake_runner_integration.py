"""
Integration tests for StreamingEngine refactored lifecycle with TickLakeWriter (Milestone v4.0 - Phase 17).

Verifies StreamingEngine initializes TickLakeWriter without opening DuckDB, implements
honest task_done acknowledgment, provides bounded queue backpressure, routes Capital.com
and Binance callbacks to Parquet, and guarantees zero data loss on graceful shutdown drain.
"""
import asyncio
from datetime import datetime, timezone
import duckdb
import pytest

from src.storage.parquet_writer import TickLakeWriter
from src.storage.publication import LakePublisherLock
from src.stream.runner import StreamingEngine


def test_engine_initializes_with_tick_lake(tmp_path):
    """
    Verifies StreamingEngine(lake_root=tmp_path) configures TickLakeWriter and
    does not open or initialize streaming.duckdb.
    """
    lake_root = tmp_path / "lake"
    engine = StreamingEngine(lake_root=lake_root)

    assert engine.lake_root == lake_root
    assert isinstance(engine.writer, TickLakeWriter)
    assert engine.db_conn is None

    # Assert no streaming.duckdb file was opened or created
    assert not (lake_root / "streaming.duckdb").exists()
    assert not (tmp_path / "streaming.duckdb").exists()
    engine.stop()


def test_honest_task_done_acknowledgment(tmp_path):
    """
    Enqueues ticks to engine.write_queue; asserts task_done() is NOT called
    prematurely before the Parquet batch is confirmed written to disk.
    """
    async def _run():
        lake_root = tmp_path / "lake"
        engine = StreamingEngine(lake_root=lake_root, flush_interval=0.05)
        engine.running = True

        in_publish = asyncio.Event()
        release_publish = asyncio.Event()

        orig_publish = engine.writer.publish_batch_async

        async def delayed_publish(batch):
            in_publish.set()
            await release_publish.wait()
            return await orig_publish(batch)

        engine.writer.publish_batch_async = delayed_publish

        tick = ("2026-10-02 14:30:00.000000", "AAPL", 150.0, 1.0, 149.9, 150.1, "CAPITAL", "REG")
        engine._enqueue_tick(tick)

        assert engine.write_queue.unfinished_tasks == 1

        worker_task = asyncio.create_task(engine._lake_writer_worker())

        try:
            # Wait until writer worker enters delayed publish
            await asyncio.wait_for(in_publish.wait(), timeout=2.0)

            # While write is in-flight to disk, task_done() must NOT have been called prematurely
            assert engine.write_queue.unfinished_tasks == 1, (
                "Dishonest task_done: task_done() was called before Parquet batch was confirmed written to disk!"
            )

            # Release the gate to finish disk publication
            release_publish.set()

            # Await write_queue.join() - must complete once disk write is acknowledged
            await asyncio.wait_for(engine.write_queue.join(), timeout=2.0)
            assert engine.write_queue.unfinished_tasks == 0
        finally:
            engine.stop()
            worker_task.cancel()
            try:
                await worker_task
            except asyncio.CancelledError:
                pass
            await engine.shutdown()

    asyncio.run(_run())


def test_bounded_queue_backpressure(tmp_path):
    """
    With max_queue_size=5, verifies engine.write_queue.full() occurs after 5 ticks,
    creating backpressure.
    """
    lake_root = tmp_path / "lake"
    engine = StreamingEngine(lake_root=lake_root, max_queue_size=5)

    assert engine.write_queue.maxsize == 5
    assert not engine.write_queue.full()

    # Enqueue 5 ticks
    for i in range(5):
        tick = ("2026-10-02 14:30:00.000000", "AAPL", 150.0 + i, 1.0, 149.9, 150.1, "CAPITAL", "REG")
        engine._enqueue_tick(tick)

    assert engine.write_queue.full(), "write_queue must be full after 5 items"

    # Attempting to enqueue 6th item without draining must raise QueueFull
    tick_6 = ("2026-10-02 14:30:00.000000", "AAPL", 155.0, 1.0, 149.9, 150.1, "CAPITAL", "REG")
    with pytest.raises(asyncio.QueueFull):
        engine.write_queue.put_nowait(tick_6)


def test_capital_callback_to_parquet_e2e(tmp_path):
    """
    Simulates Capital.com WebSocket quote callback; verifies ticks are normalized,
    batched, published to Parquet, and readable via in-memory DuckDB read_parquet.
    """
    async def _run():
        lake_root = tmp_path / "lake"
        engine = StreamingEngine(lake_root=lake_root, flush_interval=0.05)
        engine.running = True

        worker_task = asyncio.create_task(engine._lake_writer_worker())

        try:
            dt = datetime(2026, 10, 2, 14, 30, 0, 123456, tzinfo=timezone.utc)
            capital_tick = {
                "epic": "AAPL",
                "price": 182.50,
                "bid": 182.45,
                "ask": 182.55,
                "timestamp": dt,
            }
            await engine._handle_capital_tick(capital_tick)

            # Wait for queue drain and flush
            await asyncio.wait_for(engine.write_queue.join(), timeout=3.0)
            await asyncio.sleep(0.1)
        finally:
            engine.stop()
            worker_task.cancel()
            try:
                await worker_task
            except asyncio.CancelledError:
                pass
            await engine.shutdown()

        # Verify Parquet file in tick lake
        parquet_files = list(lake_root.glob("ticks/*/*/*.parquet"))
        assert len(parquet_files) >= 1, "Expected Parquet file generated in tick lake"

        con = duckdb.connect(":memory:")
        df = con.execute("SELECT symbol, price, bid, ask, source FROM read_parquet(?)", [str(parquet_files[0])]).fetchdf()
        assert len(df) == 1
        assert df.iloc[0]["symbol"] == "AAPL"
        assert df.iloc[0]["price"] == 182.50
        assert df.iloc[0]["bid"] == 182.45
        assert df.iloc[0]["ask"] == 182.55
        assert df.iloc[0]["source"] == "CAPITAL"
        con.close()

    asyncio.run(_run())


def test_binance_callback_to_parquet_e2e(tmp_path):
    """
    Simulates Binance trade and kline callbacks; verifies normalization and
    Parquet publication.
    """
    async def _run():
        lake_root = tmp_path / "lake"
        engine = StreamingEngine(lake_root=lake_root, flush_interval=0.05, enable_binance=True)
        engine.running = True

        worker_task = asyncio.create_task(engine._lake_writer_worker())

        try:
            # 1. Binance trade tick
            binance_trade = ("2026-10-02 14:30:00.654321", "BTCUSDT", 65000.0, 0.75, None, None, "BINANCE", "REG")
            await engine._handle_binance_tick(binance_trade)

            # 2. Binance closed kline bar
            binance_bar = ("2026-10-02 14:30:00.000000", "ETHUSDT", 2500.0, 2510.0, 2490.0, 2505.0, 15.0, "REG", "BINANCE")
            await engine._handle_binance_bar(binance_bar, is_closed=True)

            await asyncio.wait_for(engine.write_queue.join(), timeout=3.0)
            await asyncio.sleep(0.1)
        finally:
            engine.stop()
            worker_task.cancel()
            try:
                await worker_task
            except asyncio.CancelledError:
                pass
            await engine.shutdown()

        con = duckdb.connect(":memory:")
        df = con.execute(f"SELECT symbol, price, source FROM read_parquet('{lake_root}/ticks/*/*/*.parquet')").fetchdf()
        assert len(df) == 2
        symbols = set(df["symbol"].tolist())
        assert "BTCUSDT" in symbols
        assert "ETHUSDT" in symbols
        assert all(df["source"] == "BINANCE")
        con.close()

    asyncio.run(_run())


def test_graceful_shutdown_drain(tmp_path):
    """
    Enqueues ticks, calls engine.stop(), awaits engine.shutdown(); verifies all
    remaining queued ticks are flushed to Parquet and lock is released before
    process termination.
    """
    async def _run():
        lake_root = tmp_path / "lake"
        # Long flush interval so ticks stay in queue until shutdown is initiated
        engine = StreamingEngine(lake_root=lake_root, flush_interval=60.0)
        engine.running = True

        worker_task = asyncio.create_task(engine._lake_writer_worker())

        for i in range(10):
            tick = (f"2026-10-02 14:30:{i:02d}.000000", "AAPL", 150.0 + i, 1.0, 149.9, 150.1, "CAPITAL", "REG")
            engine._enqueue_tick(tick)

        # Initiate graceful stop and drain shutdown
        engine.stop()
        await engine.shutdown()
        worker_task.cancel()
        try:
            await worker_task
        except asyncio.CancelledError:
            pass

        assert engine.write_queue.empty()
        assert engine.write_queue.unfinished_tasks == 0

        con = duckdb.connect(":memory:")
        df = con.execute(f"SELECT count(*) as cnt FROM read_parquet('{lake_root}/ticks/*/*/*.parquet')").fetchdf()
        assert df.iloc[0]["cnt"] == 10
        con.close()

        # Verify lock released: another instance can acquire ownership immediately
        new_lock = LakePublisherLock(root=lake_root, writer_id="drain_checker")
        assert new_lock.acquire() is True
        new_lock.release()

    asyncio.run(_run())
