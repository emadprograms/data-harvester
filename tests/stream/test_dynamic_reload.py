"""
Tests for Capital.com Exclusive WebSocket Streamer & Dynamic Reload (Phase 6).
Validates exclusive Capital.com ingestion, dynamic subscription update over live WebSocket,
symbol inventory reloading from the registry, and queue-to-Parquet writing.
"""
import asyncio
import json
import pytest
import duckdb
from datetime import datetime, timezone
from unittest.mock import AsyncMock

from src.stream.capital_stream import CapitalStreamer
from src.stream.runner import StreamingEngine


def make_mock_ws():
    """Mock WebSocket client with async send and recv methods."""
    ws = AsyncMock()
    ws.closed = False
    ws.send = AsyncMock()
    ws.recv = AsyncMock()
    return ws


def test_capital_exclusive_engine_configuration(tmp_path):
    """Verify that by default the StreamingEngine targets Capital.com exclusively and does not enable Binance."""
    engine = StreamingEngine(lake_root=tmp_path / "lake")
    try:
        assert engine.enable_binance is False
        assert engine.binance_streamer is None
    finally:
        engine.stop()


def test_capital_streamer_dynamic_subscription_update():
    """Verify that update_subscriptions sends appropriate subscribe and unsubscribe messages."""
    mock_ws = make_mock_ws()

    async def _test():
        streamer = CapitalStreamer(epics=["SPY", "QQQ"])
        streamer.running = True
        streamer.ws = mock_ws
        streamer._cst = "test-cst"
        streamer._sec_token = "test-token"

        # Dynamic update: remove QQQ, keep SPY, add AAPL and NVDA
        success = await streamer.update_subscriptions(["SPY", "AAPL", "NVDA"])
        assert success is True
        assert set(streamer.epics) == {"SPY", "AAPL", "NVDA"}

        # Verify WebSocket calls: one unsubscribe for QQQ, one subscribe for AAPL & NVDA
        assert mock_ws.send.call_count == 2

        call_msgs = [json.loads(call.args[0]) for call in mock_ws.send.call_args_list]
        destinations = [msg["destination"] for msg in call_msgs]
        assert "marketData.unsubscribe" in destinations
        assert "marketData.subscribe" in destinations

        unsub_msg = next(m for m in call_msgs if m["destination"] == "marketData.unsubscribe")
        assert unsub_msg["payload"]["epics"] == ["QQQ"]

        sub_msg = next(m for m in call_msgs if m["destination"] == "marketData.subscribe")
        assert set(sub_msg["payload"]["epics"]) == {"AAPL", "NVDA"}

    asyncio.run(_test())


def test_capital_streamer_update_when_disconnected():
    """Verify that updating epics when disconnected updates self.epics without throwing errors."""
    async def _test():
        streamer = CapitalStreamer(epics=["SPY"])
        streamer.running = False
        streamer.ws = None

        success = await streamer.update_subscriptions(["SPY", "TSLA"])
        assert success is True
        assert streamer.epics == ["SPY", "TSLA"]

    asyncio.run(_test())


def test_streaming_engine_reload_symbols(tmp_path):
    """Verify reload_symbols reads the registry and delegates to capital_streamer."""
    async def _test():
        lake_root = tmp_path / "lake"
        engine = StreamingEngine(lake_root=lake_root)
        try:
            engine.registry.add_symbol("SPY", display_name="SPY", capital_ticker="SPY")
            engine.registry.add_symbol("NVDA", display_name="NVDA", capital_ticker="NVDA")

            mock_streamer = AsyncMock()
            mock_streamer.update_subscriptions = AsyncMock(return_value=True)
            mock_streamer.stop = AsyncMock()
            engine.capital_streamer = mock_streamer

            success = await engine.reload_symbols()
            assert success is True
            mock_streamer.update_subscriptions.assert_called_once()
            called_symbols = mock_streamer.update_subscriptions.call_args[0][0]
            assert set(called_symbols) == {"SPY", "NVDA"}
        finally:
            engine.stop()

    asyncio.run(_test())


def test_streaming_engine_trigger_reload_signal(tmp_path):
    """Verify trigger_reload sets the reload event."""
    engine = StreamingEngine(lake_root=tmp_path / "lake")
    try:
        assert not engine.reload_event.is_set()
        engine.trigger_reload()
        assert engine.reload_event.is_set()
    finally:
        engine.stop()


def test_streaming_engine_writer_batches_ticks_into_tick_lake(tmp_path):
    """Verify that ticks enqueued into StreamingEngine are written to the Parquet tick lake."""
    async def _test():
        lake_root = tmp_path / "lake"
        engine = StreamingEngine(lake_root=lake_root, flush_interval=0.05)
        engine.running = True

        for symbol, price, bid, ask in (("SPY", 575.25, 575.20, 575.30), ("QQQ", 490.50, 490.45, 490.55)):
            engine._enqueue_tick(
                ("2026-09-25 12:00:00.000000", symbol, price, 1.0, bid, ask, "CAPITAL", "REG")
            )

        worker_task = asyncio.create_task(engine._lake_writer_worker())
        await asyncio.sleep(0.3)
        engine.running = False
        await asyncio.sleep(0.1)
        worker_task.cancel()
        try:
            await worker_task
        except asyncio.CancelledError:
            pass

        files = sorted((lake_root / "ticks").rglob("*.parquet"))
        assert files, "tick lake received no Parquet batches"
        with duckdb.connect() as con:
            df = con.execute(
                "SELECT symbol, price, source FROM read_parquet(?)",
                [[str(f) for f in files]],
            ).fetchdf()
        engine.stop()

        assert set(df["symbol"]) == {"SPY", "QQQ"}
        spy = df[df["symbol"] == "SPY"].iloc[0]
        assert spy["price"] == pytest.approx(575.25)
        assert spy["source"] == "CAPITAL"
        qqq = df[df["symbol"] == "QQQ"].iloc[0]
        assert qqq["price"] == pytest.approx(490.50)

    asyncio.run(_test())
