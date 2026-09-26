"""
Tests for Capital.com Exclusive WebSocket Streamer & Dynamic Reload (Phase 6).
Validates exclusive Capital.com ingestion, dynamic subscription update over live WebSocket,
symbol inventory reloading, and queue-to-DuckDB writing.
"""
import asyncio
import json
import os
import tempfile
import pytest
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch

from src.database.connection import DuckDBClient
from src.database.schema import init_streaming_db
from src.database.operations import query_ticks
from src.stream.capital_stream import CapitalStreamer
from src.stream.runner import StreamingEngine


def make_mock_ws():
    """Mock WebSocket client with async send and recv methods."""
    ws = AsyncMock()
    ws.closed = False
    ws.send = AsyncMock()
    ws.recv = AsyncMock()
    return ws


def test_capital_exclusive_engine_configuration():
    """Verify that by default the StreamingEngine targets Capital.com exclusively and does not enable Binance."""
    engine = StreamingEngine()
    assert engine.enable_binance is False
    assert engine.binance_streamer is None


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


def test_streaming_engine_reload_symbols():
    """Verify that engine.reload_symbols queries DB and delegates to capital_streamer."""
    async def _test():
        engine = StreamingEngine()
        mock_streamer = AsyncMock()
        mock_streamer.update_subscriptions = AsyncMock(return_value=True)
        engine.capital_streamer = mock_streamer

        mock_db_map = {
            "SPY": {"capital_ticker": "SPY"},
            "NVDA": {"capital_ticker": "NVDA"},
            "BTCUSDT": {"binance_ticker": "btcusdt"}  # Crypto without capital_ticker
        }

        with patch("src.stream.runner.get_streaming_symbol_map_from_db", return_value=mock_db_map):
            success = await engine.reload_symbols()
            assert success is True
            mock_streamer.update_subscriptions.assert_called_once()
            called_symbols = mock_streamer.update_subscriptions.call_args[0][0]
            assert set(called_symbols) == {"SPY", "NVDA"}

    asyncio.run(_test())


def test_streaming_engine_trigger_reload_signal():
    """Verify trigger_reload sets the reload event."""
    engine = StreamingEngine()
    assert not engine.reload_event.is_set()
    engine.trigger_reload()
    assert engine.reload_event.is_set()


def test_streaming_engine_writer_batches_ticks_into_streaming_duckdb():
    """Verify that ticks enqueued into StreamingEngine are written directly to streaming.duckdb."""
    async def _test():
        with tempfile.TemporaryDirectory() as tmp_dir:
            db_path = os.path.join(tmp_dir, "test_streaming.duckdb")
            client = DuckDBClient(db_path)
            init_streaming_db(client)

            engine = StreamingEngine(db_path=db_path, flush_interval=0.05)
            engine.db_conn = client
            engine.running = True

            # Enqueue sample Capital.com ticks
            now = datetime(2026, 9, 25, 12, 0, 0, tzinfo=timezone.utc)
            ticks = [
                {"epic": "SPY", "price": 575.25, "bid": 575.20, "ask": 575.30, "timestamp": now},
                {"epic": "QQQ", "price": 490.50, "bid": 490.45, "ask": 490.55, "timestamp": now},
            ]
            for t in ticks:
                await engine._handle_capital_tick(t)

            # Run writer worker briefly to flush queue
            worker_task = asyncio.create_task(engine._duckdb_writer_worker())
            await asyncio.sleep(0.15)
            engine.running = False
            await asyncio.sleep(0.05)
            worker_task.cancel()
            try:
                await worker_task
            except asyncio.CancelledError:
                pass

            # Verify ticks in database
            spy_df = query_ticks("SPY", client=client)
            assert len(spy_df) == 1
            assert spy_df.iloc[0]["price"] == pytest.approx(575.25)
            assert spy_df.iloc[0]["source"] == "CAPITAL"

            qqq_df = query_ticks("QQQ", client=client)
            assert len(qqq_df) == 1
            assert qqq_df.iloc[0]["price"] == pytest.approx(490.50)

            client.close()

    asyncio.run(_test())
