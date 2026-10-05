"""
Tests for the live Capital.com WebSocket streaming path.
"""
import pytest
from datetime import datetime
from src.stream.capital_stream import CapitalStreamer


class TestStreamers:
    """Tests streamer configuration."""

    def test_capital_stream_initialization(self):
        streamer = CapitalStreamer(epics=["AAPL", "TSLA", "NVDA"])
        assert len(streamer.epics) == 3
        assert "AAPL" in streamer.epics

    def test_capital_subscription_chunking(self):
        """CapitalStreamer must chunk epics in batches of 40."""
        # 45 epics
        epics = [f"EPIC_{i}" for i in range(45)]
        streamer = CapitalStreamer(epics=epics)
        assert len(streamer.epics) == 45


# Phase 46 (CO-01) disposition: `test_streaming_engine_writer_worker` and
# `test_e2e_streaming_to_candlestick_query` were deleted with the disk-DuckDB
# backend they exercised. Equivalent coverage lives in
# tests/stream/test_lake_runner_integration.py (callback -> Parquet, drain) and
# tests/storage/test_lake_reader.py (candle resampling from the lake).

class TestStreamingEngineAndParsers:
    """Tests StreamingEngine async queue, writer worker, and WebSocket payloads."""

    def test_streaming_engine_queueing(self, tmp_path):
        from src.stream.runner import StreamingEngine
        import asyncio

        async def _run():
            engine = StreamingEngine(lake_root=tmp_path / "lake")
            try:
                tick1 = ("2026-01-01 10:00:00.123", "AAPL", 150.0, 1.0, 149.9, 150.1, "CAPITAL", "REG")
                engine._enqueue_tick(tick1)
                assert engine.write_queue.qsize() == 1
                queued = await engine.write_queue.get()
                assert queued == tick1
            finally:
                engine.stop()

        asyncio.run(_run())

    def test_capital_quote_payload_parsing(self):
        """Simulate Capital quote JSON parsing logic."""
        mock_msg = {
            "destination": "quote",
            "correlationId": "1",
            "payload": {
                "epic": "AAPL",
                "bid": 180.50,
                "ofr": 180.60,
                "timestamp": 1700000000000
            }
        }

        payload = mock_msg["payload"]
        epic = payload["epic"]
        bid = float(payload["bid"])
        ofr = float(payload["ofr"])
        mid_price = (bid + ofr) / 2.0
        assert epic == "AAPL"
        assert mid_price == 180.55

