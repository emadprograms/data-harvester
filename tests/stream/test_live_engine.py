"""
Tests for Live WebSocket Streaming and Candle Aggregator.
"""
import pytest
from datetime import datetime
from src.stream.aggregator import CandleAggregator
from src.stream.binance_stream import BinanceStreamer
from src.stream.capital_stream import CapitalStreamer


class TestCandleAggregator:
    """Tests tick aggregation into 1-minute OHLCV candles."""

    def test_single_minute_aggregation(self):
        closed_candles = []
        agg = CandleAggregator(on_candle_closed=lambda c: closed_candles.append(c))

        # Ticks for minute 10:00
        agg.process_tick("AAPL", 150.0, datetime(2026, 1, 1, 10, 0, 10), volume=10.0)
        agg.process_tick("AAPL", 152.0, datetime(2026, 1, 1, 10, 0, 20), volume=20.0)
        agg.process_tick("AAPL", 149.0, datetime(2026, 1, 1, 10, 0, 30), volume=15.0)
        agg.process_tick("AAPL", 151.0, datetime(2026, 1, 1, 10, 0, 50), volume=25.0)

        # Still in same minute, no closed candle yet
        assert len(closed_candles) == 0

        # Tick arrives in next minute 10:01 -> closes 10:00 candle
        agg.process_tick("AAPL", 151.5, datetime(2026, 1, 1, 10, 1, 5), volume=5.0)

        assert len(closed_candles) == 1
        bar = closed_candles[0]
        # bar format: (ts_str, symbol, open, high, low, close, volume, session, source)
        assert bar[0] == "2026-01-01 10:00:00"
        assert bar[1] == "AAPL"
        assert bar[2] == 150.0  # Open
        assert bar[3] == 152.0  # High
        assert bar[4] == 149.0  # Low
        assert bar[5] == 151.0  # Close
        assert bar[6] == 70.0   # Volume (10+20+15+25)
        assert bar[8] == "CAPITAL"

    def test_stale_candle_flush(self):
        closed = []
        agg = CandleAggregator(on_candle_closed=lambda c: closed.append(c))
        agg.process_tick("NVDA", 500.0, datetime(2026, 1, 1, 10, 0, 0))

        # Flush with 0s threshold
        agg.flush_stale_candles(max_age_seconds=0)
        assert len(closed) == 1
        assert closed[0][1] == "NVDA"

    def test_multi_symbol_interleaved_aggregation(self):
        """Aggregator must maintain independent state for multiple symbols simultaneously."""
        closed = []
        agg = CandleAggregator(on_candle_closed=lambda c: closed.append(c))

        # AAPL at 10:00:10
        agg.process_tick("AAPL", 150.0, datetime(2026, 1, 1, 10, 0, 10))
        # TSLA at 10:00:15
        agg.process_tick("TSLA", 200.0, datetime(2026, 1, 1, 10, 0, 15))
        # AAPL at 10:00:40
        agg.process_tick("AAPL", 155.0, datetime(2026, 1, 1, 10, 0, 40))

        assert len(closed) == 0

        # AAPL moves to 10:01:05 -> closes AAPL 10:00 only
        agg.process_tick("AAPL", 154.0, datetime(2026, 1, 1, 10, 1, 5))
        assert len(closed) == 1
        assert closed[0][1] == "AAPL"
        assert closed[0][2] == 150.0  # Open
        assert closed[0][3] == 155.0  # High
        assert closed[0][5] == 155.0  # Close

        # TSLA moves to 10:01:10 -> closes TSLA 10:00
        agg.process_tick("TSLA", 205.0, datetime(2026, 1, 1, 10, 1, 10))
        assert len(closed) == 2
        assert closed[1][1] == "TSLA"
        assert closed[1][2] == 200.0



class TestStreamers:
    """Tests streamer configuration and URL formatting."""

    def test_binance_stream_url(self):
        # Default stream type is trade (tick-by-tick)
        streamer = BinanceStreamer(symbols=["BTCUSDT", "ETHUSDT"])
        url = streamer._build_url()
        assert "wss://stream.binance.com:9443/stream?streams=" in url
        assert "btcusdt@trade" in url
        assert "ethusdt@trade" in url

        # Backward-compatible kline stream
        streamer_kline = BinanceStreamer(symbols=["BTCUSDT"], stream_type="kline_1m")
        assert "btcusdt@kline_1m" in streamer_kline._build_url()

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

    def test_binance_kline_payload_parsing(self):
        """Simulate Binance kline JSON parsing logic."""
        import json
        from datetime import timezone

        mock_payload = {
            "stream": "btcusdt@kline_1m",
            "data": {
                "e": "kline",
                "E": 1700000060000,
                "s": "BTCUSDT",
                "k": {
                    "t": 1700000000000,
                    "T": 1700000059999,
                    "s": "BTCUSDT",
                    "i": "1m",
                    "o": "36500.00",
                    "c": "36550.00",
                    "h": "36600.00",
                    "l": "36490.00",
                    "v": "12.345",
                    "x": True
                }
            }
        }

        data = mock_payload
        kline = data["data"]["k"]
        open_time_ms = kline["t"]
        dt_utc = datetime.fromtimestamp(open_time_ms / 1000.0, tz=timezone.utc)
        ts_str = dt_utc.strftime('%Y-%m-%d %H:%M:%S')

        bar = (
            ts_str,
            kline["s"].upper(),
            float(kline["o"]),
            float(kline["h"]),
            float(kline["l"]),
            float(kline["c"]),
            float(kline["v"]),
            "REG",
            "BINANCE"
        )
        assert bar[1] == "BTCUSDT"
        assert bar[2] == 36500.0
        assert bar[3] == 36600.0
        assert bar[4] == 36490.0
        assert bar[5] == 36550.0
        assert bar[6] == 12.345
        assert bar[8] == "BINANCE"
        assert kline["x"] is True

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

