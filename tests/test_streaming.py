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


class TestStreamers:
    """Tests streamer configuration and URL formatting."""

    def test_binance_stream_url(self):
        streamer = BinanceStreamer(symbols=["BTCUSDT", "ETHUSDT"])
        url = streamer._build_url()
        assert "wss://stream.binance.com:9443/stream?streams=" in url
        assert "btcusdt@kline_1m" in url
        assert "ethusdt@kline_1m" in url

    def test_capital_stream_initialization(self):
        streamer = CapitalStreamer(epics=["AAPL", "TSLA", "NVDA"])
        assert len(streamer.epics) == 3
        assert "AAPL" in streamer.epics
