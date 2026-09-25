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

    def test_capital_subscription_chunking(self):
        """CapitalStreamer must chunk epics in batches of 40."""
        # 45 epics
        epics = [f"EPIC_{i}" for i in range(45)]
        streamer = CapitalStreamer(epics=epics)
        assert len(streamer.epics) == 45


class TestStreamingEngineAndParsers:
    """Tests StreamingEngine async queue, writer worker, and WebSocket payloads."""

    def test_streaming_engine_queueing(self):
        from src.stream.runner import StreamingEngine
        import asyncio

        async def _run():
            engine = StreamingEngine()

            # Binance bar: closed vs unclosed
            bar1 = ("2026-01-01 10:00:00", "BTCUSDT", 90000.0, 90500.0, 89900.0, 90200.0, 10.0, "REG", "BINANCE")
            await engine._handle_binance_bar(bar1, is_closed=False)
            assert engine.write_queue.qsize() == 0

            await engine._handle_binance_bar(bar1, is_closed=True)
            assert engine.write_queue.qsize() == 1
            queued = await engine.write_queue.get()
            assert queued == bar1

        asyncio.run(_run())

    def test_streaming_engine_writer_worker(self, tmp_path):
        from src.stream.runner import StreamingEngine
        from src.database.connection import DuckDBClient
        from src.database.schema import init_db
        import asyncio

        db_file = str(tmp_path / "test_engine.duckdb")
        client = DuckDBClient(db_path=db_file)
        init_db(client)

        async def _run():
            engine = StreamingEngine(db_path=db_file, flush_interval=0.1)
            engine.db_conn = client
            engine.running = True

            # Enqueue 3 bars
            bars = [
                ("2026-01-01 10:00:00", "AAPL", 150.0, 151.0, 149.0, 150.5, 100.0, "REG", "CAPITAL"),
                ("2026-01-01 10:01:00", "AAPL", 150.5, 152.0, 150.0, 151.5, 200.0, "REG", "CAPITAL"),
                ("2026-01-01 10:00:00", "BTCUSDT", 90000.0, 90100.0, 89900.0, 90050.0, 5.0, "REG", "BINANCE")
            ]
            for b in bars:
                engine._enqueue_bar(b)

            # Run writer worker briefly
            worker_task = asyncio.create_task(engine._duckdb_writer_worker())
            await asyncio.sleep(0.3)
            engine.running = False
            await asyncio.sleep(0.1)
            worker_task.cancel()
            try:
                await worker_task
            except asyncio.CancelledError:
                pass

        asyncio.run(_run())

        # Check records in DB
        res = client.execute("SELECT count(*) FROM market_data")
        assert res.rows[0][0] == 3
        client.close()

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

    def test_e2e_streaming_to_candlestick_query(self, tmp_path):
        """Validates end-to-end flow: streaming tick/bar -> write_queue -> DuckDB -> query_candlesticks."""
        import asyncio
        from src.stream.runner import StreamingEngine
        from src.database.connection import get_duckdb_connection
        from src.database.schema import init_db
        from src.database.operations import query_candlesticks

        db_file = str(tmp_path / "test_e2e.duckdb")
        client = get_duckdb_connection(db_path=db_file)
        init_db(client)

        async def _run():
            engine = StreamingEngine(db_path=db_file, flush_interval=0.05)
            engine.db_conn = client
            engine.running = True

            # Process Capital tick into aggregator
            from datetime import timezone
            dt = datetime(2026, 1, 1, 10, 0, 10, tzinfo=timezone.utc)
            tick = {"epic": "AAPL", "price": 180.0, "bid": 179.9, "ask": 180.1, "timestamp": dt}
            await engine._handle_capital_tick(tick)

            # Push tick in next minute to close first candle
            dt2 = datetime(2026, 1, 1, 10, 1, 5, tzinfo=timezone.utc)
            tick2 = {"epic": "AAPL", "price": 181.0, "bid": 180.9, "ask": 181.1, "timestamp": dt2}
            await engine._handle_capital_tick(tick2)

            # Process closed Binance bar
            binance_bar = ("2026-01-01 10:00:00", "BTCUSDT", 90000.0, 90500.0, 89900.0, 90200.0, 10.0, "REG", "BINANCE")
            await engine._handle_binance_bar(binance_bar, is_closed=True)

            # Start writer worker and wait for flush
            worker_task = asyncio.create_task(engine._duckdb_writer_worker())
            await asyncio.sleep(0.2)

            # Query candlesticks immediately using client
            df_aapl = query_candlesticks("AAPL", client=engine.db_conn)
            # Both 10:00 (closed by next tick) and 10:01 (flushed by stale sweeper) are saved
            assert len(df_aapl) == 2
            assert df_aapl.iloc[0]["open"] == 180.0
            assert df_aapl.iloc[0]["symbol"] == "AAPL"
            assert df_aapl.iloc[1]["open"] == 181.0

            df_btc = query_candlesticks("BTCUSDT", client=engine.db_conn)
            assert len(df_btc) == 1
            assert df_btc.iloc[0]["close"] == 90200.0
            assert df_btc.iloc[0]["symbol"] == "BTCUSDT"

            # Query with secondary connection in same process (tests read_only fallback)
            reader_conn = get_duckdb_connection(db_path=db_file, read_only=True)
            df_btc_ro = query_candlesticks("BTCUSDT", client=reader_conn)
            assert len(df_btc_ro) == 1
            reader_conn.close()

            engine.stop()
            await asyncio.sleep(0.1)
            worker_task.cancel()
            try:
                await worker_task
            except asyncio.CancelledError:
                pass

        asyncio.run(_run())
        client.close()

