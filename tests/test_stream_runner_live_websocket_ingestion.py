"""
Tests for 24/7 Live WebSocket Streaming Engine and Multistream Ingestion.
Verifies streamer symbol discovery, async multi-broker buffering, writer batching,
and live public Binance WebSocket connectivity.
"""
import asyncio
import json
import pytest
import websockets
from datetime import datetime, timezone
from src.database.connection import get_duckdb_connection
from src.database.schema import init_db
from src.database.operations import query_candlesticks, get_symbol_map_from_db
from src.stream.runner import StreamingEngine


class TestLiveWebSocketIngestion:
    """Verifies streaming engine lifecycle, symbol mapping, and live ingestion."""

    def test_engine_symbol_discovery_from_db(self, tmp_path):
        """StreamingEngine must discover both Binance and Capital symbols from seeded DB."""
        db_file = str(tmp_path / "test_engine_discovery.duckdb")
        client = get_duckdb_connection(db_file)
        init_db(client)

        symbol_map = get_symbol_map_from_db(client)
        binance_symbols = []
        capital_symbols = []

        for display_name, tickers in symbol_map.items():
            b_ticker = tickers.get("binance_ticker")
            c_ticker = tickers.get("capital_ticker")
            if b_ticker:
                binance_symbols.append(b_ticker)
            if c_ticker:
                capital_symbols.append(c_ticker)

        client.close()

        # Assert symbol discovery found expected symbols
        assert len(binance_symbols) >= 3
        binance_symbols_upper = [s.upper() for s in binance_symbols]
        assert "BTCUSDT" in binance_symbols_upper
        assert "ETHUSDT" in binance_symbols_upper

        assert len(capital_symbols) >= 8
        assert "AAPL" in capital_symbols
        assert "TSLA" in capital_symbols

    def test_end_to_end_multistream_ingestion_and_resampling(self, tmp_path):
        """
        Verify complete flow:
        Simulated Capital tick + Binance bar -> StreamingEngine write_queue -> DuckDB writer -> query_candlesticks.
        """
        db_file = str(tmp_path / "test_stream_ingest.duckdb")
        client = get_duckdb_connection(db_file)
        init_db(client)

        async def _lifecycle_run():
            engine = StreamingEngine(db_path=db_file, flush_interval=0.05)
            engine.db_conn = client
            engine.running = True

            # 1. Feed Capital ticks (spanning 2 minutes)
            t1 = datetime(2026, 1, 1, 10, 0, 15, tzinfo=timezone.utc)
            await engine._handle_capital_tick({"epic": "NVDA", "price": 400.0, "timestamp": t1})

            t2 = datetime(2026, 1, 1, 10, 0, 45, tzinfo=timezone.utc)
            await engine._handle_capital_tick({"epic": "NVDA", "price": 405.0, "timestamp": t2})

            t3 = datetime(2026, 1, 1, 10, 1, 10, tzinfo=timezone.utc)
            await engine._handle_capital_tick({"epic": "NVDA", "price": 402.0, "timestamp": t3})

            # 2. Feed closed Binance kline bar
            binance_bar = (
                "2026-01-01 10:00:00",
                "BTCUSDT",
                92000.0,
                92500.0,
                91900.0,
                92300.0,
                15.5,
                "REG",
                "BINANCE"
            )
            await engine._handle_binance_bar(binance_bar, is_closed=True)

            # 3. Start writer worker and wait for queue drain
            worker_task = asyncio.create_task(engine._duckdb_writer_worker())
            await asyncio.sleep(0.2)

            # 4. Stop engine and cleanly cancel writer
            engine.stop()
            await asyncio.sleep(0.1)
            worker_task.cancel()
            try:
                await worker_task
            except asyncio.CancelledError:
                pass

        asyncio.run(_lifecycle_run())

        # 5. Query persisted data via query_candlesticks
        df_nvda = query_candlesticks("NVDA", client=client)
        assert len(df_nvda) >= 1
        assert df_nvda.iloc[0]["open"] == 400.0
        assert df_nvda.iloc[0]["high"] == 405.0
        assert df_nvda.iloc[0]["symbol"] == "NVDA"

        df_btc = query_candlesticks("BTCUSDT", client=client)
        assert len(df_btc) == 1
        assert df_btc.iloc[0]["high"] == 92500.0
        assert df_btc.iloc[0]["close"] == 92300.0
        assert df_btc.iloc[0]["symbol"] == "BTCUSDT"

        client.close()

    def test_live_binance_public_websocket_connectivity(self):
        """
        Verify live connection to Binance 24/7 public WebSocket endpoint.
        Gracefully skips if network or DNS is unreachable.
        """
        url = "wss://stream.binance.com:9443/stream?streams=btcusdt@kline_1m"

        async def _connect():
            async with websockets.connect(url) as ws:
                msg = await asyncio.wait_for(ws.recv(), timeout=5.0)
                data = json.loads(msg)
                return data

        try:
            data = asyncio.run(_connect())
            assert "stream" in data
            assert data["stream"] == "btcusdt@kline_1m"
            assert "data" in data
            kline = data["data"]["k"]
            assert kline["s"] == "BTCUSDT"
            assert "c" in kline
        except (OSError, asyncio.TimeoutError) as e:
            pytest.skip(f"Live network test skipped due to network/timeout: {e}")
