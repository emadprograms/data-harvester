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
from src.database.schema import init_db, init_streaming_db
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

    @pytest.mark.live
    def test_live_binance_public_websocket_connectivity(self):
        """
        Verify live connection to Binance 24/7 public trade WebSocket endpoint.
        Gracefully skips if network or DNS is unreachable.
        """
        url = "wss://stream.binance.com:9443/stream?streams=btcusdt@trade"

        async def _connect():
            async with websockets.connect(url) as ws:
                msg = await asyncio.wait_for(ws.recv(), timeout=5.0)
                data = json.loads(msg)
                return data

        try:
            data = asyncio.run(_connect())
            assert "stream" in data
            assert data["stream"] == "btcusdt@trade"
            assert "data" in data
            trade = data["data"]
            assert trade["s"] == "BTCUSDT"
            assert "p" in trade
            assert "q" in trade
        except (OSError, asyncio.TimeoutError) as e:
            pytest.skip(f"Live network test skipped due to network/timeout: {e}")


# Phase 46 (CO-01) disposition: `test_end_to_end_multistream_ingestion_and_resampling`
# was deleted with the disk-DuckDB writer it exercised; it also asserted Binance crypto
# symbols (BTCUSDT/ETHUSDT), which are out of scope for v5.0. Coverage replaced by
# tests/stream/test_lake_runner_integration.py and tests/stream/test_dynamic_reload.py.
