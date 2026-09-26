"""
Unit and integration tests for Separation of Historical and Streaming Symbol Maps.
Verifies:
1. historical_symbol_map exists in historical.duckdb with backward-compatible symbol_map view.
2. streaming_symbol_map exists in streaming.duckdb with 19 single-stock assets.
3. Operations API (get, add, remove) behaves independently on both databases.
4. Stream runner filters out ticks for any symbol not present in streaming_symbol_map.
5. Databento backfiller targets symbols from streaming_symbol_map.
"""
import pytest
import asyncio
from datetime import datetime, timezone

from src.database.connection import (
    DuckDBClient,
    get_historical_db_connection,
    get_streaming_db_connection,
)
from src.database.schema import init_historical_db, init_streaming_db
from src.database.operations import (
    get_historical_symbol_map_from_db,
    get_symbol_map_from_db,
    get_symbol_inventory_list,
    add_symbol_to_db,
    remove_symbol_from_db,
    get_streaming_symbol_map_from_db,
    get_streaming_symbol_inventory_list,
    add_streaming_symbol_to_db,
    remove_streaming_symbol_from_db,
)
from src.stream.runner import StreamingEngine
from src.data.databento_backfill import get_target_stock_symbols


def test_historical_symbol_map_schema_and_view():
    """Verify historical.duckdb contains historical_symbol_map table and symbol_map view."""
    client = get_historical_db_connection(read_only=True)
    assert client is not None
    try:
        tables = [t[0] for t in client.execute("SHOW TABLES").fetchall()]
        assert "historical_symbol_map" in tables
        assert "symbol_map" in tables

        # Table and view return same row count
        t_count = client.execute("SELECT COUNT(*) FROM historical_symbol_map").fetchone()[0]
        v_count = client.execute("SELECT COUNT(*) FROM symbol_map").fetchone()[0]
        assert t_count == v_count
        assert t_count >= 40
    finally:
        client.close()


def test_streaming_symbol_map_schema_and_defaults():
    """Verify streaming.duckdb contains streaming_symbol_map with default single stocks."""
    client = get_streaming_db_connection(read_only=True)
    assert client is not None
    try:
        tables = [t[0] for t in client.execute("SHOW TABLES").fetchall()]
        assert "streaming_symbol_map" in tables

        rows = client.execute("SELECT display_name, is_active FROM streaming_symbol_map ORDER BY display_name").fetchall()
        symbols = [r[0] for r in rows]
        assert len(symbols) == 19

        # Ensure pure stocks are present
        for expected in ["AAPL", "NVDA", "MSFT", "AMZN", "GOOGL", "TSLA", "AMD"]:
            assert expected in symbols

        # Ensure ETFs and Crypto are strictly absent from streaming_symbol_map
        for excluded in ["SPY", "QQQ", "IWM", "DIA", "BTCUSDT", "ETHUSDT", "CL=F", "VIX"]:
            assert excluded not in symbols
    finally:
        client.close()


def test_independent_crud_operations():
    """Verify that adding/removing from streaming_symbol_map does not affect historical_symbol_map and vice versa."""
    # In-memory test clients
    hist_client = DuckDBClient(":memory:", read_only=False)
    stream_client = DuckDBClient(":memory:", read_only=False)

    init_historical_db(hist_client)
    init_streaming_db(stream_client)

    # 1. Add streaming-only symbol
    add_streaming_symbol_to_db("STREAM_ONLY", capital_ticker="STRM", client=stream_client)
    s_map = get_streaming_symbol_map_from_db(client=stream_client)
    assert "STREAM_ONLY" in s_map
    assert s_map["STREAM_ONLY"]["capital_ticker"] == "STRM"

    # Historical symbol map must NOT have STREAM_ONLY
    h_map = get_historical_symbol_map_from_db(client=hist_client)
    assert "STREAM_ONLY" not in h_map

    # 2. Add historical-only symbol
    add_symbol_to_db("HIST_ONLY", massive_ticker="HIST", client=hist_client)
    h_map2 = get_historical_symbol_map_from_db(client=hist_client)
    assert "HIST_ONLY" in h_map2

    # Streaming symbol map must NOT have HIST_ONLY
    s_map2 = get_streaming_symbol_map_from_db(client=stream_client)
    assert "HIST_ONLY" not in s_map2

    # 3. Clean up
    remove_streaming_symbol_from_db("STREAM_ONLY", client=stream_client)
    assert "STREAM_ONLY" not in get_streaming_symbol_map_from_db(client=stream_client)

    remove_symbol_from_db("HIST_ONLY", client=hist_client)
    assert "HIST_ONLY" not in get_historical_symbol_map_from_db(client=hist_client)

    hist_client.close()
    stream_client.close()


def test_stream_runner_drops_excluded_assets():
    """Verify that StreamingEngine strictly drops ticks for assets not in streaming_symbol_map."""
    async def _test():
        engine = StreamingEngine()
        # Configure allowed symbols to only AAPL and NVDA
        engine.active_streaming_symbols = {"AAPL", "NVDA"}
        engine.epic_to_display = {"AAPL": "AAPL", "NVDA": "NVDA"}

        # Simulate ticks from Capital.com
        now = datetime.now(timezone.utc)

        # 1. Allowed tick: NVDA
        await engine._handle_capital_tick({"epic": "NVDA", "price": 225.5, "timestamp": now, "bid": 225.4, "ask": 225.6})

        # 2. Excluded tick: SPY (ETF)
        await engine._handle_capital_tick({"epic": "SPY", "price": 550.0, "timestamp": now, "bid": 549.9, "ask": 550.1})

        # 3. Excluded tick: US30 (Dow index)
        await engine._handle_capital_tick({"epic": "US30", "price": 42000.0, "timestamp": now, "bid": 41999.0, "ask": 42001.0})

        # 4. Allowed tick: AAPL
        await engine._handle_capital_tick({"epic": "AAPL", "price": 230.0, "timestamp": now, "bid": 229.9, "ask": 230.1})

        # Queue should only contain 2 ticks (NVDA, AAPL); SPY and US30 dropped!
        assert engine.write_queue.qsize() == 2

        tick1 = engine.write_queue.get_nowait()
        assert tick1[1] == "NVDA"

        tick2 = engine.write_queue.get_nowait()
        assert tick2[1] == "AAPL"

    asyncio.run(_test())


def test_databento_backfill_targets_streaming_symbol_map():
    """Verify get_target_stock_symbols reads symbols directly from streaming_symbol_map."""
    symbols = get_target_stock_symbols()
    assert len(symbols) == 19
    assert "NVDA" in symbols
    assert "AAPL" in symbols
    assert "SPY" not in symbols
    assert "BTCUSDT" not in symbols
