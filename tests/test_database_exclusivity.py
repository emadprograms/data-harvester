"""
Automated Integration and Unit Tests for Strict Database Exclusivity.
Enforces strict segregation between data/historical.duckdb and data/streaming.duckdb:

Architectural Invariants & Exclusivity Rules:
1. data/historical.duckdb:
   - Dedicated ONLY to canonical 1-minute OHLCV candles (market_data) and historical symbols (historical_database_symbols).
   - Must NOT contain or query a ticks table.
2. data/streaming.duckdb:
   - Dedicated ONLY to live tick quotes (ticks) and streaming symbols (streaming_database_symbols).
   - Must NOT contain or query a market_data table.
3. Operations & Connection Layer Exclusivity:
   - get_historical_db_connection() points to historical.duckdb; never opens streaming.duckdb.
   - get_streaming_db_connection() points to streaming.duckdb; never opens historical.duckdb.
   - Historical operations (get_historical_candles, get_symbol_inventory_list, query_candlesticks, etc.)
     never access streaming.duckdb.
   - Streaming operations (get_streaming_candles, get_streaming_symbol_inventory_list, get_ticks, query_ticks, etc.)
     never access historical.duckdb.
   - Streaming engine (src.stream.runner.StreamingEngine) must NOT fall back to historical symbols
     if streaming symbols are missing.
4. Schema Purity:
   - init_historical_db only manages historical tables and does not create or require ticks table.
   - init_streaming_db only manages streaming tables (ticks, streaming_database_symbols).
5. Dashboard REST API Strict Exclusivity & No Silent Defaults:
   - GET /api/historical/candles strictly queries historical.duckdb.
   - GET /api/streaming/candles strictly queries streaming.duckdb.
   - GET /api/candles MUST require an explicit source query param ('historical' or 'streaming').
     Returns HTTP 400 Bad Request if missing or invalid.
   - GET /api/historical/symbols strictly queries historical.duckdb.
   - GET /api/streaming/symbols strictly queries streaming.duckdb.
   - GET /api/symbols MUST require an explicit source query param ('historical' or 'streaming').
     Returns HTTP 400 Bad Request if missing or invalid.
   - POST /api/historical/symbols adds to historical database only.
   - POST /api/streaming/symbols adds to streaming database only.
   - POST /api/symbols MUST require an explicit source param ('historical' or 'streaming')
     or return HTTP 400 Bad Request if missing or invalid.
"""
import asyncio
import os
import socket
import threading
import time
from unittest.mock import patch, MagicMock
import pytest
import requests
import pandas as pd

from src.database.connection import (
    DuckDBClient,
    get_historical_db_connection,
    get_streaming_db_connection,
    DEFAULT_HISTORICAL_DB_PATH,
    DEFAULT_STREAMING_DB_PATH,
)
from src.database.schema import (
    init_historical_db,
    init_streaming_db,
)
from src.database.operations import (
    get_symbol_inventory_list,
    get_historical_database_symbols_from_db,
    add_symbol_to_db,
    remove_symbol_from_db,
    query_candlesticks,
    get_streaming_symbol_inventory_list,
    get_streaming_database_symbols_from_db,
    add_streaming_symbol_to_db,
    remove_streaming_symbol_from_db,
    query_ticks,
    query_candlesticks_from_ticks,
    save_ticks_to_storage,
    save_data_to_storage,
)
from src.dashboard.analytics import (
    get_historical_candles,
    get_streaming_candles,
    get_candles,
    get_ticks,
    get_stream_tape,
    get_symbols_coverage,
    get_historical_overview,
)
from src.dashboard.server import create_dashboard_server
from src.stream.runner import StreamingEngine


# ============================================================================
# Fixtures
# ============================================================================

@pytest.fixture(scope="module")
def api_test_server():
    """Starts an ephemeral ThreadedHTTPServer on an open port for testing REST endpoints."""
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(("", 0))
    port = s.getsockname()[1]
    s.close()

    server = create_dashboard_server(host="127.0.0.1", port=port)
    server_thread = threading.Thread(target=server.serve_forever, daemon=True)
    server_thread.start()
    time.sleep(0.15)

    base_url = f"http://127.0.0.1:{port}"
    yield base_url

    server.shutdown()
    server.server_close()


# ============================================================================
# 1. Schema Purity & Table Isolation
# ============================================================================

def test_init_historical_db_schema_purity():
    """Verify init_historical_db creates only historical tables and does NOT create ticks or streaming symbols."""
    client = DuckDBClient(":memory:", read_only=False)
    try:
        init_historical_db(client)
        tables = [t[0] for t in client.execute("SHOW TABLES").fetchall()]

        # Must contain historical tables/views
        assert "historical_database_symbols" in tables
        assert "market_data" in tables
        assert "historical_symbol_map" in tables
        assert "symbol_map" in tables

        # Must strictly NOT contain streaming tables/views
        assert "ticks" not in tables, "historical.duckdb must NOT contain 'ticks' table"
        assert "streaming_ticks" not in tables, "historical.duckdb must NOT contain 'streaming_ticks' view"
        assert "streaming_database_symbols" not in tables, "historical.duckdb must NOT contain 'streaming_database_symbols'"
        assert "streaming_symbol_map" not in tables, "historical.duckdb must NOT contain 'streaming_symbol_map'"

        # Verify historical queries run cleanly without requiring ticks table
        count = client.execute("SELECT COUNT(*) FROM market_data").fetchone()[0]
        assert count >= 0
    finally:
        client.close()


def test_init_streaming_db_schema_purity():
    """Verify init_streaming_db creates only streaming tables and does NOT create market_data or historical symbols."""
    client = DuckDBClient(":memory:", read_only=False)
    try:
        init_streaming_db(client)
        tables = [t[0] for t in client.execute("SHOW TABLES").fetchall()]

        # Must contain streaming tables/views
        assert "streaming_database_symbols" in tables
        assert "ticks" in tables
        assert "streaming_symbol_map" in tables
        assert "streaming_ticks" in tables

        # Must strictly NOT contain historical tables/views
        assert "market_data" not in tables, "streaming.duckdb must NOT contain 'market_data' table"
        assert "historical_database_symbols" not in tables, "streaming.duckdb must NOT contain 'historical_database_symbols'"
        assert "historical_symbol_map" not in tables, "streaming.duckdb must NOT contain 'historical_symbol_map'"

        # Verify streaming queries run cleanly without requiring market_data table
        count = client.execute("SELECT COUNT(*) FROM ticks").fetchone()[0]
        assert count >= 0
    finally:
        client.close()


def test_disk_databases_schema_exclusivity():
    """Verify existing disk databases (data/historical.duckdb and data/streaming.duckdb) enforce exclusivity."""
    if os.path.exists(DEFAULT_HISTORICAL_DB_PATH):
        h_client = get_historical_db_connection(read_only=True)
        try:
            h_tables = [t[0] for t in h_client.execute("SHOW TABLES").fetchall()]
            assert "market_data" in h_tables
            assert "ticks" not in h_tables, "data/historical.duckdb must not have a ticks table"
            assert "streaming_database_symbols" not in h_tables
        finally:
            h_client.close()

    if os.path.exists(DEFAULT_STREAMING_DB_PATH):
        s_client = get_streaming_db_connection(read_only=True)
        try:
            s_tables = [t[0] for t in s_client.execute("SHOW TABLES").fetchall()]
            assert "ticks" in s_tables
            assert "market_data" not in s_tables, "data/streaming.duckdb must not have a market_data table"
            assert "historical_database_symbols" not in s_tables
        finally:
            s_client.close()


# ============================================================================
# 2. Connection Layer Exclusivity
# ============================================================================

def test_connection_paths_exclusivity():
    """Verify get_historical_db_connection and get_streaming_db_connection target distinct database files."""
    h_client = get_historical_db_connection(read_only=True)
    s_client = get_streaming_db_connection(read_only=True)
    assert h_client is not None
    assert s_client is not None
    try:
        assert h_client.db_path != s_client.db_path, "Historical and Streaming connections must target distinct files"
        assert "historical.duckdb" in h_client.db_path or "market_data.duckdb" in h_client.db_path
        assert "streaming.duckdb" in s_client.db_path
    finally:
        h_client.close()
        s_client.close()


# ============================================================================
# 3. Operations Layer Exclusivity
# ============================================================================

def test_historical_operations_never_touch_streaming_db():
    """Verify historical queries and operations never attempt to open or query streaming.duckdb."""
    def forbidden_streaming_call(*args, **kwargs):
        raise AssertionError("VIOLATION: Historical operation touched streaming.duckdb connection!")

    with patch("src.dashboard.analytics.get_streaming_db_connection", side_effect=forbidden_streaming_call), \
         patch("src.database.operations.get_streaming_db_connection", side_effect=forbidden_streaming_call):

        # Historical candles
        res = get_historical_candles("AAPL", timeframe="1m", limit=5)
        assert res.get("database") == "historical"

        # Historical symbol inventory
        symbols = get_symbol_inventory_list()
        assert isinstance(symbols, list)

        # Historical symbol map dict
        smap = get_historical_database_symbols_from_db()
        assert isinstance(smap, dict)

        # Historical overview
        overview = get_historical_overview()
        assert "database" in overview
        assert "historical" in overview["database"]

        # Historical coverage
        coverage = get_symbols_coverage()
        assert "symbols" in coverage


def test_streaming_operations_never_touch_historical_db():
    """Verify streaming queries and operations never attempt to open or query historical.duckdb."""
    def forbidden_historical_call(*args, **kwargs):
        raise AssertionError("VIOLATION: Streaming operation touched historical.duckdb connection!")

    with patch("src.dashboard.analytics.get_historical_db_connection", side_effect=forbidden_historical_call), \
         patch("src.database.operations.get_historical_db_connection", side_effect=forbidden_historical_call), \
         patch("src.database.operations.get_archive_db_connection", side_effect=forbidden_historical_call):

        # Streaming candles
        res = get_streaming_candles("AAPL", timeframe="1m", limit=5)
        assert res.get("database") == "streaming"

        # Streaming symbol inventory
        symbols = get_streaming_symbol_inventory_list()
        assert isinstance(symbols, list)

        # Streaming symbol map dict
        smap = get_streaming_database_symbols_from_db()
        assert isinstance(smap, dict)

        # Raw ticks query
        ticks_res = get_ticks("AAPL", limit=5)
        assert isinstance(ticks_res.get("ticks"), list)

        # Stream tape query
        tape = get_stream_tape("AAPL", limit=5)
        assert isinstance(tape.get("ticks"), list)

        # Raw ticks dataframe query
        ticks_df = query_ticks("AAPL", limit=5)
        assert hasattr(ticks_df, "columns")

        # Dynamic candlestick resampling from ticks
        candles_df = query_candlesticks_from_ticks("AAPL", timeframe="1m")
        assert hasattr(candles_df, "columns")


def test_storage_operations_never_cross_databases():
    """Verify save_data_to_storage never touches streaming db and save_ticks_to_storage never touches historical db."""
    def forbidden_streaming_call(*args, **kwargs):
        raise AssertionError("VIOLATION: save_data_to_storage touched streaming.duckdb connection!")

    def forbidden_historical_call(*args, **kwargs):
        raise AssertionError("VIOLATION: save_ticks_to_storage touched historical.duckdb connection!")

    # 1. Historical save must not touch streaming connection
    with patch("src.database.operations.get_streaming_db_connection", side_effect=forbidden_streaming_call):
        hist_client = DuckDBClient(":memory:", read_only=False)
        init_historical_db(hist_client)
        df = pd.DataFrame([{
            "timestamp": "2026-09-25 14:00:00",
            "symbol": "AAPL",
            "open": 150.0,
            "high": 151.0,
            "low": 149.0,
            "close": 150.5,
            "volume": 1000.0,
            "session": "REG",
            "source": "MASSIVE"
        }])
        saved = save_data_to_storage(df, archive_client=hist_client)
        assert saved is True
        hist_client.close()

    # 2. Streaming save must not touch historical connection
    with patch("src.database.operations.get_historical_db_connection", side_effect=forbidden_historical_call), \
         patch("src.database.operations.get_archive_db_connection", side_effect=forbidden_historical_call):
        stream_client = DuckDBClient(":memory:", read_only=False)
        init_streaming_db(stream_client)
        ticks = [
            ("2026-09-25 14:00:01", "AAPL", 150.2, 1.0, 150.1, 150.3, "CAPITAL", "REG")
        ]
        saved_ticks = save_ticks_to_storage(stream_client, ticks)
        assert saved_ticks is True
        stream_client.close()


def test_crud_data_segregation_on_independent_databases():
    """Verify adding/removing symbols on one database has zero side effects on the other."""
    hist_client = DuckDBClient(":memory:", read_only=False)
    stream_client = DuckDBClient(":memory:", read_only=False)

    try:
        init_historical_db(hist_client)
        init_streaming_db(stream_client)

        # 1. Add historical-only symbol
        add_symbol_to_db("HIST_ONLY", massive_ticker="HIST_TICKER", client=hist_client)
        assert "HIST_ONLY" in get_historical_database_symbols_from_db(client=hist_client)
        assert "HIST_ONLY" not in get_streaming_database_symbols_from_db(client=stream_client)

        # 2. Add streaming-only symbol
        add_streaming_symbol_to_db("STREAM_ONLY", capital_ticker="STRM_TICKER", client=stream_client)
        assert "STREAM_ONLY" in get_streaming_database_symbols_from_db(client=stream_client)
        assert "STREAM_ONLY" not in get_historical_database_symbols_from_db(client=hist_client)

        # 3. Remove historical symbol
        remove_symbol_from_db("HIST_ONLY", client=hist_client)
        assert "HIST_ONLY" not in get_historical_database_symbols_from_db(client=hist_client)

        # 4. Remove streaming symbol
        remove_streaming_symbol_from_db("STREAM_ONLY", client=stream_client)
        assert "STREAM_ONLY" not in get_streaming_database_symbols_from_db(client=stream_client)
    finally:
        hist_client.close()
        stream_client.close()


# ============================================================================
# 4. Streaming Engine (runner.py) Exclusivity
# ============================================================================

def test_runner_reload_symbols_does_not_fallback_to_historical():
    """Verify that StreamingEngine.reload_symbols() strictly does NOT fall back to historical symbols if streaming symbols are missing."""
    async def _run():
        engine = StreamingEngine()

        # Mock streaming database returning NO symbols
        with patch("src.stream.runner.get_streaming_database_symbols_from_db", return_value={}), \
             patch("src.stream.runner.get_historical_database_symbols_from_db", create=True) as mock_historical:

            mock_historical.return_value = {
                "SPY": {"capital_ticker": "SPY", "is_active": True},
                "QQQ": {"capital_ticker": "QQQ", "is_active": True},
            }

            await engine.reload_symbols()

            # Architectural Rule: Must NOT call get_historical_database_symbols_from_db as a fallback!
            assert not mock_historical.called, (
                "Exclusivity Violation: runner.py fell back to get_historical_database_symbols_from_db() "
                "when streaming symbols were missing!"
            )
            # Ensure historical symbols like SPY / QQQ were NOT loaded into active_streaming_symbols
            assert "SPY" not in engine.active_streaming_symbols
            assert "QQQ" not in engine.active_streaming_symbols

    asyncio.run(_run())


def test_runner_source_code_has_no_historical_fallback():
    """Verify that StreamingEngine implementation does not reference or call historical database symbol functions."""
    import inspect
    start_src = inspect.getsource(StreamingEngine.start)
    reload_src = inspect.getsource(StreamingEngine.reload_symbols)

    assert "get_historical_database_symbols_from_db" not in start_src, (
        "StreamingEngine.start() contains forbidden fallback to get_historical_database_symbols_from_db()"
    )
    assert "get_historical_database_symbols_from_db" not in reload_src, (
        "StreamingEngine.reload_symbols() contains forbidden fallback to get_historical_database_symbols_from_db()"
    )
    assert "get_symbol_map_from_db" not in start_src, (
        "StreamingEngine.start() contains forbidden fallback to get_symbol_map_from_db()"
    )
    assert "get_symbol_map_from_db" not in reload_src, (
        "StreamingEngine.reload_symbols() contains forbidden fallback to get_symbol_map_from_db()"
    )


# ============================================================================
# 5. Dashboard REST API: Candlestick Exclusivity & No Silent Defaults
# ============================================================================

def test_api_historical_candles_endpoint(api_test_server):
    """GET /api/historical/candles strictly queries historical.duckdb."""
    resp = requests.get(f"{api_test_server}/api/historical/candles?symbol=AAPL&tf=1m&limit=5")
    assert resp.status_code == 200
    data = resp.json()
    assert data.get("database") == "historical"
    assert "candles" in data


def test_api_streaming_candles_endpoint(api_test_server):
    """GET /api/streaming/candles strictly queries streaming.duckdb."""
    resp = requests.get(f"{api_test_server}/api/streaming/candles?symbol=AAPL&tf=1m&limit=5")
    assert resp.status_code == 200
    data = resp.json()
    assert data.get("database") == "streaming"
    assert "candles" in data


def test_api_candles_requires_explicit_source_param(api_test_server):
    """
    GET /api/candles MUST require an explicit source query param ('historical' or 'streaming').
    If missing, it MUST return HTTP 400 Bad Request with an error message.
    """
    resp = requests.get(f"{api_test_server}/api/candles?symbol=AAPL&tf=1m&limit=5")
    assert resp.status_code == 400, (
        f"Expected HTTP 400 when 'source' parameter is missing from /api/candles, got {resp.status_code}. "
        "The API must not guess or silently default to a database."
    )
    data = resp.json()
    assert "source" in data.get("error", "").lower() or "source parameter is required" in str(data).lower()


def test_api_candles_rejects_invalid_source_param(api_test_server):
    """GET /api/candles MUST return HTTP 400 Bad Request if source query param is invalid."""
    resp = requests.get(f"{api_test_server}/api/candles?symbol=AAPL&tf=1m&limit=5&source=invalid_db")
    assert resp.status_code == 400, (
        f"Expected HTTP 400 for invalid source='invalid_db' on /api/candles, got {resp.status_code}."
    )
    data = resp.json()
    assert "error" in data


def test_api_candles_with_explicit_historical_source(api_test_server):
    """GET /api/candles?source=historical queries historical.duckdb."""
    resp = requests.get(f"{api_test_server}/api/candles?symbol=AAPL&tf=1m&limit=5&source=historical")
    assert resp.status_code == 200
    data = resp.json()
    assert data.get("database") == "historical"


def test_api_candles_with_explicit_streaming_source(api_test_server):
    """GET /api/candles?source=streaming queries streaming.duckdb."""
    resp = requests.get(f"{api_test_server}/api/candles?symbol=AAPL&tf=1m&limit=5&source=streaming")
    assert resp.status_code == 200
    data = resp.json()
    assert data.get("database") == "streaming"


# ============================================================================
# 6. Dashboard REST API: Symbol Inventory Exclusivity & No Silent Defaults
# ============================================================================

def test_api_historical_symbols_endpoint(api_test_server):
    """GET /api/historical/symbols strictly queries historical.duckdb."""
    resp = requests.get(f"{api_test_server}/api/historical/symbols")
    assert resp.status_code == 200
    data = resp.json()
    assert data.get("database") == "historical"
    assert "symbols" in data


def test_api_streaming_symbols_endpoint(api_test_server):
    """GET /api/streaming/symbols strictly queries streaming.duckdb."""
    resp = requests.get(f"{api_test_server}/api/streaming/symbols")
    assert resp.status_code == 200
    data = resp.json()
    assert data.get("database") == "streaming"
    assert "symbols" in data


def test_api_symbols_requires_explicit_source_param(api_test_server):
    """
    GET /api/symbols MUST require an explicit source query param ('historical' or 'streaming').
    If missing, it MUST return HTTP 400 Bad Request indicating that source parameter is required.
    """
    resp = requests.get(f"{api_test_server}/api/symbols")
    assert resp.status_code == 400, (
        f"Expected HTTP 400 when 'source' parameter is missing from /api/symbols, got {resp.status_code}. "
        "The API must not guess or silently default to a database."
    )
    data = resp.json()
    assert "source" in data.get("error", "").lower() or "source parameter is required" in str(data).lower()


def test_api_symbols_rejects_invalid_source_param(api_test_server):
    """GET /api/symbols MUST return HTTP 400 Bad Request if source query param is invalid."""
    resp = requests.get(f"{api_test_server}/api/symbols?source=invalid_db")
    assert resp.status_code == 400, (
        f"Expected HTTP 400 for invalid source='invalid_db' on /api/symbols, got {resp.status_code}."
    )
    data = resp.json()
    assert "error" in data


def test_api_symbols_with_explicit_historical_source(api_test_server):
    """GET /api/symbols?source=historical returns historical symbols."""
    resp = requests.get(f"{api_test_server}/api/symbols?source=historical")
    assert resp.status_code == 200
    data = resp.json()
    assert data.get("database") == "historical"
    assert "symbols" in data


def test_api_symbols_with_explicit_streaming_source(api_test_server):
    """GET /api/symbols?source=streaming returns streaming symbols."""
    resp = requests.get(f"{api_test_server}/api/symbols?source=streaming")
    assert resp.status_code == 200
    data = resp.json()
    assert data.get("database") == "streaming"
    assert "symbols" in data


# ============================================================================
# 7. Dashboard REST API: Symbol Mutation Exclusivity (POST / DELETE)
# ============================================================================

def test_api_post_historical_symbols_dedicated_endpoint(api_test_server):
    """POST /api/historical/symbols adds symbol strictly to historical database."""
    symbol_name = "TEST_HIST_EXCL_01"
    try:
        post_resp = requests.post(
            f"{api_test_server}/api/historical/symbols",
            json={"display_name": symbol_name, "capital_ticker": symbol_name}
        )
        assert post_resp.status_code == 200

        # Check historical inventory has it
        h_resp = requests.get(f"{api_test_server}/api/historical/symbols")
        h_names = [s.get("display_name") for s in h_resp.json().get("symbols", [])]
        assert symbol_name in h_names

        # Check streaming inventory does NOT have it
        s_resp = requests.get(f"{api_test_server}/api/streaming/symbols")
        s_names = [s.get("display_name") for s in s_resp.json().get("symbols", [])]
        assert symbol_name not in s_names
    finally:
        requests.delete(f"{api_test_server}/api/historical/symbols/{symbol_name}")


def test_api_post_streaming_symbols_dedicated_endpoint(api_test_server):
    """POST /api/streaming/symbols adds symbol strictly to streaming database."""
    symbol_name = "TEST_STRM_EXCL_01"
    try:
        post_resp = requests.post(
            f"{api_test_server}/api/streaming/symbols",
            json={"display_name": symbol_name, "capital_ticker": symbol_name}
        )
        assert post_resp.status_code == 200

        # Check streaming inventory has it
        s_resp = requests.get(f"{api_test_server}/api/streaming/symbols")
        s_names = [s.get("display_name") for s in s_resp.json().get("symbols", [])]
        assert symbol_name in s_names

        # Check historical inventory does NOT have it
        h_resp = requests.get(f"{api_test_server}/api/historical/symbols")
        h_names = [s.get("display_name") for s in h_resp.json().get("symbols", [])]
        assert symbol_name not in h_names
    finally:
        requests.delete(f"{api_test_server}/api/streaming/symbols/{symbol_name}")


def test_api_post_symbols_requires_source_param(api_test_server):
    """
    POST /api/symbols without source parameter MUST return HTTP 400 Bad Request.
    The API must not assume or default to historical or streaming.
    """
    symbol_name = "TEST_NO_SRC_01"
    try:
        resp = requests.post(
            f"{api_test_server}/api/symbols",
            json={"display_name": symbol_name, "capital_ticker": symbol_name}
        )
        assert resp.status_code == 400, (
            f"Expected HTTP 400 when source param is omitted on POST /api/symbols, got {resp.status_code}. "
            "Dedicated endpoints or explicit source param must be enforced."
        )
        data = resp.json()
        assert "source" in data.get("error", "").lower() or "source is required" in str(data).lower()
    finally:
        requests.delete(f"{api_test_server}/api/symbols/{symbol_name}")


def test_api_post_symbols_with_explicit_historical_source(api_test_server):
    """POST /api/symbols?source=historical adds symbol strictly to historical database."""
    symbol_name = "TEST_HIST_SRC_01"
    try:
        resp = requests.post(
            f"{api_test_server}/api/symbols?source=historical",
            json={"display_name": symbol_name, "capital_ticker": symbol_name}
        )
        assert resp.status_code == 200

        # Check historical has it
        h_resp = requests.get(f"{api_test_server}/api/historical/symbols")
        h_names = [s.get("display_name") for s in h_resp.json().get("symbols", [])]
        assert symbol_name in h_names

        # Check streaming does not have it
        s_resp = requests.get(f"{api_test_server}/api/streaming/symbols")
        s_names = [s.get("display_name") for s in s_resp.json().get("symbols", [])]
        assert symbol_name not in s_names
    finally:
        requests.delete(f"{api_test_server}/api/historical/symbols/{symbol_name}")


def test_api_post_symbols_with_explicit_streaming_source(api_test_server):
    """POST /api/symbols?source=streaming adds symbol strictly to streaming database."""
    symbol_name = "TEST_STRM_SRC_01"
    try:
        resp = requests.post(
            f"{api_test_server}/api/symbols?source=streaming",
            json={"display_name": symbol_name, "capital_ticker": symbol_name}
        )
        assert resp.status_code == 200

        # Check streaming has it
        s_resp = requests.get(f"{api_test_server}/api/streaming/symbols")
        s_names = [s.get("display_name") for s in s_resp.json().get("symbols", [])]
        assert symbol_name in s_names, "Symbol added with source=streaming must be present in streaming symbols"

        # Check historical does not have it
        h_resp = requests.get(f"{api_test_server}/api/historical/symbols")
        h_names = [s.get("display_name") for s in h_resp.json().get("symbols", [])]
        assert symbol_name not in h_names, "Symbol added with source=streaming must NOT be present in historical symbols"
    finally:
        requests.delete(f"{api_test_server}/api/streaming/symbols/{symbol_name}")
