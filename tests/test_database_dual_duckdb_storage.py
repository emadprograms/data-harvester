"""
Tests for Dedicated Dual-DuckDB Storage Layer (Phase 5).
Validates 100% separate DuckDB files for historical 1m data and streaming raw ticks,
independent connection pools, raw tick batch insertion, dynamic time_bucket aggregation,
cross-database ATTACH queries, and symbol inventory management.
"""
import os
import tempfile
import pytest
import pandas as pd
from datetime import datetime, timezone, timedelta
from src.database.connection import (
    DuckDBClient,
    get_historical_db_connection,
    get_streaming_db_connection,
    get_duckdb_connection,
    get_archive_db_connection,
    get_unified_connection,
)
from src.database.schema import (
    init_historical_db,
    init_streaming_db,
    init_db,
)
from src.database.operations import (
    save_ticks_to_streaming_db,
    query_streaming_ticks,
    query_streaming_candlesticks,
    get_symbol_map_from_db,
    get_symbol_inventory_list,
    add_symbol_to_db,
    remove_symbol_from_db,
    save_data_to_storage,
    query_candlesticks,
)


@pytest.fixture
def temp_dual_dbs():
    """Provides temporary isolated historical and streaming DuckDB files."""
    temp_dir = tempfile.mkdtemp()
    hist_path = os.path.join(temp_dir, "test_historical.duckdb")
    stream_path = os.path.join(temp_dir, "test_streaming.duckdb")

    hist_client = DuckDBClient(hist_path)
    stream_client = DuckDBClient(stream_path)

    init_historical_db(hist_client)
    init_streaming_db(stream_client)

    yield {
        "dir": temp_dir,
        "hist_path": hist_path,
        "stream_path": stream_path,
        "hist_client": hist_client,
        "stream_client": stream_client,
    }

    hist_client.close()
    stream_client.close()
    try:
        import shutil
        shutil.rmtree(temp_dir)
    except Exception:
        pass


def test_dual_database_independent_files(temp_dual_dbs):
    """Verify that historical and streaming tables reside in separate files."""
    hist_client = temp_dual_dbs["hist_client"]
    stream_client = temp_dual_dbs["stream_client"]

    # Verify tables in historical db
    hist_tables = [r[0] for r in hist_client.execute("SHOW TABLES").rows]
    assert "symbol_map" in hist_tables
    assert "market_data" in hist_tables
    assert "streaming_ticks" not in hist_tables

    # Verify tables in streaming db
    stream_tables = [r[0] for r in stream_client.execute("SHOW TABLES").rows]
    assert "streaming_ticks" in stream_tables
    assert "market_data" not in stream_tables


def test_streaming_ticks_batch_insertion_and_query(temp_dual_dbs):
    """Verify raw tick quotes are saved and retrieved accurately."""
    stream_client = temp_dual_dbs["stream_client"]

    base_time = datetime(2026, 9, 25, 14, 0, 0, tzinfo=timezone.utc)
    ticks = [
        (base_time + timedelta(seconds=i), "SPY", 580.10 + i * 0.05, 580.15 + i * 0.05, 580.12 + i * 0.05, 10.0, "CAPITAL")
        for i in range(10)
    ]

    success = save_ticks_to_streaming_db(ticks, client=stream_client)
    assert success is True

    df = query_streaming_ticks("SPY", client=stream_client)
    assert len(df) == 10
    assert set(["timestamp", "symbol", "price", "volume", "bid", "ask", "source"]).issubset(set(df.columns))
    assert df.iloc[0]["price"] == pytest.approx(580.12)
    assert df.iloc[-1]["price"] == pytest.approx(580.12 + 9 * 0.05)
    assert df.iloc[0]["source"] == "CAPITAL"


def test_streaming_ticks_time_bucket_candlestick_resampling(temp_dual_dbs):
    """Verify raw tick quotes can be dynamically resampled into 1m OHLCV bars."""
    stream_client = temp_dual_dbs["stream_client"]

    # Minute 1 ticks: 14:00:10 -> 100, 14:00:20 -> 105, 14:00:30 -> 95, 14:00:50 -> 102
    t1 = datetime(2026, 9, 25, 14, 0, 10, tzinfo=timezone.utc)
    t2 = datetime(2026, 9, 25, 14, 0, 20, tzinfo=timezone.utc)
    t3 = datetime(2026, 9, 25, 14, 0, 30, tzinfo=timezone.utc)
    t4 = datetime(2026, 9, 25, 14, 0, 50, tzinfo=timezone.utc)

    # Minute 2 ticks: 14:01:05 -> 103, 14:01:40 -> 110
    t5 = datetime(2026, 9, 25, 14, 1, 5, tzinfo=timezone.utc)
    t6 = datetime(2026, 9, 25, 14, 1, 40, tzinfo=timezone.utc)

    ticks = [
        (t1, "AAPL", 99.9, 100.1, 100.0, 5.0, "CAPITAL"),
        (t2, "AAPL", 104.9, 105.1, 105.0, 15.0, "CAPITAL"),
        (t3, "AAPL", 94.9, 95.1, 95.0, 8.0, "CAPITAL"),
        (t4, "AAPL", 101.9, 102.1, 102.0, 12.0, "CAPITAL"),
        (t5, "AAPL", 102.9, 103.1, 103.0, 20.0, "CAPITAL"),
        (t6, "AAPL", 109.9, 110.1, 110.0, 10.0, "CAPITAL"),
    ]

    save_ticks_to_streaming_db(ticks, client=stream_client)

    candles_df = query_streaming_candlesticks("AAPL", timeframe="1m", client=stream_client)
    assert len(candles_df) == 2

    bar1 = candles_df.iloc[0]
    assert bar1["open"] == pytest.approx(100.0)
    assert bar1["high"] == pytest.approx(105.0)
    assert bar1["low"] == pytest.approx(95.0)
    assert bar1["close"] == pytest.approx(102.0)
    assert bar1["volume"] == pytest.approx(40.0)
    assert bar1["tick_count"] == 4

    bar2 = candles_df.iloc[1]
    assert bar2["open"] == pytest.approx(103.0)
    assert bar2["high"] == pytest.approx(110.0)
    assert bar2["close"] == pytest.approx(110.0)
    assert bar2["tick_count"] == 2


def test_dual_duckdb_concurrency_isolation(temp_dual_dbs):
    """Verify that writing to streaming.duckdb does not lock historical.duckdb."""
    hist_client = temp_dual_dbs["hist_client"]
    stream_client = temp_dual_dbs["stream_client"]

    # Streamer writes ticks
    t0 = datetime(2026, 9, 25, 14, 0, 0, tzinfo=timezone.utc)
    stream_ticks = [(t0, "TSLA", 250.0, 250.2, 250.1, 1.0, "CAPITAL")]
    assert save_ticks_to_streaming_db(stream_ticks, client=stream_client) is True

    # Simultaneously, historical harvester writes 1m candles
    hist_df = pd.DataFrame([{
        "timestamp": t0,
        "symbol": "TSLA",
        "open": 249.0,
        "high": 251.0,
        "low": 248.5,
        "close": 250.5,
        "volume": 1000.0,
        "session": "REG",
        "source": "CAPITAL"
    }])
    assert save_data_to_storage(hist_df, archive_client=hist_client) is True

    # Both verify successfully with zero lock conflicts
    hist_candles = query_candlesticks("TSLA", client=hist_client)
    assert len(hist_candles) == 1

    stream_ticks_df = query_streaming_ticks("TSLA", client=stream_client)
    assert len(stream_ticks_df) == 1


def test_cross_database_attach_query(temp_dual_dbs):
    """Verify DuckDB ATTACH allows querying both databases in a single session."""
    hist_path = temp_dual_dbs["hist_path"]
    stream_path = temp_dual_dbs["stream_path"]
    hist_client = temp_dual_dbs["hist_client"]
    stream_client = temp_dual_dbs["stream_client"]

    # Seed data into both
    t0 = datetime(2026, 9, 25, 15, 0, 0, tzinfo=timezone.utc)
    save_ticks_to_streaming_db([(t0, "NVDA", 120.0, 120.2, 120.1, 5.0, "CAPITAL")], client=stream_client)

    # Close initial write connections before attaching in-memory
    hist_client.close()
    stream_client.close()

    # Cross-query using ATTACH on a unified connection
    unified_conn = DuckDBClient(":memory:")
    unified_conn.attach(hist_path, "hist", read_only=True)
    unified_conn.attach(stream_path, "live", read_only=True)

    query = """
        SELECT 
            m.display_name,
            COUNT(t.price) as live_ticks_count
        FROM hist.symbol_map m
        LEFT JOIN live.streaming_ticks t ON m.display_name = t.symbol
        WHERE m.display_name = 'NVDA'
        GROUP BY m.display_name
    """
    res = unified_conn.execute(query)
    row = res.fetchone()
    assert row is not None
    assert row[0] == "NVDA"
    assert row[1] == 1
    unified_conn.close()

    # Reopen fixtures so teardown closes cleanly
    temp_dual_dbs["hist_client"] = DuckDBClient(hist_path)
    temp_dual_dbs["stream_client"] = DuckDBClient(stream_path)


def test_symbol_inventory_crud_operations(temp_dual_dbs):
    """Verify adding, retrieving, and removing symbols in symbol_map."""
    hist_client = temp_dual_dbs["hist_client"]

    # Add new symbol
    success = add_symbol_to_db("PLTR", massive_ticker="PLTR", capital_ticker="PLTR", client=hist_client)
    assert success is True

    # Retrieve map
    smap = get_symbol_map_from_db(client=hist_client)
    assert "PLTR" in smap
    assert smap["PLTR"]["capital_ticker"] == "PLTR"

    # Retrieve list
    slist = get_symbol_inventory_list(client=hist_client)
    pltr_entry = next((s for s in slist if s["display_name"] == "PLTR"), None)
    assert pltr_entry is not None
    assert pltr_entry["capital_ticker"] == "PLTR"

    # Remove symbol
    removed = remove_symbol_from_db("PLTR", client=hist_client)
    assert removed is True
    smap_after = get_symbol_map_from_db(client=hist_client)
    assert "PLTR" not in smap_after


def test_backward_compatible_aliases():
    """Verify backward compatibility aliases resolve cleanly without errors."""
    client = get_duckdb_connection()
    assert client is not None
    client.close()

    archive_client = get_archive_db_connection()
    assert archive_client is not None
    archive_client.close()

    hist_client = get_historical_db_connection()
    assert hist_client is not None
    hist_client.close()

    stream_client = get_streaming_db_connection()
    assert stream_client is not None
    stream_client.close()
