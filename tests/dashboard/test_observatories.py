"""
Tests for Separated Observatories Architecture:
Verifies that historical.duckdb (permanent canonical archive) and streaming.duckdb
(ephemeral live tick buffer) are observed, queried, and analyzed completely independently.
"""
import pytest
import pandas as pd
from datetime import datetime, timezone
from src.dashboard.analytics import (
    get_historical_candles,
    get_streaming_candles,
    get_candles,
    get_historical_overview,
    get_symbols_coverage,
    get_stream_status,
)
from src.database.connection import (
    get_historical_db_connection,
    get_streaming_db_connection,
)
from src.database.operations import _save_to_client, save_ticks_to_storage


def test_get_historical_candles_pure_isolation():
    """get_historical_candles must only query market_data and never blend streaming ticks."""
    res = get_historical_candles("AAPL", timeframe="1m", limit=10)
    assert res.get("database") == "historical"
    assert res.get("symbol") == "AAPL"
    assert isinstance(res.get("candles"), list)
    if res["candles"]:
        for candle in res["candles"]:
            # Sources in historical should be Tier 1 or Tier 2 REST sources, never CAPITAL_STREAM
            assert candle.get("source") != "CAPITAL_STREAM"
            assert "open" in candle
            assert "high" in candle
            assert "low" in candle
            assert "close" in candle
            assert "volume" in candle


def test_get_streaming_candles_pure_isolation():
    """get_streaming_candles must query streaming ticks and return database='streaming'."""
    # Ensure there is at least one tick in streaming.duckdb for testing
    s_client = get_streaming_db_connection()
    now_ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")
    save_ticks_to_storage(s_client, [
        (now_ts, "TEST_STREAM", 100.0, 1.0, 99.9, 100.1, "CAPITAL", "REG")
    ])
    s_client.close()

    res = get_streaming_candles("TEST_STREAM", timeframe="1m", limit=10)
    assert res.get("database") == "streaming"
    assert res.get("symbol") == "TEST_STREAM"
    assert len(res.get("candles")) >= 1
    candle = res["candles"][0]
    assert candle["source"] in ("CAPITAL", "CAPITAL_STREAM")
    assert candle["tick_count"] >= 1


def test_get_candles_routing():
    """get_candles router routes cleanly based on db_source argument."""
    res_hist = get_candles("AAPL", timeframe="1m", limit=5, db_source="historical")
    assert res_hist.get("database") == "historical"

    res_stream = get_candles("AAPL", timeframe="1m", limit=5, db_source="streaming")
    assert res_stream.get("database") == "streaming"


def test_historical_overview_breakdown():
    """get_historical_overview returns total rows, date span, and source-tier breakdown."""
    overview = get_historical_overview()
    assert "total_rows" in overview
    assert overview["total_rows"] > 0
    assert "sources" in overview
    assert "MASSIVE" in overview["sources"]
    assert overview["database"] == "data/historical.duckdb"
    assert "unique_symbols" in overview
    assert overview["unique_symbols"] >= 30


def test_symbols_coverage_matrix():
    """get_symbols_coverage returns matrix of symbols with bar counts and date spans."""
    cov = get_symbols_coverage()
    assert "symbols" in cov
    assert cov["total_symbols"] >= 30
    symbol_names = [s["display_name"] for s in cov["symbols"]]
    assert "AAPL" in symbol_names
    assert "NVDA" in symbol_names
    assert "SPY" in symbol_names
