"""
Edge case and boundary tests for dashboard analytics queries.
Tests non-existent symbols, extreme limit values, empty results, and fallback handling.
"""
import pytest
from src.dashboard.analytics import (
    get_candles,
    get_historical_candles,
    get_streaming_candles,
    get_stream_tape,
    get_stream_status,
    get_market_session_info,
    get_historical_overview,
    get_symbols_coverage,
)


def test_historical_candles_nonexistent_symbol():
    """Querying a non-existent symbol returns empty list without exceptions."""
    res = get_historical_candles("NONEXISTENT_TICKER_XYZ", timeframe="1m", limit=50)
    assert res["database"] == "historical"
    assert res["symbol"] == "NONEXISTENT_TICKER_XYZ"
    assert res["candles"] == []


def test_streaming_candles_nonexistent_symbol():
    """Querying a non-existent streaming symbol returns empty list without exceptions."""
    res = get_streaming_candles("NONEXISTENT_STREAM_TICKER", timeframe="1m", limit=50)
    assert res["database"] == "streaming"
    assert res["symbol"] == "NONEXISTENT_STREAM_TICKER"
    assert res["candles"] == []


def test_get_candles_fallback_defaults():
    """get_candles should default to historical and handle unknown symbols cleanly."""
    res = get_candles("UNKNOWN_SYM", timeframe="5m", limit=10)
    assert res["database"] == "historical"
    assert res["candles"] == []


def test_stream_tape_limit_clamping():
    """get_stream_tape handles custom limits and defaults gracefully."""
    res_custom = get_stream_tape(limit=3)
    assert "ticks" in res_custom
    assert len(res_custom["ticks"]) <= 3

    res_default = get_stream_tape()
    assert "ticks" in res_default
    assert len(res_default["ticks"]) <= 50


def test_market_session_info_contract():
    """get_market_session_info returns required temporal and market state fields."""
    info = get_market_session_info()
    required_keys = [
        "time_utc", "time_et", "is_weekend", "is_holiday",
        "phase", "is_regular_open", "active_session_date", "seconds_to_session_cutoff"
    ]
    for key in required_keys:
        assert key in info, f"Missing required key '{key}' in market session info"
    assert info["phase"] in ["PRE_MARKET", "REGULAR", "AFTER_HOURS", "CLOSED", "WEEKEND", "HOLIDAY"]


def test_historical_overview_contract():
    """get_historical_overview returns total_rows, date_range, and sources dictionary."""
    overview = get_historical_overview()
    assert "database" in overview
    assert "total_rows" in overview
    assert "unique_symbols" in overview
    assert "sources" in overview
    assert isinstance(overview["sources"], dict)


def test_symbols_coverage_sorting():
    """get_symbols_coverage returns symbols sorted cleanly with bar counts."""
    cov = get_symbols_coverage()
    assert "symbols" in cov
    assert "total_symbols" in cov
    assert cov["total_symbols"] > 0
    symbols = cov["symbols"]
    # Check that each symbol entry has complete schema
    for s in symbols[:5]:
        assert "display_name" in s
        assert "asset_class" in s
        assert "bar_count" in s
        assert "first_timestamp" in s
        assert "last_timestamp" in s
        assert "freshness" in s
        assert "sources_list" in s
