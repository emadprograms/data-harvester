"""
Edge case and boundary tests for dashboard analytics queries.
Tests non-existent symbols, extreme limit values, empty results, and fallback handling.
"""
import pytest
from src.dashboard.analytics import (
    get_candles,
    get_streaming_candles,
    get_stream_tape,
    get_stream_status,
    get_market_session_info,
)


def test_streaming_candles_nonexistent_symbol():
    """Querying a non-existent streaming symbol returns empty list without exceptions."""
    res = get_streaming_candles("NONEXISTENT_STREAM_TICKER", timeframe="1m", limit=50)
    assert res["database"] == "streaming"
    assert res["symbol"] == "NONEXISTENT_STREAM_TICKER"
    assert res["candles"] == []


def test_get_candles_fallback_defaults():
    """get_candles resolves to the lake and handles unknown symbols cleanly."""
    res = get_candles("UNKNOWN_SYM", timeframe="5m", limit=10)
    assert res["database"] == "streaming"
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
    assert info["phase"] in ["PRE_MARKET", "REGULAR", "AFTER_HOURS", "CLOSED", "WEEKEND", "HOLIDAY", "OVERNIGHT"]


