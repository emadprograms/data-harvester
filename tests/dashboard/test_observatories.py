"""
Tests for the single-store observatory.

v5.0 has one store - the Parquet tick lake - so candles are observed, queried and
analyzed from lake ticks alone. This file previously verified that a permanent
historical DuckDB archive and an ephemeral streaming DuckDB buffer were queried
independently; that separation no longer exists.
"""
from src.dashboard.analytics import get_streaming_candles, get_candles

LAKE_DATE = "2026-07-10"


def test_lake_candles_come_from_lake_ticks():
    """get_streaming_candles returns candles derived from lake ticks."""
    res = get_streaming_candles("NVDA", timeframe="1m", limit=10, date=LAKE_DATE)
    assert res.get("database") == "streaming"
    assert res.get("symbol") == "NVDA"
    assert len(res.get("candles")) >= 1

    candle = res["candles"][0]
    assert candle["source"] in ("CAPITAL", "CAPITAL_STREAM")
    assert candle["tick_count"] >= 1


def test_get_candles_serves_the_lake_regardless_of_source():
    """The unified entry point has one backing store; db_source is ignored."""
    for source in ("historical", "streaming", "live", None):
        res = get_candles("NVDA", timeframe="1m", limit=5, date=LAKE_DATE, db_source=source)
        assert res.get("database") == "streaming", f"db_source={source!r} did not resolve to the lake"
        assert res.get("candles"), f"db_source={source!r} returned no candles"
