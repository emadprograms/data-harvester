"""
Candle timezone determinism on the Parquet tick lake.

v5.0 has one store, so there is no historical/streaming column-type split to
parameterize over. These tests keep the guarantees that still matter - the
opening bell renders at 09:30 ET, daily buckets align to exchange midnight,
intraday buckets snap to the exchange clock, and the host timezone cannot move
any of them - now asserted against lake candles.

The previous version of this file parameterized every test over the historical
DuckDB's storage shape (TIMESTAMP vs TIMESTAMPTZ) and over DuckDBClient session
pinning. Both the column-type branch and DuckDBClient are removed in v5.0.
"""
import time

import pytest

from src.dashboard.analytics import get_candles

LAKE_DATE = "2026-07-10"  # 13:30-15:00 UTC = 09:30-11:00 America/New_York (EDT)
OPENING_BELL_ET = "09:30:00"


def test_opening_bell_renders_at_0930_et():
    res = get_candles("NVDA", timeframe="1m", limit=2000, date=LAKE_DATE)
    assert res["count"] > 0
    assert res["candles"][0]["time_str"].split(" ")[1] == OPENING_BELL_ET


def test_daily_buckets_align_to_exchange_midnight():
    res = get_candles("NVDA", timeframe="1d", limit=50, date=LAKE_DATE)
    assert res["count"] > 0
    for candle in res["candles"]:
        assert candle["time_str"].endswith("00:00:00"), f"daily bucket not on a day boundary: {candle['time_str']}"


def test_intraday_buckets_snap_to_exchange_clock():
    res = get_candles("NVDA", timeframe="5m", limit=500, date=LAKE_DATE)
    assert res["count"] > 0
    for candle in res["candles"]:
        minute = int(candle["time_str"].split(" ")[1].split(":")[1])
        assert minute % 5 == 0, f"5m bucket not snapped to the exchange clock: {candle['time_str']}"


@pytest.mark.parametrize("host_tz", ["UTC", "America/New_York", "Asia/Bahrain"])
def test_host_timezone_does_not_move_the_opening_bell(monkeypatch, host_tz):
    monkeypatch.setenv("TZ", host_tz)
    if hasattr(time, "tzset"):
        time.tzset()
    res = get_candles("NVDA", timeframe="1m", limit=2000, date=LAKE_DATE)
    assert res["count"] > 0
    assert res["candles"][0]["time_str"].split(" ")[1] == OPENING_BELL_ET
