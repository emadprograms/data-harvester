"""
Tests for US Eastern / NYSE Stock Exchange Timezone on Charts.

v5.0 serves candles from the Parquet tick lake only. Verifies that:
- lake candle payloads declare timezone America/New_York (NYSE exchange time)
- regular session candles align with the 09:30-16:00 NYSE clock
- aggregated timeframes also reflect the exchange clock
- static files include NYSE (ET) badges and formatters
"""
from src.dashboard.analytics import get_streaming_candles, get_candles

LAKE_DATE = "2026-07-10"


def test_lake_candles_timezone_metadata():
    """The candle payload must declare timezone as America/New_York."""
    res = get_candles("NVDA", timeframe="1m", limit=10, date=LAKE_DATE)
    assert res.get("timezone") == "America/New_York"
    assert res.get("symbol") == "NVDA"
    assert isinstance(res.get("candles"), list)


def test_lake_candles_align_with_nyse_market_hours():
    """Every candle time must fall inside the 09:30-16:00 NYSE session."""
    res = get_candles("NVDA", timeframe="1m", limit=2000, date=LAKE_DATE)
    assert res["count"] > 0

    for c in res["candles"]:
        time_part = c["time_str"].split(" ")[1]
        assert "09:30:00" <= time_part <= "16:00:00", f"candle outside NYSE hours: {c['time_str']}"


def test_lake_candles_aggregated_buckets_timezone():
    """Aggregated timeframes (1h, 1d) must also reflect the NYSE exchange clock."""
    res_1h = get_candles("NVDA", timeframe="1h", limit=10, date=LAKE_DATE)
    assert res_1h["count"] > 0
    assert res_1h.get("timezone") == "America/New_York"
    for c in res_1h["candles"]:
        assert ":" in c["time_str"]

    res_1d = get_candles("NVDA", timeframe="1d", limit=10, date=LAKE_DATE)
    assert res_1d["count"] > 0
    assert res_1d.get("timezone") == "America/New_York"
    for c in res_1d["candles"]:
        # Daily candles in NY time should end with 00:00:00 (day boundary)
        assert c["time_str"].endswith("00:00:00")


def test_streaming_candles_timezone_metadata():
    """get_streaming_candles must declare timezone as America/New_York."""
    res = get_streaming_candles("AAPL", timeframe="1m", limit=10, date=LAKE_DATE)
    assert res.get("database") == "streaming"
    assert res.get("timezone") == "America/New_York"


def test_static_ui_nyse_timezone_indicators():
    """Frontend static files must contain explicit US/Eastern NYSE badges and headers."""
    with open("src/dashboard/static/index.html", "r", encoding="utf-8") as f:
        html = f.read()
    assert "legend-tz-badge" in html
    assert "NYSE (ET)" in html
    assert "Timestamp (US/Eastern)" in html

    with open("src/dashboard/static/js/chart.js", "r", encoding="utf-8") as f:
        js_chart = f.read()
    assert "tickMarkFormatter" in js_chart
    assert "timeFormatter" in js_chart
    assert " ET" in js_chart

    with open("src/dashboard/static/js/tables.js", "r", encoding="utf-8") as f:
        js_tables = f.read()
    assert "Timestamp (US/Eastern)" in js_tables
    assert "ET" in js_tables
