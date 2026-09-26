"""
Tests for US Eastern / NYSE Stock Exchange Timezone on Historical Database Charts.
Verifies that:
- get_historical_candles converts UTC timestamps to America/New_York (NYSE exchange time)
- Regular session market open is at 09:30:00 and market close is at 16:00:00 in America/New_York
- get_streaming_candles returns America/New_York timezone
- Static files include NYSE (ET) badges and formatters
"""
from src.dashboard.analytics import get_historical_candles, get_streaming_candles, get_candles


def test_historical_candles_timezone_metadata():
    """get_historical_candles payload must declare timezone as America/New_York."""
    res = get_historical_candles("NVDA", timeframe="1m", limit=10)
    assert res.get("database") == "historical"
    assert res.get("timezone") == "America/New_York"
    assert isinstance(res.get("candles"), list)


def test_historical_candles_nyse_market_hours():
    """Regular session (REG) candles must align with 09:30 open and 16:00 close in America/New_York."""
    res = get_historical_candles("NVDA", timeframe="1m", limit=2000)
    assert res["count"] > 0

    reg_candles = [c for c in res["candles"] if c.get("session") == "REG"]
    assert len(reg_candles) > 0

    # Every regular session candle time_str should fall between 09:30 and 16:00 NYSE time
    for c in reg_candles:
        time_part = c["time_str"].split(" ")[1]
        assert "09:30:00" <= time_part <= "16:00:00", f"REG session candle outside NYSE hours: {c['time_str']}"


def test_historical_candles_aggregated_buckets_timezone():
    """Aggregated timeframes (1h, 1d) must also reflect NYSE stock exchange clock."""
    res_1h = get_candles("NVDA", timeframe="1h", limit=10, db_source="historical")
    assert res_1h["count"] > 0
    assert res_1h.get("timezone") == "America/New_York"
    for c in res_1h["candles"]:
        assert ":" in c["time_str"]

    res_1d = get_candles("NVDA", timeframe="1d", limit=10, db_source="historical")
    assert res_1d["count"] > 0
    assert res_1d.get("timezone") == "America/New_York"
    for c in res_1d["candles"]:
        # Daily candles in NY time should end with 00:00:00 (day boundary)
        assert c["time_str"].endswith("00:00:00")


def test_streaming_candles_timezone_metadata():
    """get_streaming_candles must declare timezone as America/New_York."""
    res = get_streaming_candles("AAPL", timeframe="1m", limit=10)
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
