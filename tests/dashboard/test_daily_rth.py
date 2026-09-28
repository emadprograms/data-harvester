"""
Unit tests for daily 1D chart RTH (Regular Trading Hours) isolation.
Verifies that 1D candle queries strictly aggregate only RTH hours (09:30 - 16:00 ET, session='REG')
by default, discarding pre-market (PRE) and post-market (POST) extremes unless explicitly requested.
"""
import duckdb
import pytest
from unittest.mock import patch, MagicMock
from src.dashboard.analytics import get_historical_candles, get_candles


@pytest.fixture
def mock_duckdb_with_full_day():
    """
    Creates an in-memory DuckDB database with market_data containing
    PRE (04:00 ET), REG (09:30 ET), and POST (18:00 ET) 1-minute bars on 2026-09-25.
    """
    con = duckdb.connect(":memory:")
    con.execute("""
        CREATE TABLE market_data (
            symbol VARCHAR,
            timestamp TIMESTAMP,
            open DOUBLE,
            high DOUBLE,
            low DOUBLE,
            close DOUBLE,
            volume DOUBLE,
            source VARCHAR,
            session VARCHAR
        )
    """)

    # 2026-09-25 UTC timestamps:
    # 04:00 ET = 08:00 UTC (PRE) -> open: 100.0, high: 105.0, low: 99.0, close: 102.0
    # 09:30 ET = 13:30 UTC (REG open) -> open: 103.0, high: 104.0, low: 101.0, close: 102.5
    # 15:59 ET = 19:59 UTC (REG close) -> open: 107.0, high: 110.0, low: 106.0, close: 108.0
    # 18:00 ET = 22:00 UTC (POST) -> open: 108.5, high: 115.0, low: 107.0, close: 112.0
    con.execute("""
        INSERT INTO market_data VALUES
            ('SPY', '2026-09-25 08:00:00'::TIMESTAMP, 100.0, 105.0, 99.0,  102.0, 1000.0, 'TEST', 'PRE'),
            ('SPY', '2026-09-25 13:30:00'::TIMESTAMP, 103.0, 104.0, 101.0, 102.5, 5000.0, 'TEST', 'REG'),
            ('SPY', '2026-09-25 19:59:00'::TIMESTAMP, 107.0, 110.0, 106.0, 108.0, 4000.0, 'TEST', 'REG'),
            ('SPY', '2026-09-25 22:00:00'::TIMESTAMP, 108.5, 115.0, 107.0, 112.0, 2000.0, 'TEST', 'POST')
    """)

    # Mock wrapper conforming to analytics.py client interface
    class DuckDBClientWrapper:
        def __init__(self, db_conn):
            self.conn = db_conn

        def execute(self, sql, params=None):
            if params:
                res = self.conn.execute(sql, params)
            else:
                res = self.conn.execute(sql)
            rows = res.fetchall()
            return MagicMock(rows=rows)

        def close(self):
            pass

    wrapper = DuckDBClientWrapper(con)
    return wrapper


def test_1d_historical_candles_defaults_to_rth(mock_duckdb_with_full_day):
    """
    1D timeframe without session parameter MUST strictly aggregate Regular Trading Hours (REG):
    - Open should be REG open (103.0), NOT PRE open (100.0)
    - High should be REG high (110.0), NOT POST high (115.0)
    - Low should be REG low (101.0), NOT PRE low (99.0)
    - Close should be REG close (108.0), NOT POST close (112.0)
    - Session must be 'REG', NOT 'POST, PRE, REG'
    """
    with patch("src.dashboard.analytics.get_historical_db_connection", return_value=mock_duckdb_with_full_day):
        res = get_historical_candles("SPY", timeframe="1d")
        assert res["database"] == "historical"
        assert res["count"] == 1
        candle = res["candles"][0]
        assert candle["open"] == 103.0, f"Expected RTH open 103.0, got {candle['open']}"
        assert candle["high"] == 110.0, f"Expected RTH high 110.0, got {candle['high']}"
        assert candle["low"] == 101.0, f"Expected RTH low 101.0, got {candle['low']}"
        assert candle["close"] == 108.0, f"Expected RTH close 108.0, got {candle['close']}"
        assert candle["volume"] == 9000.0, f"Expected RTH volume (5000+4000)=9000, got {candle['volume']}"
        assert candle["session"] == "REG", f"Expected session 'REG', got {candle['session']}"


def test_1d_historical_candles_explicit_all_sessions(mock_duckdb_with_full_day):
    """
    When session='ALL' is explicitly requested, full ETH day should be aggregated:
    - Open: 100.0 (PRE)
    - High: 115.0 (POST)
    - Low: 99.0 (PRE)
    - Close: 112.0 (POST)
    """
    with patch("src.dashboard.analytics.get_historical_db_connection", return_value=mock_duckdb_with_full_day):
        res = get_historical_candles("SPY", timeframe="1d", session="ALL")
        assert res["count"] == 1
        candle = res["candles"][0]
        assert candle["open"] == 100.0
        assert candle["high"] == 115.0
        assert candle["low"] == 99.0
        assert candle["close"] == 112.0
        assert candle["volume"] == 12000.0


def test_get_candles_unified_routing_supports_session(mock_duckdb_with_full_day):
    """Verify get_candles passes session argument to get_historical_candles."""
    with patch("src.dashboard.analytics.get_historical_db_connection", return_value=mock_duckdb_with_full_day):
        res = get_candles("SPY", timeframe="1d", session="REG")
        assert res["count"] == 1
        candle = res["candles"][0]
        assert candle["open"] == 103.0
        assert candle["close"] == 108.0
