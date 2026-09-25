"""
Unit and integration tests for Databento tick backfill engine.
Validates symbol filtering (single-stocks only), trading session calculation,
TBBO record normalization, and budget cap safety guards.
"""
from datetime import date, datetime, timezone
from zoneinfo import ZoneInfo
from unittest.mock import MagicMock, patch
import pytest
import pandas as pd

from src.data.databento_backfill import (
    get_target_stock_symbols,
    get_day_trading_bounds,
    generate_candidate_trading_days,
    insert_ticks_to_streaming_db,
    run_databento_backfill,
    EXCLUDED_SYMBOLS,
    NY_TZ
)
from src.database.connection import DuckDBClient


def test_target_stock_symbols_filtering():
    """Verify that only single stock symbols are returned, excluding ETFs, crypto, and futures."""
    symbols = get_target_stock_symbols()
    assert len(symbols) > 0

    # Ensure major stocks are present
    for expected in ["AAPL", "NVDA", "MSFT", "AMZN", "GOOGL"]:
        assert expected in symbols, f"Expected stock {expected} not found in target symbols"

    # Ensure all ETFs, Crypto, and Commodities are excluded
    for excluded in ["SPY", "QQQ", "IWM", "DIA", "BTCUSDT", "ETHUSDT", "CL=F", "VIX"]:
        assert excluded not in symbols, f"Excluded asset {excluded} was unexpectedly included"

    for sym in symbols:
        assert sym not in EXCLUDED_SYMBOLS
        assert not sym.endswith("USDT")
        assert "=" not in sym


def test_day_trading_bounds_timezones():
    """Verify 09:00 ET to 16:00 ET boundaries are correctly converted to UTC with EDT/EST awareness."""
    # Summer/Fall EDT date (UTC-4)
    edt_day = date(2026, 9, 24)
    start_utc, reg_open_utc, end_utc = get_day_trading_bounds(edt_day)

    assert start_utc.hour == 13 and start_utc.minute == 0  # 09:00 EDT = 13:00 UTC
    assert reg_open_utc.hour == 13 and reg_open_utc.minute == 30  # 09:30 EDT = 13:30 UTC
    assert end_utc.hour == 20 and end_utc.minute == 0  # 16:00 EDT = 20:00 UTC
    assert start_utc.tzinfo == timezone.utc

    # Winter EST date (UTC-5)
    est_day = date(2026, 1, 15)
    start_est, reg_open_est, end_est = get_day_trading_bounds(est_day)
    assert start_est.hour == 14 and start_est.minute == 0  # 09:00 EST = 14:00 UTC
    assert end_est.hour == 21 and end_est.minute == 0  # 16:00 EST = 21:00 UTC


def test_generate_candidate_trading_days():
    """Verify candidate trading days going backward are strictly weekdays (Mon-Fri)."""
    start = date(2026, 9, 25)
    days = generate_candidate_trading_days(start, count=15)
    assert len(days) == 15

    for d in days:
        assert d.weekday() < 5, f"Date {d} is a weekend day (weekday {d.weekday()})"

    # Verify strictly descending order
    for i in range(len(days) - 1):
        assert days[i] > days[i + 1]


def test_duckdb_ticks_insertion_in_memory():
    """Verify that normalized ticks DataFrame is inserted accurately into DuckDB ticks table."""
    mem_client = DuckDBClient(":memory:", read_only=False)
    mem_client.execute("""
        CREATE TABLE ticks (
            timestamp TIMESTAMP,
            symbol VARCHAR,
            price DOUBLE,
            volume DOUBLE,
            bid DOUBLE,
            ask DOUBLE,
            source VARCHAR,
            session VARCHAR
        )
    """)

    mock_df = pd.DataFrame({
        "timestamp": ["2026-09-24 13:15:00.123456", "2026-09-24 14:00:00.654321"],
        "symbol": ["NVDA", "AAPL"],
        "price": [125.50, 220.10],
        "volume": [10.0, 50.0],
        "bid": [125.48, 220.08],
        "ask": [125.52, 220.12],
        "source": ["DATABENTO", "DATABENTO"],
        "session": ["PRE", "REG"]
    })

    inserted = insert_ticks_to_streaming_db(mock_df, streaming_client=mem_client)
    assert inserted == 2

    rows = mem_client.execute("SELECT symbol, price, source, session FROM ticks ORDER BY timestamp").fetchall()
    assert len(rows) == 2
    assert rows[0] == ("NVDA", 125.50, "DATABENTO", "PRE")
    assert rows[1] == ("AAPL", 220.10, "DATABENTO", "REG")
    mem_client.close()


def test_budget_cap_stops_execution():
    """Verify that the backfill loop respects max_budget and stops immediately when cap is reached."""
    mock_client = MagicMock()
    # Mock cost estimate: $10 per day
    with patch("src.data.databento_backfill.estimate_day_cost", return_value=10.0):
        with patch("src.data.databento_backfill.is_day_already_backfilled", return_value=False):
            with patch("src.data.databento_backfill.fetch_and_normalize_day") as mock_fetch:
                mock_fetch.return_value = pd.DataFrame({
                    "timestamp": ["2026-09-24 13:30:00.000000"],
                    "symbol": ["NVDA"],
                    "price": [120.0],
                    "volume": [1.0],
                    "bid": [119.9],
                    "ask": [120.1],
                    "source": ["DATABENTO"],
                    "session": ["REG"]
                })
                with patch("src.data.databento_backfill.insert_ticks_to_streaming_db", return_value=1):
                    # Budget is $25.0 -> can only afford 2 days ($20 total). 3rd day ($30) exceeds $25.
                    res = run_databento_backfill(
                        max_budget=25.0,
                        max_days=10,
                        start_date=date(2026, 9, 24),
                        client=mock_client
                    )

                    assert res["success"] is True
                    assert res["completed_days_count"] == 2
                    assert res["total_cost_usd"] == 20.0
                    assert res["remaining_budget_usd"] == 5.0
                    assert mock_fetch.call_count == 2
