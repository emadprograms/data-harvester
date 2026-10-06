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

import src.data.databento_backfill as backfill_module
from src.data.databento_backfill import (
    get_target_stock_symbols,
    get_day_trading_bounds,
    generate_candidate_trading_days,
    run_databento_backfill,
    EXCLUDED_SYMBOLS,
    NY_TZ
)
from src.storage.config import init_tick_lake


def test_databento_sdk_is_only_required_for_live_client(monkeypatch):
    """Offline backfill helpers remain importable without the optional SDK."""
    monkeypatch.setattr(backfill_module, "db", None)
    with pytest.raises(RuntimeError, match="Databento SDK is required for live backfill"):
        backfill_module.get_databento_client(api_key="test-key")


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


def test_f6_post_market_tbbo_is_labeled_post():
    """F6: a 17:00 ET quote is POST, not REG."""
    from src.data.databento_backfill import normalize_tbbo_frame

    raw = pd.DataFrame(
        {
            "ts_event": pd.to_datetime(
                [
                    "2026-10-02 08:30:00+00:00",  # 04:30 ET
                    "2026-10-02 14:30:00+00:00",  # 10:30 ET
                    "2026-10-02 21:00:00+00:00",  # 17:00 ET
                ]
            ),
            "symbol": ["NVDA", "NVDA", "NVDA"],
            "bid_px_00": [100.00, 100.10, 100.20],
            "ask_px_00": [100.04, 100.14, 100.24],
        }
    )
    frame = normalize_tbbo_frame(raw, date(2026, 10, 2))
    assert list(frame["session"]) == ["PRE", "REG", "POST"]


def test_budget_cap_stops_execution(tmp_path):
    """Verify that the backfill loop respects max_budget and stops immediately when cap is reached."""
    from src.data.gap_fill import GapFillBudgetExceeded

    lake = tmp_path / "lake"
    init_tick_lake(lake)
    mock_client = MagicMock()

    def fake_fill(trading_date, client, lake_root, symbols, remaining_budget=None, **kwargs):
        if remaining_budget is not None and remaining_budget < 10.0:
            raise GapFillBudgetExceeded("interval estimate exceeds remaining budget")
        return {
            "requests": 1,
            "follow_up_requests": 0,
            "rows": 1,
            "stretches": [],
            "estimated_cost": 10.0,
        }

    with patch("src.data.databento_backfill.get_target_stock_symbols", return_value=["NVDA"]):
        with patch("src.data.gap_fill.fill_named_day", side_effect=fake_fill) as mock_fetch:
            # Budget is $25.0 -> can only afford 2 days ($20 total). 3rd day exceeds $25.
            res = run_databento_backfill(
                max_budget=25.0,
                max_days=10,
                start_date=date(2026, 9, 24),
                client=mock_client,
                lake_root=lake,
            )

    assert res["success"] is True
    assert res["completed_days_count"] == 2
    assert res["total_cost_usd"] == 20.0
    assert res["remaining_budget_usd"] == 5.0
    assert mock_fetch.call_count == 3
