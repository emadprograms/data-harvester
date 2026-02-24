"""
Tests for src/data/harvester.py — Verify Primary -> Fallback logic.
Uses mocking to avoid real API calls.
"""
import pytest
from unittest.mock import patch, MagicMock
import pandas as pd
from datetime import date, datetime
from src.data.harvester import fetch_from_source, run_harvest_logic
from src.config import US_EASTERN, UTC


class MockLogger:
    def __init__(self):
        self.messages = []
    def log(self, msg):
        self.messages.append(msg)


class TestFetchFromSource:

    def test_unknown_source_returns_empty(self, safe_test_range):
        """Unknown source name must return empty DF."""
        start_dt, end_dt = safe_test_range
        logger = MockLogger()
        df, msg = fetch_from_source("UNKNOWN_API", "AAPL", start_dt, end_dt, logger)
        assert df.empty
        assert "Unknown" in msg

    def test_none_source_returns_empty(self, safe_test_range):
        """NONE source must return empty DF without error."""
        start_dt, end_dt = safe_test_range
        logger = MockLogger()
        df, msg = fetch_from_source("NONE", "AAPL", start_dt, end_dt, logger)
        assert df.empty
        assert msg == "No Source"

    @patch("src.data.harvester.fetch_yahoo_market_data")
    def test_yahoo_source_success(self, mock_yahoo, safe_test_range, safe_test_date_str):
        """YAHOO source must call fetch_yahoo_market_data and normalize."""
        start_dt, end_dt = safe_test_range
        idx = pd.DatetimeIndex(
            pd.date_range(f"{safe_test_date_str} 09:30", periods=2, freq="1min", tz="US/Eastern"),
            name="Datetime"
        )
        mock_yahoo.return_value = pd.DataFrame({
            "Open": [150.0, 151.0], "High": [151.0, 151.5],
            "Low": [149.0, 149.5], "Close": [150.5, 151.0],
            "Volume": [1000, 2000]
        }, index=idx)
        
        logger = MockLogger()
        df, msg = fetch_from_source("YAHOO", "AAPL", start_dt, end_dt, logger)
        assert not df.empty
        assert "Yahoo" in msg

    @patch("src.data.harvester.fetch_massive_data")
    def test_massive_source_success(self, mock_massive, safe_test_range, safe_test_date_str):
        """MASSIVE source must call fetch_massive_data and return data."""
        start_dt, end_dt = safe_test_range
        mock_massive.return_value = pd.DataFrame({
            "timestamp": pd.to_datetime([f"{safe_test_date_str} 15:00:00"]).tz_localize("UTC"),
            "open": [150.0], "high": [151.0], "low": [149.0],
            "close": [150.5], "volume": [1000.0], "symbol": ["AAPL"]
        })

        logger = MockLogger()
        df, msg = fetch_from_source("MASSIVE", "AAPL", start_dt, end_dt, logger)
        assert not df.empty
        assert "Massive" in msg

    @patch("src.data.harvester.fetch_binance_range")
    def test_binance_source_success(self, mock_binance, safe_test_range, safe_test_date_str):
        """BINANCE source must call fetch_binance_range."""
        start_dt, end_dt = safe_test_range
        mock_binance.return_value = pd.DataFrame({
            "timestamp": pd.to_datetime([f"{safe_test_date_str} 15:00:00"]).tz_localize("UTC"),
            "symbol": ["BTCUSDT"], "open": [40000.0], "high": [41000.0],
            "low": [39000.0], "close": [40500.0], "volume": [100.0],
            "session": ["REG"]
        })
        
        logger = MockLogger()
        df, msg = fetch_from_source("BINANCE", "BTCUSDT", start_dt, end_dt, logger)
        assert not df.empty
        assert "Binance" in msg


class TestHarvestPipeline:

    def _make_inventory(self):
        return {
            "AAPL": {"yahoo_ticker": None, "massive_ticker": "AAPL", "binance_ticker": None, "capital_ticker": "AAPL"},
            "BTCUSDT": {"yahoo_ticker": "BTC-USD", "massive_ticker": None, "binance_ticker": "BTCUSDT", "capital_ticker": None},
            "GC=F": {"yahoo_ticker": "GC=F", "massive_ticker": None, "binance_ticker": "PAXGUSDT", "capital_ticker": None}
        }

    @patch("src.data.harvester.fetch_from_source")
    def test_primary_fallback_logic(self, mock_fetch, safe_test_range, safe_test_date_str):
        """If primary fails, it must try fallback."""
        start_dt, end_dt = safe_test_range
        
        def side_effect(source, ticker, s_dt, e_dt, logger, massive_provider=None, audit_trail=None):
            if source == "MASSIVE":
                return pd.DataFrame(), "❌ Empty"
            if source == "CAPITAL":
                # Must provide valid columns for post-processing
                return pd.DataFrame({
                    "timestamp": pd.to_datetime([f"{safe_test_date_str} 15:00:00"]).tz_localize("UTC"),
                    "symbol": [ticker],
                    "close": [150.0],
                    "session": ["REG"]
                }), "✅ Capital"
            return pd.DataFrame(), "Error"
            
        mock_fetch.side_effect = side_effect
        logger = MockLogger()
        
        final_df, report = run_harvest_logic(["AAPL"], start_dt, end_dt, self._make_inventory(), logger, massive_provider=MagicMock())
        
        assert not final_df.empty
        # Should be labeled as FB-CAPITAL
        assert any("FB-CAPITAL" in str(row) for _, row in report.iterrows())

    @patch("src.data.harvester.fetch_from_source")
    def test_gold_priority(self, mock_fetch, safe_test_range):
        """GC=F should try Binance (PAXGUSDT) first."""
        start_dt, end_dt = safe_test_range
        inventory = self._make_inventory()
        logger = MockLogger()
        
        # We just want to see if Binance was the first call for GC=F
        run_harvest_logic(["GC=F"], start_dt, end_dt, inventory, logger, massive_provider=MagicMock())
        
        # First call for this ticker should be BINANCE
        first_call = mock_fetch.call_args_list[0]
        assert first_call.args[0] == "BINANCE"
        assert first_call.args[1] == "PAXGUSDT"
