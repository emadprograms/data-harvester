"""
Tests for src/data/harvester.py — Verify Primary -> Fallback logic.
Uses mocking to avoid real API calls.
"""
import unittest
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


class TestFetchFromSource(unittest.TestCase):

    def test_unknown_source_returns_empty(self):
        """Unknown source name must return empty DF."""
        logger = MockLogger()
        df, msg = fetch_from_source("UNKNOWN_API", "AAPL", date(2025, 1, 15), logger)
        self.assertTrue(df.empty)
        self.assertIn("Unknown", msg)

    def test_none_source_returns_empty(self):
        """NONE source must return empty DF without error."""
        logger = MockLogger()
        df, msg = fetch_from_source("NONE", "AAPL", date(2025, 1, 15), logger)
        self.assertTrue(df.empty)
        self.assertEqual(msg, "No Source")

    @patch("src.data.harvester.fetch_yahoo_market_data")
    def test_yahoo_source_success(self, mock_yahoo):
        """YAHOO source must call fetch_yahoo_market_data and normalize."""
        idx = pd.DatetimeIndex(
            pd.date_range("2025-01-15 09:30", periods=2, freq="1min", tz="US/Eastern"),
            name="Datetime"
        )
        mock_yahoo.return_value = pd.DataFrame({
            "Open": [150.0, 151.0], "High": [151.0, 151.5],
            "Low": [149.0, 149.5], "Close": [150.5, 151.0],
            "Volume": [1000, 2000]
        }, index=idx)
        
        logger = MockLogger()
        df, msg = fetch_from_source("YAHOO", "AAPL", date(2025, 1, 15), logger)
        self.assertFalse(df.empty)
        self.assertIn("Yahoo", msg)

    @patch("src.data.harvester.fetch_massive_data")
    def test_massive_source_success(self, mock_massive):
        """MASSIVE source must call fetch_massive_data and return data."""
        mock_massive.return_value = pd.DataFrame({
            "timestamp": pd.to_datetime(["2025-01-15 15:00:00"]).tz_localize("UTC"),
            "open": [150.0], "high": [151.0], "low": [149.0],
            "close": [150.5], "volume": [1000.0], "symbol": ["AAPL"]
        })

        logger = MockLogger()
        df, msg = fetch_from_source("MASSIVE", "AAPL", date(2025, 1, 15), logger)
        self.assertFalse(df.empty)
        self.assertIn("Massive", msg)

    @patch("src.data.harvester.fetch_binance_daily")
    def test_binance_source_success(self, mock_binance):
        """BINANCE source must call fetch_binance_daily."""
        mock_binance.return_value = pd.DataFrame({
            "timestamp": pd.to_datetime(["2025-01-15 15:00:00"]).tz_localize("UTC"),
            "symbol": ["BTCUSDT"], "open": [40000.0], "high": [41000.0],
            "low": [39000.0], "close": [40500.0], "volume": [100.0],
            "session": ["REG"]
        })
        
        logger = MockLogger()
        df, msg = fetch_from_source("BINANCE", "BTCUSDT", date(2025, 1, 15), logger)
        self.assertFalse(df.empty)
        self.assertIn("Binance", msg)


class TestHarvestPipeline(unittest.TestCase):

    def _make_inventory(self):
        return {
            "AAPL": {"yahoo_ticker": "AAPL", "massive_ticker": "AAPL", "binance_ticker": None},
            "BTCUSDT": {"yahoo_ticker": "BTC-USD", "massive_ticker": None, "binance_ticker": "BTCUSDT"},
            "GC=F": {"yahoo_ticker": "GC=F", "massive_ticker": None, "binance_ticker": "PAXGUSDT"}
        }

    @patch("src.data.harvester.fetch_from_source")
    def test_primary_fallback_logic(self, mock_fetch):
        """If primary fails, it must try fallback."""
        
        def side_effect(source, ticker, target_date, logger):
            if source == "MASSIVE":
                return pd.DataFrame(), "❌ Empty"
            if source == "YAHOO":
                # Must provide valid columns for post-processing
                return pd.DataFrame({
                    "timestamp": pd.to_datetime(["2025-01-15 15:00:00"]).tz_localize("UTC"),
                    "symbol": [ticker],
                    "close": [150.0],
                    "session": ["REG"]
                }), "✅ Yahoo"
            return pd.DataFrame(), "Error"
            
        mock_fetch.side_effect = side_effect
        logger = MockLogger()
        
        final_df, report = run_harvest_logic(["AAPL"], date(2025, 1, 15), self._make_inventory(), logger)
        
        self.assertFalse(final_df.empty)
        # Should be labeled as FB-YAHOO
        self.assertTrue(any("FB-YAHOO" in str(row) for _, row in report.iterrows()))

    @patch("src.data.harvester.fetch_from_source")
    def test_gold_priority(self, mock_fetch):
        """GC=F should try Binance (PAXGUSDT) first."""
        
        inventory = self._make_inventory()
        logger = MockLogger()
        
        # We just want to see if Binance was the first call for GC=F
        run_harvest_logic(["GC=F"], date(2025, 1, 15), inventory, logger)
        
        # First call for this ticker should be BINANCE
        first_call = mock_fetch.call_args_list[0]
        self.assertEqual(first_call.args[0], "BINANCE")
        self.assertEqual(first_call.args[1], "PAXGUSDT")


if __name__ == '__main__':
    unittest.main()
