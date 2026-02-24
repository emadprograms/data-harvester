"""
Tests for src/data/harvester.py — Verify spliced hybrid logic, fallback, session slicing, edge cases.
Uses mocking to avoid real API calls.
"""
import unittest
from unittest.mock import patch, MagicMock
import pandas as pd
from datetime import date, datetime, time as dt_time
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

    def test_empty_source_returns_empty(self):
        """Empty string source must return empty DF without error."""
        logger = MockLogger()
        df, msg = fetch_from_source("", "AAPL", date(2025, 1, 15), logger)
        self.assertTrue(df.empty)

    @patch("src.data.harvester.fetch_yahoo_market_data")
    def test_yahoo_source_success(self, mock_yahoo):
        """YAHOO source must call fetch_yahoo_market_data and normalize."""
        idx = pd.DatetimeIndex(
            pd.date_range("2025-01-15 09:30", periods=2, freq="1min", tz="US/Eastern"),
            name="Datetime"
        )
        mock_yahoo.return_value = pd.DataFrame({
            "Open": [150.0, 150.5], "High": [151.0, 151.5],
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

    def _make_stock_map(self):
        """Standard stock map using the Massive+Yahoo model."""
        return {
            "AAPL": {
                "yahoo_ticker": "AAPL", "massive_ticker": "AAPL", "binance_ticker": None,
                "p1": "MASSIVE", "p2": "YAHOO", "p3": None
            }
        }

    def _make_crypto_map(self):
        """Crypto map using Binance."""
        return {
            "BTC/USD": {
                "yahoo_ticker": "BTC-USD", "massive_ticker": None, "binance_ticker": "BTCUSDT",
                "p1": "BINANCE", "p2": "YAHOO", "p3": None
            }
        }

    @patch("src.data.harvester.fetch_from_source")
    def test_spliced_hybrid_both_sources(self, mock_fetch):
        """When both Massive and Yahoo return data, harvester must splice PRE/POST from Massive and REG from Yahoo."""
        import pytz
        et = pytz.timezone("US/Eastern")
        
        # Massive returns full day data
        m_ts = pd.to_datetime([
            "2025-01-15 08:00:00",  # 03:00 ET → PRE
            "2025-01-15 15:00:00",  # 10:00 ET → REG
            "2025-01-15 21:30:00",  # 16:30 ET → POST
        ]).tz_localize("UTC")
        m_df = pd.DataFrame({
            "timestamp": m_ts, "symbol": ["AAPL"] * 3,
            "open": [149.0, 150.0, 150.5], "high": [149.5, 151.0, 151.0],
            "low": [148.5, 149.5, 150.0], "close": [149.2, 150.5, 150.8],
            "volume": [0.0, 0.0, 0.0], "session": ["REG"] * 3
        })
        
        # Yahoo returns full day data
        yho_ts = pd.to_datetime([
            "2025-01-15 08:00:00",  # 03:00 ET → PRE
            "2025-01-15 15:00:00",  # 10:00 ET → REG
            "2025-01-15 21:30:00",  # 16:30 ET → POST
        ]).tz_localize("UTC")
        yho_df = pd.DataFrame({
            "timestamp": yho_ts, "symbol": ["AAPL"] * 3,
            "open": [149.0, 150.0, 150.5], "high": [149.5, 151.0, 151.0],
            "low": [148.5, 149.5, 150.0], "close": [149.2, 150.5, 150.8],
            "volume": [500.0, 10000.0, 300.0], "session": ["REG"] * 3
        })
        
        def side_effect(source, ticker, target_date, logger):
            if source == "MASSIVE":
                return m_df, "✅ Massive"
            elif source == "YAHOO":
                return yho_df, "✅ Yahoo"
            return pd.DataFrame(), "❌ Error"
        
        mock_fetch.side_effect = side_effect
        logger = MockLogger()
        
        # We need p1=YAHOO p2=MASSIVE for the hybrid mode in the code
        db_map = {
            "AAPL": {
                "yahoo_ticker": "AAPL", "massive_ticker": "AAPL",
                "p1": "YAHOO", "p2": "MASSIVE"
            }
        }
        
        final_df, report = run_harvest_logic(
            ["AAPL"], date(2025, 1, 15), db_map, logger
        )
        
        self.assertFalse(final_df.empty)
        sessions = final_df['session'].unique()
        self.assertIn("PRE", sessions)
        self.assertIn("REG", sessions)
        self.assertIn("POST", sessions)

    @patch("src.data.harvester.fetch_from_source")
    def test_massive_only_fallback(self, mock_fetch):
        """When only Massive returns data, it must be used for ALL sessions."""
        m_ts = pd.to_datetime([
            "2025-01-15 15:00:00",
        ]).tz_localize("UTC")
        m_df = pd.DataFrame({
            "timestamp": m_ts, "symbol": ["AAPL"],
            "open": [150.0], "high": [151.0], "low": [149.0],
            "close": [150.5], "volume": [10000.0], "session": ["REG"]
        })
        
        def side_effect(source, ticker, target_date, logger):
            if source == "MASSIVE":
                return m_df, "✅ Massive"
            return pd.DataFrame(), "❌ Error"
        
        mock_fetch.side_effect = side_effect
        logger = MockLogger()
        
        final_df, report = run_harvest_logic(
            ["AAPL"], date(2025, 1, 15), self._make_stock_map(), logger
        )
        
        self.assertFalse(final_df.empty)
        self.assertTrue(any("MASSIVE" in str(row) for _, row in report.iterrows()))


if __name__ == '__main__':
    unittest.main()
