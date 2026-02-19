"""
Tests for src/data/harvester.py — Verify fallback pipeline, session slicing, edge cases.
Uses mocking to avoid real API calls.
"""
import unittest
from unittest.mock import patch, MagicMock
import pandas as pd
from datetime import date, time as dt_time
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

    @patch("src.data.harvester.fetch_massive_data")
    def test_massive_source_success(self, mock_massive):
        """MASSIVE source must call fetch_massive_data and normalize."""
        mock_df = pd.DataFrame({
            "SnapshotTime": pd.to_datetime(["2025-01-15 14:30:00"]).tz_localize("UTC"),
            "Open": [150.0], "High": [151.0], "Low": [149.0], 
            "Close": [150.5], "Volume": [1000]
        })
        mock_massive.return_value = (mock_df, "")
        
        logger = MockLogger()
        df, msg = fetch_from_source("MASSIVE", "AAPL", date(2025, 1, 15), logger)
        self.assertFalse(df.empty)
        self.assertIn("Massive", msg)

    @patch("src.data.harvester.fetch_massive_data")
    def test_massive_source_failure(self, mock_massive):
        """MASSIVE source failure must return empty DF with error message."""
        mock_massive.return_value = (pd.DataFrame(), "Rate Limit (429)")
        
        logger = MockLogger()
        df, msg = fetch_from_source("MASSIVE", "AAPL", date(2025, 1, 15), logger)
        self.assertTrue(df.empty)
        self.assertIn("429", msg)

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

    @patch("src.data.harvester.fetch_yahoo_market_data")
    def test_yahoo_source_empty(self, mock_yahoo):
        """YAHOO returning empty must return proper error message."""
        mock_yahoo.return_value = pd.DataFrame()
        
        logger = MockLogger()
        df, msg = fetch_from_source("YAHOO", "AAPL", date(2025, 1, 15), logger)
        self.assertTrue(df.empty)
        self.assertIn("Yahoo Empty", msg)

    @patch("src.data.harvester.fetch_binance_daily")
    def test_binance_source_success(self, mock_binance):
        """BINANCE source must call fetch_binance_daily."""
        from src.config import SCHEMA_COLS
        mock_binance.return_value = pd.DataFrame({
            "timestamp": pd.to_datetime(["2025-01-15 00:00:00"]).tz_localize("UTC"),
            "symbol": ["BTCUSDT"], "open": [40000.0], "high": [41000.0],
            "low": [39000.0], "close": [40500.0], "volume": [100.0],
            "session": ["REG"]
        })
        
        logger = MockLogger()
        df, msg = fetch_from_source("BINANCE", "BTCUSDT", date(2025, 1, 15), logger)
        self.assertFalse(df.empty)
        self.assertIn("Binance", msg)

    @patch("src.data.harvester.fetch_massive_data")
    def test_source_exception_caught(self, mock_massive):
        """Any exception during fetch must be caught and logged."""
        mock_massive.side_effect = Exception("Network timeout")
        
        logger = MockLogger()
        df, msg = fetch_from_source("MASSIVE", "AAPL", date(2025, 1, 15), logger)
        self.assertTrue(df.empty)
        self.assertIn("Error", msg)
        self.assertTrue(any("Error" in m for m in logger.messages))


class TestHarvestPipeline(unittest.TestCase):

    def _make_mock_map(self):
        return {
            "AAPL": {
                "yahoo_ticker": "AAPL", "massive_ticker": "AAPL", "binance_ticker": None,
                "p1": "MASSIVE", "p2": "YAHOO", "p3": None
            },
            "BTC/USD": {
                "yahoo_ticker": "BTC-USD", "massive_ticker": "X:BTCUSD", "binance_ticker": "BTCUSDT",
                "p1": "BINANCE", "p2": "MASSIVE", "p3": "YAHOO"
            }
        }

    @patch("src.data.harvester.fetch_from_source")
    def test_fallback_works(self, mock_fetch):
        """When P1 fails, harvester must fall back to P2."""
        def side_effect(source, ticker, target_date, logger):
            if source == "MASSIVE":
                return pd.DataFrame(), "❌ Error"
            elif source == "YAHOO":
                ts = pd.to_datetime(["2025-01-15 14:30:00"]).tz_localize("UTC")
                return pd.DataFrame({
                    "timestamp": ts, "symbol": ["AAPL"],
                    "open": [150.0], "high": [151.0], "low": [149.0],
                    "close": [150.5], "volume": [1000], "session": ["REG"]
                }), "✅ Yahoo"
            return pd.DataFrame(), "Unknown"
        
        mock_fetch.side_effect = side_effect
        logger = MockLogger()
        db_map = {"AAPL": {
            "yahoo_ticker": "AAPL", "massive_ticker": "AAPL", "binance_ticker": None,
            "p1": "MASSIVE", "p2": "YAHOO", "p3": None
        }}
        
        final_df, report = run_harvest_logic(["AAPL"], date(2025, 1, 15), db_map, logger)
        self.assertFalse(final_df.empty)
        # Report should show YAHOO was used
        self.assertTrue(any("YAHOO" in str(row) for _, row in report.iterrows()))

    @patch("src.data.harvester.fetch_from_source")
    def test_all_sources_fail(self, mock_fetch):
        """When ALL sources fail, ticker must appear as FAILED in report."""
        mock_fetch.return_value = (pd.DataFrame(), "❌ Error")
        
        logger = MockLogger()
        db_map = {"AAPL": {
            "yahoo_ticker": "AAPL", "massive_ticker": "AAPL", "binance_ticker": None,
            "p1": "MASSIVE", "p2": "YAHOO", "p3": None
        }}
        
        final_df, report = run_harvest_logic(["AAPL"], date(2025, 1, 15), db_map, logger)
        self.assertTrue(final_df.empty)
        self.assertTrue(any("FAILED" in str(row) for _, row in report.iterrows()))

    @patch("src.data.harvester.fetch_from_source")
    def test_ticker_not_in_inventory(self, mock_fetch):
        """Ticker not in db_map must be skipped with warning."""
        logger = MockLogger()
        final_df, report = run_harvest_logic(["UNKNOWN"], date(2025, 1, 15), {}, logger)
        self.assertTrue(final_df.empty)

    @patch("src.data.harvester.fetch_from_source")
    def test_yahoo_always_appended_as_fallback(self, mock_fetch):
        """If YAHOO is not in the pipeline, it must be auto-appended."""
        call_order = []
        def side_effect(source, ticker, target_date, logger):
            call_order.append(source)
            return pd.DataFrame(), "❌ Error"
        
        mock_fetch.side_effect = side_effect
        logger = MockLogger()
        db_map = {"AAPL": {
            "yahoo_ticker": "AAPL", "massive_ticker": "AAPL", "binance_ticker": None,
            "p1": "MASSIVE", "p2": None, "p3": None
        }}
        
        run_harvest_logic(["AAPL"], date(2025, 1, 15), db_map, logger)
        self.assertIn("YAHOO", call_order, 
                       "YAHOO must be auto-appended to the fallback pipeline")

    @patch("src.data.harvester.fetch_from_source")
    def test_session_slicing(self, mock_fetch):
        """Harvester must correctly slice data into PRE/REG/POST sessions."""
        import pytz
        et = pytz.timezone("US/Eastern")
        timestamps = pd.to_datetime([
            "2025-01-15 08:00:00",  # 03:00 ET → PRE
            "2025-01-15 15:00:00",  # 10:00 ET → REG
            "2025-01-15 21:30:00",  # 16:30 ET → POST
        ]).tz_localize("UTC")
        
        mock_df = pd.DataFrame({
            "timestamp": timestamps,
            "symbol": ["AAPL"] * 3,
            "open": [150.0] * 3, "high": [151.0] * 3,
            "low": [149.0] * 3, "close": [150.5] * 3,
            "volume": [1000] * 3, "session": ["REG"] * 3
        })
        mock_fetch.return_value = (mock_df, "✅ Massive")
        
        logger = MockLogger()
        db_map = {"AAPL": {
            "yahoo_ticker": "AAPL", "massive_ticker": "AAPL", "binance_ticker": None,
            "p1": "MASSIVE", "p2": None, "p3": None
        }}
        
        final_df, report = run_harvest_logic(["AAPL"], date(2025, 1, 15), db_map, logger)
        sessions = final_df['session'].unique()
        self.assertIn("PRE", sessions)
        self.assertIn("REG", sessions)
        self.assertIn("POST", sessions)

    @patch("src.data.harvester.fetch_from_source")
    def test_empty_harvest_returns_gracefully(self, mock_fetch):
        """Empty harvest must return two DataFrames without crashing."""
        mock_fetch.return_value = (pd.DataFrame(), "❌ Error")
        logger = MockLogger()
        
        final_df, report = run_harvest_logic([], date(2025, 1, 15), {}, logger)
        self.assertIsInstance(final_df, pd.DataFrame)
        self.assertIsInstance(report, pd.DataFrame)


if __name__ == '__main__':
    unittest.main()
