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

    @patch("src.data.harvester.datetime")
    @patch("src.data.harvester.fetch_capital_data")
    def test_capital_source_success(self, mock_capital, mock_dt):
        """CAPITAL source must call fetch_capital_data and return data."""
        from src.config import UTC
        mock_dt.now.return_value = datetime(2025, 1, 15, 22, 0, 0, tzinfo=UTC)
        mock_dt.combine = datetime.combine
        mock_dt.side_effect = lambda *a, **kw: datetime(*a, **kw)
        
        mock_df = pd.DataFrame({
            "timestamp": pd.to_datetime(["2025-01-15 15:00:00"]).tz_localize("UTC"),
            "open": [150.0], "high": [151.0], "low": [149.0],
            "close": [150.5], "volume": [0.0]
        })
        mock_capital.return_value = (mock_df, "")
        
        logger = MockLogger()
        df, msg = fetch_from_source("CAPITAL", "AAPL", date(2025, 1, 15), logger)
        self.assertFalse(df.empty)
        self.assertIn("Capital", msg)

    @patch("src.data.harvester.datetime")
    @patch("src.data.harvester.fetch_capital_data")
    def test_capital_source_failure(self, mock_capital, mock_dt):
        """CAPITAL source failure must return empty DF with error message."""
        from src.config import UTC
        mock_dt.now.return_value = datetime(2025, 1, 15, 22, 0, 0, tzinfo=UTC)
        mock_dt.combine = datetime.combine
        mock_dt.side_effect = lambda *a, **kw: datetime(*a, **kw)
        
        mock_capital.return_value = (pd.DataFrame(), "Session Failed")
        
        logger = MockLogger()
        df, msg = fetch_from_source("CAPITAL", "AAPL", date(2025, 1, 15), logger)
        self.assertTrue(df.empty)
        self.assertIn("Session Failed", msg)

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

    @patch("src.data.harvester.datetime")
    @patch("src.data.harvester.fetch_capital_data")
    def test_source_exception_caught(self, mock_capital, mock_dt):
        """Any exception during fetch must be caught and logged."""
        from src.config import UTC
        mock_dt.now.return_value = datetime(2025, 1, 15, 22, 0, 0, tzinfo=UTC)
        mock_dt.combine = datetime.combine
        mock_dt.side_effect = lambda *a, **kw: datetime(*a, **kw)
        
        mock_capital.side_effect = Exception("Network timeout")
        
        logger = MockLogger()
        df, msg = fetch_from_source("CAPITAL", "AAPL", date(2025, 1, 15), logger)
        self.assertTrue(df.empty)
        self.assertIn("Error", msg)
        self.assertTrue(any("Error" in m for m in logger.messages))


class TestHarvestPipeline(unittest.TestCase):

    def _make_stock_map(self):
        """Standard stock map using the new Capital+Yahoo hybrid model."""
        return {
            "AAPL": {
                "yahoo_ticker": "AAPL", "capital_epic": "AAPL", "binance_ticker": None,
                "p1": "YAHOO", "p2": "CAPITAL", "p3": None
            }
        }

    def _make_crypto_map(self):
        """Crypto map using Binance."""
        return {
            "BTC/USD": {
                "yahoo_ticker": "BTC-USD", "capital_epic": None, "binance_ticker": "BTCUSDT",
                "p1": "BINANCE", "p2": "YAHOO", "p3": None
            }
        }

    @patch("src.data.harvester.fetch_from_source")
    def test_spliced_hybrid_both_sources(self, mock_fetch):
        """When both Capital and Yahoo return data, harvester must splice PRE/POST from Capital and REG from Yahoo."""
        import pytz
        et = pytz.timezone("US/Eastern")
        
        # Capital returns full day data (with volume=0)
        cap_ts = pd.to_datetime([
            "2025-01-15 08:00:00",  # 03:00 ET → PRE
            "2025-01-15 15:00:00",  # 10:00 ET → REG
            "2025-01-15 21:30:00",  # 16:30 ET → POST
        ]).tz_localize("UTC")
        cap_df = pd.DataFrame({
            "timestamp": cap_ts, "symbol": ["AAPL"] * 3,
            "open": [149.0, 150.0, 150.5], "high": [149.5, 151.0, 151.0],
            "low": [148.5, 149.5, 150.0], "close": [149.2, 150.5, 150.8],
            "volume": [0.0, 0.0, 0.0], "session": ["REG"] * 3
        })
        
        # Yahoo returns full day data (with volume!)
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
            if source == "CAPITAL":
                return cap_df, "✅ Capital"
            elif source == "YAHOO":
                return yho_df, "✅ Yahoo"
            return pd.DataFrame(), "❌ Error"
        
        mock_fetch.side_effect = side_effect
        logger = MockLogger()
        
        final_df, report = run_harvest_logic(
            ["AAPL"], date(2025, 1, 15), self._make_stock_map(), logger
        )
        
        self.assertFalse(final_df.empty)
        sessions = final_df['session'].unique()
        self.assertIn("PRE", sessions)
        self.assertIn("REG", sessions)
        self.assertIn("POST", sessions)

    @patch("src.data.harvester.fetch_from_source")
    def test_capital_down_yahoo_only_fallback(self, mock_fetch):
        """When Capital fails, Yahoo data must be used for ALL sessions."""
        yho_ts = pd.to_datetime([
            "2025-01-15 15:00:00",  # 10:00 ET → REG
        ]).tz_localize("UTC")
        yho_df = pd.DataFrame({
            "timestamp": yho_ts, "symbol": ["AAPL"],
            "open": [150.0], "high": [151.0], "low": [149.0],
            "close": [150.5], "volume": [10000.0], "session": ["REG"]
        })
        
        def side_effect(source, ticker, target_date, logger):
            if source == "CAPITAL":
                return pd.DataFrame(), "❌ Capital Empty"
            elif source == "YAHOO":
                return yho_df, "✅ Yahoo"
            return pd.DataFrame(), "❌ Error"
        
        mock_fetch.side_effect = side_effect
        logger = MockLogger()
        
        final_df, report = run_harvest_logic(
            ["AAPL"], date(2025, 1, 15), self._make_stock_map(), logger
        )
        
        self.assertFalse(final_df.empty)
        # Should still work with YAHOO-ONLY label
        self.assertTrue(any("YAHOO-ONLY" in str(row) for _, row in report.iterrows()))

    @patch("src.data.harvester.fetch_from_source")
    def test_both_sources_fail(self, mock_fetch):
        """When both Capital and Yahoo fail, ticker must appear as FAILED."""
        mock_fetch.return_value = (pd.DataFrame(), "❌ Error")
        
        logger = MockLogger()
        final_df, report = run_harvest_logic(
            ["AAPL"], date(2025, 1, 15), self._make_stock_map(), logger
        )
        self.assertTrue(final_df.empty)
        self.assertTrue(any("FAILED" in str(row) for _, row in report.iterrows()))

    @patch("src.data.harvester.fetch_from_source")
    def test_binance_crypto_path(self, mock_fetch):
        """Crypto symbols with p1=BINANCE must use the Binance path directly."""
        binance_ts = pd.to_datetime([
            "2025-01-15 00:00:00",
            "2025-01-15 12:00:00",
        ]).tz_localize("UTC")
        binance_df = pd.DataFrame({
            "timestamp": binance_ts, "symbol": ["BTC/USD"] * 2,
            "open": [40000.0, 41000.0], "high": [41000.0, 42000.0],
            "low": [39000.0, 40000.0], "close": [40500.0, 41500.0],
            "volume": [100.0, 200.0], "session": ["REG"] * 2
        })
        
        def side_effect(source, ticker, target_date, logger):
            if source == "BINANCE":
                return binance_df, "✅ Binance"
            return pd.DataFrame(), "❌ Error"
        
        mock_fetch.side_effect = side_effect
        logger = MockLogger()
        
        final_df, report = run_harvest_logic(
            ["BTC/USD"], date(2025, 1, 15), self._make_crypto_map(), logger
        )
        self.assertFalse(final_df.empty)
        self.assertTrue(any("BINANCE" in str(row) for _, row in report.iterrows()))

    @patch("src.data.harvester.fetch_from_source")
    def test_ticker_not_in_inventory(self, mock_fetch):
        """Ticker not in db_map must be skipped with warning."""
        logger = MockLogger()
        final_df, report = run_harvest_logic(["UNKNOWN"], date(2025, 1, 15), {}, logger)
        self.assertTrue(final_df.empty)

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
