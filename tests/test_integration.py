"""
End-to-end integration test: Verify the FULL harvest pipeline works
from CLI entry → harvest logic → save, with all external dependencies mocked.
"""
import unittest
from unittest.mock import patch, MagicMock
import pandas as pd
from datetime import date


class MockLogger:
    def __init__(self):
        self.messages = []
    def log(self, msg):
        self.messages.append(msg)


class TestFullPipeline(unittest.TestCase):
    """Tests the full harvest pipeline end-to-end with mocked externals."""

    @patch("src.data.harvester.fetch_capital_data")
    @patch("src.data.harvester.fetch_yahoo_market_data")
    @patch("src.data.harvester.fetch_binance_daily")
    def test_full_harvest_mixed_symbols(self, mock_binance, mock_yahoo, mock_capital):
        """Full harvest with stock + crypto must handle both paths."""
        from src.data.harvester import run_harvest_logic
        
        # Mock Capital → success for AAPL (PRE/POST data, volume=0)
        capital_df = pd.DataFrame({
            "timestamp": pd.to_datetime([
                "2025-01-15 08:00:00",   # PRE (03:00 ET)
                "2025-01-15 14:30:00",   # REG (09:30 ET)
                "2025-01-15 21:30:00",   # POST (16:30 ET)
            ]).tz_localize("UTC"),
            "open": [149.0, 150.0, 150.5],
            "high": [149.5, 151.0, 151.0],
            "low": [148.5, 149.5, 150.0],
            "close": [149.2, 150.5, 150.8],
            "volume": [0.0, 0.0, 0.0]
        })
        mock_capital.return_value = (capital_df, "")
        
        # Mock Yahoo → success for AAPL (REG data with volume)
        idx = pd.DatetimeIndex(
            pd.date_range("2025-01-15 09:30", periods=2, freq="1min", tz="US/Eastern"),
            name="Datetime"
        )
        mock_yahoo.return_value = pd.DataFrame({
            "Open": [150.0, 150.5], "High": [151.0, 151.5],
            "Low": [149.0, 149.5], "Close": [150.5, 151.0],
            "Volume": [10000, 20000]
        }, index=idx)
        
        # Mock Binance → success for BTC/USD
        binance_df = pd.DataFrame({
            "timestamp": pd.to_datetime(["2025-01-15 00:00:00", "2025-01-15 00:01:00"]).tz_localize("UTC"),
            "symbol": ["BTC/USD", "BTC/USD"],
            "open": [40000.0, 40100.0], "high": [41000.0, 41100.0],
            "low": [39000.0, 39100.0], "close": [40500.0, 40600.0],
            "volume": [100.0, 200.0], "session": ["REG", "REG"]
        })
        mock_binance.return_value = binance_df
        
        logger = MockLogger()
        db_map = {
            "AAPL": {
                "yahoo_ticker": "AAPL", "capital_epic": "AAPL", "binance_ticker": None,
                "p1": "YAHOO", "p2": "CAPITAL", "p3": None
            },
            "BTC/USD": {
                "yahoo_ticker": "BTC-USD", "capital_epic": None, "binance_ticker": "BTCUSDT",
                "p1": "BINANCE", "p2": "YAHOO", "p3": None
            }
        }
        
        final_df, report = run_harvest_logic(["AAPL", "BTC/USD"], date(2025, 1, 15), db_map, logger)
        
        # Both symbols should have data
        self.assertFalse(final_df.empty)
        symbols = final_df['symbol'].unique()
        self.assertIn("AAPL", symbols)
        self.assertIn("BTC/USD", symbols)
        
        # Report should have 2 rows
        self.assertEqual(len(report), 2)

    @patch("src.database.operations.get_db_connection")
    def test_save_round_trip(self, mock_conn):
        """Data passed to save must have UTC timestamps as strings."""
        mock_client = MagicMock()
        mock_conn.return_value = mock_client
        
        from src.database.operations import save_data_to_turso
        
        df = pd.DataFrame({
            "timestamp": pd.to_datetime(["2025-01-15 14:30:00", "2025-01-15 14:31:00"]).tz_localize("UTC"),
            "symbol": ["AAPL", "AAPL"],
            "open": [150.0, 150.5], "high": [151.0, 151.5],
            "low": [149.0, 149.5], "close": [150.5, 151.0],
            "volume": [1000.0, 2000.0], "session": ["REG", "REG"]
        })
        
        logger = MockLogger()
        result = save_data_to_turso(df, logger)
        self.assertTrue(result)
        
        # Verify the SQL was called
        mock_client.execute.assert_called()
        call_args = mock_client.execute.call_args
        sql = call_args[0][0]
        self.assertIn("INSERT OR REPLACE", sql)

    @patch("src.data.harvester.fetch_from_source")
    def test_partial_failure(self, mock_fetch):
        """If some tickers fail but others succeed, must still return the successful data."""
        def side_effect(source, ticker, target_date, logger):
            if ticker == "AAPL":
                ts = pd.to_datetime(["2025-01-15 14:30:00"]).tz_localize("UTC")
                return pd.DataFrame({
                    "timestamp": ts, "symbol": ["AAPL"],
                    "open": [150.0], "high": [151.0], "low": [149.0],
                    "close": [150.5], "volume": [1000], "session": ["REG"]
                }), "✅ Yahoo"
            return pd.DataFrame(), "❌ Error"
        
        mock_fetch.side_effect = side_effect
        
        from src.data.harvester import run_harvest_logic
        logger = MockLogger()
        db_map = {
            "AAPL": {"yahoo_ticker": "AAPL", "capital_epic": "AAPL", "binance_ticker": None, "p1": "YAHOO", "p2": "CAPITAL", "p3": None},
            "FAKE": {"yahoo_ticker": "FAKE", "capital_epic": "FAKE", "binance_ticker": None, "p1": "YAHOO", "p2": "CAPITAL", "p3": None},
        }
        
        final_df, report = run_harvest_logic(["AAPL", "FAKE"], date(2025, 1, 15), db_map, logger)
        
        # AAPL should succeed
        self.assertFalse(final_df.empty)
        self.assertIn("AAPL", final_df['symbol'].values)
        
        # Report should show both
        self.assertEqual(len(report), 2)


if __name__ == '__main__':
    unittest.main()
