import unittest
from unittest.mock import MagicMock, patch
import pandas as pd
from datetime import datetime
import pytz

# Import the code to test
# We need to ensure src is in path or installed. 
# Assuming tests are run from root, 'src' is importable.
from src.data.harvester import run_harvest_logic
from src.config import SCHEMA_COLS

class TestHarvester(unittest.TestCase):
    def setUp(self):
        # Setup common objects
        self.logger = MagicMock()
        self.target_date = datetime(2025, 12, 10).date()
        self.db_map = {
            "BTCUSDT": {"epic": "BTCUSD", "strategy": "HYBRID"},
            "EURUSDT": {"epic": "EURUSD", "strategy": "HYBRID"},
            "AAPL": {"epic": "AAPL", "strategy": "HYBRID"}
        }

    @patch('src.data.harvester.create_capital_session')
    @patch('src.data.harvester.fetch_binance_daily')
    @patch('src.data.harvester.fetch_yahoo_market_data')
    @patch('src.data.harvester.fetch_capital_data_range')
    def test_lane1_binance_success(self, mock_cap, mock_yahoo, mock_bin, mock_sess):
        """Test standard Binance success for Lane 1"""
        # Setup Mocks
        mock_sess.return_value = ("fake_cst", "fake_xst")
        
        # Binance returns data
        df_bin = pd.DataFrame([{
            "timestamp": pd.Timestamp("2025-12-10 12:00:00", tz="UTC"),
            "open": 100, "high": 105, "low": 95, "close": 102, "volume": 1000,
            "symbol": "BTCUSDT", "session": "REG"
        }])
        mock_bin.return_value = df_bin
        
        # Run
        final_df, report = run_harvest_logic(["BTCUSDT"], self.target_date, self.db_map, self.logger)
        
        # Verify
        mock_bin.assert_called_with("BTCUSDT", self.target_date)
        mock_yahoo.assert_not_called() # Should not fallback
        self.assertFalse(final_df.empty)
        self.assertEqual(len(final_df), 1)
        self.assertEqual(report.iloc[0]['Status'], "✅ Complete")
        self.assertEqual(report.iloc[0]['Mode'], "Binance/Yahoo")

    @patch('src.data.harvester.create_capital_session')
    @patch('src.data.harvester.fetch_binance_daily')
    @patch('src.data.harvester.fetch_yahoo_market_data')
    def test_lane1_fallback_to_yahoo(self, mock_yahoo, mock_bin, mock_sess):
        """Test Binance failure falling back to Yahoo (Logic Update)"""
        mock_sess.return_value = ("fake_cst", "fake_xst")
        
        # Binance fails (returns empty)
        mock_bin.return_value = pd.DataFrame()
        
        # Yahoo returns data for fallback ticker
        # EURUSDT check -> Should map to EURUSD=X (Forex)
        df_yahoo = pd.DataFrame([{
            "timestamp": pd.Timestamp("2025-12-10 12:00:00", tz="UTC"),
            "open": 1.05, "high": 1.06, "low": 1.04, "close": 1.05, "volume": 0,
            "Datetime": pd.Timestamp("2025-12-10 12:00:00", tz="UTC") # normalized uses this maybe
        }])
        # Mock normalized columns if needed, but harvester calls normalize_yahoo_df
        # Let's mock normalize_yahoo_df? Or let it run?
        # Ideally unit tests should test logic.
        # normalize_yahoo_df is imported. If we don't mock it, we depend on it.
        # For simplicity, let's assume fetch_yahoo_market_data returns columns Yahoo typically sends
        df_yahoo_raw = pd.DataFrame([{
            "Datetime": pd.Timestamp("2025-12-10 12:00:00", tz="US/Eastern"),
            "Open": 1.05, "High": 1.06, "Low": 1.04, "Close": 1.05, "Volume": 1000
        }])
        df_yahoo_raw.set_index("Datetime", inplace=True)
        # normalize_yahoo_df handles timezone.
        
        mock_yahoo.return_value = df_yahoo_raw
        
        # Run
        final_df, report = run_harvest_logic(["EURUSDT"], self.target_date, self.db_map, self.logger)
        
        # Verify
        mock_bin.assert_called()
        # Expect call with mapped ticker
        mock_yahoo.assert_called_with("EURUSD=X", self.target_date, self.logger)
        
        self.assertFalse(final_df.empty)
        self.assertTrue("Binance/Yahoo" in report.iloc[0]['Mode'])
        
    @patch('src.data.harvester.create_capital_session')
    @patch('src.data.harvester.fetch_binance_daily')
    @patch('src.data.harvester.fetch_yahoo_market_data')
    def test_lane1_fallback_crypto(self, mock_yahoo, mock_bin, mock_sess):
        """Test fallback mapping for Crypto (BTCUSDT -> BTC-USD)"""
        mock_sess.return_value = ("fake_cst", "fake_xst")
        mock_bin.return_value = pd.DataFrame() # Fail
        
        mock_yahoo.return_value = pd.DataFrame() # Yahoo fail too for brevity, just checking call
        
        run_harvest_logic(["BTCUSDT"], self.target_date, self.db_map, self.logger)
        
        mock_yahoo.assert_called_with("BTC-USD", self.target_date, self.logger)


if __name__ == '__main__':
    unittest.main()
