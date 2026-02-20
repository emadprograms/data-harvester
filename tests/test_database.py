"""
Tests for src/database/ — Verify DB operations handle failures gracefully.
Uses mocking to avoid needing a real Turso connection.
"""
import unittest
from unittest.mock import patch, MagicMock
import pandas as pd
from datetime import datetime


class MockLogger:
    def __init__(self):
        self.messages = []
    def log(self, msg):
        self.messages.append(msg)


class TestDatabaseOperations(unittest.TestCase):

    @patch("src.database.operations.get_db_connection")
    def test_get_symbol_map_no_connection(self, mock_conn):
        """No DB connection must return empty dict, not crash."""
        mock_conn.return_value = None
        from src.database.operations import get_symbol_map_from_db
        result = get_symbol_map_from_db()
        self.assertEqual(result, {})

    @patch("src.database.operations.get_db_connection")
    def test_get_symbol_map_success(self, mock_conn):
        """Successful query must return properly structured dict."""
        mock_client = MagicMock()
        mock_result = MagicMock()
        mock_result.rows = [
            ("AAPL", "AAPL", "AAPL", None, "MASSIVE", "YAHOO", None),
            ("BTC/USD", "BTC-USD", "X:BTCUSD", "BTCUSDT", "BINANCE", "MASSIVE", "YAHOO"),
        ]
        mock_client.execute.return_value = mock_result
        mock_conn.return_value = mock_client
        
        from src.database.operations import get_symbol_map_from_db
        result = get_symbol_map_from_db()
        
        self.assertIn("AAPL", result)
        self.assertIn("BTC/USD", result)
        self.assertEqual(result["AAPL"]["p1"], "MASSIVE")
        self.assertEqual(result["BTC/USD"]["binance_ticker"], "BTCUSDT")

    @patch("src.database.operations.get_db_connection")
    def test_save_data_empty_df(self, mock_conn):
        """Saving empty DF must return False without touching DB."""
        from src.database.operations import save_data_to_storage
        result = save_data_to_storage(pd.DataFrame())
        self.assertFalse(result)
        mock_conn.assert_not_called()

    @patch("src.database.operations.get_db_connection")
    def test_save_data_no_connection(self, mock_conn):
        """No DB connection must return False gracefully."""
        mock_conn.return_value = None
        from src.database.operations import save_data_to_storage
        
        df = pd.DataFrame({"timestamp": [datetime.now()], "symbol": ["AAPL"],
                           "open": [150], "high": [151], "low": [149],
                           "close": [150.5], "volume": [1000], "session": ["REG"]})
        result = save_data_to_storage(df)
        self.assertFalse(result)

    @patch("src.database.operations.get_db_connection")
    def test_save_data_success(self, mock_conn):
        """Successful save must return True."""
        mock_client = MagicMock()
        mock_conn.return_value = mock_client
        
        from src.database.operations import save_data_to_storage
        import pytz
        
        df = pd.DataFrame({
            "timestamp": pd.to_datetime(["2025-01-15 14:30:00"]).tz_localize("UTC"),
            "symbol": ["AAPL"], "open": [150.0], "high": [151.0],
            "low": [149.0], "close": [150.5], "volume": [1000.0],
            "session": ["REG"]
        })
        
        logger = MockLogger()
        result = save_data_to_storage(df, logger)
        self.assertTrue(result)
        mock_client.execute.assert_called()

    @patch("src.database.operations.get_db_connection")
    def test_save_data_db_error(self, mock_conn):
        """DB error during save must return False and log error."""
        mock_client = MagicMock()
        mock_client.execute.side_effect = Exception("SQL Error: table locked")
        mock_conn.return_value = mock_client
        
        from src.database.operations import save_data_to_storage
        
        df = pd.DataFrame({
            "timestamp": pd.to_datetime(["2025-01-15 14:30:00"]).tz_localize("UTC"),
            "symbol": ["AAPL"], "open": [150.0], "high": [151.0],
            "low": [149.0], "close": [150.5], "volume": [1000.0],
            "session": ["REG"]
        })
        
        logger = MockLogger()
        result = save_data_to_storage(df, logger)
        self.assertFalse(result)
        self.assertTrue(any("Save Error" in m for m in logger.messages))


class TestDatabaseSchema(unittest.TestCase):

    @patch("src.database.schema.get_db_connection")
    def test_init_db_no_connection(self, mock_conn):
        """init_db must not crash when DB connection fails."""
        mock_conn.return_value = None
        from src.database.schema import init_db
        # Must not raise
        init_db()

    @patch("src.database.schema.get_db_connection")
    def test_init_db_creates_tables(self, mock_conn):
        """init_db must execute CREATE TABLE statements."""
        mock_client = MagicMock()
        
        # Mock count queries for migration check
        count_result = MagicMock()
        count_result.rows = [[5]]  # Non-zero means tables have data
        mock_client.execute.return_value = count_result
        mock_conn.return_value = mock_client
        
        from src.database.schema import init_db
        init_db()
        
        # Should have called execute multiple times for CREATE TABLE
        calls = [str(c) for c in mock_client.execute.call_args_list]
        create_calls = [c for c in calls if "CREATE TABLE" in c]
        self.assertGreaterEqual(len(create_calls), 2, 
                                "Must create at least symbol_map and market_data tables")

    def test_no_streamlit_import(self):
        """schema.py must not import streamlit."""
        import inspect
        import src.database.schema as schema_module
        source = inspect.getsource(schema_module)
        self.assertNotIn("import streamlit", source)

    def test_no_streamlit_in_operations(self):
        """operations.py must not import streamlit."""
        import inspect
        import src.database.operations as ops_module
        source = inspect.getsource(ops_module)
        self.assertNotIn("import streamlit", source)


if __name__ == '__main__':
    unittest.main()
