"""
Tests for src/database/schema.py and operations.py.
"""
import unittest
import pandas as pd
from unittest.mock import patch, MagicMock
import src.database.schema as schema_module
import src.database.operations as ops_module


class TestDatabaseSchema(unittest.TestCase):

    @patch("src.database.schema.get_archive_db_connection")
    @patch("src.database.schema.get_mirror_db_connection")
    def test_init_db_creates_tables(self, mock_mirror, mock_archive):
        """init_db should call execute on the provided client or fetch ones."""
        mock_client = MagicMock()
        schema_module.init_db(mock_client)
        self.assertTrue(mock_client.execute.called)

    @patch("src.database.schema.get_archive_db_connection", return_value=None)
    @patch("src.database.schema.get_mirror_db_connection", return_value=None)
    def test_init_db_no_connection(self, mock_mirror, mock_archive):
        """init_db should not crash if connections fail."""
        schema_module.init_db()
        # Should just return without error

    def test_no_streamlit_import(self):
        """Ensure schema.py does not import streamlit (Pure CLI)."""
        with open("src/database/schema.py", "r") as f:
            content = f.read()
            self.assertNotIn("import streamlit", content)
            self.assertNotIn("from streamlit", content)


class TestDatabaseOperations(unittest.TestCase):

    @patch("src.database.operations.get_archive_db_connection", return_value=None)
    @patch("src.database.operations.get_mirror_db_connection", return_value=None)
    def test_get_symbol_map_no_connection(self, mock_mirror, mock_archive):
        """get_symbol_map_from_db must return empty dict if connection fails."""
        res = ops_module.get_symbol_map_from_db()
        self.assertEqual(res, {})

    @patch("src.database.operations.get_archive_db_connection")
    def test_get_symbol_map_success(self, mock_conn):
        """get_symbol_map_from_db must parse rows into dict."""
        mock_client = MagicMock()
        mock_conn.return_value = mock_client
        
        # Mocking the ResultSet object from libsql-client
        mock_res = MagicMock()
        mock_res.rows = [
            ("AAPL", "AAPL", "AAPL", None),
            ("BTCUSDT", "BTC-USD", None, "BTCUSDT")
        ]
        mock_client.execute.return_value = mock_res
        
        res = ops_module.get_symbol_map_from_db()
        self.assertIn("AAPL", res)
        self.assertEqual(res["AAPL"]["massive_ticker"], "AAPL")

    @patch("src.database.operations.get_archive_db_connection", return_value=None)
    @patch("src.database.operations.get_mirror_db_connection", return_value=None)
    def test_save_data_no_connection(self, mock_mirror, mock_archive):
        """save_data_to_storage must return False if connections fail."""
        df = pd.DataFrame([{"timestamp": "2025-01-15", "symbol": "AAPL"}])
        res = ops_module.save_data_to_storage(df)
        self.assertFalse(res)

    def test_save_data_empty_df(self):
        """save_data_to_storage must return False if DF is empty."""
        res = ops_module.save_data_to_storage(pd.DataFrame())
        self.assertFalse(res)

    @patch("src.database.operations.get_archive_db_connection")
    @patch("src.database.operations.get_mirror_db_connection")
    def test_save_data_success(self, mock_mirror, mock_archive):
        """save_data_to_storage must return True if both saves succeed."""
        mock_archive.return_value = MagicMock()
        mock_mirror.return_value = MagicMock()
        
        df = pd.DataFrame({
            "timestamp": ["2025-01-15 10:00:00"],
            "symbol": ["AAPL"],
            "open": [150.0], "high": [151.0], "low": [149.0], 
            "close": [150.5], "volume": [1000.0], "session": ["REG"]
        })
        
        res = ops_module.save_data_to_storage(df)
        self.assertTrue(res)

    @patch("src.database.operations.get_archive_db_connection")
    @patch("src.database.operations.get_mirror_db_connection")
    def test_save_data_db_error(self, mock_mirror, mock_archive):
        """save_data_to_storage must return False if a DB error occurs."""
        mock_client = MagicMock()
        mock_client.execute.side_effect = Exception("DB Error")
        mock_archive.return_value = mock_client
        
        df = pd.DataFrame([{"timestamp": "2025-01-15 10:00:00", "symbol": "AAPL"}])
        res = ops_module.save_data_to_storage(df)
        self.assertFalse(res)

    def test_no_streamlit_in_operations(self):
        """Ensure operations.py does not import streamlit (Pure CLI)."""
        with open("src/database/operations.py", "r") as f:
            content = f.read()
            self.assertNotIn("import streamlit", content)


if __name__ == "__main__":
    unittest.main()
