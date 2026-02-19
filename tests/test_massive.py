"""
Tests for src/api/massive.py — Verify key rotation, error handling, retry logic.
Uses mocking to avoid real API calls.
"""
import unittest
from unittest.mock import patch, MagicMock
import pandas as pd
from datetime import datetime
from src.config import UTC
import requests


class MockLogger:
    def __init__(self):
        self.messages = []
    def log(self, msg):
        self.messages.append(msg)


class TestMassiveKeyRotation(unittest.TestCase):

    @patch("src.api.massive.InfisicalManager")
    @patch("src.api.massive.get_retry_session")
    def test_no_keys_returns_empty(self, mock_session, mock_mgr_cls):
        """If no API keys are available, must return empty DF + error message."""
        mock_mgr = MagicMock()
        mock_mgr.get_massive_api_keys.return_value = []
        mock_mgr_cls.return_value = mock_mgr
        
        from src.api.massive import fetch_massive_data
        logger = MockLogger()
        start = datetime(2025, 1, 15, 5, 0, tzinfo=UTC)
        end = datetime(2025, 1, 16, 5, 0, tzinfo=UTC)
        
        df, msg = fetch_massive_data("AAPL", start, end, logger)
        self.assertTrue(df.empty)
        self.assertIn("Missing", msg)

    @patch("src.api.massive.get_retry_session")
    @patch("src.api.massive.InfisicalManager")
    def test_success_returns_data(self, mock_mgr_cls, mock_session_fn):
        """Successful API call must return DataFrame with data."""
        mock_mgr = MagicMock()
        mock_mgr.get_massive_api_keys.return_value = ["key1"]
        mock_mgr_cls.return_value = mock_mgr
        
        # Timestamp: 2025-01-15 14:30 UTC = within range
        # start_utc = 2025-01-15 05:00 UTC, end_utc = 2025-01-16 05:00 UTC
        ts_ms = int(datetime(2025, 1, 15, 14, 30, tzinfo=UTC).timestamp() * 1000)
        
        mock_response = MagicMock()
        mock_response.json.return_value = {
            "status": "OK",
            "results": [
                {"t": ts_ms, "o": 150.0, "h": 151.0, "l": 149.0, "c": 150.5, "v": 1000}
            ]
        }
        mock_response.raise_for_status = MagicMock()  # No exception
        mock_session = MagicMock()
        mock_session.get.return_value = mock_response
        mock_session_fn.return_value = mock_session
        
        # Reset key rotation state
        import src.api.massive as massive_module
        massive_module._MASSIVE_KEY_IDX = 0
        
        from src.api.massive import fetch_massive_data
        logger = MockLogger()
        start = datetime(2025, 1, 15, 5, 0, tzinfo=UTC)
        end = datetime(2025, 1, 16, 5, 0, tzinfo=UTC)
        
        df, msg = fetch_massive_data("AAPL", start, end, logger)
        self.assertFalse(df.empty)
        self.assertEqual(msg, "")

    @patch("src.api.massive.InfisicalManager")
    @patch("src.api.massive.get_retry_session")
    def test_rate_limit_rotates_key(self, mock_session_fn, mock_mgr_cls):
        """429 rate limit must rotate to next key, not give up."""
        mock_mgr = MagicMock()
        mock_mgr.get_massive_api_keys.return_value = ["key1", "key2"]
        mock_mgr_cls.return_value = mock_mgr
        
        # First call: 429, second call: success
        response_429 = MagicMock()
        response_429.status_code = 429
        http_error = requests.exceptions.HTTPError(response=response_429)
        
        response_ok = MagicMock()
        response_ok.status_code = 200
        response_ok.json.return_value = {
            "status": "OK",
            "results": [{"t": 1705312200000, "o": 150.0, "h": 151.0, "l": 149.0, "c": 150.5, "v": 1000}]
        }
        response_ok.raise_for_status = MagicMock()
        
        mock_session = MagicMock()
        mock_session.get.side_effect = [
            MagicMock(raise_for_status=MagicMock(side_effect=http_error)),
            response_ok
        ]
        mock_session_fn.return_value = mock_session
        
        from src.api.massive import fetch_massive_data
        logger = MockLogger()
        start = datetime(2025, 1, 15, 5, 0, tzinfo=UTC)
        end = datetime(2025, 1, 16, 5, 0, tzinfo=UTC)
        
        df, msg = fetch_massive_data("AAPL", start, end, logger)
        # Should have tried the second key
        self.assertEqual(mock_session.get.call_count, 2)

    @patch("src.api.massive.InfisicalManager")
    @patch("src.api.massive.get_retry_session")
    def test_all_keys_exhausted(self, mock_session_fn, mock_mgr_cls):
        """When all keys fail, must return error with 'All Keys' message."""
        mock_mgr = MagicMock()
        mock_mgr.get_massive_api_keys.return_value = ["key1", "key2"]
        mock_mgr_cls.return_value = mock_mgr
        
        response_429 = MagicMock()
        response_429.status_code = 429
        http_error = requests.exceptions.HTTPError(response=response_429)
        
        mock_session = MagicMock()
        mock_session.get.return_value = MagicMock(
            raise_for_status=MagicMock(side_effect=http_error)
        )
        mock_session_fn.return_value = mock_session
        
        from src.api.massive import fetch_massive_data
        logger = MockLogger()
        start = datetime(2025, 1, 15, 5, 0, tzinfo=UTC)
        end = datetime(2025, 1, 16, 5, 0, tzinfo=UTC)
        
        df, msg = fetch_massive_data("AAPL", start, end, logger)
        self.assertTrue(df.empty)
        self.assertIn("All Keys", msg)

    @patch("src.api.massive.InfisicalManager")
    @patch("src.api.massive.get_retry_session")
    def test_empty_results_ok(self, mock_session_fn, mock_mgr_cls):
        """API returning status OK but no results must return 'No Data (OK)'."""
        mock_mgr = MagicMock()
        mock_mgr.get_massive_api_keys.return_value = ["key1"]
        mock_mgr_cls.return_value = mock_mgr
        
        mock_response = MagicMock()
        mock_response.json.return_value = {"status": "OK", "results": []}
        mock_response.raise_for_status = MagicMock()
        mock_session = MagicMock()
        mock_session.get.return_value = mock_response
        mock_session_fn.return_value = mock_session
        
        from src.api.massive import fetch_massive_data
        logger = MockLogger()
        start = datetime(2025, 1, 15, 5, 0, tzinfo=UTC)
        end = datetime(2025, 1, 16, 5, 0, tzinfo=UTC)
        
        df, msg = fetch_massive_data("AAPL", start, end, logger)
        self.assertTrue(df.empty)
        self.assertEqual(msg, "No Data (OK)")


if __name__ == '__main__':
    unittest.main()
