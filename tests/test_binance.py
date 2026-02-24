"""
Tests for src/api/binance.py — Verify domain fallback, error handling, logger usage.
"""
import pytest
from unittest.mock import patch, MagicMock
import pandas as pd
from datetime import date


class MockLogger:
    def __init__(self):
        self.messages = []
    def log(self, msg):
        self.messages.append(msg)


class TestBinanceFetch:

    @patch("src.api.binance.requests.get")
    def test_success_returns_data(self, mock_get, safe_test_date):
        """Successful Binance response must return valid DataFrame."""
        kline = [
            1705276800000, "40000.0", "41000.0", "39000.0", "40500.0", "100.0",
            1705276859999, "4000000.0", 500, "50.0", "2000000.0", "0"
        ]
        # First call returns data, second call returns empty (end pagination)
        response_data = MagicMock()
        response_data.status_code = 200
        response_data.json.return_value = [kline]
        
        response_empty = MagicMock()
        response_empty.status_code = 200
        response_empty.json.return_value = []
        
        mock_get.side_effect = [response_data, response_empty]
        
        from src.api.binance import fetch_binance_daily
        logger = MockLogger()
        df = fetch_binance_daily("BTCUSDT", safe_test_date, logger)
        assert not df.empty
        assert "timestamp" in df.columns
        assert "close" in df.columns

    @patch("src.api.binance.requests.get")
    def test_invalid_symbol_returns_empty(self, mock_get, safe_test_date):
        """Invalid symbol returning error dict must return empty DF."""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"code": -1121, "msg": "Invalid symbol."}
        mock_get.return_value = mock_response
        
        from src.api.binance import fetch_binance_daily
        logger = MockLogger()
        df = fetch_binance_daily("INVALIDXYZ", safe_test_date, logger)
        assert df.empty
        # Should have logged the comprehensive error report
        assert any("All Binance domains failed" in m for m in logger.messages)

    @patch("src.api.binance.requests.get")
    def test_geo_block_tries_next_domain(self, mock_get, safe_test_date):
        """403 geo-block must try the next domain."""
        response_403 = MagicMock()
        response_403.status_code = 403
        
        kline = [
            1705276800000, "40000.0", "41000.0", "39000.0", "40500.0", "100.0",
            1705276859999, "4000000.0", 500, "50.0", "2000000.0", "0"
        ]
        response_ok = MagicMock()
        response_ok.status_code = 200
        response_ok.json.return_value = [kline]
        
        response_empty = MagicMock()
        response_empty.status_code = 200
        response_empty.json.return_value = []
        
        # First domain: 403, second domain: data then empty
        mock_get.side_effect = [response_403, response_ok, response_empty]
        
        from src.api.binance import fetch_binance_daily
        logger = MockLogger()
        df = fetch_binance_daily("BTCUSDT", safe_test_date, logger)
        # Should have tried at least 2 domains
        assert mock_get.call_count >= 2

    @patch("src.api.binance.requests.get")
    def test_exception_caught(self, mock_get, safe_test_date):
        """Network exception must be caught and logged."""
        mock_get.side_effect = Exception("Connection refused")
        
        from src.api.binance import fetch_binance_daily
        logger = MockLogger()
        df = fetch_binance_daily("BTCUSDT", safe_test_date, logger)
        assert df.empty
        assert any("All Binance domains failed" in m for m in logger.messages)

    @patch("src.api.binance.requests.get")
    def test_logger_is_optional(self, mock_get, safe_test_date):
        """fetch_binance_daily must work without a logger (backward compat)."""
        mock_get.side_effect = Exception("test")
        
        from src.api.binance import fetch_binance_daily
        # Must NOT crash when logger=None
        df = fetch_binance_daily("BTCUSDT", safe_test_date)
        assert df.empty
