import pytest
import pandas as pd
import requests
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch
from src.api.capital import fetch_capital_data

@pytest.fixture
def mock_logger():
    logger = MagicMock()
    logger.log = MagicMock()
    return logger

def test_capital_chunking_and_clamping(mock_logger):
    """
    Simulate running at 2:00 AM UTC for a session that ended at 1:00 AM UTC.
    The session started at 1:00 AM UTC the previous day.
    Verify that it makes TWO calls:
    1. 10:10 AM -> 10:10 PM
    2. 10:10 PM -> 01:00 AM
    """
    fixed_now = datetime(2026, 2, 26, 2, 0, 0, tzinfo=timezone.utc)
    start_dt = datetime(2026, 2, 25, 1, 0, 0, tzinfo=timezone.utc)
    end_dt = datetime(2026, 2, 26, 1, 0, 0, tzinfo=timezone.utc)
    expected_limit = fixed_now - timedelta(hours=15, minutes=50) # 10:10 AM
    
    with patch('src.api.capital.datetime') as mock_dt_class:
        mock_dt_class.now.return_value = fixed_now
        mock_dt_class.side_effect = lambda *args, **kwargs: datetime(*args, **kwargs)
        
        with patch('src.api.capital._get_session') as mock_get_session, \
             patch('src.api.capital.get_retry_session') as mock_retry_session:
            
            mock_get_session.return_value = {"api_key": "test", "CST": "test", "X-SECURITY-TOKEN": "test"}
            mock_session_obj = MagicMock()
            mock_retry_session.return_value = mock_session_obj
            mock_session_obj.get.return_value.status_code = 200
            mock_session_obj.get.return_value.json.return_value = {"prices": []}
            
            fetch_capital_data("DXY", start_dt, end_dt, mock_logger)
            
            # Verify TWO calls were made
            assert mock_session_obj.get.call_count == 2
            
            # First call
            args1, kwargs1 = mock_session_obj.get.call_args_list[0]
            assert kwargs1['params']['from'] == "2026-02-25T10:10:00"
            assert kwargs1['params']['to'] == "2026-02-25T22:10:00"
            
            # Second call
            args2, kwargs2 = mock_session_obj.get.call_args_list[1]
            assert kwargs2['params']['from'] == "2026-02-25T22:10:00"
            assert kwargs2['params']['to'] == "2026-02-26T01:00:00"

def test_capital_out_of_window(mock_logger):
    """Verify that requesting data older than 16 hours returns empty but doesn't crash."""
    fixed_now = datetime(2026, 2, 26, 2, 0, 0, tzinfo=timezone.utc)
    # Session from 3 days ago
    start_dt = fixed_now - timedelta(days=3)
    end_dt = fixed_now - timedelta(days=2)
    
    with patch('src.api.capital.datetime') as mock_dt_class:
        mock_dt_class.now.return_value = fixed_now
        mock_dt_class.side_effect = lambda *args, **kwargs: datetime(*args, **kwargs)
        
        with patch('src.api.capital._get_session') as mock_get_session:
            mock_get_session.return_value = {"api_key": "test", "CST": "test", "X-SECURITY-TOKEN": "test"}
            df = fetch_capital_data("DXY", start_dt, end_dt, mock_logger)
            assert df.empty
            mock_logger.log.assert_any_call("   ℹ️ DXY: Requested range is beyond Capital.com 16h window. Skipping.")

def test_capital_run_during_active_session(mock_logger):
    """
    Simulate running at 10:00 AM UTC for a session that is currently active.
    Session started at 1:00 AM UTC today.
    It should fetch from 1:00 AM to 10:00 AM (current time).
    """
    fixed_now = datetime(2026, 2, 26, 10, 0, 0, tzinfo=timezone.utc)
    start_dt = datetime(2026, 2, 26, 1, 0, 0, tzinfo=timezone.utc)
    end_dt = datetime(2026, 2, 27, 1, 0, 0, tzinfo=timezone.utc)
    
    with patch('src.api.capital.datetime') as mock_dt_class:
        mock_dt_class.now.return_value = fixed_now
        mock_dt_class.side_effect = lambda *args, **kwargs: datetime(*args, **kwargs)
        
        with patch('src.api.capital._get_session') as mock_get_session, \
             patch('src.api.capital.get_retry_session') as mock_retry_session:
            
            mock_get_session.return_value = {"api_key": "test", "CST": "test", "X-SECURITY-TOKEN": "test"}
            mock_session_obj = MagicMock()
            mock_retry_session.return_value = mock_session_obj
            mock_session_obj.get.return_value.status_code = 200
            mock_session_obj.get.return_value.json.return_value = {"prices": []}
            
            fetch_capital_data("DXY", start_dt, end_dt, mock_logger)
            
            # Should have made 1 call
            assert mock_session_obj.get.call_count == 1
            args, kwargs = mock_session_obj.get.call_args
            # 'to' should be exactly 'now' (10:00)
            assert kwargs['params']['to'] == "2026-02-26T10:00:00"

def test_capital_auth_error_retry(mock_logger):
    """Verify that a 401 triggers a session refresh and retry."""
    start_dt = datetime.now(timezone.utc) - timedelta(hours=1)
    end_dt = datetime.now(timezone.utc)
    
    with patch('src.api.capital._get_session') as mock_get_session, \
         patch('src.api.capital.get_retry_session') as mock_retry_session:
        
        # First call to _get_session returns a token, second call (after refresh) returns a new one
        mock_get_session.side_effect = [
            {"api_key": "key1", "CST": "CST1", "X-SECURITY-TOKEN": "TOK1"},
            {"api_key": "key2", "CST": "CST2", "X-SECURITY-TOKEN": "TOK2"}
        ]
        
        mock_session_obj = MagicMock()
        mock_retry_session.return_value = mock_session_obj
        
        # First API call returns 401, second returns 200
        mock_resp1 = MagicMock()
        mock_resp1.status_code = 401
        
        mock_resp2 = MagicMock()
        mock_resp2.status_code = 200
        mock_resp2.json.return_value = {"prices": []}
        
        mock_session_obj.get.side_effect = [mock_resp1, mock_resp2]
        
        fetch_capital_data("DXY", start_dt, end_dt, mock_logger)
        
        # Should have called get twice
        assert mock_session_obj.get.call_count == 2
        # Second call should have used TOK2
        args2, kwargs2 = mock_session_obj.get.call_args_list[1]
        assert kwargs2['headers']['X-SECURITY-TOKEN'] == "TOK2"
