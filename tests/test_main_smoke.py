"""
Smoke tests for main.py entrypoint behavior.
"""
import pytest
from unittest.mock import MagicMock, patch
import sys
import importlib
import pandas as pd

class TestMainSmoke:

    @patch("src.database.connection.get_mirror_db_connection")
    @patch("src.database.connection.get_archive_db_connection")
    @patch("src.infisical_manager.InfisicalManager")
    @patch("src.utils.logger.CLILogger")
    @patch("sys.exit")
    def test_main_exits_gracefully_without_credentials(
        self,
        mock_exit,
        mock_logger_cls,
        mock_infisical_cls,
        mock_archive_conn,
        mock_mirror_conn
    ):
        """Entrypoint should return cleanly when DB credentials are unavailable."""
        import main
        importlib.reload(main)

        mock_logger = MagicMock()
        mock_logger_cls.return_value = mock_logger

        mock_infisical = MagicMock()
        mock_infisical.get_secret.return_value = "https://discord.com/fake"
        mock_infisical_cls.return_value = mock_infisical

        # Connection Failure
        mock_archive_conn.return_value = None
        mock_mirror_conn.return_value = None

        with patch.object(sys, 'argv', ['main.py']):
            main.main()
            
        assert mock_exit.called
        assert mock_exit.call_args[0][0] == 1
        assert any("CRITICAL" in str(c) for c in mock_logger.log.call_args_list)

    @patch("src.api.massive.MassiveProvider")
    @patch("src.utils.logger.CLILogger")
    @patch("src.infisical_manager.InfisicalManager")
    @patch("src.database.connection.get_archive_db_connection")
    @patch("src.database.connection.get_mirror_db_connection")
    @patch("src.database.schema.init_db")
    @patch("src.database.operations.get_symbol_map_from_db")
    @patch("src.database.operations.clear_market_data_for_dates")
    @patch("src.database.operations.save_data_to_storage")
    @patch("src.data.harvester.run_harvest_logic")
    @patch("src.utils.discord.send_discord_harvest_report")
    @patch("src.utils.integrity.ensure_database_parity")
    @patch("sys.exit")
    def test_main_flow_with_mocked_harvest(
        self,
        mock_exit,
        mock_parity,
        mock_discord_report,
        mock_run_harvest,
        mock_save_storage,
        mock_clear_dates,
        mock_get_map,
        mock_init_db,
        mock_mirror_conn,
        mock_archive_conn,
        mock_infisical_cls,
        mock_logger_cls,
        mock_massive_cls,
        safe_test_date_str
    ):
        """Standard main() flow with full mocks to ensure wiring is correct."""
        import main
        importlib.reload(main)

        # 1. Setup Mocks
        mock_logger = MagicMock()
        mock_logger_cls.return_value = mock_logger
        
        mock_infisical = MagicMock()
        mock_infisical.get_secret.return_value = "https://discord.com/webhook"
        mock_infisical_cls.return_value = mock_infisical
        
        mock_archive = MagicMock()
        mock_archive_conn.return_value = mock_archive
        mock_mirror = MagicMock()
        mock_mirror_conn.return_value = mock_mirror
        
        mock_get_map.return_value = {"AAPL": {"yahoo_ticker": None, "massive_ticker": "AAPL", "binance_ticker": None, "capital_ticker": "AAPL"}}
        
        # Ensure timestamp is datetime for the clear_market_data_for_dates logic
        mock_df = pd.DataFrame([{"timestamp": pd.to_datetime(f"{safe_test_date_str} 10:00:00", utc=True), "symbol": "AAPL", "close": 150.0}])
        mock_report = pd.DataFrame([{"Ticker": "AAPL", "Status": "✅ Massive", "Total": 1}])
        mock_run_harvest.return_value = (mock_df, mock_report)
        
        mock_save_storage.return_value = True
        mock_parity.return_value = (True, "✅ PARITY MATCH")

        with patch.object(sys, 'argv', ['main.py']):
            main.main()
            
        # 3. Verify
        assert mock_init_db.called
        assert mock_get_map.called
        assert mock_run_harvest.called
        assert mock_clear_dates.called
        assert mock_save_storage.called
        assert mock_discord_report.called
        
        # Verify the call arguments specifically for the new integrity fields
        call_args = mock_discord_report.call_args[1] # get kwargs
        assert "integrity_pre" in call_args
        assert "integrity_post" in call_args
