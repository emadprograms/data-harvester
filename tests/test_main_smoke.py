"""
Smoke tests for main.py entrypoint behavior.
"""
import pytest
from unittest.mock import MagicMock, patch
import os
import sys
import importlib
import pandas as pd

class TestMainSmoke:

    @patch("src.database.connection.get_archive_db_connection")
    @patch("src.infisical_manager.InfisicalManager")
    @patch("src.utils.logger.CLILogger")
    @patch("sys.exit")
    def test_main_exits_gracefully_without_credentials(
        self,
        mock_exit,          # Bottom-most patch
        mock_logger_cls,
        mock_infisical_cls,
        mock_archive_conn
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

        with patch.object(sys, 'argv', ['main.py']):
            main.main()
            
        assert mock_exit.called
        # Check that it exited with 1
        assert mock_exit.call_args[0][0] == 1
        assert any("CRITICAL" in str(c) for c in mock_logger.log.call_args_list)

    @patch("src.database.operations.get_session_row_counts")
    @patch("src.api.massive.MassiveProvider")
    @patch("src.utils.logger.CLILogger")
    @patch("src.infisical_manager.InfisicalManager")
    @patch("src.database.connection.get_archive_db_connection")
    @patch("src.database.schema.init_db")
    @patch("src.database.operations.get_symbol_map_from_db")
    @patch("src.database.operations.clear_market_data_for_range")
    @patch("src.database.operations.save_data_to_storage")
    @patch("src.data.harvester.run_harvest_logic")
    @patch("src.utils.discord.send_discord_harvest_report")
    @patch("sys.exit")
    def test_main_flow_with_mocked_harvest(
        self,
        mock_exit,          # Bottom
        mock_discord_report,
        mock_run_harvest,
        mock_save_storage,
        mock_clear_range,
        mock_get_map,
        mock_init_db,
        mock_archive_conn,
        mock_infisical_cls,
        mock_logger_cls,
        mock_massive_cls,
        mock_get_row_counts,
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
        
        mock_get_map.return_value = {"AAPL": {"yahoo_ticker": None, "massive_ticker": "AAPL", "binance_ticker": None, "capital_ticker": "AAPL"}}
        
        # Mock the health grid query to return clean data 
        mock_get_row_counts.return_value = {"AAPL": 500}
        
        # Create a DF with a target date row and a rogue row
        from datetime import date, timedelta
        target_dt = pd.to_datetime(f"{safe_test_date_str} 10:00:00", utc=True)
        rogue_dt = target_dt - timedelta(days=1)
        
        mock_df = pd.DataFrame([
            {"timestamp": target_dt, "symbol": "AAPL", "close": 150.0, "source": "MASSIVE"},
            {"timestamp": rogue_dt, "symbol": "AAPL", "close": 149.0, "source": "MASSIVE"}
        ])
        mock_report = pd.DataFrame([{"Ticker": "AAPL", "Status": "✅ Massive", "Total": 2}])
        mock_run_harvest.return_value = (mock_df, mock_report)
        
        mock_save_storage.return_value = True

        # Explicitly pass --date to match safe_test_date_str
        with patch.object(sys, 'argv', ['main.py', '--date', safe_test_date_str]), \
             patch.dict(os.environ, {"SKIP_DISCORD": ""}, clear=False):
            main.main()
            
        # 3. Verify
        assert mock_init_db.called
        assert mock_get_map.called
        assert mock_run_harvest.called
        
        # Verify Clean-Before-Write called once (Archive only, no Mirror)
        assert mock_clear_range.call_count == 1
        
        # Verify save_data_to_storage called ONCE for single-DB write
        assert mock_save_storage.call_count == 1
        # Verify save was called with archive_client only (no mirror_client)
        assert mock_save_storage.call_args.kwargs.get("archive_client") is not None
        assert "mirror_client" not in mock_save_storage.call_args.kwargs
        
        assert mock_discord_report.called
        # Verify db_health_grid was passed to Discord report
        discord_call_kwargs = mock_discord_report.call_args.kwargs
        assert "db_health_grid" in discord_call_kwargs
        # Verify parity fields are NOT passed
        assert "integrity_pre" not in discord_call_kwargs
        assert "integrity_post" not in discord_call_kwargs
        
        assert mock_exit.called
        assert mock_exit.call_args[0][0] == 0
