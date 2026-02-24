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
        mock_exit,          # Bottom-most patch (13th Arg)
        mock_logger_cls,    # 4th from top
        mock_infisical_cls, # 3rd from top
        mock_archive_conn,  # 2nd from top
        mock_mirror_conn    # Top-most patch (1st Arg)
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
        # Check that it exited with 1
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
        mock_exit,          # 1. sys.exit (Bottom)
        mock_parity,        # 2. ensure_database_parity
        mock_discord_report,# 3. send_discord_harvest_report
        mock_run_harvest,   # 4. run_harvest_logic
        mock_save_storage,  # 5. save_data_to_storage
        mock_clear_dates,   # 6. clear_market_data_for_dates
        mock_get_map,       # 7. get_symbol_map_from_db
        mock_init_db,       # 8. init_db
        mock_mirror_conn,   # 9. get_mirror_db_connection
        mock_archive_conn,  # 10. get_archive_db_connection
        mock_infisical_cls, # 11. InfisicalManager
        mock_logger_cls,    # 12. CLILogger
        mock_massive_cls,   # 13. MassiveProvider (Top)
        safe_test_date_str  # Fixture
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
        
        # Create a DF with a target date row and a rogue row
        from datetime import date, timedelta
        target_dt = pd.to_datetime(f"{safe_test_date_str} 10:00:00", utc=True)
        rogue_dt = target_dt - timedelta(days=1)
        
        mock_df = pd.DataFrame([
            {"timestamp": target_dt, "symbol": "AAPL", "close": 150.0},
            {"timestamp": rogue_dt, "symbol": "AAPL", "close": 149.0}
        ])
        mock_report = pd.DataFrame([{"Ticker": "AAPL", "Status": "✅ Massive", "Total": 2}])
        mock_run_harvest.return_value = (mock_df, mock_report)
        
        mock_save_storage.return_value = True
        mock_parity.return_value = (True, "✅ PARITY MATCH")

        # Explicitly pass --date to match safe_test_date_str
        with patch.object(sys, 'argv', ['main.py', '--date', safe_test_date_str]):
            main.main()
            
        # 3. Verify
        assert mock_init_db.called
        assert mock_get_map.called
        assert mock_run_harvest.called
        
        # Verify Clean-Before-Write called only for target date
        assert mock_clear_dates.called
        # Check that it was called with the target date (not the rogue date)
        # Argument 1 is the dates list
        call_args = mock_clear_dates.call_args_list[0][0]
        assert len(call_args[1]) == 1
        assert str(call_args[1][0]) == safe_test_date_str
        
        # Verify save_data_to_storage called twice (Target REPLACE, Rogue IGNORE)
        assert mock_save_storage.call_count == 2
        
        # Target call (REPLACE)
        target_call = mock_save_storage.call_args_list[0]
        assert target_call.kwargs.get("mode") == "REPLACE"
        
        # Rogue call (IGNORE)
        rogue_call = mock_save_storage.call_args_list[1]
        assert rogue_call.kwargs.get("mode") == "IGNORE"
        
        assert mock_discord_report.called
