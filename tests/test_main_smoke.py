"""
Smoke tests for main.py entrypoint behavior.
"""
import pytest
from unittest.mock import MagicMock, patch
import sys
import importlib
import pandas as pd


class TestMainSmoke:

    @pytest.fixture(autouse=True)
    def setup(self):
        # Ensure main is fresh
        if 'main' in sys.modules:
            importlib.reload(sys.modules['main'])

    @patch("src.database.connection.get_mirror_db_connection")
    @patch("src.database.connection.get_archive_db_connection")
    @patch("src.infisical_manager.InfisicalManager")
    @patch("src.utils.logger.CLILogger")
    def test_main_exits_gracefully_without_credentials(
        self,
        mock_logger_cls,
        mock_infisical_cls,
        mock_archive_conn,
        mock_mirror_conn,
    ):
        """Entrypoint should return cleanly when DB credentials are unavailable."""
        mock_logger = MagicMock()
        mock_logger_cls.return_value = mock_logger

        mock_infisical = MagicMock()
        mock_infisical.get_secret.return_value = "https://discord.com/fake"
        mock_infisical_cls.return_value = mock_infisical

        # Connection Failure
        mock_archive_conn.return_value = None
        mock_mirror_conn.return_value = None

        import main
        with patch.object(sys, 'argv', ['main.py']):
            with pytest.raises(SystemExit) as cm:
                main.main()
            assert cm.value.code == 1

        assert mock_logger.log.called
        assert any("CRITICAL" in str(c) for c in mock_logger.log.call_args_list)

    @patch("src.utils.integrity.verify_db_md5")
    @patch("src.utils.discord.send_discord_harvest_report")
    @patch("src.data.harvester.run_harvest_logic")
    @patch("src.database.operations.save_data_to_storage")
    @patch("src.database.operations.get_symbol_map_from_db")
    @patch("src.database.schema.init_db")
    @patch("src.database.connection.get_mirror_db_connection")
    @patch("src.database.connection.get_archive_db_connection")
    @patch("src.infisical_manager.InfisicalManager")
    @patch("src.utils.logger.CLILogger")
    @patch("src.api.massive.MassiveProvider")
    def test_main_flow_with_mocked_harvest(
        self,
        mock_massive_cls,
        mock_logger_cls,
        mock_infisical_cls,
        mock_archive_conn,
        mock_mirror_conn,
        mock_init_db,
        mock_get_map,
        mock_save_storage,
        mock_run_harvest,
        mock_discord_report,
        mock_verify_md5,
        safe_test_date_str
    ):
        """Standard main() flow with full mocks to ensure wiring is correct."""
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
        
        mock_get_map.return_value = {"AAPL": {"yahoo_ticker": "AAPL", "massive_ticker": "AAPL", "binance_ticker": None}}
        
        mock_df = pd.DataFrame([{"timestamp": f"{safe_test_date_str} 10:00:00", "symbol": "AAPL", "close": 150.0}])
        mock_report = pd.DataFrame([{"Ticker": "AAPL", "Status": "✅ Massive", "Total": 1}])
        mock_run_harvest.return_value = (mock_df, mock_report)
        
        mock_save_storage.return_value = True
        mock_verify_md5.return_value = (True, "✅ MD5 MATCH")
        mock_discord_report.return_value = True

        # 2. Run Main
        import main
        importlib.reload(main)
        
        with patch.object(sys, 'argv', ['main.py']):
            with patch("sys.exit") as mock_exit:
                main.main()
            
        # 3. Verify
        assert mock_init_db.called
        assert mock_get_map.called
        assert mock_run_harvest.called
        assert mock_save_storage.called
        assert mock_discord_report.called
