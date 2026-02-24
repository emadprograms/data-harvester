"""
Integration test — simulates the full pipeline:
1. Fetch Inventory from DB.
2. Harvest (Mocked APIs).
3. Save to DB.
4. Verify results.
"""
import unittest
import pandas as pd
import os
from datetime import date, datetime
from unittest.mock import patch, MagicMock
from src.database.connection import create_client_sync
from src.database.operations import get_symbol_map_from_db, save_data_to_storage
from src.data.harvester import run_harvest_logic
from src.database.schema import init_db


class TestIntegrationPipeline(unittest.TestCase):

    def setUp(self):
        self.db_files = ["memdb1.db", "memdb2.db"]
        self._clients = []
        for f in self.db_files:
            if os.path.exists(f):
                os.remove(f)

    def tearDown(self):
        for client in self._clients:
            try:
                client.close()
            except Exception:
                pass
        for f in self.db_files:
            if os.path.exists(f):
                os.remove(f)

    def _new_client(self, db_path: str):
        client = create_client_sync(url=f"file:{db_path}")
        self._clients.append(client)
        return client

    @patch("src.data.harvester.fetch_binance_daily")
    @patch("src.api.yahoo.fetch_yahoo_market_data")
    @patch("src.data.harvester.fetch_massive_data")
    @patch("src.database.connection.get_db_connection")
    @patch("src.database.connection.get_mirror_db_connection")
    def test_full_harvest_mixed_symbols(self, mock_mirror, mock_archive, mock_massive, mock_yahoo, mock_binance):
        """Pipeline should correctly handle a mix of crypto, stock, and fallback scenarios."""
        
        # 0. Setup Mock DB (Local file for libSQL driver consistency)
        mem_client = self._new_client("memdb1.db")
        init_db(mem_client)
        mock_archive.return_value = mem_client
        mock_mirror.return_value = mem_client
        
        # Mock Massive → success for AAPL (PRE/POST data, volume=0)
        massive_df = pd.DataFrame({
            "timestamp": pd.to_datetime(["2025-01-15 08:00:00", "2025-01-15 21:00:00"]).tz_localize("UTC"),
            "symbol": ["AAPL", "AAPL"],
            "open": [150.0, 151.0], "high": [150.5, 151.5], "low": [149.5, 150.5],
            "close": [150.2, 151.2], "volume": [0.0, 0.0]
        })
        mock_massive.return_value = massive_df

        # Mock Yahoo → success for AAPL (REG data)
        idx = pd.DatetimeIndex(pd.date_range("2025-01-15 09:30", periods=1, freq="1min", tz="US/Eastern"), name="Datetime")
        yho_df = pd.DataFrame({
            "Open": [150.5], "High": [151.0], "Low": [150.0], "Close": [150.7], "Volume": [50000]
        }, index=idx)
        mock_yahoo.return_value = yho_df
        
        # Mock Binance → success for BTCUSDT
        mock_binance.return_value = pd.DataFrame({
            "timestamp": pd.to_datetime(["2025-01-15 12:00:00"]).tz_localize("UTC"),
            "symbol": ["BTCUSDT"], "open": [40000.0], "high": [41000.0], "low": [39000.0],
            "close": [40500.0], "volume": [100.0], "session": ["REG"]
        })

        # 1. Seed the DB with hybrid rules
        mem_client.execute(
            """INSERT INTO symbol_map (display_name, yahoo_ticker, massive_ticker, binance_ticker, priority_1, priority_2)
               VALUES (?, ?, ?, ?, ?, ?)""",
            ["AAPL", "AAPL", "AAPL", None, "YAHOO", "MASSIVE"]
        )
        mem_client.execute(
            """INSERT INTO symbol_map (display_name, yahoo_ticker, massive_ticker, binance_ticker, priority_1, priority_2)
               VALUES (?, ?, ?, ?, ?, ?)""",
            ["BTC/USD", "BTC-USD", None, "BTCUSDT", "BINANCE", "YAHOO"]
        )

        # 2. Execution logic (same as main.py)
        # Fetch Inventory
        symbol_map = get_symbol_map_from_db(mem_client)
        
        # Run Harvest
        target_date = date(2025, 1, 15)
        logger = MagicMock()
        
        final_df, report = run_harvest_logic(
            tickers_to_harvest=list(symbol_map.keys()),
            target_date=target_date,
            db_map=symbol_map,
            logger=logger
        )

        # 3. Validation
        self.assertFalse(final_df.empty)
        self.assertEqual(len(symbol_map), 2)
        
        # Verify Dual Write
        save_success = save_data_to_storage(final_df, logger, archive_client=mem_client, mirror_client=mem_client)
        self.assertTrue(save_success)
        
        # Verify data actually hit the DB
        res = mem_client.execute("SELECT COUNT(*) FROM market_data")
        self.assertGreater(res.rows[0][0], 0)

    @patch("src.database.connection.get_db_connection")
    @patch("src.database.connection.get_mirror_db_connection")
    def test_broken_inventory_exits_cleanly(self, mock_mirror, mock_archive):
        """Pipeline must handle empty inventory without crashing."""
        mem_client = self._new_client("memdb2.db")
        init_db(mem_client)
        mock_archive.return_value = mem_client
        mock_mirror.return_value = mem_client
        
        symbol_map = get_symbol_map_from_db(mem_client)
        self.assertEqual(len(symbol_map), 0)
        
        final_df, report = run_harvest_logic([], date(2025, 1, 15), symbol_map, MagicMock())
        self.assertTrue(final_df.empty)


if __name__ == "__main__":
    unittest.main()
