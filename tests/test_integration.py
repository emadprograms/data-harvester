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
                try: os.remove(f)
                except: pass

    def tearDown(self):
        for client in self._clients:
            try: client.close()
            except: pass
        for f in self.db_files:
            if os.path.exists(f):
                try: os.remove(f)
                except: pass

    def _new_client(self, db_path: str):
        client = create_client_sync(url=f"file:{db_path}")
        self._clients.append(client)
        return client

    @patch("src.data.harvester.fetch_binance_daily")
    @patch("src.data.harvester.fetch_yahoo_market_data")
    @patch("src.data.harvester.fetch_massive_data")
    @patch("src.database.connection.get_archive_db_connection")
    @patch("src.database.connection.get_mirror_db_connection")
    def test_full_harvest_mixed_symbols(self, mock_mirror, mock_archive, mock_massive, mock_yahoo, mock_binance):
        """Pipeline should correctly handle a mix of crypto, stock, and fallback scenarios."""
        
        # 0. Setup Mock DB
        mem_client = self._new_client("memdb1.db")
        init_db(mem_client)
        mock_archive.return_value = mem_client
        mock_mirror.return_value = mem_client
        
        # Mock Massive success
        massive_df = pd.DataFrame({
            "timestamp": pd.to_datetime(["2026-02-18 10:00:00"]).tz_localize("UTC"),
            "symbol": ["AAPL"],
            "open": [150.0], "high": [150.5], "low": [149.5],
            "close": [150.2], "volume": [1000.0]
        })
        mock_massive.return_value = massive_df

        # Mock Binance success
        mock_binance.return_value = pd.DataFrame({
            "timestamp": pd.to_datetime(["2026-02-18 12:00:00"]).tz_localize("UTC"),
            "symbol": ["BTCUSDT"], "open": [40000.0], "high": [41000.0], "low": [39000.0],
            "close": [40500.0], "volume": [100.0]
        })

        # 1. Seed the DB (Using REPLACE to handle auto-seeded defaults)
        mem_client.execute(
            "INSERT OR REPLACE INTO symbol_map (display_name, yahoo_ticker, massive_ticker, binance_ticker) VALUES (?, ?, ?, ?)",
            ["AAPL", "AAPL", "AAPL", None]
        )
        mem_client.execute(
            "INSERT OR REPLACE INTO symbol_map (display_name, yahoo_ticker, massive_ticker, binance_ticker) VALUES (?, ?, ?, ?)",
            ["BTCUSDT", "BTC-USD", None, "BTCUSDT"]
        )

        # 2. Execution logic
        symbol_map = get_symbol_map_from_db(mem_client)
        target_date = date(2026, 2, 18)
        logger = MagicMock()
        
        final_df, report = run_harvest_logic(
            tickers_to_harvest=list(symbol_map.keys()),
            target_date=target_date,
            db_map=symbol_map,
            logger=logger
        )

        # 3. Validation
        self.assertFalse(final_df.empty)
        self.assertGreaterEqual(len(symbol_map), 2)
        
        # Verify Dual Write
        save_success = save_data_to_storage(final_df, logger, archive_client=mem_client, mirror_client=mem_client)
        self.assertTrue(save_success)
        
        # Verify data hit DB
        res = mem_client.execute("SELECT COUNT(*) FROM market_data")
        self.assertGreater(res.rows[0][0], 0)


if __name__ == "__main__":
    unittest.main()
