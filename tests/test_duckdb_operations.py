"""
Tests for DuckDB Storage Layer, Schema, and Candlestick Resampling.
"""
import pytest
import os
import pandas as pd
from datetime import datetime
from src.database.connection import DuckDBClient, get_duckdb_connection
import src.database.schema as schema_module
import src.database.operations as ops_module


class TestDuckDBStorage:
    """Verifies local DuckDB operations, schema initialization, and source-tiering."""

    @pytest.fixture(autouse=True)
    def setup_teardown(self, tmp_path):
        self.db_path = str(tmp_path / "test_duckdb_storage.duckdb")
        yield

    def _new_client(self):
        return DuckDBClient(db_path=self.db_path)

    def test_schema_initialization(self):
        """init_db must create symbol_map and market_data tables with seeded symbols."""
        client = self._new_client()
        try:
            schema_module.init_db(client)
            symbols = ops_module.get_symbol_map_from_db(client)
            assert len(symbols) >= 15
            assert "AAPL" in symbols
            assert "BTCUSDT" in symbols
        finally:
            client.close()

    def test_source_tiering_protection(self):
        """Tier 1 source (MASSIVE) must NOT be overwritten by Tier 2 (CAPITAL)."""
        client = self._new_client()
        try:
            schema_module.init_db(client)

            # Insert Tier 1 bar from MASSIVE
            tier1_rows = [
                ("2026-01-01 10:00:00", "AAPL", 150.0, 155.0, 149.0, 154.0, 1000.0, "REG", "MASSIVE")
            ]
            ops_module._save_to_client(client, tier1_rows)

            # Attempt overwrite with Tier 2 data from CAPITAL
            tier2_rows = [
                ("2026-01-01 10:00:00", "AAPL", 999.0, 999.0, 999.0, 999.0, 9999.0, "REG", "CAPITAL")
            ]
            ops_module._save_to_client(client, tier2_rows)

            # Verify original MASSIVE data is preserved
            res = client.execute(
                "SELECT open, close, source FROM market_data WHERE symbol = 'AAPL' AND timestamp = '2026-01-01 10:00:00'"
            )
            row = res.rows[0]
            assert row[0] == 150.0
            assert row[1] == 154.0
            assert row[2] == "MASSIVE"

            # Now verify Tier 1 can update Tier 2
            # Insert Tier 2 for TSLA
            tsla_tier2 = [
                ("2026-01-01 10:00:00", "TSLA", 200.0, 205.0, 199.0, 204.0, 500.0, "REG", "CAPITAL")
            ]
            ops_module._save_to_client(client, tsla_tier2)

            # Overwrite with Tier 1
            tsla_tier1 = [
                ("2026-01-01 10:00:00", "TSLA", 201.0, 206.0, 200.0, 205.0, 600.0, "REG", "BINANCE")
            ]
            ops_module._save_to_client(client, tsla_tier1)

            res_tsla = client.execute(
                "SELECT open, close, source FROM market_data WHERE symbol = 'TSLA' AND timestamp = '2026-01-01 10:00:00'"
            )
            row_tsla = res_tsla.rows[0]
            assert row_tsla[0] == 201.0
            assert row_tsla[2] == "BINANCE"

        finally:
            client.close()

    def test_range_cleanup(self):
        """clear_market_data_for_range must remove only records in the specified range."""
        client = self._new_client()
        try:
            schema_module.init_db(client)

            rows = [
                ("2026-01-01 10:00:00", "AAPL", 150.0, 151.0, 149.0, 150.5, 100.0, "REG", "MASSIVE"),
                ("2026-01-01 11:00:00", "AAPL", 151.0, 152.0, 150.0, 151.5, 100.0, "REG", "MASSIVE"),
                ("2026-01-01 12:00:00", "AAPL", 152.0, 153.0, 151.0, 152.5, 100.0, "REG", "MASSIVE"),
            ]
            ops_module._save_to_client(client, rows)

            # Clean 10:00 to 11:00
            ops_module.clear_market_data_for_range(
                client,
                datetime(2026, 1, 1, 10, 0, 0),
                datetime(2026, 1, 1, 11, 0, 0),
                symbols=["AAPL"]
            )

            res = client.execute("SELECT timestamp FROM market_data WHERE symbol = 'AAPL' ORDER BY timestamp")
            remaining = [str(r[0]) for r in res.rows]
            assert len(remaining) == 2
            assert "2026-01-01 11:00:00" in remaining[0]
            assert "2026-01-01 12:00:00" in remaining[1]

        finally:
            client.close()

    def test_candlestick_resampling(self):
        """query_candlesticks must correctly aggregate 1m bars into 5m candles."""
        client = self._new_client()
        try:
            schema_module.init_db(client)

            bars = [
                ("2026-01-01 10:00:00", "AAPL", 150.0, 151.0, 149.0, 150.5, 100.0, "REG", "MASSIVE"),
                ("2026-01-01 10:01:00", "AAPL", 150.5, 152.0, 150.0, 151.5, 200.0, "REG", "MASSIVE"),
                ("2026-01-01 10:02:00", "AAPL", 151.5, 153.0, 151.0, 152.5, 300.0, "REG", "MASSIVE"),
                ("2026-01-01 10:03:00", "AAPL", 152.5, 153.5, 152.0, 153.0, 400.0, "REG", "MASSIVE"),
                ("2026-01-01 10:04:00", "AAPL", 153.0, 154.0, 152.5, 153.5, 500.0, "REG", "MASSIVE"),
            ]
            ops_module._save_to_client(client, bars)

            df_5m = ops_module.query_candlesticks("AAPL", timeframe="5m", client=client)
            assert len(df_5m) == 1
            candle = df_5m.iloc[0]
            assert candle["open"] == 150.0
            assert candle["high"] == 154.0
            assert candle["low"] == 149.0
            assert candle["close"] == 153.5
            assert candle["volume"] == 1500.0

        finally:
            client.close()

    def test_candlestick_various_intervals_and_filters(self):
        """query_candlesticks must support 1m, 15m, 1h, 1d timeframes and date bounds."""
        client = self._new_client()
        try:
            schema_module.init_db(client)

            # Insert 15 minutes of bars
            bars = []
            for i in range(15):
                bars.append((
                    f"2026-01-01 10:{i:02d}:00",
                    "NVDA",
                    100.0 + i,
                    101.0 + i,
                    99.0 + i,
                    100.5 + i,
                    100.0,
                    "REG",
                    "MASSIVE"
                ))
            ops_module._save_to_client(client, bars)

            # 1m query
            df_1m = ops_module.query_candlesticks("NVDA", timeframe="1m", client=client)
            assert len(df_1m) == 15

            # 15m query
            df_15m = ops_module.query_candlesticks("NVDA", timeframe="15m", client=client)
            assert len(df_15m) == 1
            assert df_15m.iloc[0]["open"] == 100.0
            assert df_15m.iloc[0]["close"] == 114.5
            assert df_15m.iloc[0]["volume"] == 1500.0

            # 1h query
            df_1h = ops_module.query_candlesticks("NVDA", timeframe="1h", client=client)
            assert len(df_1h) == 1

            # Date range filtering
            df_filtered = ops_module.query_candlesticks(
                "NVDA",
                start_time=datetime(2026, 1, 1, 10, 5, 0),
                end_time=datetime(2026, 1, 1, 10, 9, 0),
                timeframe="1m",
                client=client
            )
            assert len(df_filtered) == 5

            # Non-existent symbol
            df_empty = ops_module.query_candlesticks("UNKNOWN_SYM", client=client)
            assert df_empty.empty
            assert list(df_empty.columns) == ["time", "symbol", "open", "high", "low", "close", "volume"]

        finally:
            client.close()

    def test_save_data_to_storage_dataframe(self):
        """save_data_to_storage must successfully persist a pandas DataFrame with NaNs handled."""
        client = self._new_client()
        try:
            schema_module.init_db(client)

            df = pd.DataFrame([
                {"timestamp": pd.Timestamp("2026-01-01 12:00:00"), "symbol": "MSFT", "open": 300.0, "high": 305.0, "low": 299.0, "close": 304.0, "volume": 1000.0, "session": "REG", "source": "CAPITAL"},
                {"timestamp": pd.Timestamp("2026-01-01 12:01:00"), "symbol": "MSFT", "open": 304.0, "high": 306.0, "low": 303.0, "close": 305.0, "volume": float("nan"), "session": "REG", "source": "CAPITAL"}
            ])

            success = ops_module.save_data_to_storage(df, archive_client=client)
            assert success is True

            counts = ops_module.get_session_row_counts(
                client,
                symbols=["MSFT"],
                start_utc=datetime(2026, 1, 1, 12, 0, 0),
                end_utc=datetime(2026, 1, 1, 12, 2, 0)
            )
            assert counts.get("MSFT") == 2

        finally:
            client.close()

    def test_duckdb_result_proxying_and_executemany(self):
        """DuckDBResult must support .rows, .fetchall(), .fetchone(), .df(), and .arrow()."""
        client = self._new_client()
        try:
            schema_module.init_db(client)

            # Test executemany
            sql = "INSERT OR REPLACE INTO symbol_map (display_name, yahoo_ticker, massive_ticker, binance_ticker, capital_ticker) VALUES (?, ?, ?, ?, ?)"
            params = [
                ("TEST1", "T1", None, None, None),
                ("TEST2", "T2", None, None, None),
            ]
            client.executemany(sql, params)
            client.commit()

            res = client.execute("SELECT display_name, yahoo_ticker FROM symbol_map WHERE display_name IN ('TEST1', 'TEST2') ORDER BY display_name")
            assert len(res.rows) == 2
            assert res.fetchall() == res.rows
            assert res.fetchone() == ("TEST1", "T1")

            # Test df()
            df = res.df()
            assert len(df) == 2
            assert list(df["display_name"]) == ["TEST1", "TEST2"]

            # Test arrow()
            arrow_table = res.arrow()
            assert arrow_table.num_rows == 2

        finally:
            client.close()

    def test_get_duckdb_connection_factory_and_fallback(self):
        """get_duckdb_connection must open database and gracefully handle read_only fallback."""
        conn1 = get_duckdb_connection(db_path=self.db_path, read_only=False)
        assert conn1 is not None

        # Requesting read_only on an actively open file falls back to read_only=False seamlessly
        conn2 = get_duckdb_connection(db_path=self.db_path, read_only=True)
        assert conn2 is not None

        conn1.close()
        conn2.close()


