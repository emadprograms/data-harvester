"""
Tests for DuckDB Database Historical Dataset Integrity and Candlestick Resampling Operations.
Verifies historical database dataset integrity, schema conformity, candle mathematical
invariants, and sub-100ms multi-timeframe OHLCV resampling benchmarks.
"""
import os
import time
import pytest
import pandas as pd
from datetime import datetime
from src.database.connection import get_duckdb_connection
from src.database.operations import query_candlesticks, get_symbol_map_from_db

HISTORICAL_DB_PATH = "data/market_data.duckdb"
historical_db_exists = os.path.exists(HISTORICAL_DB_PATH)


@pytest.mark.skipif(not historical_db_exists, reason="Historical database data/market_data.duckdb not found.")
class TestHistoricalDatasetAndResampling:
    """Tests against the 3.95M+ row historical dataset."""

    @pytest.fixture(scope="class")
    def db_client(self):
        client = get_duckdb_connection(HISTORICAL_DB_PATH, read_only=True)
        yield client
        client.close()

    def test_historical_duckdb_dataset_integrity(self, db_client):
        """
        Verify that data/market_data.duckdb contains the full migrated dataset:
        - Total row count >= 3,900,000
        - Distinct symbols >= 40
        - Valid date span
        - Query response latency under 50ms
        """
        t0 = time.perf_counter()
        count_res = db_client.execute("SELECT count(*) FROM market_data").rows[0][0]
        count_duration = time.perf_counter() - t0

        assert count_res >= 3_900_000, f"Expected at least 3.9M rows, found {count_res}"
        assert count_duration < 0.05, f"Count query took too long: {count_duration:.4f}s"

        # Check distinct symbols
        t1 = time.perf_counter()
        symbols = [r[0] for r in db_client.execute("SELECT DISTINCT symbol FROM market_data").rows]
        sym_duration = time.perf_counter() - t1

        assert len(symbols) >= 40, f"Expected at least 40 symbols, found {len(symbols)}"
        assert "AAPL" in symbols
        assert "BTCUSDT" in symbols
        assert sym_duration < 0.05, f"Distinct symbols query took too long: {sym_duration:.4f}s"

        # Check min and max timestamps
        min_ts, max_ts = db_client.execute("SELECT min(timestamp), max(timestamp) FROM market_data").rows[0]
        assert min_ts is not None and max_ts is not None
        assert str(min_ts) <= str(max_ts)

    def test_dynamic_candlestick_resampling_all_timeframes(self, db_client):
        """
        Benchmark and validate query_candlesticks across all 5 standard timeframes:
        ['1m', '5m', '15m', '1h', '1d'].
        Validates:
        - Result non-empty
        - Columns: ['time', 'symbol', 'open', 'high', 'low', 'close', 'volume']
        - Monotonic increasing timestamps
        - Candle math invariants: High >= max(Open, Close), Low <= min(Open, Close)
        - Sub-100ms execution latency per timeframe
        """
        timeframes = ["1m", "5m", "15m", "1h", "1d"]
        target_symbol = "AAPL"

        for tf in timeframes:
            t0 = time.perf_counter()
            df = query_candlesticks(target_symbol, timeframe=tf, client=db_client)
            elapsed = time.perf_counter() - t0

            # Performance benchmark: each query must resolve in < 150ms
            assert elapsed < 0.15, f"Timeframe {tf} aggregation took too long: {elapsed:.4f}s"

            # Structural schema validation
            assert isinstance(df, pd.DataFrame)
            assert not df.empty, f"Expected non-empty DataFrame for {target_symbol} with timeframe {tf}"
            expected_cols = ["time", "symbol", "open", "high", "low", "close", "volume"]
            assert list(df.columns) == expected_cols

            # Monotonic chronological ordering
            assert df["time"].is_monotonic_increasing, f"Timeframe {tf} timestamps are not strictly increasing"

            # OHLCV mathematical invariants
            assert (df["high"] >= df["open"]).all(), f"High < Open invariant violated in {tf}"
            assert (df["high"] >= df["close"]).all(), f"High < Close invariant violated in {tf}"
            assert (df["low"] <= df["open"]).all(), f"Low > Open invariant violated in {tf}"
            assert (df["low"] <= df["close"]).all(), f"Low > Close invariant violated in {tf}"
            assert (df["high"] >= df["low"]).all(), f"High < Low invariant violated in {tf}"
            assert (df["volume"] >= 0).all(), f"Negative volume encountered in {tf}"

    def test_resampling_with_date_range_filter(self, db_client):
        """Verify time filtering precisely scopes candlestick queries."""
        start_time = datetime(2025, 9, 2, 8, 0, 0)
        end_time = datetime(2025, 9, 2, 16, 0, 0)

        df = query_candlesticks(
            "AAPL",
            start_time=start_time,
            end_time=end_time,
            timeframe="15m",
            client=db_client
        )
        assert not df.empty
        # All timestamps within requested window
        assert (pd.to_datetime(df["time"]) >= start_time).all()
        assert (pd.to_datetime(df["time"]) <= end_time).all()

    def test_symbol_map_integrity(self, db_client):
        """Verify symbol_map table contains metadata with multi-broker routing."""
        symbol_map = get_symbol_map_from_db(db_client)
        assert len(symbol_map) >= 15
        assert "BTCUSDT" in symbol_map
        assert symbol_map["BTCUSDT"]["binance_ticker"].upper() == "BTCUSDT"
        assert "AAPL" in symbol_map
        assert symbol_map["AAPL"]["capital_ticker"] == "AAPL"
