# Quick Task Summary: Rename market_data to minute_data and ticks to tick_data

## Overview
Renamed the primary market data and tick tables across both DuckDB databases to clarify intent and enforce database separation:
- `data/historical.duckdb`: Base table renamed from `market_data` to `minute_data`. Added `market_data` view for seamless backward compatibility.
- `data/streaming.duckdb`: Base table renamed from `ticks` to `tick_data`. Added `ticks` and `streaming_ticks` views for seamless backward compatibility.

## Execution Details
1. **Live DuckDB Storage Migrations**:
   - `data/historical.duckdb`: Dropped secondary indexes on `market_data`, executed `ALTER TABLE market_data RENAME TO minute_data`, recreated indexes `idx_minute_data_ts` and `idx_minute_data_sym_ts`, and created view `market_data AS SELECT * FROM minute_data`. (Verified 8,900,258 rows accessible).
   - `data/streaming.duckdb`: Dropped obsolete empty `streaming_ticks` table and secondary indexes on `ticks`, executed `ALTER TABLE ticks RENAME TO tick_data`, recreated indexes `idx_tick_data_ts` and `idx_tick_data_sym_ts`, and created views `ticks` and `streaming_ticks`. (Verified 103,978,454 rows accessible).

2. **Schema & Write Redirection Layer**:
   - `src/database/schema.py`: Updated `init_historical_db` and `init_streaming_db` to automatically detect legacy tables, execute index drop + rename + index recreation + view creation.
   - `src/database/connection.py`: Added regex-based query redirection in `DuckDBClient.execute()` and `executemany()` to rewrite write operations (`INSERT INTO market_data` -> `minute_data`, `INSERT INTO ticks` -> `tick_data`), preventing `Catalog Error: <view> is not a table`.

3. **Application Queries & Analytics**:
   - `src/database/operations.py`: Updated query and storage functions (`clear_market_data_for_range`, `_save_to_client`, `query_candlesticks`, `save_ticks_to_storage`, `query_ticks`, `query_candlesticks_from_ticks`) to use `minute_data` and `tick_data`.
   - `src/dashboard/analytics.py`: Implemented dynamic table name resolution (`minute_data` fallback to `market_data`, `tick_data` fallback to `ticks`) across `get_historical_candles`, `get_streaming_candles`, `get_historical_overview`, `get_symbols_coverage`, `get_stream_tape`, `get_ticks`, and `get_stream_status`.
   - `src/utils/integrity.py`: Updated fingerprinting, quiet intervals, gap detection, and health checks to `minute_data` and `tick_data`.
   - `src/data/harvester.py`: Updated missing historical data check to `minute_data`.
   - `src/data/databento_backfill.py`: Updated TBBO ingestion to target `tick_data` with dynamic fallback.
   - `src/dashboard/server.py`: Updated max timestamp discovery to `minute_data`.

4. **Testing & Verification**:
   - `tests/test_database_exclusivity.py`: Updated table assertions to verify `minute_data` and `tick_data` presence along with backward-compatible views.
   - Full test suite execution: **295 passed, 4 skipped, 0 failed** in 31.32s.
