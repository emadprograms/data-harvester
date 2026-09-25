# Phase 5 Plan: Dedicated Dual-DuckDB Storage Layer

**Goal:** Decouple database storage into 100% separate dedicated DuckDB files (`data/historical.duckdb` and `data/streaming.duckdb`) with zero write-lock contention, independent connection factories, dedicated raw tick schema, and cross-database query ATTACH support.

**Requirements:** DUAL-01, DUAL-02, DUAL-03

## Tasks

### Task 1: Connection Architecture Refactoring (`src/database/connection.py`)
- Define `DEFAULT_HISTORICAL_DB_PATH` (`data/historical.duckdb`) and `DEFAULT_STREAMING_DB_PATH` (`data/streaming.duckdb`).
- Provide independent connection factories:
  - `get_historical_db_connection(db_path=None, read_only=False)`
  - `get_streaming_db_connection(db_path=None, read_only=False)`
  - Backward compatible alias: `get_duckdb_connection` and `get_archive_db_connection` mapping to historical DB.
- Provide `attach_streaming_to_historical(conn, streaming_path=None)` and `get_unified_connection()` helper for cross-database querying.

### Task 2: Dual Database Schema Initialization (`src/database/schema.py`)
- `init_historical_db(client=None)`:
  - Initializes `symbol_map` table and default tickers.
  - Initializes `market_data` table (`timestamp`, `symbol`, `open`, `high`, `low`, `close`, `volume`, `session`, `source`) with primary key `(symbol, timestamp)` and time-series indexes.
- `init_streaming_db(client=None)`:
  - Initializes `streaming_ticks` table (`timestamp TIMESTAMP`, `symbol VARCHAR`, `bid DOUBLE`, `ask DOUBLE`, `price DOUBLE NOT NULL`, `volume DOUBLE DEFAULT 1.0`, `source VARCHAR DEFAULT 'CAPITAL'`).
  - Creates indexes on `timestamp` and `(symbol, timestamp)`.
- `init_db(historical_client=None, streaming_client=None)`:
  - Initializes both databases cleanly.

### Task 3: Streaming Operations & Raw Tick Management (`src/database/operations.py`)
- Implement `save_ticks_to_streaming_db(ticks, client=None, logger=None)`:
  - Batched insertion of raw tick quotes into `streaming_ticks`.
  - Cleans NaN/inf values and executes fast batch inserts.
- Implement `query_streaming_ticks(symbol, start_time=None, end_time=None, limit=None, client=None)`:
  - Returns raw tick quotes as a pandas DataFrame.
- Implement `query_streaming_candlesticks(symbol, start_time=None, end_time=None, timeframe="1m", client=None)`:
  - Aggregates raw tick quotes on the fly using DuckDB's native `time_bucket()`:
    - `open = first(price ORDER BY timestamp)`
    - `high = max(price)`
    - `low = min(price)`
    - `close = last(price ORDER BY timestamp)`
    - `volume = sum(volume)`
- Maintain all existing historical operations (`save_data_to_storage`, `query_candlesticks`, `clear_market_data_for_range`, `get_symbol_map_from_db`).

### Task 4: Comprehensive Test Suite (`tests/test_database_dual_duckdb_storage.py`)
- Test separate database file creation and path overrides.
- Test isolated connection pooling and concurrency (writing to streaming while reading from historical).
- Test `streaming_ticks` table schema and index creation.
- Test batch tick insertion and validation.
- Test raw tick query filtering by time and symbol.
- Test dynamic candlestick resampling on raw ticks via `time_bucket()`.
- Test cross-database `ATTACH` query execution.
- Test backward-compatible aliases.

### Task 5: Execution & Verification
- Run full pytest test suite to ensure zero regressions across old and new tests.
- Produce `05-SUMMARY.md`.
