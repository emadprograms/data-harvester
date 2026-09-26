# Quick Task 260926-y2k: Separate Historical and Streaming Symbol Maps

## Description
Separate the monolithic `symbol_map` into two distinct, dedicated catalog tables:
1. `historical_symbol_map` in `data/historical.duckdb` for the REST API and historical dataset.
2. `streaming_symbol_map` in `data/streaming.duckdb` for the real-time streaming engine (Capital.com, Databento), ensuring `streaming.duckdb` only subscribes to and saves data for explicitly allowed streaming assets (filtering out all excluded assets like ETFs, crypto, commodities).

## Tasks
1. **Schema & Migration (`src/database/schema.py`)**:
   - In `historical.duckdb`: migrate `symbol_map` table to `historical_symbol_map` (with a backward-compatible view `symbol_map`).
   - In `streaming.duckdb`: create `streaming_symbol_map (display_name VARCHAR PRIMARY KEY, capital_ticker VARCHAR, databento_ticker VARCHAR, binance_ticker VARCHAR, is_active BOOLEAN)` initialized with the 19 single-stock equities.
2. **Operations API (`src/database/operations.py`)**:
   - Provide `get_historical_symbol_map_from_db()` (with `get_symbol_map_from_db` alias for backward compatibility).
   - Provide `get_streaming_symbol_map_from_db()`, `add_or_update_streaming_symbol()`, and `delete_streaming_symbol_from_db()`.
3. **Stream Runner Excluded Asset Filtering (`src/stream/runner.py`)**:
   - Discover symbols from `get_streaming_symbol_map_from_db()`.
   - In `_handle_capital_tick` / queue, drop ticks for any symbol not present in `streaming_symbol_map` to guarantee zero excluded asset writes to `streaming.duckdb`.
4. **Databento Backfiller Alignment (`src/data/databento_backfill.py`)**:
   - Query `streaming_symbol_map` from `streaming.duckdb` as the source of truth for target symbols.
5. **Dashboard Endpoints & Tooling (`src/dashboard/server.py`, `tools/audit_database_integrity.py`)**:
   - Add `/api/streaming/symbols` endpoint.
   - Update `tools/audit_database_integrity.py` to audit both `historical_symbol_map` and `streaming_symbol_map`.
6. **Tests & Verification (`tests/test_symbol_maps_separation.py`)**:
   - Test independent CRUD on both tables.
   - Test tick filtering drops excluded assets.
   - Run full 225+ test suite.
