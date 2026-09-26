# Quick Task Summary: Separate Historical and Streaming Symbol Maps

**Task ID:** `260926-y2k`
**Status:** complete
**Date:** 2026-09-26

## Objective
Split the monolithic `symbol_map` concept into two distinct catalogs:
1. `historical.duckdb`: `historical_symbol_map` table (with legacy `symbol_map` view preserved for full backward compatibility) containing all 40 assets (equities, ETFs, crypto, commodities).
2. `streaming.duckdb`: `streaming_symbol_map` table containing strictly allowed live streaming instruments (the 19 core single-stock equities), ensuring excluded assets (ETFs, commodities, crypto) are never stored in `streaming.duckdb`.

## Key Changes
1. **Schema Initialization (`src/database/schema.py`)**:
   - `init_historical_db`: Creates `historical_symbol_map` table and creates `VIEW symbol_map AS SELECT * FROM historical_symbol_map`. Migrates legacy tables seamlessly if needed.
   - `init_streaming_db`: Creates and seeds `streaming_symbol_map` with 19 single-stock assets (`AAPL`, `ADBE`, `AMD`, `AMZN`, `APP`, `AVGO`, `BABA`, `GOOGL`, `META`, `MSFT`, `MU`, `NDAQ`, `NVDA`, `ORCL`, `PANW`, `QCOM`, `SHOP`, `TSLA`, `TSM`).
2. **Operations API (`src/database/operations.py`)**:
   - Added `get_historical_symbol_map_from_db`, `get_symbol_map_from_db` (backward-compatible alias), `get_symbol_inventory_list`, `add_symbol_to_db`, `remove_symbol_from_db`.
   - Added `get_streaming_symbol_map_from_db`, `get_streaming_symbol_inventory_list`, `add_streaming_symbol_to_db`, `remove_streaming_symbol_from_db`.
3. **Stream Ingestion Filtering (`src/stream/runner.py`)**:
   - Live stream runner loads `streaming_symbol_map`.
   - In `_handle_capital_tick`, checks `if display_symbol not in self.active_streaming_symbols: return`. Any incoming tick from Capital.com for an excluded asset (e.g. SPY, US30, BTC) is strictly dropped before queueing/writing to DuckDB.
4. **Databento Alignment (`src/data/databento_backfill.py`)**:
   - `get_target_stock_symbols()` now reads from `streaming_symbol_map`, defaulting to the 19 single-stock equities.
5. **Dashboard Server & API (`src/dashboard/server.py`)**:
   - Added REST endpoints: `/api/streaming/symbols` (GET/POST/DELETE) and `/api/historical/symbols`.
   - Added dynamic port fallback and CLI `--port` argument (running smoothly on port 8001 when 8000 is occupied).
6. **Audit Tool (`tools/audit_database_integrity.py`)**:
   - Updated audit checks to verify `historical_symbol_map` (40 assets), `symbol_map` view (40 assets), and `streaming_symbol_map` (19 assets). Both databases pass 100%.
7. **Automated Tests (`tests/test_symbol_maps_separation.py`)**:
   - 5 dedicated unit and integration tests verifying schema, CRUD isolation, streaming runner drop behavior, and backfill target list.

## Verification
- `PYTHONPATH=. .venv/bin/pytest tests/test_symbol_maps_separation.py -v`: 5/5 PASSED.
- `tools/audit_database_integrity.py`: 100% PASS for both `historical.duckdb` and `streaming.duckdb`.
