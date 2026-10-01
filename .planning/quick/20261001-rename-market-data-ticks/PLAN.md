# Quick Task: Rename market_data to minute_data and ticks to tick_data

## Objective
Rename the main data tables in both DuckDB databases:
- `data/historical.duckdb`: `market_data` -> `minute_data`
- `data/streaming.duckdb`: `ticks` -> `tick_data`
Maintain backward-compatible views and write redirects so no existing queries or callers break.

## Tasks
1. Update `src/database/schema.py` to create `minute_data` and `tick_data` base tables with migration from existing tables and backward-compatible views.
2. Update `src/database/connection.py` to auto-redirect write queries to the new base tables.
3. Update `src/database/operations.py`, `src/dashboard/analytics.py`, `src/utils/integrity.py`, `src/data/harvester.py`, `src/data/databento_backfill.py`, and `src/dashboard/server.py` to use `minute_data` and `tick_data`.
4. Run migration on `data/historical.duckdb` and `data/streaming.duckdb`.
5. Update tests and verify full suite passes.
6. Commit and push to git.
