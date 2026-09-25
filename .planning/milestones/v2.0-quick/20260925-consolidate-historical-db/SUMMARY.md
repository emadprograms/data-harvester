---
task: consolidate-historical-db
created: 2026-09-25T21:08:45Z
completed: 2026-09-25T21:13:00Z
status: complete
---

# Quick Task Summary: Consolidate Market-Rewind Release Data into historical.duckdb

## Executive Summary
Successfully downloaded release assets from GitHub repository `emadprograms/market-rewind` (`archive_data.db` and `market_data.db`) and consolidated them with existing `data/historical.duckdb` into a unified, high-performance dataset with Source-Tiering deduplication.

## Source Assets Consolidated
1. **`archive_data.db`** (471.62 MB):
   - 4,056,733 rows of 1-minute historical candles (100% MASSIVE source).
   - Date range: `2024-10-01 08:00:00` to `2026-01-31 00:59:00`.
2. **`market_data.db`** (165.25 MB):
   - 1,378,040 rows of 1-minute historical candles across 40 symbols.
   - Date range: `2025-08-30 00:00:00` to `2026-04-12 19:33:00`.
3. **`data/historical.duckdb`** (pre-existing):
   - 3,949,885 rows of 1-minute historical candles.

## Final Consolidated Dataset Metrics
- **Destination Database**: `data/historical.duckdb` (and legacy link `data/market_data.duckdb`).
- **Total Consolidated Rows**: **7,993,726** 1-minute OHLCV candles (deduplicated with Source-Tiering precedence: MASSIVE/BINANCE > CAPITAL > YAHOO).
- **Date Range**: **`2024-10-01 08:00:00`** to **`2026-07-28 03:54:00`** (covering over 21 continuous months).
- **Distinct Symbols**: **40** symbols.
- **Database File Size**: **920.26 MB**.
- **Consolidation Duration**: **11.64 seconds** using DuckDB's native SQLite vectorized engine.

## Verification & Status
- Live web dashboard (`http://localhost:8000/api/status`) verified: returns `HEALTHY` status and displays 7,993,726 rows.
- Automated Test Suite: **163 passed, 0 failures** in 19.59s.
- Temporary download files purged from disk.
