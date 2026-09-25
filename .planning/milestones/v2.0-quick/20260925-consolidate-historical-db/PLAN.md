---
task: consolidate-historical-db
created: 2026-09-25T21:08:45Z
status: complete
completed: 2026-09-25T21:13:00Z
---

# Quick Task: Consolidate Market-Rewind Release Data into historical.duckdb

## Objective
Download `archive_data.db` (Oct 2024 - Dec 2025, ~471MB) and `market_data.db` (2026 data, ~165MB) from GitHub releases of `emadprograms/market-rewind` and consolidate all historical 1-minute market data and symbols into `data/historical.duckdb`.

## Context & Assets
- Source 1: `https://github.com/emadprograms/market-rewind/releases/download/latest-archive/archive_data.db`
- Source 2: `https://github.com/emadprograms/market-rewind/releases/download/latest-data/market_data.db`
- Destination: `data/historical.duckdb` (`market_data` table + `symbol_map` table)

## Execution Steps
1. **Download Assets**:
   - Fetch `archive_data.db` and `market_data.db` to local temporary scratch directory.
2. **Inspect Schemas & Contents**:
   - Examine tables, columns, row counts, and date spans in both SQLite databases.
3. **Consolidate into historical.duckdb**:
   - Merge `symbol_map` entries (insert any missing symbols).
   - Ingest `market_data` rows with Source-Tiering and primary key de-duplication (`timestamp`, `symbol`).
4. **Validation & Verification**:
   - Verify total consolidated rows in `data/historical.duckdb`.
   - Verify date span spans from October 2024 through 2026.
   - Run data integrity checks and automated test suite.
5. **Cleanup**:
   - Remove temporary downloaded `.db` files from disk.
   - Update `SUMMARY.md` and `STATE.md`.
