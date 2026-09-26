---
status: complete
quick_id: 260925-vhi
date: 2026-09-25
commit: 2acfc40b
description: Separate historical and streaming databases on dashboard
---

# Quick Task Summary: 260925-vhi

## Task Overview
Separated the historical database (`data/historical.duckdb`) and streaming database (`data/streaming.duckdb`) across backend APIs and the web dashboard UI, eliminating artificial blending of streaming ticks into historical candles.

## Key Changes
1. **Backend Candle Query Decoupling (`src/dashboard/analytics.py`)**:
   - Removed automatic appending of streaming ticks into `get_candles()`.
   - Created `get_historical_candles()` for querying exclusively `historical.duckdb`.
   - Created `get_streaming_candles()` for querying dynamic resampled candles from `streaming.duckdb` ticks with `tick_count` reporting.
   - Updated `get_candles()` router with `db_source` parameter (`historical` vs `streaming`).
   - Added `get_historical_overview()` returning total canonical rows, date bounds, and provider source-tier distribution.
2. **REST API Extensions (`src/dashboard/server.py`)**:
   - Supported `?source=historical` and `?source=streaming` query parameters on `/api/candles`.
   - Added dedicated `/api/historical/candles` and `/api/streaming/candles` endpoints.
   - Added `/api/historical/overview` endpoint.
3. **Frontend Dashboard UI Redesign (`src/dashboard/static/index.html`)**:
   - Reorganized top navigation tabs into dedicated first-class observatories:
     - `🏛️ Historical Database (Archive)`
     - `⚡ Streaming Database (Live Buffer)`
     - `📋 Historical Symbol Coverage`
     - `🛡️ Data Integrity & Gaps`
     - `🚀 REST Harvester Runner`
   - Added interactive Database Source Switcher to chart toolbar:
     - `[ 🏛️ Historical Archive (historical.duckdb) ]` (default)
     - `[ ⚡ Live Stream Buffer (streaming.duckdb) ]`
   - Added clear architectural notices explaining the difference between the permanent backfillable historical archive and the live ephemeral streaming buffer.
   - Added `legend-db-badge` and dynamic table columns (`Session` vs `Ticks`) in the Raw Candle Inspector.
   - Fixed KPI strip to display historical database metrics (8,879,381 rows, Oct 2024 → Sep 2026).
4. **Automated Testing (`tests/test_dashboard_separated_observatories.py`)**:
   - 5/5 new tests covering database query isolation, routing, and overview metadata.
   - 187/187 tests passing across the entire test suite.

## Verification
- Queried `/api/historical/overview`: 8.88M rows, 40 symbols, 83.4% Massive, 8.7% Binance.
- Queried `/api/historical/candles`: Returns pure historical rows with zero streaming ticks.
- Queried `/api/streaming/candles`: Returns pure resampled ticks with tick counts.
- Full pytest test suite: 187 passed in 23.67s.
