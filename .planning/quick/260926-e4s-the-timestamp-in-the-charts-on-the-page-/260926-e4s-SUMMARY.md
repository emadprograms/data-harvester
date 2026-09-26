---
status: complete
quick_id: 260926-e4s
date: 2026-09-26
commit: pending
description: Set chart and table timestamps on Historical Database page to US Eastern / NYSE stock exchange time
---

# Quick Task Summary: 260926-e4s

## Task Overview
Aligned timestamps across the Historical Database page (`tab-charts`), interactive candlestick charts, crosshair tooltips, hover legends, raw candle inspector table, and CSV exports to US Eastern Timezone (`America/New_York`), corresponding to the official New York Stock Exchange (NYSE) trading clock (09:30 market open, 16:00 market close, pre-market, and post-market).

## Key Changes
1. **Backend Analytics Engine (`src/dashboard/analytics.py`)**:
   - Converted DuckDB `timestamp` fields in `get_historical_candles` using `((timestamp AT TIME ZONE 'UTC') AT TIME ZONE 'America/New_York')` for both raw 1-minute bars and multi-interval time buckets (`time_bucket`).
   - Aligned dynamic timeframe resampling (`1m`, `5m`, `15m`, `30m`, `1h`, `4h`, `1d`) to NYSE exchange calendar boundaries (midnight-to-midnight NYSE trading days and regular session hours).
   - Converted `get_streaming_candles` queries to also resample in `America/New_York` timezone.
   - Added `"timezone": "America/New_York"` to candle API responses.

2. **Frontend Chart Engine (`src/dashboard/static/js/chart.js`)**:
   - Configured TradingView Lightweight Charts `localization.timeFormatter` for the crosshair time badge to display formatted time in US Eastern Time (`YYYY-MM-DD HH:mm ET`).
   - Added `timeScale.tickMarkFormatter` to format timescale X-axis labels cleanly as NYSE hours (`HH:mm`), days (`MMM D`), months (`MMM`), and years (`YYYY`).
   - Updated `updateLegend()` to render the hovered or latest candle timestamp with the `ET` timezone designation (`${candle.time_str} ET`).
   - Updated source notice banner to explicitly declare `US Eastern Time (NYSE: EST/EDT)`.

3. **Frontend Dashboard UI & Table Inspector (`src/dashboard/static/index.html`, `src/dashboard/static/js/tables.js`)**:
   - Added `NYSE (ET)` badge in the chart legend bar for instant visual confirmation.
   - Updated Raw Candle Inspector table headers from `Timestamp (UTC)` to `Timestamp (US/Eastern)`.
   - Updated table row timestamps to include `ET` badge (`<span class="text-[10px] text-slate-500 font-bold">ET</span>`).
   - Updated CSV export headers to `"Timestamp (US/Eastern)"`.

4. **Automated Testing (`tests/dashboard/test_chart_timezone.py`)**:
   - Added 5 new automated tests verifying:
     - Historical and streaming candle responses include `"timezone": "America/New_York"`.
     - Regular session (REG) candles strictly conform to NYSE market hours (09:30:00 to 16:00:00).
     - Multi-timeframe aggregated candles (1h, 1d) align with NYSE day and hour boundaries.
     - Static assets contain NYSE badges, time formatters, and table headers.
   - All 230 tests in the repository pass cleanly with zero regressions.

## Verification
- Queried regular session candles: First bar is `09:30:00` (NYSE open bell) and final bar is `16:00:00` (NYSE closing bell).
- Daily bars align to `00:00:00` of the NYSE calendar date.
- Pytest suite: 230 passed in 26.89s.
