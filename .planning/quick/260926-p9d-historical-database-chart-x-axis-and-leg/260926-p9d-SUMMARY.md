---
status: complete
quick_id: 260926-p9d
date: 2026-09-26
commit: 70b7bc5
description: Historical Database chart X-axis and legend must open at 09:30 ET, not 13:30
---

# Quick Task Summary: 260926-p9d

## Task Overview
The **🏛️ Historical Database (Archive)** page charted the first bar — and the huge opening volume
spike — at `13:30` on the X-axis and in the hover legend, while the UI labelled everything `ET`.
NYSE/NASDAQ regular trading opens at **09:30 America/New_York**, which is where the opening-bell
volume spike belongs. `13:30` is the raw UTC storage value of the 09:30 EDT open, so the
UTC → exchange-time conversion was silently not happening.

## Root Cause
`src/dashboard/analytics.py` converted storage UTC to exchange time with:

```sql
((timestamp AT TIME ZONE 'UTC') AT TIME ZONE 'America/New_York')::TIMESTAMP
```

That expression is **not deterministic**:

1. **Column type** — over a `TIMESTAMPTZ` column the first hop produces a *naive UTC wall clock*
   that the second hop then re-interprets as New York time (a double conversion).
2. **Session `TimeZone`** — DuckDB inherits it from the host OS and uses it to resolve the trailing
   `TIMESTAMPTZ -> TIMESTAMP` cast.

On a US-Eastern host with a tz-aware archive the two effects cancel out and raw UTC reaches the
browser (`13:30` labelled `ET`, volume spike included); the same archive on a UTC host renders
`17:30`. The failure mode therefore moved with the machine's timezone.

The front-end compounded it: `chart.js` formatted `candle.time` with `toISOString()` / `getUTC*()`
and appended `" ET"` — correct only while the backend deliberately shifted the epoch, an implicit
contract nothing enforced.

## Key Changes
1. **Exchange-time layer (`src/dashboard/analytics.py`)**
   - New helpers: `EXCHANGE_TZ`, `TIME_EPOCH_BASIS`, `detect_timestamp_column_type()`,
     `is_tz_aware_column()`, `build_utc_instant_sql()`, `build_exchange_local_sql()`,
     `build_utc_epoch_sql()`, `build_instant_from_exchange_local_sql()`,
     `build_timestamp_range_clause()`.
   - The conversion is built from the *physical* column type with explicit `timezone()` calls, so
     `TIMESTAMP` (canonical naive UTC) and `TIMESTAMPTZ` (legacy backfill shape) archives render
     identically and no implicit, session-timezone-dependent cast remains.
   - Contract now explicit: `time` = **true UTC epoch seconds**, `time_str` = NYSE wall clock,
     plus `timezone`, `time_epoch_basis` and `storage_timestamp_type` in the payload.
   - `time_bucket()` still buckets on the exchange-local clock, so `1h`/`4h`/`1D` bars snap to NYSE
     hour/day edges and daily bars anchor at exchange midnight (04:00 UTC in EDT, 05:00 UTC in EST).
   - `start`/`end` range filters keep their UTC wall-clock semantics for both storage shapes.
   - Same treatment for `get_streaming_candles()` (tick-to-bar resampling from `streaming.duckdb`).
2. **Deterministic connections (`src/database/connection.py`)**
   - `SESSION_TIMEZONE = "UTC"` pinned with `SET TimeZone` on every connection (read-only included),
     so no query in the codebase depends on the host OS timezone.
3. **Front-end (`src/dashboard/static/js/chart.js`)**
   - X-axis ticks, crosshair badge and hover legend are formatted through `Intl.DateTimeFormat` with
     an explicit `timeZone` adopted from the API's `timezone` field; all `toISOString()` /
     `getUTC*()` labelling removed.
   - Date tick marks are drawn only at exchange-local midnight, so a UTC-midnight bar can no longer
     inject a stray date label in the middle of an ET session.
   - Candles indexed by epoch for O(1) crosshair lookups; `legend-tz-badge` reflects the API timezone.
4. **Consistency & hygiene**
   - `tools/audit_database_integrity.py`: fragile ET conversion replaced with the same type-aware
     expression; session timezone pinned.
   - `src/database/operations.py`: documented that `query_candlesticks()` stays raw UTC and that the
     dashboard uses the analytics layer for exchange-local rendering.
   - `src/dashboard/static/index.html`: static asset cache-buster bumped to `?v=nyse-et`.
   - `requirements.txt`: added `psutil` (imported by `src/dashboard/analytics.py`).
   - `README.md`: documented the UTC storage mandate and the NYSE exchange-time rendering contract.

## Verification
- **New suite** `tests/dashboard/test_chart_timezone_determinism.py` (37 tests):
  - opening bell renders `09:30:00` and the opening volume spike sits on that bar — for both storage
    shapes (`TIMESTAMP`, `TIMESTAMPTZ`) and for EDT (13:30 UTC) and EST (14:30 UTC) sessions;
  - SQL conversion is identical under session timezones `Etc/UTC`, `America/New_York`,
    `Asia/Bahrain`, `Europe/London`;
  - subprocess probes with `TZ=UTC|America/New_York|Asia/Bahrain` (host timezone) return 09:30 for
    both storage shapes — the exact reported symptom;
  - daily buckets anchor at exchange midnight, intraday buckets keep exchange-local labels,
    candle times stay unique and ascending (Lightweight Charts requirement);
  - `DuckDBClient` pins the session timezone to UTC (read-write and read-only);
  - front-end static contract: explicit IANA timezone formatting, no `toISOString()`/`getUTCHours()`.
- **Full suite**: `PYTHONPATH=. pytest tests -q` → **262 passed, 5 skipped** (was 225 passed).
- **End-to-end over HTTP** against a `TIMESTAMPTZ` archive with the server running under
  `TZ=America/New_York` (the reported scenario): `/api/candles` returns
  `time_str=2026-07-15 09:30:00`, `time=1784122200` (13:30 UTC), `storage_timestamp_type=TIMESTAMP WITH TIME ZONE`,
  and the real front-end modules render axis `09:30`, crosshair `2026-07-15 09:30:00 ET` and legend
  `09:30:00 ET` for the opening bar and the volume spike — in browser timezones UTC, Asia/Tokyo,
  America/Los_Angeles and Asia/Bahrain alike.
