# Phase 7 Plan: Data Integrity & Health Engine

**Goal:** Build a robust, automated data integrity auditing engine in `src/utils/integrity.py` capable of detecting missing time intervals/gaps, streaming quiet periods, abnormal OHLCV price action, and cross-database price drift between REST and streaming tables.

**Requirements:** INTG-01, INTG-02, INTG-03, INTG-04

## Tasks

### Task 1: 1-Minute Historical Gap Detection (`detect_1m_gaps`)
- Implements gap scanning over historical market data in `data/historical.duckdb`.
- Flags missing 1m bars during Regular Trading Hours (09:30–16:00 ET for equities) and continuous intervals for crypto.
- Returns list of gap intervals, total missing minutes, and percentage coverage score.

### Task 2: Live Stream Continuity & Quiet Interval Monitor (`detect_stream_quiet_intervals`)
- Scans `ticks` in `data/streaming.duckdb` for quiet intervals where no ticks were recorded for > threshold seconds.
- Computes stream latency (`seconds_since_last_tick`) and stalled status per symbol.

### Task 3: OHLCV Anomaly & Sanity Validator (`validate_ohlcv_anomalies`)
- Validates logical OHLC bounds (`high >= low`, `high >= open`, `high >= close`, `low <= open`, `low <= close`).
- Detects non-positive prices (`price <= 0`), negative volume, NaN/nulls, and extreme 1-minute price jumps.

### Task 4: Cross-Database Price Drift Analyzer (`analyze_price_drift`)
- Cross-queries `historical.duckdb` and `streaming.duckdb` using multi-database `ATTACH`.
- Resamples streaming ticks into 1-minute bars and compares close prices against canonical REST candles.
- Computes Mean Absolute Drift, maximum drift, timestamp of maximum drift, and tolerance pass/fail.

### Task 5: Database Health Report (`get_database_health_report`)
- Aggregates file existence, file size (MB), row counts, min/max timestamps across both DuckDB databases.
- Produces overall health indicator (`HEALTHY`, `DEGRADED`, `CRITICAL`).

### Task 6: Comprehensive Test Suite (`tests/test_data_integrity_health_engine.py`)
- Test gap detection with known missing intervals.
- Test stream quiet interval detection with artificial quiet gaps.
- Test anomaly detection with deliberate bad rows (high < low, negative prices, nulls).
- Test cross-database drift analyzer with matching vs drifting price feeds.
- Test database health report calculation.

### Task 7: Verification & Regression Testing
- Run full pytest test suite to ensure 100% pass rate.
- Produce `07-SUMMARY.md`.
