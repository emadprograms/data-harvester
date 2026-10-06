---
phase: 51
plan: 01
title: Computer-offline gap fill
status: complete
completed_at: 2026-10-06T12:00:00Z
requirements-completed: [QFILL-01, QFILL-02, QFILL-03, QFILL-04, QFILL-05]
---

# Phase 51 Summary: Computer-offline gap fill

## Accomplishments

1. **QFILL-01**: `python -m src.data.gap_fill --date YYYY-MM-DD` fills exactly one named day. Only all-symbol silence inside 04:00–20:00 ET is requested.
2. **QFILL-02**: Pre/post silence counts at 15 minutes; regular hours at 2 minutes. Stretches that cross 09:30 or 16:00 ET are split.
3. **QFILL-03**: Weekends, full NYSE holidays (including 2025-01-09), and time after an official 13:00 ET early close are not requested.
4. **QFILL-04**: Requests use Databento `tbbo`. Returned bid/ask are stored as schema v2. Sparse or empty success is recorded and not requested again.
5. **QFILL-05**: Gap fill refuses if the live writer holds the publisher lock, and otherwise holds that lock through download and publish.

Dead 09:00–16:00 whole-day helpers (`get_day_trading_bounds`, `estimate_day_cost`, `fetch_and_normalize_day`, `is_day_already_backfilled`) were removed. Cost is `estimate_interval_cost` on the selected stretches.

## Verification

- `tests/data/test_phase51_gap_fill.py`: 20 passed.
- `tests/data/test_databento.py`: 6 passed.

## Not done here

The production lake was not opened and was not rewritten. Gap fill was not run against it.
