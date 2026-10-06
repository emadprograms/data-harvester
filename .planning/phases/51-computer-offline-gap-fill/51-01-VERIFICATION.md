# Phase 51 verification

**Date:** 2026-10-06

## Evidence

`tests/data/test_phase51_gap_fill.py` and `tests/data/test_databento.py`: 26 passed. Phase 50 quote tests still pass.

- QFILL-01: a 5-minute regular-hours stretch where every symbol is silent is the only request. `python -m src.data.gap_fill --date YYYY-MM-DD` processes exactly one named day.
- QFILL-02: 14 minutes of pre-market silence is not requested. 15 minutes is. One regular-hours minute is not requested. Two minutes is. A stretch crossing 09:30 or 16:00 is split, and each piece uses its own rule.
- QFILL-03: Saturday, Good Friday 2026-04-03, the observed Independence Day close 2026-07-03, and 2025-01-09 produce no request. An early close stops at 13:00 ET.
- QFILL-04: the request schema is `tbbo`. Returned bid and ask are stored as `bid_price` and `ask_price`. Sparse or empty success is not requested again. Interval cost is estimated on the selected stretches.
- QFILL-05: while the publisher lock is held, the one-day command and the multi-day command raise before any Databento call. `fill_named_day` holds the lock through download and publish.
- The unused 09:00–16:00 helpers (`get_day_trading_bounds`, `estimate_day_cost`, `fetch_and_normalize_day`, `is_day_already_backfilled`) are gone.

## Not done here

The production lake was not opened and was not rewritten. The gap fill was not run against it.
