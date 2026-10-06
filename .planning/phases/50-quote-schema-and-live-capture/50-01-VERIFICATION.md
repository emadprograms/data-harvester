# Phase 50 verification

**Date:** 2026-10-06

## Evidence

`tests/storage/test_phase50_quote_schema.py`: 5 passed.

- QUOTE-01: a Capital.com bid 100.00 / ask 100.04 is stored as `bid_price` / `ask_price`. The file has no `price`, `volume`, or size column, and the stored bid is not the midpoint 100.02.
- QUOTE-02: a Databento `tbbo` row stores `bid_px_00` and `ask_px_00`. The trade price 100.50 and the trade size 12 are not stored. A missing bid is dropped, not replaced by the trade price.
- QUOTE-03: `chart.js` no longer calls `addHistogramSeries`. The dashboard candle open/high/low/close for that v2 minute are the bids 100.00 and 100.10.
- A schema v1 file is still readable, and its candle still uses the stored `price`.

## Regression

`tests/storage`, `tests/stream`, `tests/dashboard`, and `tests/data`: 525 passed, 2 failed.

The two failures are `test_failed_start_is_reported` and `test_drain_failure_maps_to_its_own_exit_code`. They fail the same way on the pre-phase-50 tree: the ingestion-window watchdog calls `stop()` on a test double that does not implement it. They are not a quote-schema regression.

## Not done here

The production lake was not rewritten. On 2026-10-06 the owner moved that work to Phase 53, the last phase. It is not implemented or run from this checkout. Compaction skips a partition that mixes v1 and v2 files instead of casting it.
