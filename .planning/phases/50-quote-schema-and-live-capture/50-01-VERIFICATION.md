# Phase 50 verification

**Date:** 2026-10-06

## Evidence

`tests/storage/test_phase50_quote_schema.py`: 5 passed.

- QUOTE-01: a Capital.com bid 100.00 / ask 100.04 is stored as `bid_price` / `ask_price`. The file has no `price`, `volume`, or size column, and the stored bid is not the midpoint 100.02.
- QUOTE-02: a Databento `tbbo` row stores `bid_px_00` and `ask_px_00`. The trade price 100.50 and the trade size 12 are not stored. A missing bid is dropped, not replaced by the trade price.
- QUOTE-03: `chart.js` no longer calls `addHistogramSeries`. The dashboard candle open/high/low/close for that v2 minute are the bids 100.00 and 100.10.
- A schema v1 file is still readable, and its candle still uses the stored `price`.

`tests/storage/test_v6_recovery_and_mixed_snapshot.py`: 4 passed.

- v2 receipt-durability recovery commits a valid batch.
- Mixed v1/v2 Arrow snapshots preserve both quote column sets in either file order.

## Regression

Recorded with the full offline suite at closeout (`not live and not performance`). See the v6.0 completion report for the current count.

## Not done here

The production lake was not rewritten. Compaction skips a partition that mixes v1 and v2 files instead of casting it.
