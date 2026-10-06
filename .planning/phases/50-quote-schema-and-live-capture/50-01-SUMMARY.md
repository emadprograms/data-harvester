---
phase: 50
plan: 01
title: Quote schema and live capture
status: complete
completed_at: 2026-10-06T12:00:00Z
requirements-completed: [QUOTE-01, QUOTE-02, QUOTE-03]
---

# Phase 50 Summary: Quote schema and live capture

## Accomplishments

1. **QUOTE-01**: Capital.com quote-change `bid` / `ofr` are stored as `bid_price` / `ask_price`. New rows have no midpoint, no `price`, no `volume`, and no size column.
2. **QUOTE-02**: Databento `tbbo` stores `bid_px_00` / `ask_px_00` and drops trade price and trade size. A missing bid is dropped, not replaced by the trade price.
3. **QUOTE-03**: Inspection candles for schema v2 use `bid_price`. `chart.js` no longer calls `addHistogramSeries`.
4. Schema v1 files stay readable. Mixed partitions are skipped by compaction instead of being cast. Arrow `snapshot()` uses an explicit union schema.

## Verification

- `tests/storage/test_phase50_quote_schema.py`: 5 passed.
- `tests/storage/test_v6_recovery_and_mixed_snapshot.py`: 4 passed (v2 receipt recovery and mixed snapshot).

## Not done here

The production lake was not rewritten. That remains an owner-run Phase 53 action.
