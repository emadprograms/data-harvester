# Phase 51 verification

**Date:** 2026-10-06

## Evidence

`tests/data/test_phase51_gap_fill.py` and `tests/data/test_databento.py`: 16 passed. Phase 50 quote tests still pass.

- QFILL-01: a 5-minute regular-hours stretch where every symbol is silent is the only request. A minute with one symbol ticking is not requested. The lake scan asks Databento for that stretch only.
- QFILL-02: 14 minutes of pre-market silence is not requested. 15 minutes is. One regular-hours minute is not requested. Two minutes is. A stretch crossing 09:30 or 16:00 is split, and each piece uses its own rule. Qualifying pieces stay separate requests.
- QFILL-03: Saturday, Good Friday 2026-04-03, and the observed Independence Day close 2026-07-03 produce no request. The 2024–2026 holiday and early-close dates match the NYSE Group calendar. An early close stops at 13:00 ET. Time after that close is not requested.
- QFILL-04: the request schema is `tbbo`. The returned bid and ask are stored as `bid_price` and `ask_price`. Trade price and trade size are not stored. One returned row does not cause a second request. The existing file bytes are unchanged.
- QFILL-05: while the publisher lock is held, the one-day command and the multi-day command raise before any Databento call, including the cost estimate.

## Not done here

The production lake was not opened and was not rewritten. The gap fill was not run against it. That rewrite remains Phase 53.
