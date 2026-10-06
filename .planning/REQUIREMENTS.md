# Requirements: Data Harvester

**Defined:** 2026-10-06
**Core Value:** Zero-cloud, zero-quota persistent market data ingestion and storage.
**Milestone:** v6.0 Bid and Ask Prices

A requirement is complete only when its stated evidence exists. Shortfalls are recorded honestly.

## v6.0 Requirements

### Quote schema

- [ ] **QUOTE-01**: A new Capital.com quote stores the feed bid in `bid_price` and the feed ask (`ofr`) in `ask_price`. The stored row has no midpoint, no `price` column, no `volume` column, and no size column.
- [ ] **QUOTE-02**: A new Databento row stores `bid_px_00` in `bid_price` and `ask_px_00` in `ask_price`. The trade price and the trade size are not stored.
- [ ] **QUOTE-03**: The inspection chart builds candles from `bid_price` and does not draw a volume series.

### Existing lake rewrite

- [ ] **REWRITE-01**: The operator can rewrite every file under `ticks/` so each kept row's `bid_price` equals the previously stored `bid` and `ask_price` equals the previously stored `ask`.
- [ ] **REWRITE-02**: The rewrite does not copy the old midpoint or the old trade price into `bid_price`.
- [ ] **REWRITE-03**: A row missing `bid` or `ask` is quarantined and counted. It is not filled in from `price`.
- [ ] **REWRITE-04**: The rewrite refuses to start unless there is room for a second copy of the lake. It holds the publisher lock and the maintenance guard, and a stopped run can be resumed.
- [ ] **REWRITE-05**: After a file is swapped, its publication receipt matches the new bytes. `lake.json` becomes schema version 2 only after every file has passed. Retired v1 bytes are deleted only after that full check.

### Computer-offline gap fill

- [ ] **QFILL-01**: The operator can name one day and the tool requests only the stretches inside 04:00–20:00 ET where every active registry symbol is silent.
- [ ] **QFILL-02**: Pre-market (04:00–09:30 ET) and post-market (16:00–20:00 ET) silence counts only at 15 minutes or more. Regular hours (09:30–16:00 ET) counts at 2 minutes or more. A stretch that crosses 09:30 or 16:00 is split, and each piece uses its own rule.
- [ ] **QFILL-03**: Weekends, full NYSE holidays, and the time after an early close are not requested.
- [ ] **QFILL-04**: Databento `tbbo` is asked only for those stretches. Returned bid and ask are stored as `bid_price` and `ask_price`. Fewer Databento rows than Capital.com rows is accepted and is not treated as a remaining hole by itself.
- [ ] **QFILL-05**: Gap fill does not start while the live writer holds the lake lock.

### Documentation

- [ ] **DOCS-01**: The README, the operations guide, and the Repo B contract describe `bid_price` and `ask_price`, the rewrite, and the gap-fill rule. They no longer tell a consumer to read `price` or `volume` as the stored quote.

## Future Requirements

None. Sizes are not deferred. They were dropped.

## Out of Scope

| Feature | Reason |
|---------|--------|
| Bid quantity and ask quantity | Owner dropped them on 2026-10-06. They were never stored, so the rewrite cannot recover them. |
| `volume` column and cross-feed volume scaling | Capital.com quote size was never saved. Databento trade size is a different number. One column cannot hold both. |
| Stored midpoint | Redundant with bid and ask. The chart uses the bid. |
| Databento `mbp-1` instead of `tbbo` | Owner accepted that Databento emits a row on a trade and Capital.com emits a row on a quote change. |
| Equal row counts across the two feeds | Same decision. The reader does not need to know which feed produced a row. |
| Changing Repo B's code | This repo updates the read contract. The other repository is not in this milestone. |

## Traceability

Filled when the roadmap is written.

| Requirement | Phase | Status |
|-------------|-------|--------|
| QUOTE-01 | Phase 50 | Pending |
| QUOTE-02 | Phase 50 | Pending |
| QUOTE-03 | Phase 50 | Pending |
| REWRITE-01 | Phase 51 | Pending |
| REWRITE-02 | Phase 51 | Pending |
| REWRITE-03 | Phase 51 | Pending |
| REWRITE-04 | Phase 51 | Pending |
| REWRITE-05 | Phase 51 | Pending |
| QFILL-01 | Phase 52 | Pending |
| QFILL-02 | Phase 52 | Pending |
| QFILL-03 | Phase 52 | Pending |
| QFILL-04 | Phase 52 | Pending |
| QFILL-05 | Phase 52 | Pending |
| DOCS-01 | Phase 53 | Pending |

**Coverage:**
- v6.0 requirements: 14 total
- Mapped to phases: 14
- Unmapped: 0

---
*Requirements defined: 2026-10-06*
*Last updated: 2026-10-06 after roadmap creation*
