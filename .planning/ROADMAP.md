# Roadmap: Data Harvester

## Milestones

- ✅ **v1.0 Local DuckDB & 24/7 Live Streaming Engine** — Phases 1–4 (shipped 2026-09-25)
- ✅ **v2.0 Dedicated Dual-DuckDB Storage, Capital.com Tick Streamer & Data Integrity Web Dashboard** — Phases 5–9 (shipped 2026-09-25)
- ✅ **v3.0 Observability Command Center, Interactive Financial Charts & Live Telemetry Dashboard** — Phases 10–14 (shipped 2026-09-26)
- ✅ **v4.0 Partitioned Parquet Tick Lake (Decoupled High-Concurrency Storage)** — Phases 15–21 (shipped 2026-10-03)
- ✅ **v4.1 Partitioned Parquet Lake Deep Testing & Hardening** — Phases 22–27 (shipped 2026-10-03)
- ✅ **v4.2 Tick Lake Qualification & Scoped Signoff** — Phases 28–36 (closed 2026-10-04)
- ✅ **v4.3 Final Tick-Lake Implementation and Verification** — Phases 37–43 (44–45 waived; closed 2026-10-05)
- ✅ **v5.0 DuckDB-Free Tick-Only Parquet** — Phases 46–49 (shipped 2026-10-06)
- 🚧 **v6.0 Bid and Ask Prices** — Phases 50–53 (started 2026-10-06)

## Phases

### v6.0 Bid and Ask Prices (In Progress)

**Phase numbering continues from v5.0.** v5.0 ended at Phase 49. v6.0 starts at Phase 50.

- [ ] **Phase 50: Quote schema and live capture** - Store bid price and ask price only
- [ ] **Phase 51: Rewrite the existing lake** - Convert the 7,367 schema v1 files
- [ ] **Phase 52: Computer-offline gap fill** - Fill all-symbol silence from Databento
- [ ] **Phase 53: Contract and operator docs** - Describe the new row and the rewrite

### Phase 50: Quote schema and live capture
**Goal**: New rows store `bid_price` and `ask_price` only, and the inspection chart reads the bid.
**Depends on**: Nothing (first phase of v6.0)
**Requirements**: QUOTE-01, QUOTE-02, QUOTE-03
**Success Criteria** (what must be TRUE):
  1. A Capital.com quote of bid 100.00 and ask 100.04 is stored as `bid_price` 100.00 and `ask_price` 100.04, with no midpoint and no volume.
  2. A Databento `tbbo` record stores `bid_px_00` and `ask_px_00` in those columns and drops the trade price and the trade size.
  3. The dashboard candle for that symbol uses `bid_price`, and the volume histogram is gone.
**Plans**: 1 plan

Plans:
- [ ] 50-01: Change schema v2, the live writer, the reader, and the chart together, with failing tests first.

### Phase 51: Rewrite the existing lake
**Goal**: The operator can convert the lake already on disk without inventing a bid from the old midpoint.
**Depends on**: Phase 50
**Requirements**: REWRITE-01, REWRITE-02, REWRITE-03, REWRITE-04, REWRITE-05
**Success Criteria** (what must be TRUE):
  1. On a copy of a schema v1 lake, every kept row's `bid_price` equals the old `bid` and `ask_price` equals the old `ask`.
  2. A file is not swapped until the row count and the value check pass. A stopped run resumes without rewriting finished files.
  3. A row with a null bid or ask is listed in the quarantine report and is not given the old `price` value.
  4. The tool refuses to start if a second copy will not fit. After the full check, receipts match the new bytes, `lake.json` says schema version 2, and the retired v1 bytes are deleted.
**Plans**: 1 plan

Plans:
- [ ] 51-01: Offline rewrite tool, receipt update, resume, and a fixture-lake test. The production lake is not rewritten from this checkout.

### Phase 52: Computer-offline gap fill
**Goal**: Naming one day fetches only the stretches where every symbol was silent, and writes bid and ask.
**Depends on**: Phase 50
**Requirements**: QFILL-01, QFILL-02, QFILL-03, QFILL-04, QFILL-05
**Success Criteria** (what must be TRUE):
  1. A 5-minute stretch in regular hours where every symbol is silent is requested, and a minute where one symbol has a tick is not.
  2. A 10-minute all-symbol silence in the pre-market is not requested. A 15-minute one is. A stretch crossing 09:30 or 16:00 ET is split.
  3. A weekend, a full NYSE holiday, and the time after an early close are not requested.
  4. Returned Databento bid and ask are stored as `bid_price` and `ask_price`. The command refuses to start if the live writer holds the lock.
**Plans**: 1 plan

Plans:
- [ ] 52-01: Replace the backward whole-day backfill with the all-symbol silence rule and schema v2 writes.

### Phase 53: Contract and operator docs
**Goal**: A downstream reader can follow the new columns without reading the old contract.
**Depends on**: Phases 50, 51, 52
**Requirements**: DOCS-01
**Success Criteria** (what must be TRUE):
  1. The README, the operations guide, and the Repo B contract name `bid_price` and `ask_price`, describe the rewrite, and describe the gap-fill rule.
  2. Those documents no longer tell a consumer to read `price` or `volume` as the stored quote.
**Plans**: 1 plan

Plans:
- [ ] 53-01: Update the three documents and the tests that pin their wording.

### Completed Milestones

<details>
<summary>✅ v5.0 DuckDB-Free Tick-Only Parquet (Phases 46–49) — SHIPPED 2026-10-06</summary>

- [x] Phase 46: Rewire Off the Disk Databases (1/1 plan) — completed 2026-10-05
- [x] Phase 47: Remove Bar Subsystem & Dead Providers (1/1 plan) — completed 2026-10-05
- [x] Phase 48: Schedule, Compaction, Notifications & Test Disposition (1/1 plan) — completed 2026-10-05
- [x] Phase 49: Final Gate, Deletion & Closure (1/1 plan) — completed 2026-10-05

Result: 33/33 requirements verified; 105,894,626 legacy ticks migrated to Parquet lake across 7,367 files and 7,355 partitions with zero discrepancies. All `.duckdb` files on disk permanently deleted. Pure in-memory DuckDB query engine. Net −10,415 lines of dead code removed. Full offline test suite: 988 passed. Terminal milestone complete.

See: [.planning/milestones/v5.0-ROADMAP.md](milestones/v5.0-ROADMAP.md) · [.planning/milestones/v5.0-REQUIREMENTS.md](milestones/v5.0-REQUIREMENTS.md) · [.planning/milestones/v5.0-MILESTONE-AUDIT.md](milestones/v5.0-MILESTONE-AUDIT.md) · [reports/v5.0_completion_report.md](../reports/v5.0_completion_report.md)

</details>

<details>
<summary>✅ v4.3 Final Tick-Lake Implementation and Verification (Phases 37–45) — CLOSED 2026-10-05</summary>

- [x] Phase 37: Preflight Test Isolation & Fail-Closed Validator (1/1 plan) — completed 2026-10-04
- [x] Phase 38: Migration Overlap Protection, Provenance & Scoped Verification (1/1 plan) — completed 2026-10-04
- [x] Phase 39: Reader Root Correctness & Portable Executable Contract (1/1 plan) — completed 2026-10-04
- [x] Phase 40: Honest Durability Boundaries & Provider Gap Ledger (1/1 plan) — completed 2026-10-04
- [x] Phase 41: Capacity Monitoring, Offline Compaction & Physical Purge (1/1 plan) — completed 2026-10-04
- [x] Phase 42: Market Rewind Bounded Replay Iterator Decision (1/1 plan) — completed 2026-10-04
- [x] Phase 43: Production-Scale Benchmarks & Performance Qualification (1/1 plan) — completed 2026-10-04
- [x] Phase 44: 24-Hour Sustained Multi-Process Endurance Run (waived by owner) — closed 2026-10-05
- [x] Phase 45: Operational Rehearsal, Candidate CI & Milestone Closeout Audit (waived by owner) — closed 2026-10-05

Result: Phases 37–43 delivered; phases 44–45 waived. Suite at close: 1,045 passed offline tests.

See: [.planning/milestones/v4.3-ROADMAP.md](milestones/v4.3-ROADMAP.md) · [.planning/milestones/v4.3-REQUIREMENTS.md](milestones/v4.3-REQUIREMENTS.md) · [.planning/milestones/v4.3-phases/](milestones/v4.3-phases/)

</details>

<details>
<summary>✅ v4.2 Tick Lake Qualification & Scoped Signoff (Phases 28–36) — CLOSED 2026-10-04</summary>

- [x] Phase 28: CI Evidence & Requirement Traceability (1/1 plan) — completed 2026-10-04
- [x] Phase 29: Test Isolation & Independent Oracles (1/1 plan) — completed 2026-10-04
- [x] Phase 30: Production-Scale Performance Qualification (1/1 plan) — completed 2026-10-04
- [x] Phase 31: Endurance & Sustained Recovery (deferred to v4.3) — closed 2026-10-04
- [x] Phase 32: Durability Boundary & Capture-Gap Contract (1/1 plan) — completed 2026-10-04
- [x] Phase 33: Repo B Contract & Integration (1/1 plan) — completed 2026-10-04
- [x] Phase 34: Migration, Backup & Restore Rehearsal (1/1 plan) — completed 2026-10-04
- [x] Phase 35: Append-Only Capacity & Maintenance Safety (deferred to v4.3) — closed 2026-10-04
- [x] Phase 36: Contract Repair & Signoff Documentation (1/1 plan) — completed 2026-10-04

Result: 30 of 45 requirements verified; 15 requirements explicitly excluded/deferred.

See: [.planning/milestones/v4.2-ROADMAP.md](milestones/v4.2-ROADMAP.md) · [.planning/milestones/v4.2-REQUIREMENTS.md](milestones/v4.2-REQUIREMENTS.md) · [.planning/milestones/v4.2-MILESTONE-AUDIT.md](milestones/v4.2-MILESTONE-AUDIT.md)

</details>

<details>
<summary>✅ v4.1 Partitioned Parquet Lake Deep Testing & Hardening (Phases 22–27) — SHIPPED 2026-10-03</summary>

- [x] Phase 22: Storage Foundation & Publication Edge Case Tests (1/1 plan) — completed 2026-10-03
- [x] Phase 23: Streaming Writer & Runner Stress & Lifecycle Tests (1/1 plan) — completed 2026-10-03
- [x] Phase 24: Versioned Symbol Registry & Dynamic Reload Stress Tests (1/1 plan) — completed 2026-10-03
- [x] Phase 25: In-Memory DuckDB Lake Reader & Analytics Edge Tests (1/1 plan) — completed 2026-10-03
- [x] Phase 26: Migration Tooling Rehearsal & Fuzz Tests (1/1 plan) — completed 2026-10-03
- [x] Phase 27: Multi-Process Long-Running Soak & Chaos Tests (1/1 plan) — completed 2026-10-03

Result: 122 new tests expanded to 746 passed offline tests. Audit verdict: passed.

See: [.planning/milestones/v4.1-ROADMAP.md](milestones/v4.1-ROADMAP.md) · [.planning/milestones/v4.1-REQUIREMENTS.md](milestones/v4.1-REQUIREMENTS.md) · [.planning/milestones/v4.1-MILESTONE-AUDIT.md](milestones/v4.1-MILESTONE-AUDIT.md)

</details>

<details>
<summary>✅ v4.0 Partitioned Parquet Tick Lake (Decoupled High-Concurrency Storage) (Phases 15–21) — SHIPPED 2026-10-03</summary>

- [x] Phase 15: Safe Test Isolation & Baseline Characterization (P0) (1/1 plan) — completed 2026-10-03
- [x] Phase 16: Lake Schema, Configuration, Atomic Files & Recovery (P1) (1/1 plan) — completed 2026-10-03
- [x] Phase 17: Streaming Parquet Writer & Runner Lifecycle Integration (P2) (1/1 plan) — completed 2026-10-03
- [x] Phase 18: Versioned Symbol Registry & Administrative Compatibility (P3) (1/1 plan) — completed 2026-10-03
- [x] Phase 19: In-Memory DuckDB Lake Reader & Dashboard Integration (P4) (1/1 plan) — completed 2026-10-03
- [x] Phase 20: Zero-Loss Migration Tooling & Rehearsal (P6) (1/1 plan) — completed 2026-10-03
- [x] Phase 21: Production Cutover, Concurrency Validation & Handoff (P8) (1/1 plan) — completed 2026-10-03

See: [.planning/milestones/v4.0-ROADMAP.md](milestones/v4.0-ROADMAP.md)

</details>

<details>
<summary>✅ v3.0 Observability Command Center, Interactive Financial Charts & Live Telemetry Dashboard (Phases 10–14) — SHIPPED 2026-09-26</summary>

- [x] Phase 10: Backend Analytics & High-Performance Data APIs (1/1 plan) — completed 2026-09-25
- [x] Phase 11: Interactive Financial Charting & Data Explorer UI (1/1 plan) — completed 2026-09-25
- [x] Phase 12: Live Stream Tape, Telemetry & Market Operations UI (1/1 plan) — completed 2026-09-25
- [x] Phase 13: Enhanced Symbol Data Matrix & Context-Aware Integrity Engine (1/1 plan) — completed 2026-09-25
- [x] Phase 14: Comprehensive Verification, Testing & Polish (1/1 plan) — completed 2026-09-25

See: [.planning/milestones/v3.0-ROADMAP.md](milestones/v3.0-ROADMAP.md)

</details>

<details>
<summary>✅ v2.0 Dedicated Dual-DuckDB Storage, Capital.com Tick Streamer & Data Integrity Web Dashboard (Phases 5–9) — SHIPPED 2026-09-25</summary>

- [x] Phase 5: Dedicated Dual-DuckDB Storage Layer (1/1 plan) — completed 2026-09-25
- [x] Phase 6: Capital.com Exclusive WebSocket Streamer & Dynamic Reload (1/1 plan) — completed 2026-09-25
- [x] Phase 7: Data Integrity & Health Engine (1/1 plan) — completed 2026-09-25
- [x] Phase 8: Interactive JavaScript Web Dashboard & Symbol Management (1/1 plan) — completed 2026-09-25
- [x] Phase 9: End-to-End Verification & Full Test Suite (1/1 plan) — completed 2026-09-25

See: [.planning/milestones/v2.0-ROADMAP.md](milestones/v2.0-ROADMAP.md)

</details>

<details>
<summary>✅ v1.0 Local DuckDB & 24/7 Live Streaming Engine (Phases 1–4) — SHIPPED 2026-09-25</summary>

- [x] Phase 1: Turso Data Migration & Legacy Purge (1/1 plan) — completed 2026-09-25
- [x] Phase 2: DuckDB Storage Engine (1/1 plan) — completed 2026-09-25
- [x] Phase 3: 24/7 Live WebSocket Streaming (1/1 plan) — completed 2026-09-25
- [x] Phase 4: Test Suite Migration & Verification (1/1 plan) — completed 2026-09-25

See: [.planning/milestones/v1.0-ROADMAP.md](milestones/v1.0-ROADMAP.md)

</details>

---

## Milestone Backlog

v5.0 backlog is closed with that milestone. Active backlog for v6.0:

| Candidate | Disposition | Phase |
|-----------|-------------|-------|
| Store `bid_price` and `ask_price` only; drop midpoint, `price`, `volume`, and sizes | **Required** | 50 |
| Inspection chart reads `bid_price`; volume histogram removed | **Required** | 50 |
| Rewrite the existing 7,367 schema v1 files; do not invent a bid from `price` | **Required** | 51 |
| Quarantine null bid or ask; update receipts; schema version 2 only at the end | **Required** | 51 |
| Gap fill one day on all-symbol silence (15 min pre/post, 2 min regular) | **Required** | 52 |
| Skip weekends, full holidays, and post-early-close time | **Required** | 52 |
| README, operations guide, and Repo B contract | **Required** | 53 |
| Bid quantity and ask quantity | **Out of scope** (owner dropped; never stored) | — |
| Databento `mbp-1` or matching row counts across feeds | **Out of scope** (owner accepted the difference) | — |

v4.3 and v5.0 backlog items are archived with those milestones. Historical v5.0 backlog:

| Candidate | Disposition | Phase |
|-----------|-------------|-------|
| Verify legacy tick migration before any `.duckdb` deletion | **Required (gate)** | 49 |
| Runner lake-only; remove DuckDB writer fallback | **Required** | 46 |
| Dashboard, analytics, integrity off the disk databases | **Required** | 46 |
| Databento gap-fill rewired to the Parquet lake | **Required** | 46 |
| Bar subsystem and dead providers removed; Discord rewritten (not removed) | **Required** | 47 |
| Unused replay subsystem removal (`replay.py`, exports, reader methods, tests) | **Required** | 47 |
| Discord rewired to streamer events (bar-era functions removed) | **Required** | 47-48 |
| Owner deletion of `historical.duckdb` + `streaming.duckdb` (no retention period) | **Owner action** | 49 |
| Single Parquet-only dashboard (Historical view deleted) | **Required** | 46 |
| 04:00–20:00 ET weekday schedule + unattended off-hours compaction | **Required** | 48 |
| Removing the DuckDB library / PyArrow resampling | **Out of scope** (owner-rejected) | — |
| 24-hour endurance run, hosted CI log verification | **Out of scope** (waived in v4.3) | — |
| Postgres / SQLite migration | **Out of scope** (rejected) | — |
