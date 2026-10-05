# Requirements: Data Harvester

**Defined:** 2026-10-05
**Core Value:** Zero-cloud, zero-quota persistent market data ingestion and storage — capture ticks reliably and serve them concurrently, with no locks and no quotas.
**Milestone:** v5.0 DuckDB-Free Tick-Only Parquet

**Source of truth for scope:** [`PLAN-MILESTONE-5.0.md`](../PLAN-MILESTONE-5.0.md) — workstreams W0–W10, gates G1–G4.

> Every requirement below is a checkable statement about the shipped system. A requirement is **Complete** only when its stated evidence exists. Partial results, measured shortfalls and known limitations are recorded honestly — never rounded up.

---

## v5.0 Requirements by Phase

### Phase 46: Baseline Capture & Legacy Tick Migration Verification

- [ ] **BASE-01**: Current oracle-verified behaviour is captured **once** on a fixed synthetic dataset — candle outputs for the DST / timestamp-tie / duplicate / null-volume edge-case matrix, plus query and tape latency. This is the reference the PyArrow implementation must match. It is a single recorded baseline, **not** a re-qualification campaign.
- [ ] **MIG-01**: The legacy tick source (`data/streaming.duckdb`) is migrated into the Parquet lake with `plan → export → verify → verify-published → audit-lake` all completing, and row counts reconciled per symbol and per date.
- [ ] **MIG-02**: Migration completes and is confirmed **before** any DuckDB removal begins. The agent never copies, exports, archives or deletes the owner's `.duckdb` files.
- [ ] **MIG-03**: Re-running the migration over an overlapping or broader scope publishes no duplicate rows (source-coverage ledger).

### Phase 47: PyArrow Reader Core

- [ ] **READ-01**: `TickLakeReader` serves all existing public methods under identical names and return shapes, implemented with PyArrow. The module contains no DuckDB import.
- [ ] **READ-02**: Partition resolution and pruning behave as before — symbol/date predicates applied at path level, and no recursive scan of `_staging/`, `_maintenance/` or `_migration/`.
- [ ] **READ-03**: Structured lake errors are preserved exactly (`LakeUnavailableError`, `LakeCorruptedMetadataError`, `LakeIncompatibleSchemaError`), with no silent fallback and no legacy backend.
- [ ] **READ-04**: Tick reads (`query_ticks`, `get_tape`, `get_latest_tick`) preserve duplicate multiplicity, null volume, and deterministic ordering on `(timestamp, ingest_id)`.
- [ ] **READ-05**: Work is bounded per symbol/day partition, so memory does not scale with total lake size.

### Phase 48: Arrow Resampling & Multiset Verification

- [ ] **CAND-01**: OHLCV resampling output is identical to the recorded baseline oracle for `1m`, `5m`, `15m`, `1h`, `1D` across DST transitions, leap years, UTC/exchange date boundaries, timestamp ties, exact duplicates, late arrivals, and null/zero volume.
- [ ] **CAND-02**: Open/close selection uses the documented deterministic tie-break — sort by `(timestamp, ingest_id)`, then first/last — and high/low use max/min. Tie-breaking is proven by test, not asserted.
- [ ] **VER-01**: Compaction and migration verification no longer use `EXCEPT ALL`. Equivalence is proven by multiset (Counter) comparison that preserves multiplicity, float precision and nulls.
- [ ] **VER-02**: The independent oracle (`tests/support/lake_assertions.py`) remains free of application-reader imports and is the pass/fail authority for CAND-01 and VER-01.
- [ ] **VER-03**: Compaction replacement is gated on multiset equivalence and preserves lineage; any mismatch blocks replacement and is reported.

### Phase 49: Dashboard, Analytics & Reader Contract

- [ ] **DASH-01**: `/api/historical/*` endpoints and their frontend callers are removed. Retained tick routes (`/api/stream/*`, symbols, coverage, continuity, integrity) are served by the PyArrow reader.
- [ ] **DASH-02**: The chart renders tick-derived candles from the lake only. Dates predating tick capture return an honest empty state and never imply that bars were converted.
- [ ] **DASH-03**: Tick-health integrity checks are preserved; bar-era and cross-store drift checks are removed.
- [ ] **DASH-04**: The frontend removes historical navigation, the data-source selector, harvester controls, and stale database labels.
- [ ] **CONT-01**: The Repo B contract is rewritten for pyarrow-only consumption with no DuckDB in any example, and its published examples execute in an isolated subprocess with zero `src` imports.
- [ ] **CONT-02**: The owner confirms whether Repo B currently reads these files through DuckDB SQL; if so, the breaking change is coordinated before release (gate G1).

### Phase 50: Removal

- [ ] **RMV-01**: `grep -r duckdb` returns no hits in `src/`, `tools/`, `main.py`, `tests/` or `requirements.txt`.
- [ ] **RMV-02**: The bar subsystem is deleted — `main.py` harvest CLI, `src/data/harvester.py`, `src/data/normalizer.py`, `src/api/massive.py`, `src/api/yahoo.py`, `src/api/binance.py`, `tools/backfill_massive.py`, `tools/benchmark_baseline.py`, `tools/audit_database_integrity.py`, `src/dashboard/harvester_job.py`, `src/utils/discord.py`.
- [ ] **RMV-03**: No code path can open or create a disk-backed DuckDB database; a startup regression proves it and would fail if one were reintroduced.
- [ ] **RMV-04**: Removed dependencies (`duckdb`, `yfinance`, `polygon-api-client`) are gone; retained ones (`pyarrow`, `pandas`, `pytz`/`tzdata`, `websockets`, `requests`, `python-dotenv`, `psutil`, `pytest`) remain.
- [ ] **RMV-05**: The owner deletes `data/historical.duckdb` and `data/streaming.duckdb` after Phase 46 confirmation.
- [ ] **GAP-01**: Databento gap-fill publishes ticks into the Parquet lake, reads symbols from `_control/registry.json`, performs its already-backfilled check against the lake (never a database), and honours maintenance/publisher fences.
- [ ] **GAP-02**: `databento` is declared in `requirements.txt`, and its operational symbol scope is restricted to the 19 approved symbols.

### Phase 51: Schedule, Maintenance & Closure

- [ ] **SCHED-01**: A single timezone policy module computes ingestion eligibility in `ZoneInfo("America/New_York")` over `[04:00, 20:00)`, using an injectable clock. Behaviour is identical under DST transitions and when the host is not in ET.
- [ ] **SCHED-02**: The supervisor owns lifecycle states (`WAITING_FOR_WINDOW`, `STARTING`, `INGESTING`, `DRAINING`, `MAINTENANCE`, `ERROR`). No provider authentication or subscription occurs outside the window, and an intentional off-hours stop is never treated as a crash.
- [ ] **SCHED-03**: The direct runner entry point enforces the same window, so a manual start cannot bypass the schedule.
- [ ] **SCHED-04**: At 20:00 admission stops, accepted ticks drain exactly once, and a failed drain blocks compaction and is reported honestly.
- [ ] **SCHED-05**: Compaction runs unattended once per eligible closed interval, idempotently, after confirmed drain. A no-op interval counts as success, and a single maintenance lease prevents duplicate launchers.
- [ ] **SYMB-01**: `_control/registry.json` is the single symbol authority containing exactly the 19 approved equities. Unsolicited and out-of-scope symbols are rejected at the callback boundary and cannot be added through the UI.
- [ ] **CO-01**: Tests are dispositioned retain / retarget / delete with a one-line reason each, and the retained offline suite passes with no required xfails.
- [ ] **CO-02**: Reference cleanup covers code and current user-facing docs; `.planning/` archives are left intact.
- [ ] **CO-03**: One performance measurement is recorded on the reference workload and compared honestly against the Phase 46 baseline. No threshold re-tuning to force a pass.
- [ ] **CO-04**: The milestone closes with one completion report and **stops** — no new phases, audits, or follow-up programme.

---

## Out of Scope

| Item | Reason |
|---|---|
| 24-hour endurance run | Waived in v4.3 by owner decision; not revived here. |
| Hosted CI log verification | Requires external credentials; not a product requirement. |
| Replay feature work, durable spool, benchmark campaigns | Not required by this refactor. |
| Postgres / SQLite migration | Rejected — Parquet stays. |
| Editing `.planning/` historical archives | Falsifying past audit records is not permitted. |
| Agent deletion or archival of the owner's `.duckdb` files | Owner action only, after Phase 46 confirmation. |

---

## Traceability Mapping

| Requirement | Phase | Status |
|-------------|-------|--------|
| BASE-01 | Phase 46 | Pending |
| MIG-01 | Phase 46 | Pending |
| MIG-02 | Phase 46 | Pending |
| MIG-03 | Phase 46 | Pending |
| READ-01 | Phase 47 | Pending |
| READ-02 | Phase 47 | Pending |
| READ-03 | Phase 47 | Pending |
| READ-04 | Phase 47 | Pending |
| READ-05 | Phase 47 | Pending |
| CAND-01 | Phase 48 | Pending |
| CAND-02 | Phase 48 | Pending |
| VER-01 | Phase 48 | Pending |
| VER-02 | Phase 48 | Pending |
| VER-03 | Phase 48 | Pending |
| DASH-01 | Phase 49 | Pending |
| DASH-02 | Phase 49 | Pending |
| DASH-03 | Phase 49 | Pending |
| DASH-04 | Phase 49 | Pending |
| CONT-01 | Phase 49 | Pending |
| CONT-02 | Phase 49 | Pending |
| RMV-01 | Phase 50 | Pending |
| RMV-02 | Phase 50 | Pending |
| RMV-03 | Phase 50 | Pending |
| RMV-04 | Phase 50 | Pending |
| RMV-05 | Phase 50 | Pending |
| GAP-01 | Phase 50 | Pending |
| GAP-02 | Phase 50 | Pending |
| SCHED-01 | Phase 51 | Pending |
| SCHED-02 | Phase 51 | Pending |
| SCHED-03 | Phase 51 | Pending |
| SCHED-04 | Phase 51 | Pending |
| SCHED-05 | Phase 51 | Pending |
| SYMB-01 | Phase 51 | Pending |
| CO-01 | Phase 51 | Pending |
| CO-02 | Phase 51 | Pending |
| CO-03 | Phase 51 | Pending |
| CO-04 | Phase 51 | Pending |

**Coverage:**
- Total v5.0 requirements: 37
- Mapped to phases: 37
- Unmapped: 0 ✓

---

## Open Gates (owner decisions carried from the plan)

| Gate | Question | Status |
|---|---|---|
| G1 | Does Repo B read these files through DuckDB today? | **Open** — confirm before Phase 49 |
| G2 | Accept that candle computation moves out of DuckDB, with any latency change reported honestly? | **Open** |
| G3 | Accept that legacy `.duckdb` files become unreadable by this application after Phase 50? | **Open** |
| G4 | Confirm deletion of `historical.duckdb` **and** `streaming.duckdb` by the owner after Phase 46? | **Open** |

---

*Requirements defined: 2026-10-05*
*Derived from: `PLAN-MILESTONE-5.0.md` (workstreams W0–W10)*
