# Roadmap: Data Harvester

## Milestones

- ✅ **v1.0 Local DuckDB & 24/7 Live Streaming Engine** — Phases 1–4 (shipped 2026-09-25)
- ✅ **v2.0 Dedicated Dual-DuckDB Storage, Capital.com Tick Streamer & Data Integrity Web Dashboard** — Phases 5–9 (shipped 2026-09-25)
- ✅ **v3.0 Observability Command Center, Interactive Financial Charts & Live Telemetry Dashboard** — Phases 10–14 (shipped 2026-09-26)
- ✅ **v4.0 Partitioned Parquet Tick Lake (Decoupled High-Concurrency Storage)** — Phases 15–21 (shipped 2026-10-03)
- ✅ **v4.1 Partitioned Parquet Lake Deep Testing & Hardening** — Phases 22–27 (shipped 2026-10-03)
- 🚧 **v4.2 Tick Lake Qualification & Scoped Signoff** — Phases 28–36 (in progress)

## Phases

### Completed Milestones

<details>
<summary>✅ v4.1 Partitioned Parquet Lake Deep Testing & Hardening (Phases 22–27) — SHIPPED 2026-10-03</summary>

- [x] Phase 22: Storage Foundation & Publication Edge Case Tests (1/1 plan) — completed 2026-10-03 (36/36 tests)
- [x] Phase 23: Streaming Writer & Runner Stress & Lifecycle Tests (1/1 plan) — completed 2026-10-03 (14/14 tests)
- [x] Phase 24: Versioned Symbol Registry & Dynamic Reload Stress Tests (1/1 plan) — completed 2026-10-03 (13/13 tests)
- [x] Phase 25: In-Memory DuckDB Lake Reader & Analytics Edge Tests (1/1 plan) — completed 2026-10-03 (17/17 tests)
- [x] Phase 26: Migration Tooling Rehearsal & Fuzz Tests (1/1 plan) — completed 2026-10-03 (32/32 tests)
- [x] Phase 27: Multi-Process Long-Running Soak & Chaos Tests (1/1 plan) — completed 2026-10-03 (10/10 tests)

Result: 122 new tests expanded to **746 passed offline tests** post-remediation (0 failures, 0 errors at closeout). Audit verdict: ✅ **`passed`** (F01–F11 remediated and verified; offline qualification passed; local performance gates passed).

See: [.planning/milestones/v4.1-ROADMAP.md](milestones/v4.1-ROADMAP.md) · [.planning/milestones/v4.1-REQUIREMENTS.md](milestones/v4.1-REQUIREMENTS.md) · [.planning/milestones/v4.1-MILESTONE-AUDIT.md](milestones/v4.1-MILESTONE-AUDIT.md) · [.planning/milestones/v4.1-quick/](milestones/v4.1-quick/)

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

### 🚧 v4.2 Tick Lake Qualification & Scoped Signoff (In Progress)

**Milestone Goal:** Produce reproducible evidence for the Milestone 4.0/4.1 requirements, resolve the failures that verification uncovers, and issue an explicitly scoped append-only release signoff. This milestone adds **evidence**, not features — production code changes only where a test demonstrates a defect or an approved capability is absent.

**Scope:** Q01–Q09 are in scope (Phases 28–36). Q10 (replay/rewind and offline compaction/purge) is **explicitly deferred** to v4.3 and remains excluded from the signed release scope.

**Evidence rules:** Every gate records `PASS`, `FAIL`, `BLOCKED`, or `DEFERRED` with requirement ID, test node or command, tested commit, environment, fixture identity, expected and observed result, and an artifact reference. Missing data, an unavailable repository, a skipped required test, and an empty CI rollup cannot count as `PASS`. Thresholds are never widened and regressions never removed to reach a pass.

**Source plan:** [`docs/plans/milestone-4.2-signoff-and-verification.md`](../docs/plans/milestone-4.2-signoff-and-verification.md)

#### Phase 28: CI Evidence & Requirement Traceability

**Goal**: Freeze evidence and repair traceability; confirm hosted CI for the release candidate
**Depends on**: Nothing (first phase of v4.2)
**Requirements**: [EVID-01, EVID-02, EVID-03]
**Success Criteria** (what must be TRUE):
  1. A hosted CI run URL and SHA exist for the release candidate with a green offline workflow
  2. Every `LAKE-*`, `TEST-P22-*` through `TEST-P27-*`, and F01–F11 item maps to a test node carrying real assertions
  3. The release report validator rejects missing artifacts, SHA mismatch, missing or zero metrics, skips, and non-passing statuses — each rejection unit-tested
  4. Unqualified platforms are listed explicitly rather than left implied

**Plans**: TBD

#### Phase 29: Test Isolation & Independent Oracles

**Goal**: Guarantee the larger datasets, subprocesses, and faults used by later phases cannot touch production data
**Depends on**: Phase 28
**Requirements**: [ISOL-01, ISOL-02, ISOL-03, ISOL-04, ISOL-05]
**Success Criteria** (what must be TRUE):
  1. New fixtures and benchmarks fail closed on an inherited production path, symlink alias, or unsafe output before any write
  2. Spawned processes run on explicit isolated configuration and cannot reach the production database or lake
  3. The independent oracle catches a corrupted value, a removed duplicate, and a phantom row while preserving duplicate multiplicity
  4. Interrupted runs leave no orphaned processes, threads, ports, or file handles

**Plans**: TBD

#### Phase 30: Production-Scale Performance Qualification

**Goal**: Establish the original 1M/10M-row workload, CPU reduction, freshness, and pruning evidence
**Depends on**: Phase 29
**Requirements**: [PERF-01, PERF-02, PERF-03, PERF-04, PERF-05, PERF-06, PERF-07, PERF-08]
**Success Criteria** (what must be TRUE):
  1. 1M and 10M-row datasets build reproducibly with a recorded manifest of rows, partitions, files, and seed
  2. CPU, lag, query, candle, and freshness measurements meet their declared gates, with raw samples, sample counts, and percentile method recorded
  3. Partition pruning is proven through selected file inventories or query profiling, not by a fast small query
  4. Required historical benchmarks run without skips, or their exclusion is documented instead of marked passed

**Plans**: TBD

#### Phase 31: Endurance & Sustained Recovery

**Goal**: Complete at least 24 hours of sustained concurrent load with scheduled faults and exact reconciliation
**Depends on**: Phase 30 (harness self-tests and fault scenarios pass before the long run)
**Requirements**: [ENDR-01, ENDR-02, ENDR-03, ENDR-04, ENDR-05]
**Success Criteria** (what must be TRUE):
  1. A ≥24-hour continuous run completes with continuous resource, queue, and latency sampling
  2. Every scheduled fault recovers within its declared deadline with durable records preserved
  3. Final reconciliation matches the independent input ledger exactly, distinguishing emitted, admitted, durable, published, and rejected records
  4. A deliberately aborted harness reports `INCOMPLETE` or failed, never a green report

**Plans**: TBD

#### Phase 32: Durability Boundary & Capture-Gap Contract

**Goal**: Prove what survives a crash, document the RAM-only boundary honestly, and define provider-gap behavior
**Depends on**: Phase 29
**Requirements**: [DURB-01, DURB-02, DURB-03, DURB-04, DURB-05]
**Success Criteria** (what must be TRUE):
  1. The crash matrix at every durability barrier reconciles against an independent durability ledger after restart
  2. Faulted fsync, promotion, and receipt paths produce no false committed counts, no corrupt visible files, and no duplicate retry
  3. The RAM-only loss boundary is documented as an explicit guarantee limit, not as a power-loss guarantee
  4. Provider interruption yields exact recovery where replay is supported and a visible capture gap where it is not

**Plans**: TBD

#### Phase 33: Repo B Contract & Integration

**Goal**: Verify the shared contract and the actual Repo B consumer against the candidate lake
**Depends on**: Phase 29
**Requirements**: [REPB-01, REPB-02, REPB-03, REPB-04, REPB-05]
**Success Criteria** (what must be TRUE):
  1. Portable contract tests pass in a separate process with no `src` imports
  2. Candles from the contract examples match an independent oracle across DST, timestamp ties, null volume, and late data
  3. Real Repo B reads the lake with a dummy legacy tick database exclusively locked, showing no legacy attachment
  4. Fresh reads see finalized files only, excluding staging, migration, and retired files

**Plans**: TBD

#### Phase 34: Migration, Backup & Restore Rehearsal

**Goal**: Reconcile the real historical inventory through final publication and rehearse cutover, restore, and rollback
**Depends on**: Phase 29, Phase 32
**Requirements**: [MIGR-01, MIGR-02, MIGR-03, MIGR-04, MIGR-05]
**Success Criteria** (what must be TRUE):
  1. A large frozen source with inactive symbols, ties, duplicates, and float edges reconciles through final publication by bidirectional `EXCEPT ALL`
  2. Crashes across export/checkpoint/verify/publish/receipt resume correctly, and repeat/append migrations preserve prior immutable files
  3. Cutover rehearsal stays honest under an injected stalled drain and a failed restart, with no competing writer
  4. A backup restores to a scratch destination, is queryable and reconcilable, and rollback preserves newly written live Parquet data

**Plans**: TBD

#### Phase 35: Append-Only Capacity & Maintenance Safety

**Goal**: Measure capacity growth and prove maintenance fencing and failure behavior before replacement is enabled
**Depends on**: Phase 30, Phase 31
**Requirements**: [CAPA-01, CAPA-02, CAPA-03, CAPA-04, CAPA-05]
**Success Criteria** (what must be TRUE):
  1. A capacity model reports files/day, disk usage, and discovery cost with thresholds derived from measured data
  2. Low-space and file-count thresholds alert before exhaustion and retain pending work on ENOSPC
  3. Administrative symbol deletion reports pending purge honestly and removes no active files
  4. Maintenance fences all writers, unmanaged external readers are refused, and stale markers fail closed

**Plans**: TBD

#### Phase 36: Contract Repair & Signoff Documentation

**Goal**: Make documentation, contracts, and the published signoff agree with executable evidence
**Depends on**: Phases 28–35
**Requirements**: [DOCS-01, DOCS-02, DOCS-03, DOCS-04]
**Success Criteria** (what must be TRUE):
  1. Schema, configuration, and runbook documentation match the code and their public examples execute successfully
  2. Every lake-selected runtime entry point fails closed with no silent legacy tick DB fallback
  3. The v4.2 execution report and audit report are published with the full requirement matrix and artifact links
  4. Deferred scope (Q10 replay and compaction) is visibly excluded from the signed release scope

**Plans**: TBD

## Progress

**Completed milestones:** v1.0 4/4 plans · v2.0 5/5 · v3.0 5/5 · v4.0 7/7 · v4.1 6/6 — all Complete (see collapsed sections above).

| Phase | Milestone | Plans Complete | Status | Completed |
|-------|-----------|----------------|--------|-----------|
| 28. CI Evidence & Traceability | v4.2 | 0/TBD | Not started | - |
| 29. Test Isolation & Oracles | v4.2 | 0/TBD | Not started | - |
| 30. Production-Scale Performance | v4.2 | 0/TBD | Not started | - |
| 31. Endurance & Recovery | v4.2 | 0/TBD | Not started | - |
| 32. Durability Boundary | v4.2 | 0/TBD | Not started | - |
| 33. Repo B Contract | v4.2 | 0/TBD | Not started | - |
| 34. Migration & Restore Rehearsal | v4.2 | 0/TBD | Not started | - |
| 35. Capacity & Maintenance | v4.2 | 0/TBD | Not started | - |
| 36. Contract Repair & Signoff | v4.2 | 0/TBD | Not started | - |

## Milestone Backlog

Previously unplanned follow-ups, now triaged against v4.2:

| Candidate | Disposition |
|-----------|-------------|
| Repo B end-to-end integration rehearsal against the published contract | **Planned** — Phase 33 |
| Off-hours compaction / partition consolidation | **Deferred** — v4.3 Q10b (COMP-01–05) |
| Physical `PENDING_PURGE` symbol data retirement automation | **Deferred** — v4.3 Q10b |
| Repo B-facing replay iterator (P5 gate) | **Deferred** — v4.3 Q10a (RPLY-01–05) |
| Durable disk spool (`_spool/`) for zero-loss capture across power failure | **Out of scope** — new capability, see REQUIREMENTS.md Out of Scope |
| Wire documented `STREAM_*` env vars into `StreamingEngine` / `TickLakeWriter` | **Planned** — Phase 36 (DOCS-02), only if tests show a discrepancy |

### Phase 37: CI Evidence & Requirement Traceability

**Goal:** [To be planned]
**Requirements**: TBD
**Depends on:** Phase 36
**Plans:** 0 plans

Plans:
- [ ] TBD (run /gsd-plan-phase 37 to break down)
