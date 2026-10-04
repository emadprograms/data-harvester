# Requirements: Data Harvester

**Defined:** 2026-10-04
**Core Value:** Zero-cloud, zero-quota persistent market data ingestion and storage — capture real-time market data reliably and provide sub-millisecond OHLCV querying without hitting API limits or heating up hardware.
**Milestone:** v4.2 Tick Lake Qualification & Scoped Signoff

**Source of truth for scope:** [`docs/plans/milestone-4.2-signoff-and-verification.md`](../docs/plans/milestone-4.2-signoff-and-verification.md) — gates Q01–Q10.

> **This milestone produces evidence, not features.** Every requirement below is a qualification claim. A requirement is *Complete* only when its evidence artifact exists and records `PASS`. Missing data, an unavailable repository, a skipped required test, or an empty CI rollup is `BLOCKED` or `DEFERRED` — never `PASS`.

## v4.2 Requirements

### Evidence & Traceability (Q01)

- [x] **EVID-01**: Operator can point to a hosted CI run for the exact release-candidate SHA with a green offline workflow (`.github/workflows/offline-tests.yml`), recorded as run URL + SHA
- [x] **EVID-02**: Operator can read a requirement-to-evidence matrix mapping every `LAKE-*`, `TEST-P22-*` through `TEST-P27-*`, and F01–F11 item to an assertion-bearing test node
- [x] **EVID-03**: Release report validator rejects a gate whose artifact is missing, whose code SHA mismatches, whose metrics are absent or zero, which skipped a required test, or whose status is not passing — each rejection unit-tested

### Test Isolation & Independent Oracles (Q02)

- [ ] **ISOL-01**: New fixture and benchmark tools write only inside a designated scratch directory; an inherited production `DATA_DIR` / `TICK_LAKE_ROOT`, symlink alias, or unsafe output path fails before any write
- [ ] **ISOL-02**: Every spawned process receives explicit isolated configuration and cannot fall back to the production database or external lake
- [x] **ISOL-03**: Deterministic generator produces stable IDs, timestamp ties, duplicate observations, null/zero volume, late arrivals, encoded symbols, and session boundaries
- [x] **ISOL-04**: Independent oracle detects a corrupted value, a removed duplicate, and a phantom row while preserving duplicate multiplicity
- [x] **ISOL-05**: Barrier timeouts, child failures, cancellation, and test exceptions clean up processes, threads, temporary ports, and file handles; a child exit before readiness fails the parent

### Production-Scale Performance (Q03)

- [x] **PERF-01**: Reproducible 1M and 10M-row datasets can be built with ≥19 symbols, hot-symbol skew, session and month windows, with a manifest recording rows, partitions, file sizes, distribution, and seed
- [ ] **PERF-02**: *(BLOCKED — no reproducible baseline; gap LAKE-P0-03)* Writer CPU seconds per million ticks show ≥50% reduction against a documented comparable baseline under matched input, durability semantics, machine, filesystem, and publication counts
- [ ] **PERF-03**: *(NOT MEASURED — needs live runner and provider)* Event-loop scheduling lag is p99 <20 ms at the declared peak input rate
- [ ] **PERF-04**: *(FAIL on the executing container at 1M/10M rows; not a production-host verdict — see execution report finding F2)* Warm one-symbol/session 1m and 5m queries are p95 <100 ms, and one-symbol/month daily candles are p95 <250 ms, with output correctness checked alongside timing
- [x] **PERF-05**: Visibility freshness (receive time → finalized file, monotonic clock, separate reader) is p99 ≤ configured flush interval + 1 second under healthy load
- [x] **PERF-06**: Partition pruning is demonstrated through selected file inventories or query profiling, including unrelated-symbol fixtures
- [ ] **PERF-07**: *(DEFERRED — needs production historical database)* The four historical resampling benchmarks execute with `PERFORMANCE_HISTORICAL_DB_PATH` set; the qualification command fails on a missing dataset or a skipped required benchmark
- [x] **PERF-08**: No sustained healthy-load backlog; bounded queue depth and aggregate RSS stay within the predeclared host budget, with sample counts and cache conditions predeclared

### Endurance & Sustained Recovery (Q04)

- [ ] **ENDR-01**: A ≥24 continuous hour run completes with writer, dashboard, independent reader, and registry activity under predeclared rates, query mix, and fault schedule
- [ ] **ENDR-02**: Per-process and aggregate CPU/RSS, handles, threads, queue depth, oldest pending age, file/receipt/intent counts, free space, and latency windows are sampled throughout, with explicit warm-up and post-warm-up growth limits
- [ ] **ENDR-03**: Scheduled kill/restart, provider disconnect/reconnect, transient storage errors, registry changes, and failed drains recover within the declared deadline with durable records preserved
- [ ] **ENDR-04**: Durable/committed IDs reconcile continuously and at final drain against an independent input ledger held outside the killed process
- [ ] **ENDR-05**: An aborted harness reports `INCOMPLETE` / failed qualification; an unexpected child death or absent telemetry fails the run rather than producing a green report

### Durability Boundary & Capture Gap (Q05)

- [x] **DURB-01**: Crash matrix kills the writer at each durability barrier (queue admission, intent, staged file, promotion, receipt, acknowledgment) and reconciles IDs after restart in a fresh process
- [x] **DURB-02**: Faulted file writes, fsync, promotion, directory fsync, receipt/status writes, and recovery produce no false committed counts, no corrupt visible files, and no duplicate retry — including a batch spanning multiple partitions
- [x] **DURB-03**: The RAM-only loss boundary is demonstrated and documented honestly, and is not presented as a power-loss guarantee
- [ ] **DURB-04**: *(DEFERRED — needs a provider replay protocol and retention guarantees)* Provider disconnects during queue saturation and during handoff yield exact recovery where replay is supported and a visible capture-gap interval/counter where it is not
- [x] **DURB-05**: Status and exit codes distinguish healthy stop, failed drain, durable pending recovery, and unrecoverable RAM-only pending work, with the affected interval discoverable by operators

### Repo B Contract & Integration (Q06)

- [x] **REPB-01**: A consumer in a separate process with no `src` imports validates physical schema, types, nullability, encoded symbols, UTC partition selection, empty-lake behavior, ties, and duplicate multiplicity
- [x] **REPB-02**: Contract examples execute as tests; 1m/5m/1d candles match an independent oracle including UTC/exchange date boundaries, DST, null/zero volume, and late data
- [x] **REPB-03**: Real Repo B opens charts and switches symbols while a dummy legacy tick database is exclusively locked, with no accidental legacy attachment and exact results verified
- [x] **REPB-04**: A fresh request observes newly finalized files and excludes staging, migration, and retired files; snapshot semantics are documented
- [x] **REPB-05**: Cancellation, connection cleanup, concurrent readers, missing roots, maintenance pause/resume, and stale snapshot handling all behave as specified

### Migration, Backup & Restore Rehearsal (Q07)

- [x] **MIGR-01**: A large frozen source containing inactive symbols, duplicates, ties, nulls, float edge values, and late events migrates through to the final published inventory
- [x] **MIGR-02**: *(reconciliation against final output is performed by the suite; the tool's own verify mode remains staging-based — see finding F11)* Source and final published files reconcile by bidirectional `EXCEPT ALL` on all mapped fields plus per-symbol/date counts and registry mapping, verified against final output rather than staging
- [x] **MIGR-03**: *(re-running with a different date filter duplicates partitions — see finding F10)* Crashes between export/checkpoint/verify/publish/receipt restart in a fresh CLI process; repeat and append migrations preserve exact multiplicity and all immutable prior files
- [x] **MIGR-04**: Cutover rehearsal (stop/drain, frozen-source capture, ownership release, publish, restart, reconcile) behaves honestly under an injected stalled drain and a failed restart
- [x] **MIGR-05**: A retained backup restores into a new scratch destination, is queryable and reconcilable, and rollback preserves newly written live Parquet data

### Capacity & Maintenance Safety (Q08)

- [ ] **CAPA-01**: Files/day by symbol and date, receipt/intent growth, disk usage, and query discovery cost are simulated for realistic quiet/busy schedules, including late ticks to old partitions
- [ ] **CAPA-02**: Low-space and file-count thresholds produce actionable status before exhaustion, rate-limited reporting, and safe retention of pending work on ENOSPC
- [ ] **CAPA-03**: Administrative symbol deletion reports pending purge honestly and removes no active files; no startup/cleanup path performs uncoordinated retention
- [ ] **CAPA-04**: Maintenance fences writer, migration, recovery, and service restart paths; consumer drain is verified and an unmanaged external reader is refused
- [ ] **CAPA-05**: Stale maintenance markers and interrupted operations fail closed with a documented recovery path; a marker is never cleared merely because its process exited

### Documentation & Signoff (Q09)

- [ ] **DOCS-01**: Schema documentation matches `src/storage/schema.py` (nine columns including `ingest_id`) and public examples execute successfully against generated fixtures
- [ ] **DOCS-02**: Configuration precedence, actual flush defaults, data-root selection, empty registry startup, and legacy compatibility selection match verified behavior in README and runbooks
- [ ] **DOCS-03**: Every supported runtime streaming entry point maps to its intended backend, and lake-selected paths fail closed instead of silently reopening the legacy tick DB
- [ ] **DOCS-04**: v4.2 execution report and audit report are published with the requirement matrix, CI evidence, benchmark artifacts, migration/restore reports, Repo B result, durability contract, and explicit deferred scope

## v4.3 Requirements

Deferred from v4.2 by design (Q10). Tracked, not in the current roadmap, and **excluded from the append-only release scope**.

### Replay / Rewind (Q10a)

- **RPLY-01**: Bounded snapshot replay over an explicit immutable file list with deterministic total ordering and a documented multi-symbol tie-breaker
- **RPLY-02**: Resumable cursor tied to the snapshot survives process restart, with no whole-history materialization or large-OFFSET iteration
- **RPLY-03**: Actual Repo B playback uses this API, with warm time-to-first-batch <250 ms at 1M/10M rows and memory bounded by a predeclared budget
- **RPLY-04**: Late-arrival and snapshot semantics are captured explicitly; maintenance waits for active replay or invalidates unsupported resume snapshots
- **RPLY-05**: Resource cleanup on early iterator close, plus cancellation, empty intervals, and symbol/date boundary tests

### Offline Compaction & Physical Purge (Q10b)

- **COMP-01**: Prebuilt outputs in excluded staging verify full multiset equivalence before any replacement
- **COMP-02**: Unique replacement names and a recoverable journal let recovery finish or roll back before any reader restarts
- **COMP-03**: Repeat compaction, late arrivals, partition boundaries, inactive-symbol purge, disk full, and competing owners are tested
- **COMP-04**: A crash after every retirement/promotion/journal step leaves the resumed reader with exactly the expected multiset
- **COMP-05**: Backups survive failed replacement; compaction time and headroom fit the tested pause budget

## Out of Scope

| Feature | Reason |
|---------|--------|
| Durable inbox / disk spool for zero-loss live capture | New capability requiring group-fsync before acknowledgment, not a documentation fix; zero loss is scoped to verified frozen-source migration (Q05) |
| Provider acknowledgment/replay protocol | Depends on provider retention guarantees not yet verified; capture-gap accounting covers the interim (DURB-04) |
| Enabling physical replacement / compaction | Blocked until Q10b passes and all readers can be drained and fenced (CAPA-04 keeps it disabled) |
| Replay / market-rewind signoff | Requires Q10a; the append-only signoff explicitly lists rewind as deferred |
| Cross-platform qualification claims | Only the deployment host and Linux CI are qualified; other platforms are listed as unqualified (EVID-01) |
| Live / provider tests in hosted CI | Explicitly excluded from the offline workflow; run manually and recorded separately |

## Traceability

| Requirement | Phase | Status |
|-------------|-------|--------|
| EVID-01 | Phase 28 | Complete |
| EVID-02 | Phase 28 | Complete |
| EVID-03 | Phase 28 | Complete |
| ISOL-01 | Phase 29 | Pending |
| ISOL-02 | Phase 29 | Pending |
| ISOL-03 | Phase 29 | Complete |
| ISOL-04 | Phase 29 | Complete |
| ISOL-05 | Phase 29 | Complete |
| PERF-01 | Phase 30 | Complete |
| PERF-02 | Phase 30 | Pending |
| PERF-03 | Phase 30 | Pending |
| PERF-04 | Phase 30 | Pending |
| PERF-05 | Phase 30 | Complete |
| PERF-06 | Phase 30 | Complete |
| PERF-07 | Phase 30 | Pending |
| PERF-08 | Phase 30 | Complete |
| ENDR-01 | Phase 31 | Pending |
| ENDR-02 | Phase 31 | Pending |
| ENDR-03 | Phase 31 | Pending |
| ENDR-04 | Phase 31 | Pending |
| ENDR-05 | Phase 31 | Pending |
| DURB-01 | Phase 32 | Complete |
| DURB-02 | Phase 32 | Complete |
| DURB-03 | Phase 32 | Complete |
| DURB-04 | Phase 32 | Pending |
| DURB-05 | Phase 32 | Complete |
| REPB-01 | Phase 33 | Complete |
| REPB-02 | Phase 33 | Complete |
| REPB-03 | Phase 33 | Complete |
| REPB-04 | Phase 33 | Complete |
| REPB-05 | Phase 33 | Complete |
| MIGR-01 | Phase 34 | Complete |
| MIGR-02 | Phase 34 | Complete |
| MIGR-03 | Phase 34 | Complete |
| MIGR-04 | Phase 34 | Complete |
| MIGR-05 | Phase 34 | Complete |
| CAPA-01 | Phase 35 | Pending |
| CAPA-02 | Phase 35 | Pending |
| CAPA-03 | Phase 35 | Pending |
| CAPA-04 | Phase 35 | Pending |
| CAPA-05 | Phase 35 | Pending |
| DOCS-01 | Phase 36 | Pending |
| DOCS-02 | Phase 36 | Pending |
| DOCS-03 | Phase 36 | Pending |
| DOCS-04 | Phase 36 | Pending |

**Coverage:**
- v4.2 requirements: 45 total
- Mapped to phases: 45
- Unmapped: 0 ✓
- Deferred to v4.3: 10 (RPLY-01–05, COMP-01–05)

---
*Requirements defined: 2026-10-04*
*Last updated: 2026-10-04 after Phase 28 and Phase 29 execution*
*Derived from: docs/plans/milestone-4.2-signoff-and-verification.md (Q01–Q10)*
