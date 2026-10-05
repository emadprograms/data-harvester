# Requirements: Data Harvester

**Defined:** 2026-10-04
**Core Value:** Zero-cloud, zero-quota persistent market data ingestion and storage — capture real-time market data reliably and provide sub-millisecond OHLCV querying without hitting API limits or heating up hardware.
**Milestone:** v4.3 Final Tick-Lake Implementation and Verification

**Source of truth for scope:** [`docs/plans/milestone-4.3-final-concurrency-closeout.md`](../docs/plans/milestone-4.3-final-concurrency-closeout.md) — Findings C43-01 through C43-12, Packages A through I (Phases 37–45).

> **This milestone completes the implementation and verification of the partitioned Parquet tick lake.** Every requirement below is an assertion-bearing qualification claim. A requirement is *Complete* only when its executable evidence artifact exists and records `PASS` with zero required unresolved gates. Skips, xfails, or waivers on required gates are strictly rejected.

---

## v4.3 Requirements by Phase

### Phase 37: Preflight, Test Isolation & Fail-Closed Validator (Package A)

- [x] **VALD-01**: New fixtures, logs, and benchmark tools write only inside designated safe run directories outside tracked historical artifacts; an inherited production `DATA_DIR` / `TICK_LAKE_ROOT`, symlink alias, or unsafe output path fails closed before any write.
- [x] **VALD-02**: The release report validator consumes an authoritative, independently declared required-gate inventory, rejecting `--allow-deferred` waivers on required FAIL or BLOCKED gates; missing, incomplete, or failing gates strictly fail closed.
- [x] **VALD-03**: Table-driven report mutation tests demonstrate that the validator detects missing artifacts, directory inputs instead of files, zero latency, test count conservation errors, malformed schemas, and mutated metrics (resolving C43-06).
- [x] **VALD-04**: Preflight environment characterization records candidate commit SHA, clean/dirty working tree, dependency versions, OS, hardware, filesystem, and explicit capability probes (local socket binding, subprocess lifecycle, process metrics, Node availability, public network).

### Phase 38: Migration Overlap Protection & Provenance-Scoped Verification (Package B)

- [x] **MIGR-01**: Source-coverage ledger operates independently of run scope or query filters, preventing duplicate row publication on overlapping or subset/superset runs without payload deduplication (resolving C43-01).
- [x] **MIGR-02**: Final publication verification inspects the published Parquet inventory against the frozen source using bidirectional `EXCEPT ALL`, scoped strictly to migration-owned inventory so legitimate concurrent live rows do not trigger verification failure (resolving C43-02 without xfail).
- [x] **MIGR-03**: Complete-inventory integrity audit and whole-lake audit detect unowned, foreign, corrupted, or forged-provenance files across all lake namespaces.
- [x] **MIGR-04**: Production cutover and rollback rehearsal executes through `MigrationHandoffCoordinator` and `ProcessSupervisor` under injected stalled drain, stopped supervisor, snapshot mismatch, publication failure, and post-cutover live data preservation.

### Phase 39: Reader Root Correctness & Portable Executable Contract (Package C)

- [x] **READ-01**: The reader distinguishes uninitialized roots, lost mounts, corrupted metadata, or unreadable partitions from legitimate empty lakes, raising specific structured exceptions with zero silent fallback to legacy DuckDB.
- [x] **READ-02**: Barrier-controlled snapshot race test captures the exact file list, removes an input file during execution barrier, and asserts either complete results or an explicit snapshot-unavailable error, never silent partial reads (resolving C43-07).
- [x] **READ-03**: Published markdown reader contract examples execute in an isolated subprocess with zero internal `src` imports, verifying physical schema, types, nullability, and encoded symbols against the live lake.
- [x] **READ-04**: Resampling correctness is verified against independent candle oracles across UTC/exchange date boundaries, DST shifts, leap years, null/zero volume semantics, and deterministic tie-breaking.

### Phase 40: Honest Durability Boundaries & Provider Gap Ledger (Package D)

- [x] **DURB-01**: Real OS runner lifecycle tests under SIGINT/SIGTERM verify `_shutdown_signal_handler` execution, cooperative worker queue drain, and clean process termination.
- [x] **DURB-02**: Crash matrix kills the writer at admission, intent durability, staged fsync, final promotion, directory fsync, receipt durability, and acknowledgment, reconciling against an independent external ledger in a fresh process.
- [x] **DURB-03**: Fake provider with sequence ledgers and controllable disconnect/reconnect records explicit visible gap start/end intervals and reports loss as unknown when unquantifiable.
- [x] **DURB-04**: Guaranteed RAM-only loss boundary is verified and signed honestly as the guarantee limit; no unverified live zero-loss or power-loss claims are made.

### Phase 43: Initial & Re-run Performance Qualification (Package G - Pass 1 & Pass 2)

- [x] **PERF-01**: Reproducible benchmark datasets are generated with >=19 symbols, hot-symbol skew, and session/month windows at 1M and 10M rows, with recorded manifests (resolving C43-05).
- [x] **PERF-02**: Background sampler continuously tracks peak RSS and CPU throughout benchmark execution, replacing point-in-time `max(start, end)` snapshots.
- [x] **PERF-03**: Real receive-to-visible freshness is measured through the actual runner and an independent reader process (healthy load p99 <= configured flush interval + 1 second; resolving C43-04).
- [x] **PERF-04**: Qualification harness strictly enforces upper limits: warm session 1m/5m queries p95 <100ms, warm month daily candles p95 <250ms, writer CPU >=50% reduction vs legacy baseline, and event loop lag p99 <20ms (resolving C43-03) — Pass 1 baseline established; final qualification evaluated and passed in Pass 2 (recorded in `reports/benchmarks/pass2_qualification_report.json`).
- [x] **PERF-05**: Two-pass qualification execution: Pass 1 measures raw-partition fan-out latency to inform Phase 41 compaction; Pass 2 re-qualifies performance after runtime or compaction changes (Pass 1 baseline in `pass1_baseline_measurement.json`; Pass 2 qualification in `pass2_qualification_report.json`).

### Phase 41: Capacity Monitoring, Offline Compaction & Physical Purge (Package E)

- [x] **CAPA-01**: Capacity monitor tracks files/day by symbol, small-file distribution, intent/receipt growth, free disk space, and query discovery cost, with rate-limited warning and critical alerts.
- [x] **CAPA-02**: Durable maintenance journal and consumer drain protocol stops reader admission, drains active queries/replays, pauses supervisor restarts, and refuses unmanaged external readers during maintenance.
- [x] **CAPA-03**: Offline compaction implementation (conditional on Phase 43 Pass 1 SLA requirements) writes consolidated files outside active globs, verifies multiset equivalence before replacement, and preserves an immutable receipt lineage map.
- [x] **CAPA-04**: Physical purge automation removes fenced `PENDING_PURGE` symbol files only under an inactive fenced generation, with crash-safe recovery and rollback.

### Phase 42: Market Rewind & Bounded Replay Iterator Decision (Package F - Decision Gated)

- [x] **RPLY-01**: Release decision gate for Market Rewind evaluates whether replay iterator is included in release scope: branch YES implements and qualifies replay iterator; branch NO documents omission and routes directly to Phase 43 Pass 2.
- [x] **RPLY-02**: If included, `src/storage/replay.py` provides a bounded chronological snapshot replay iterator over frozen file inventories yielding Arrow batches without full-history RAM materialization or large-OFFSET scans.
- [x] **RPLY-03**: If included, deterministic total ordering `(timestamp, symbol, ingest_id)` with stable tie-breaking and cursor resumption survives fresh process restarts.
- [x] **RPLY-04**: Portable reader contract remains fully verified and operational regardless of the replay iterator inclusion decision.

### Phase 44: 24-Hour Sustained Multi-Process Endurance Run (Package H)

- [ ] **ENDR-01**: Multi-process endurance harness runs continuously for at least 24 hours with real runner, dashboard, and independent reader under live synthetic ingestion.
- [ ] **ENDR-02**: Telemetry continuously tracks per-process/aggregate CPU, RSS, handle counts, thread counts, and queue depth with bounded growth and zero zombie processes.
- [ ] **ENDR-03**: Independent producer ledger reconciles admitted, durable, published, and rejected records separately; provider ticks during documented gaps are not misclassified as lost durable records.
- [ ] **ENDR-04**: Scheduled faults (disconnects, restarts, transient I/O errors, maintenance cycles) recover within declared deadlines with zero durable record loss.
- [ ] **ENDR-05**: Harness fails closed: deliberate premature aborts or absent telemetry produce `INCOMPLETE` / failed qualification, never green.

### Phase 45: Operational Rehearsal, Candidate CI & Milestone Closeout Audit (Package I)

- [ ] **AUDT-01**: Candidate code freeze is established; full offline test suite executes with zero required xfails, skips, or unhandled errors.
- [ ] **AUDT-02**: Hosted candidate CI run URL, commit SHA, and test logs are verified through authenticated CI checks.
- [ ] **AUDT-03**: Complete requirement-level traceability matrix reconciles all Milestone 4.3 requirements, test nodes, and artifacts with zero contradictions.
- [ ] **AUDT-04**: Release report passes hardened validation with zero required unresolved gates; final `milestone-4.3-audit-report.md` is published.

---

## Out of Scope

| Feature | Reason |
|---------|--------|
| Durable inbox / disk spool (`_spool/`) for power failure zero-loss | Architectural extension requiring group-fsync before ack; the guaranteed release boundary is the documented RAM loss boundary. |
| Provider replay protocol & retention verification | Requires external provider-specific contracts; fake provider sequence ledger and gap accounting covers the protocol boundary honestly. |
| Online live compaction / dynamic file mutation | The architecture enforces an immutable Parquet lake; compaction operates strictly during coordinated drain maintenance windows. |
| Relaxed qualification thresholds | Thresholds are strict upper limits; no retry-until-green or threshold widening to force passes. |

---

## Traceability Mapping

| Requirement | Phase | Status |
|-------------|-------|--------|
| VALD-01 | Phase 37 | Complete |
| VALD-02 | Phase 37 | Complete |
| VALD-03 | Phase 37 | Complete |
| VALD-04 | Phase 37 | Complete |
| MIGR-01 | Phase 38 | Complete |
| MIGR-02 | Phase 38 | Complete |
| MIGR-03 | Phase 38 | Complete |
| MIGR-04 | Phase 38 | Complete |
| READ-01 | Phase 39 | Complete |
| READ-02 | Phase 39 | Complete |
| READ-03 | Phase 39 | Complete |
| READ-04 | Phase 39 | Complete |
| DURB-01 | Phase 40 | Complete |
| DURB-02 | Phase 40 | Complete |
| DURB-03 | Phase 40 | Complete |
| DURB-04 | Phase 40 | Complete |
| PERF-01 | Phase 43 | Complete |
| PERF-02 | Phase 43 | Complete |
| PERF-03 | Phase 43 | Complete |
| PERF-04 | Phase 43 | Complete |
| PERF-05 | Phase 43 | Complete |
| CAPA-01 | Phase 41 | Complete |
| CAPA-02 | Phase 41 | Complete |
| CAPA-03 | Phase 41 | Complete |
| CAPA-04 | Phase 41 | Complete |
| RPLY-01 | Phase 42 | Complete |
| RPLY-02 | Phase 42 | Complete |
| RPLY-03 | Phase 42 | Complete |
| RPLY-04 | Phase 42 | Complete |
| ENDR-01 | Phase 44 | Pending |
| ENDR-02 | Phase 44 | Pending |
| ENDR-03 | Phase 44 | Pending |
| ENDR-04 | Phase 44 | Pending |
| ENDR-05 | Phase 44 | Pending |
| AUDT-01 | Phase 45 | Pending |
| AUDT-02 | Phase 45 | Pending |
| AUDT-03 | Phase 45 | Pending |
| AUDT-04 | Phase 45 | Pending |

**Coverage:**
- Total Milestone 4.3 requirements: 37
- Mapped to phases: 37
- Unmapped: 0 ✓

---
*Requirements defined: 2026-10-04*
*Derived from: docs/plans/milestone-4.3-final-concurrency-closeout.md (Packages A–I)*
