# Milestone 4.3 — Final tick-lake implementation and verification

**Date:** 2026-10-04  
**Inspected baseline:** `8b8bddb0` (`main`, merged PR #8)  
**Status:** Implementation plan; release approval is not implied  
**Purpose:** Finish the historical DuckDB-to-Parquet migration and concurrent ingestion/analytics work, correct incomplete qualification, implement the remaining operational capabilities, and produce one final evidence-backed signoff.

This is Milestone **4.3**. The final reference to “Milestone 3” in the request is interpreted as 4.3, consistent with the rest of the request. This document is the final closeout plan for this body of work; remaining required items stay open within 4.3 until finished rather than being silently moved to another milestone.

## 1. Readiness assessment

The fundamental architecture is implemented: a Parquet writer, atomic file publication/recovery, an in-memory DuckDB reader, a separate registry, migration tooling, and a supervised handoff coordinator. Milestone 4.2 added substantial tests, executed contract examples, synthetic migration/restore rehearsals, benchmark measurements, CI artifacts, and documentation corrections. Preserve that work.

**The system is not ready for unconditional final signoff.** Two migration expectations still xfail; some passing qualification tests measure a weaker property than their requirements; capacity, coordinated physical maintenance, full replay, and sustained endurance remain unfinished. Tests of a reference consumer do not prove a separate deployed application has changed its database connections. Missing access and missing implementation must be tracked separately.

Sources reviewed:

- [4.2 plan](milestone-4.2-signoff-and-verification.md), [execution](milestone-4.2-execution.md), [audit](milestone-4.2-audit-report.md), and [traceability](milestone-4.2-traceability.md).
- [Original architecture](partitioned-parquet-tick-lake.md) and [F01–F11 remediation](tick-lake-audit-remediation.md).
- [Current requirements](../../.planning/REQUIREMENTS.md), [reader contract](../contracts/repo_b_tick_lake_contract.md), and [operations guide](../operations/tick_lake_operations_guide.md).
- Production storage/reader/migration/supervisor code; new 4.2 tests, supporting workers, generators, report validator, and workflow changes since `e3005d63`.

### Findings from this review

Use the `C43-*` identifiers below. The v4.2 audit reused F10/F11 for different issues from v4.1; do not confuse v4.2 migration findings with v4.1 dry-run/handoff regressions.

| ID | Current evidence | Required disposition |
|---|---|---|
| C43-01 | `test_rerunning_with_a_different_filter_does_not_duplicate` remains strict xfail. Migration identity includes scope, so overlapping plans can republish the same source observations. | Implement source-coverage idempotence; normal passing regression. |
| C43-02 | `verify()` verifies staging before publication and checks the receipt/loads the prior verification after publication. The expected-failure test adds an unrelated live row and expects migration verification to reject it. | Define provenance-scoped final verification, correct the test's scope, and add a separate complete-inventory audit. |
| C43-03 | Scale tests require positive latency, not latency below the target. Query calls omit explicit session/month bounds; correctness only checks a non-`None` result. | Separate characterization from strict qualification; enforce workload, independent results, and thresholds. |
| C43-04 | Freshness test times `write_ticks` plus explicit flush in one process, checks counters and positive elapsed time, but does not query a separate reader or compute p99. | Measure receive-to-visible latency through the actual runner and independent reader. |
| C43-05 | Benchmark “peak RSS” is max(start RSS, end RSS). It does not sample a peak, total service RSS, or sustained queue growth. Generator defaults to 10 symbols and a 10-hour span, not the required >=19 symbols and month workload. | Repair telemetry/workload assertions and reopen the affected PERF-01/05/08 claims. |
| C43-06 | Validator accepts no artifacts, zero latency, and 0 passed out of 10 tests. `--allow-deferred` removes all nonpassing-status findings, including FAIL/BLOCKED. It has no authoritative required-gate inventory. | Harden report validation and test invalid reports through the CLI. |
| C43-07 | The “removed between resolve and query” test resolves once, removes a file, then calls a query that resolves again. | Replace with a true barrier-controlled snapshot race; retract unsupported DuckDB behavior claims. |
| C43-08 | Durability worker tests a published/RAM boundary, writes its own ledger and explicit exit code 4. Recovery runs in the parent for several cases; ID sets can hide duplicate multiplicity. | Complete actual-runner crash/fault matrix, independent ledger and multiset reconciliation. |
| C43-09 | CAPA-01..05 and ENDR-01..05 are unfinished. No production replay/compaction module exists. | Implement and qualify in this milestone. |
| C43-10 | Traceability output still declares contract/docs gaps that later tests address; module mappings do not prove individual assertions. Some report entries contradict newer entries. | Regenerate requirement-level evidence and distinguish current results from historical claims. |
| C43-11 | Contract examples pass, but actual external Repo B is unnamed. Operational historical source and deployment filesystem are not qualified. | Complete explicit deployment preflight and operator verification; do not relabel a synthetic consumer as the actual application. |
| C43-12 | New tooling isolation tests do not exercise every unsafe output/symlink path. Scale tests overwrite `.planning/artifacts` with host-specific results. | Cover artifact destinations and subprocess entry points; immutable per-run evidence directories. |

### Tests run while preparing this plan

Focused audit command covered migration/cutover rehearsals, durability, contracts, planning validator/traceability, documentation, and tooling isolation: **106 passed, 2 xfailed in 35.39 seconds**. Both xfails were the migration tests named above.

The first full offline attempt in the restricted sandbox returned **821 passed, 6 failed, 91 errors, 2 xfailed, 16 deselected** in 139.49 seconds. The errors/failures shown were denied local socket binding and process inspection. They are environment-blocked evidence, not a successful offline qualification.

A complete unfiltered run outside the sandbox returned **934 passed, 2 xfailed, zero failed/errors/skipped/deselected in 169.64 seconds**, covering all **936 collected tests**. This includes both live tests and all four historical benchmarks, using an isolated generated **4,000,000-row, 40-symbol** historical fixture. No production historical data or provider credentials were used. The two xfails are the migration cases above, not passing qualifications.

Command: `PERFORMANCE_HISTORICAL_DB_PATH=/tmp/data-harvester-v43-evidence/historical-synthetic.duckdb .venv/bin/python -m pytest tests -q -ra --tb=short --junitxml=/tmp/data-harvester-v43-evidence/all-tests.xml`. Local evidence is in `/tmp/data-harvester-v43-evidence/`: `all-tests.log`, `all-tests.xml`, `run-manifest.json`, `historical-fixture.json`, and `benchmark-current/`. These temporary artifacts should be retained in the final execution artifact store before cleanup. Test-generated tracked benchmark files were restored to their pre-run contents after copying the new measurements into that evidence directory.

This was the entire existing suite at its default settings, including the **200,000-row** lake scale fixture. It does not establish 1M/10M strict production-host qualification, a 24-hour run, or any currently unimplemented tests. The benchmark assertion weaknesses identified above still apply despite the green test exit.

## 2. Final scope and nonnegotiable contracts

**Required release scope:** concurrent capture and analytics; exact historical migration; explicit durability/loss reporting; truthful evidence tooling; capacity controls; coordinated offline compaction and purge; bounded chronological replay; executable external-reader contract; deployment rehearsal; measured production-host performance; and a completed 24-hour endurance qualification.

Previously excluded replay and offline compaction are brought into this final closeout plan because the request is to finish the remaining original work. Online compaction with live catalogs/leases remains outside the architecture: use a proven pause/drain/maintenance window. It is not necessary to solve concurrent ingestion and immutable-file reads.

1. Readers never attach the live legacy tick database. Historical candle storage and explicitly selected legacy tools may retain DuckDB where appropriate.
2. Published files are immutable and individually complete. File-by-file atomicity is not a global multi-partition transaction.
3. Preserve duplicate observation multiplicity. Deduplicating equal values/timestamps is not acceptable migration recovery.
4. Distinguish provider-emitted, admitted in RAM, durable, published, rejected, and lost/unknown records. Only evidence at the declared durability boundary supports a durable acknowledgment.
5. Zero-loss migration means all mapped rows of the frozen source reconcile exactly. Continuous provider capture during disconnect or before durable staging is a different guarantee.
6. For RAM buffering, retain the explicitly bounded loss contract and test gap reporting. Do not introduce an unconditional live zero-loss claim. A durable inbox/replay protocol is required only if that stronger guarantee is selected; its tests are specified below.
7. Maintenance cannot replace files while affected readers/replays are active. Checking a marker once is insufficient to drain an already-running query.
8. A missing configured lake, lost mount, unreadable inventory, or interrupted maintenance must not silently look like healthy empty market data. A valid initialized empty lake or absent symbol is legitimately empty.
9. A required blocked gate keeps 4.3 open. A green pytest exit with xfails/skips is not final qualification of those requirements.

## 3. Work packages and ordering

The package numbers are local to this plan; assign global phase numbers only when updating the roadmap.

| Package | Work | Dependencies | Completion evidence |
|---|---|---|---|
| A | Preflight, safe test/evidence infrastructure, validator | None | Safe roots, capability checks, adversarial validator tests |
| B | Migration coverage and final reconciliation | A | Both migration gaps replaced by passing scoped tests |
| C | Reader root/snapshot correctness and contract repair | A | Correct race tests; no silent healthy results on unavailable roots |
| D | Durability and provider-gap accounting | A, B for handoff tests | Real-runner barrier matrix and observable gap states |
| E | Capacity and coordinated offline compaction/purge | B, C, D | Crash-safe replacement, consumer drain, provenance continuity |
| F | Bounded replay and portable consumer integration | C, E contract | Exact ordered replay, cursor recovery, maintenance coordination |
| G | Correct benchmarks, baseline and performance fixes | A–F as applicable | Enforced targets on reference host at required scale |
| H | Endurance harness and >=24-hour run | A–G | Complete duration, telemetry and final reconciliation |
| I | Operational rehearsal, CI, final audit | All | Candidate-specific signoff with no required unresolved gate |

Implement short harness tests early. Run expensive qualification only when its dependencies and assertions are correct. Continue local work when a remote dependency is inaccessible; record the exact access prerequisite, not a blanket sandbox deferral.

## 4. Package A — Preflight, test isolation and trustworthy evidence

**Files:** `tests/conftest.py`, `tests/support/`, `tests/planning/`, `tools/validate_release_report.py`, `tools/requirement_traceability.py`, `.github/workflows/offline-tests.yml`, `pytest.ini`.

### Implementation

- Record candidate SHA, clean/dirty status, Python/dependency versions, OS, hardware, filesystem and resolved scratch/artifact paths. Verify local socket binding, subprocess launch/termination, process metrics, Node availability for frontend simulations, storage headroom, and public-network capability separately.
- Use a unique run directory outside tracked historical artifacts. Route benchmark output there with an explicit option/environment variable. Preserve raw logs even when tests fail. Record seed, fixture hashes, query windows, workload settings and target profile.
- Keep production write guards in place. Canonicalize output paths, reject production-root descendants and symlink aliases before writing, and ensure subprocesses receive explicit safe configuration. Test artifact/log/report outputs as well as data files.
- Make the release validator consume an independently declared required-gate inventory, not just whatever gates a submitted report happens to contain. Distinguish characterization, functional qualification and performance qualification.
- Validate report structure and types, finite numeric values, SHA identity, unique required IDs, artifact presence/content hashes, test count conservation and JUnit outcomes. Enforce target comparators with units, sample minima and per-gate schemas. Zero errors is good; zero samples or zero measured duration is not.
- Remove the ability for a final-release invocation to waive required failures. If `--allow-deferred` remains for draft reports, it must never accept FAIL/BLOCKED or produce release approval. Label its output draft/incomplete.
- Integrate report validation into final qualification; a tool merely existing is insufficient. Refresh assertion-level traceability for 4.0, 4.1, 4.2 and the new C43 packages.

### Tests to add before fixes

Use table-driven mutations of a valid report: delete an entire required gate; empty artifacts; directory instead of artifact; wrong checksum; stale SHA; invalid node; zero/negative/nonfinite latency or samples; incomplete counts; required xfail/skip; missing metric; exceeded threshold; malformed JSON/object shapes; duplicate ID; mark failed gate optional; CLI waiver with FAIL/BLOCKED. Assert nonzero CLI exit and a specific reason. Positive tests must accept zero error counters and complete passing evidence.

Reproduce C43-06 directly: a `PASS` gate with empty artifacts, latency `0`, ten total tests and zero passed currently returns no findings. Convert this into a failing regression before implementation.

Add subprocess tests for unsafe destinations, inherited roots, symlink aliases and missing flags, including benchmark/report tools that do not import pytest guards. Verify byte-for-byte unchanged protected fixture trees. Keep the real external drive out of destructive tests.

**Exit:** required reports cannot pass through omission, invalid measurements, xfails, or waiver flags; test tools demonstrably stay in scratch space.

## 5. Package B — Migration overlap protection and final-file verification

**Files:** `tools/migrate_streaming_to_parquet.py`, existing migration tests, `tests/support/migration_cli.py`, `migration_fault_runner.py`, `cutover_rehearsal.py`; add focused tests to `tests/storage/test_migration_rehearsal.py` or a dedicated overlap module.

### B1. Coverage independent of plan scope (C43-01)

Separate frozen-source identity from run identity. Bind source identity to a verified immutable snapshot/schema/projection, with an explicit policy for a moved copy of the same source. A requested symbol/date scope or chunk size must not turn previously migrated observations into new source records. Use stable source row identity or an authoritative covered-scope ledger; never infer identity by deduplicating payload values.

For a fully covered request, validate existing publication and return an idempotent result. For partly overlapping requests, migrate only verified uncovered source coverage; when coverage cannot be proven, reject before publication with an actionable reconciliation requirement. An explicit new run UUID or `--force` must not bypass source overlap protection. Distinct independently identified source datasets may intentionally append equal-valued observations.

Update coverage under the same exclusive publication ownership as final files/receipts. Persist intent, publication and coverage completion so a crash between them reconciles correctly. Bootstrap older receipts into coverage only after validating their source binding and inventory; uncertain legacy state must fail closed. Preserve schema compatibility unless a versioned change is justified.

**Test matrix:** full→subset; subset→full; partial overlap; disjoint ranges; reordered symbol lists; changed chunk size; explicit run IDs; same snapshot copied to another path; distinct snapshots with equal rows; inactive symbols; true duplicate occurrences; two competing migration processes; corrupt ledger; crashes before/after file promotion, receipt and coverage update. Assert full multiset parity, zero rewritten prior files and correct provenance. Inject failures into migration's actual `os.link` promotion as well as metadata replacement.

Remove the overlap xfail once the desired contract passes. If an ambiguous overlap is rejected by design, assert the explicit rejection and zero mutations rather than demanding an unsafe success.

### B2. Separate staging verification, published verification and inventory audit (C43-02)

Keep staging verification as publication authorization. Add explicit final-publication verification (for example a documented `verify-published` mode), and make post-publication verification perform fresh final-file checks rather than returning a stale pre-publication report.

Capture an immutable receipt/provenance inventory. Reconcile all mapped source fields and occurrence multiplicity against the final files belonging to that source coverage using bidirectional `EXCEPT ALL`. Also check ingest identities, per-symbol/date counts, physical schema, expected file checksums, and coverage completeness. After compaction, resolve validated lineage to replacement files; preserve this verification capability through Package E.

**Do not compare the entire mixed live lake to one historical source.** The existing phantom-row xfail adds an unrelated live-writer row. Such a row is not automatically historical corruption. Replace it with separate tests:

1. Missing/changed/extra rows inside the migration-owned inventory must fail final verification, including same-count value changes and duplicate substitutions.
2. Legitimate concurrent live rows and another independent source do not invalidate source-scoped reconciliation.
3. Unowned/malformed/forged-provenance files are reported by a complete-inventory integrity audit, not silently ignored.
4. A separate frozen whole-lake audit, when supplied a complete expected provenance inventory, detects unauthorized additions across all namespaces.
5. Mutation after staging verification and before publication still fails, preserving v4.1 F04/F05 protections.

Test missing final files, a valid footer with wrong contents, checksum/receipt mismatch, interrupted final audit, stale reports, and compaction lineage. Return a nonzero exit and structured differences on failure. Do not regenerate a report that silently blesses tampered data.

### B3. Production cutover and rollback

Use the existing `MigrationHandoffCoordinator` and `ProcessSupervisor` production path for integration tests. The v4.2 `CutoverRehearsal` helper alone cannot establish that operational coordination works. Do not promote its `copy2` step into a live database backup procedure: stop/checkpoint the legacy writer or use a supported consistent snapshot, then verify the frozen copy is the actual migration input.

Test stalled drain, ownership competitor, stopped supervisor/restart suspension, snapshot mismatch, publication failure, restart failure, stale handoff journal and repeated recovery. Restore into a new destination and verify the result, keeping post-cutover live data. Rollback must not overwrite the only source backup or erase live observations.

**Exit:** no unresolved migration xfail; repeated/overlapping publication preserves exact multiplicity; final verification catches owned corruption without rejecting legitimate live data; real coordinator rehearsal passes.

## 6. Package C — Reader correctness and executable contract

**Files:** `src/storage/reader.py`, `src/dashboard/analytics.py`, `docs/contracts/repo_b_tick_lake_contract.md`, `tests/contract/test_repo_b_contract.py`, reader/dashboard tests.

Distinguish a valid initialized empty lake from an unavailable configured root, corrupt metadata, unsupported schema, unreadable partition or lost mount. Define specific exceptions/status mapping and prevent implicit fallback to the legacy streaming database. Audit broad exception handlers that currently return empty results or continue past inventory errors. Preserve legitimate no-symbol/no-date empty results.

Replace C43-07's test with two distinct experiments:

- **Before resolution:** remove an input before discovery; fewer discovered files is expected. This cannot prove a stale-snapshot DuckDB behavior.
- **After resolution:** capture the exact file list passed to `read_parquet`, block at a barrier immediately before execution, remove one captured file in another process, then release the query. Assert the adapter returns complete snapshot results or an explicit snapshot-unavailable/integrity error, never healthy partial data. Independently record the pinned DuckDB version's response. A restored file followed by a fresh query is a recovery test, not proof that generic retry cures deletion.

Do not adopt row-count sanity checks as a substitute for provenance: fewer candles may be legitimate and equal row counts may contain wrong values. Offline maintenance will drain readers before replacement. Replay freezes its own inventory and reports invalidated cursors explicitly.

Add tests for aware datetimes versus equivalent UTC strings, invalid ranges/timeframes, UTC partition edges and exchange DST, null/zero volume semantics, equal timestamps, concurrent appends, connection cancellation/cleanup, bad physical schemas and encoded symbols. Use existing independent candle oracles; mutate SQL to demonstrate detection of missing predicates and incorrect open/close ordering. Preserve old endpoint range inclusivity where required; document new APIs' half-open intervals.

**Exit:** corrected race evidence, meaningful unavailable-root behavior, all contract examples passing, no unsupported silent-partial-result claim in documentation.

## 7. Package D — Complete durability and provider-gap behavior

**Files:** writer/publication/runner, provider adapters, supervisor/handoff, durability tests and helpers.

Define barriers at admission, intent durability, staged-file fsync, final promotion, directory fsync, receipt durability and acknowledgment. The injector must confirm it reached the intended operation exactly; generic failure of the first fsync does not establish every boundary. Spawn the actual runner for lifecycle claims and restart recovery in a fresh interpreter. Do not assert a helper's hard-coded exit code as the runner's behavior.

Use an independent producer ledger outside the killed child. Assert full rows and multiplicities, not only ID sets. Test a multi-partition batch where only some complete files became visible before failure. Preserve durable records, exact retry semantics and truthful counters, without promising all-or-nothing batch visibility to glob readers.

Add parameterized transient/persistent ENOSPC/EIO cases for each filesystem boundary, status-write-only failures, torn intents/receipts, repeated interrupted recovery, stale locks and directory-sync errors. Confirm faults were triggered, previous publications remain unchanged, and pending work is never reported committed or healthy-drained.

Build a fake provider with a durable sequence ledger and controllable disconnect/reconnect. Exercise disconnection while capacity is exhausted, during handoff, and during shutdown. Test both replay-capable and non-replayable modes. For non-replayable feeds, persist visible gap start/end/reason and “loss unknown” where exact count is impossible; no invented count of lost ticks. Provider credentials are not needed to implement this deterministic protocol test. A real provider's retention/replay promise requires provider-specific evidence before claiming replay support.

If durable live admission is selected, implement framed checksummed inbox segments with stable occurrence IDs, group fsync before durable acknowledgment, idempotent replay and reclamation only after verified publication. Test truncated last frames, corrupt interior frames, disk full, receipt/checkpoint races and segment cleanup. Rerun CPU/freshness qualification with that mode enabled. Otherwise sign the documented RAM loss boundary; do not mark provider-wide zero loss as delivered.

**Exit:** each boundary has a named assertion-bearing test; real-runner status/exit behavior is verified; capture gaps are visible; SIGKILL evidence is not described as a power-loss guarantee.

## 8. Package E — Capacity, offline compaction and physical purge

**Files:** add a capacity monitor and `src/storage/compaction.py` plus operational CLI; extend publication/registry/supervisor interfaces and storage/integration tests.

### Capacity

Measure files/day by symbol and UTC date, small-file sizes, receipt/intent growth, free space and discovery latency under quiet/busy schedules and late arrivals. Define warning/critical thresholds from measured query performance and storage headroom, including source backup, compaction outputs, rollback inputs and live accumulation. Surface actionable and rate-limited status. Test thresholds using injected disk statistics and real small fixtures; test ENOSPC retention without filling a production disk. `PENDING_PURGE` must stay truthful until physical cleanup completes.

### Maintenance protocol

Use a durable maintenance journal and shared publication ownership. Stop new reader admission, drain active queries/replays, pause managed restarts, and verify every configured consumer is stopped before replacing files. A marker cannot automatically stop an arbitrary external glob reader; default to refusing replacement when consumer shutdown cannot be established. A scheduled market close alone is insufficient.

For the first implementation, perform build and replacement inside the tested maintenance window. If the pause budget is too small, add a separately tested immutable-input prebuild with final inventory revalidation. In either case measure downtime and capture-gap behavior; do not assume an unbounded RAM buffer.

Write compacted outputs outside active globs, sort consistently, verify multiset equivalence, and publish unique generation filenames. Retire inputs outside active globs through a journaled sequence; never reuse a retired filename. Recovery completes or rolls back before consumers resume. Preserve an immutable lineage map from old publication IDs/files to outputs so migration audits and receipt validation still work after compaction. Integrate lineage into writer retry/recovery paths that currently expect original receipt files.

Use bounded target sizes/row groups rather than forcing an arbitrarily large day into one file. A later late-arrival batch can append to the same event date and participate in a future compaction. Purge requires an inactive fenced registry generation and explicit retention policy; do not delete source backups automatically.

### Tests

- Real writer + long-running reader/replay: maintenance waits for drain; new admissions and automatic restarts are blocked. Unknown consumers cause refusal.
- Compact exact duplicates, ties, multiple symbols/dates, existing compacted files and late arrivals. Compare every field/multiplicity before and after; validate schema and lineage.
- Kill at each journal/output/input-retirement/promotion/lineage/commit step, then recover in a new process. Consumers never resume while old+new copies or neither copy are active.
- Exercise repeated recovery, corrupt journal, stale marker, disk full, competing maintenance owners, canceled maintenance and failed resume.
- Retry an old batch receipt and run migration verification after compaction; no false corruption, missing-original-file error, or republished duplicate is allowed.
- Purge one fenced symbol while retaining others, abort on generation change, and update pending/completed status only after verified completion. Validate no active reader or retained replay snapshot loses files unexpectedly.

**Exit:** CAPA and COMP requirements pass; runbook steps are executable and rehearsed; replacement is enabled only under the tested protocol.

## 9. Package F — Bounded replay and external consumer completion

**Files:** new `src/storage/replay.py`, `tests/replay/`, portable contract fixtures/examples, actual Repo B adapter only after its repository is identified.

Provide an iterator over an explicit snapshot file inventory, bounded Arrow batches/fetches and chronological windows. Use a total order such as `(timestamp, symbol, ingest_id)` and validate the occurrence-ID uniqueness contract; if existing data violates it, fail explicitly or introduce a documented stable tie-breaker. Never silently collapse duplicate observations. Avoid full-history `fetchall`, pandas materialization and large OFFSET scans.

A cursor binds to the snapshot identity, schema/order version, selected symbols/range, window, and last emitted ordering key. Resume uses a strict lexicographic successor. New late files are excluded from a frozen replay; refreshed/live-follow behavior is separate. Offline compaction either waits for live replay and explicitly invalidates retired persisted snapshots, or retains files under an implemented retention contract. State which behavior is supported.

**Tests:** empty range; batch size one; ties across files; duplicate values; out-of-order arrivals; multi-symbol order; overlapping file ranges; resume at every batch boundary in a new process; invalid/corrupt/wrong-lake cursor; canceled iteration; connection closure; newly appended late data; maintenance waiting; retired snapshot rejection. Compare full ordered rows with an independently sorted fixture, not the replay's own SQL. At 1M/10M scale verify first-batch latency and process RSS rather than retaining output in the test.

Retain the reference consumer without `src` imports and run charts/symbol switching while another process holds the legacy DB lock. For actual Repo B integration, identify its repository, commit and entry points, update the adapter, and run the same contract suite plus its UI/API path. If the deliverable is explicitly only a portable contract with no named external deployment, label that completed boundary accurately; do not mark “actual Repo B integrated” complete. Missing repository access remains a concrete operational gate, not a fabricated test result.

**Exit:** RPLY requirements pass and the portable consumer is verified; the named deployment, when part of release scope, has its own recorded end-to-end result.

## 10. Package G — Repair performance qualification, then optimize

**Files:** `tests/performance/test_lake_scale_benchmarks.py`, `tests/support/deterministic_dataset.py`, `tools/benchmark_baseline.py`, `tools/validate_concurrency.py`, historical benchmark tests and new harness unit tests.

### Measurement fixes before performance fixes

1. Generate >=19 symbols with a hot-symbol skew; explicitly generate session and month datasets at 1M/10M rows. Assert the manifest's actual symbol count/time span, duplicate/null/late-event cases, partitions and file-size distribution. Existing 10-symbol/10-hour defaults do not satisfy those gates.
2. Exclude generation/setup consistently from matched writer CPU measurements. Include worker/child CPU, final drain, transaction/durability settings, and receipt-confirmed row counts. Reconstruct a labeled legacy baseline using the existing baseline tool/identified legacy code and the same fixture. Lack of a pre-existing benchmark artifact does not prevent a new reproducible comparison.
3. Sample current/peak per-process and aggregate RSS/CPU, queue depth and age continuously. Test sampler correctness with a known temporary allocation and a backlog spike; start/end samples alone cannot claim peak or bounded backlog.
4. Measure arrival-to-visible per ID through real runner callbacks and a separate in-memory reader process, using monotonic time and normal timer/row-trigger flushing. Compute p99 with a declared polling resolution. Do not force a flush per observation. Artificially delay visibility to prove the test fails.
5. Measure heartbeat lag in the actual runner under a deterministic fake provider. No live credentials are needed for this. Include minimum samples and scheduled-delay accounting so a blocked heartbeat cannot disappear from the percentile.
6. Define exact query windows, cold/warm labels, warm-up, repetitions and percentile method before running. Assert candle values/counts against an independent oracle. Inspect selected files for both date and symbol pruning. Keep raw measurements and errors from every run.
7. Characterization may record slow values; qualification must assert upper limits. Unit-test the evaluator with deliberately over-limit/empty/nonfinite input. A pytest pass from a measurement-only test cannot qualify a latency requirement.
8. Generate an isolated historical candle database for the four existing tests. Include required 40 symbols, >=3.9M rows, routing metadata and the fixed test date. This closes fixture availability; it is not proof of real historical-source migration. Preserve existing thresholds and report any failure honestly.

### Mandatory target profile

| Gate | Limit |
|---|---|
| Correctness | Exact required multiset/order; zero invalid finalized files |
| Concurrent DB isolation | Zero legacy tick-DB lock errors; no read/write attachment from lake mode |
| Writer CPU | >=50% reduction in CPU seconds/M valid published ticks versus documented comparable legacy baseline |
| Event-loop lag | p99 <20 ms at the declared peak input rate |
| Warm session 1m/5m; existing dashboard/Repo B gates | p95 <100 ms |
| Warm one-symbol/month daily candles | p95 <250 ms |
| Visibility freshness | Healthy-load p99 <= configured flush interval +1 second |
| Replay first batch | Warm <250 ms for the declared session workload |
| Resources | Predeclared aggregate memory/handles/queue budgets; no sustained healthy-load backlog |

Retain stricter existing test limits where applicable. Document host, concurrency, ingest rate and dataset/file layout with each measurement. Container failures remain historical evidence, and a passing Mac run does not erase them. Do not introduce threshold relaxation or retry-until-green qualification.

### Optimization direction

Profile bounded session queries first; the old scale test scanned unbounded symbol history, so its timings alone do not establish failure of the scoped-session SLA. Measure raw-file fan-out, current-session accumulation, compacted history, and mixed ingestion/query load. Apply partition predicates, batching, bounded connection concurrency and Package E compaction where evidence supports them. If current-session latency fails before a maintenance window, solve that explicitly through safe tuning or a versioned derived query path; nightly compaction cannot fix an intraday backlog by assumption. Any rollup/cache must handle late ticks, invalidation and exact OHLCV semantics with dedicated tests.

**Exit:** strict evaluator passes on the intended reference host/storage; raw and compacted limits are explicit; no skipped required benchmarks, misleading peak/freshness labels or unmeasured CPU claims.

## 11. Package H — Endurance and failure recovery

Add a duration-driven harness with a short CI smoke mode and a separate `endurance` marker/command. Update offline workflow selection to exclude endurance before adding a 24-hour test. Provide manual or dedicated-host execution, structured progress and artifacts.

Run at least **24 continuous hours** after Packages A–G pass, with real runner, dashboard, independent reader/replay and registry activity. Predeclare normal/peak rates, quiet intervals, query mix, sample windows, memory/handle budgets and fault schedule. Include provider disconnect, writer/dashboard/supervisor termination, transient disk errors, failed drain and a coordinated maintenance cycle. Expected maintenance unavailability is reported separately from healthy-load SLA, with its own maximum pause/recovery budget.

An independent producer ledger must reconcile admitted/durable/published/rejected/unknown categories over the run and after final drain. Persist bounded telemetry for per-process/aggregate CPU/RSS, handles, threads, queue depth/oldest age, inventory/free space and latency windows. Do not let the harness consume unbounded RAM holding all samples/ticks.

**Harness tests before the long run:** terminate a child before ready, fail a poll, omit samples, force threshold breach, corrupt expected ledger, interrupt the harness, leave an unfinished recovery, and exhaust its deadline. All must produce explicit failed/incomplete qualification and bounded cleanup. A shorter rerun cannot substitute for the required duration. A successful aggregate percentile cannot hide persistent failing windows.

**Exit:** complete >=24-hour report with all stages and durations, exact durable parity, no orphaned processes, bounded resources and recovery within declared deadlines. Final required work stays open until this actually runs.

## 12. Package I — Deployment rehearsal and final evidence

### Access prerequisites

| Dependency | Work possible now | Evidence still needed for operational closure |
|---|---|---|
| GitHub CI/logs | Local tests and workflow/report improvements | Read authenticated CI results for exact candidate; retrieve the unexplained historical failure if available. Missing old logs stay “unknown,” never guessed. |
| Real historical source | Synthetic migration, overlap, restore and reconciliation | Consistent frozen copy and inventory of the intended source; source→final reconciliation |
| Deployment volume | Temporary same-host filesystem tests | Dedicated scratch area on the intended volume; filesystem/locking/rename/link/durability behavior and headroom |
| Repo B | Portable fixtures/reference consumer | Named repo/commit and actual adapter test, if claiming deployed integration |
| Provider credentials | Fake-provider protocol/gap tests and public connectivity | Only for explicitly selected authenticated provider smoke checks; do not request secrets in chat |
| Long-running host | Harness, short chaos runs | Stable host for the >=24-hour run and retained telemetry |

At preparation time `gh` was installed but unauthenticated, so hosted CI was not freshly verified. This does not block offline tests or code work. Ask the operator for the specific missing access when needed; do not use a generic sandbox explanation for tests that can run with local synthetic data.

### Final sequence

1. Freeze candidate code/config and collect all nodes. Confirm no test writes to production. Build the declared isolated datasets.
2. Run every existing test plus all new deterministic tests. Run strict performance, then 24-hour endurance on the named host after their prerequisites pass. Exercise actual deployment adapters and migration/restore against approved scratch copies.
3. Run hosted offline CI on the candidate and retain URL/SHA/JUnit/logs. Diagnose repeatable failures; do not call an unexplained failure flaky. Re-run affected checks after fixes.
4. Regenerate traceability and reconcile requirement statuses with artifacts. Preserve historical results but remove contradictions in current-status sections. Every required assertion has a node/command and outcome.
5. Publish a machine-readable release report, execution report, and final audit. Validate the report with the hardened validator and required-gate inventory. Reject required xfails, skips, absent metrics and partial runs.
6. Following final documentation changes, rerun documentation-contract tests and any affected validation. Runtime/benchmark/harness changes after qualification invalidate affected evidence; rerun those gates. Metadata-only evidence commits may reference the tested code SHA, but record the exact diff proving runtime equivalence rather than pretending a self-referential report commit was tested beforehand.

**Final output files during execution:** `docs/plans/milestone-4.3-execution.md`, `.planning/v4.3-MILESTONE-AUDIT.md`, and a versioned JSON report plus retained raw artifacts. Update roadmap/requirements/state only as real gates finish. This planning task adds this document; it does not change runtime code or retroactively mark requirements complete.

## 13. Updating tests and running them

| Existing test area | Change required |
|---|---|
| v4.1 `test_tick_lake_audit_*_regressions.py` | Preserve original F01–F11 assertions; add new cases without overwriting their meaning. |
| v4.2 migration xfails | Correct verification scope, add overlap/provenance cases, then remove markers when behavior passes. Required final gates cannot remain xfailed. |
| Scale benchmarks | Explicit workloads, independent correctness, enforced limits, actual receive→visible p99 and continuously sampled resource peaks. |
| Contract removed-file test | Split pre-discovery deletion from a barrier-controlled already-resolved snapshot race; update docs from measured results. |
| Durability helper tests | Retain as unit/scaffolding tests, add actual-runner/fresh-process recovery and independent multiset ledger. |
| Release validator/traceability | Mutation and CLI tests, authoritative required gates, count/metric/artifact validation; map to exact nodes instead of vague modules. |
| Isolation | Cover logs, benchmark artifacts, exports, backup/restore, symlinks, subprocess env and all new CLI entry points. |
| Documentation tests | Validate accurate public claims/examples; do not pin obsolete defects as intended permanent behavior. |

Test-first workflow per defect: reproduce the specific assertion failure; verify the independent oracle detects a controlled mutation; implement the smallest coherent change; run focused regression and integration tests; then run the full suite. Missing coverage for working behavior may pass on its first run—record that honestly. Do not manufacture a failure to satisfy process wording.

Current supported commands:

```sh
python -m pytest tests --collect-only -q
python -m pytest tests/test_isolation_guard.py tests/support/test_tooling_isolation.py -q
python -m pytest tests -m 'not live and not performance' -q -ra
python -m pytest tests -m performance -q -ra -s
python -m pytest tests -m live -q -ra
python -m pytest tests -q -ra --junitxml=/absolute/scratch/run/all-tests.xml
git diff --check
```

Set `PERFORMANCE_HISTORICAL_DB_PATH` to the generated isolated database before collection for historical benchmarks. The complete command intentionally has no marker exclusions. Preserve skipped reasons; a public network test may skip under current test behavior, so require a separate explicit connectivity gate if that property is part of release acceptance.

Existing scale controls are `GSD_LAKE_BENCH_ROWS`, `GSD_LAKE_BENCH_SEED`, `GSD_LAKE_BENCH_BATCH` and `GSD_LAKE_BENCH_QUERIES`. They currently characterize, not fully qualify, the workload. After Package G, document exact revised qualification commands and artifact-directory flags. After adding `endurance`, use `not live and not performance and not endurance` for offline CI and a separate explicit endurance command. Do not document invented flags as already usable.

Run performance without concurrent heavy tests/jobs. Record predeclared repetitions, every measured run and all failures. Do not use reduced dataset size, removed timeframes, widened limits or unconditional skips to turn red qualification green.

## 14. Definition of done

- [ ] A: Safe harnesses and complete, fail-closed evidence validation.
- [ ] B: Overlap-safe migration, fresh provenance-scoped final verification, and real cutover/restore rehearsal.
- [ ] C: Correct root/error/snapshot behavior and accurate executable contract.
- [ ] D: Full boundary/fault coverage and truthful live gap/durability reporting.
- [ ] E: Capacity controls plus recoverable, coordinated offline compaction/purge with receipt lineage.
- [ ] F: Bounded deterministic replay, cursor recovery and verified portable/deployed consumer boundary.
- [ ] G: All required performance metrics enforced and passed on declared deployment hardware/storage.
- [ ] H: >=24-hour qualification completed with reconciliation and recovery evidence.
- [ ] I: Required operational checks and candidate CI verified; documentation, traceability and artifacts agree.
- [ ] Entire suite executed with exact results recorded; no required known defect hidden as xfail or unexecuted test counted as pass.

**Permitted final conclusion:** concurrent capture and in-memory DuckDB analytics over immutable Parquet, historical migration, offline maintenance, and bounded replay are complete for the named deployment and documented durability boundary. An unavailable required resource keeps the corresponding operational gate open in Milestone 4.3. No declaration that this is the “final milestone” overrides an unmet test or missing evidence.
