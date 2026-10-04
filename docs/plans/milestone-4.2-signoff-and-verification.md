# Milestone 4.2 — Close qualification gaps and verify the tick lake

**Created:** 2026-10-04  
**Status:** Planning only; no new qualification results claimed  
**Code baseline inspected:** `e3005d63` on local `main`, tracking `origin/main`; during drafting, archival commit `5a2dc2bd` moved the v4.1 audit into `.planning/milestones/`  
**Goal:** Produce reproducible evidence for the Milestone 4.0/4.1 requirements, resolve failures uncovered by that verification, and issue an explicitly scoped release signoff.

## 1. Starting point and evidence rules

Milestone 4.1's F01–F11 remediation is recorded as passing. The follow-up report records **746 offline tests passed**, **5 performance tests passed**, and **4 optional historical benchmark tests skipped**. Those are historical results on the documented host, not fresh results from this planning task. The performance follow-up is present in the current local `main`; the earlier statement that it was only on an unmerged branch is obsolete. Remote branch and hosted CI state must be checked during execution.

Read these sources before implementation:

- [Original architecture and release gates](partitioned-parquet-tick-lake.md).
- [F01–F11 remediation contracts and acceptance criteria](tick-lake-audit-remediation.md).
- [Execution report and exact prior measurements](tick-lake-audit-remediation-execution.md).
- [Archived v4.1 audit](../../.planning/milestones/v4.1-MILESTONE-AUDIT.md).
- [Archived v4.0 requirements](../../.planning/milestones/v4.0-REQUIREMENTS.md) and [v4.1 requirements](../../.planning/milestones/v4.1-REQUIREMENTS.md).
- [Repo B contract](../contracts/repo_b_tick_lake_contract.md).

The gaps below are missing evidence, unresolved scope, or documentation discrepancies. They are **not all reproduced runtime defects**. Do not rewrite working subsystems merely because a qualification item is open. First add or strengthen a meaningful test, observe its result, and change production code only when the result demonstrates a defect or an approved capability is absent.

Every final claim needs: requirement ID, exact test node or reproducible command, tested commit, environment, fixture identity, expected result, observed result, and an evidence artifact. Use `PASS`, `FAIL`, `BLOCKED`, or `DEFERRED` per gate. Missing data, an unavailable repository, a skipped required test, and an empty CI rollup cannot count as `PASS`.

## 2. Scope and delivery order

Complete **Q01–Q09** for the scoped append-only release. Q10 contains features deliberately deferred by the original plan. Full market-rewind signoff additionally requires Q10 replay; safe physical compaction/purge signoff additionally requires Q10 maintenance. A deferred feature must remain visibly excluded from the final signed scope.

1. **Q01:** Freeze evidence and repair traceability; confirm CI.
2. **Q02:** Strengthen isolation and independent verification helpers.
3. **Q03–Q06:** Add tests for production-scale performance, endurance, durability boundaries, and actual Repo B integration.
4. **Q07–Q08:** Rehearse migration/restore and prove append-only capacity and maintenance boundaries.
5. Fix demonstrated failures in small changes, preserving all F01–F11 regressions.
6. **Q09:** Reconcile public contracts and operational documentation with tested behavior.
7. Run final qualification on one release candidate and publish the signoff matrix.

Do not start a 24-hour run before its short harness self-tests and fault scenarios pass. Do not change latency thresholds to make a failing implementation qualify. Any scope or target change must be explicitly recorded as a decision, never silently converted into a pass.

## 3. Q01 — Current CI and requirement traceability

**Gap:** The audit still records hosted CI pending and preserves historical phase tables whose verification claims are withdrawn. Archived requirements say shipped despite missing formal per-phase evidence. A local suite run does not validate the Linux workflow or all archived requirements.

**Tests and verification**

- Inspect GitHub runs for the actual candidate SHA, including workflow/job conclusions and logs. The existing workflow is `.github/workflows/offline-tests.yml`; it runs offline tests on pull requests and `main` pushes using Linux/Python 3.11. Record the run URL and SHA; no checks means no evidence.
- Collect current test nodes and map every `LAKE-*`, `TEST-P22-*` through `TEST-P27-*`, and F01–F11 requirement to assertions. A matching filename or test name alone is insufficient. Identify tests that only prove startup, nonempty output, or row counts where stronger behavior was promised.
- Add a release report validator that rejects required gates with missing artifacts, mismatched code SHAs, missing metrics, skips, or nonpassing statuses. Unit-test each rejection, including zero measurements and an empty test selection.
- Confirm current tests pass on the deployment host and Linux CI. Test other platforms only if they are claimed supported; list unqualified platforms explicitly.

**Implementation direction:** Preserve the offline workflow, add artifact upload for test results/logs even on failure, and use separate explicit jobs or commands for expensive performance/endurance work. Do not put a 24-hour job inside the existing 20-minute offline job. Add manual qualification dispatch if useful. A GitHub runner is functional portability evidence, not a substitute for the named performance reference host.

**Exit evidence:** Green hosted offline run for the candidate, host/environment manifest, and a complete requirement-to-evidence matrix. Write fresh v4.2 verification evidence; retain older results as history without presenting them as current passes.

## 4. Q02 — Safe fixtures and independent oracles

**Gap:** The next tests will use much larger datasets, subprocesses, faults, and possibly an external volume. Existing isolation must cover these new paths before they run.

**Extend:** `tests/conftest.py`, `tests/test_isolation_guard.py`, and `tests/support/{lake_assertions,faults,process_harness,migration_factory,tree_snapshot}.py`.

**Required tests**

1. New fixture/benchmark tools create only a designated scratch directory. Inherited production `DATA_DIR`, `TICK_LAKE_ROOT`, symlink aliases, and unsafe output paths must fail before any write. Resolve paths before checking containment; reject production aliases even if spelled differently.
2. Every spawned process receives explicit isolated configuration. Assert no child can fall back to the real database or external lake. Faults and cleanup remain confined to fixtures.
3. Use a deterministic generator with stable IDs, timestamp ties, duplicate observations, null/zero volume, late arrivals, encoded symbols, and session boundaries. Assert its expected row multiset independently of the writer and reader being tested.
4. Corrupt one value, remove one duplicate, and add one phantom row to prove the oracle catches all three. Preserve duplicate multiplicity; a set or row count cannot establish parity.
5. Ensure barrier timeouts, child failures, cancellation, and test exceptions clean up processes, threads, temporary ports, and file handles. A child exit before readiness must fail the parent promptly.

**Implementation direction:** Reuse existing helpers. Add only missing independent metrics/dataset helpers, with deterministic seeds and bounded memory. For large fixtures, use partitioned reconciliation or external sorting rather than loading millions of rows into a Python list. Do not build expected results by calling the production reader.

**Exit evidence:** Isolation tests pass before expensive tests; deliberately broken fixture variants fail their independent oracle.

## 5. Q03 — Complete performance qualification at meaningful scale

**Gap:** The 6k/10k tick results establish useful local regressions, but do not establish the original 1M/10M-row workload, CPU reduction, historical file fan-out, daily candles, or visibility freshness. Four historical benchmarks remain unexecuted.

**Extend:** `tools/validate_concurrency.py`, `tests/integration/test_supervisor_chaos_soak.py`, and `tests/database/test_resampling.py`. Add lake-specific benchmarks under `tests/performance/`; the historical database suite must not be treated as a substitute for lake benchmarks.

**Test construction**

- Build reproducible 1M and 10M-row datasets with at least 19 symbols, a hot-symbol skew, one-session and month windows, and realistic file counts from both active and quiet periods. Record row counts, partition counts, file sizes, distribution, and seed.
- Compare legacy DuckDB and Parquet writer CPU seconds per million ticks using the same valid input, batching/durability semantics, machine, filesystem, and completed publication counts. Include worker/child CPU and final drain; exclude fixture generation and query setup consistently. Document any unavoidable baseline difference. If a historical executable baseline is unavailable, reconstruct a labeled baseline from an identified commit in isolation.
- Measure active ingestion plus 1m/5m charts, daily candles, symbol switching, continuity, and independent reader queries. Keep output correctness checks alongside timing checks.
- Measure receive-to-finalized-file visibility using a monotonic clock and a separate reader. Use receive time for freshness; old event timestamps in late ticks are not publication latency.
- Verify pruning through selected file inventories or query profiling, including unrelated-symbol fixtures. A fast small query alone does not prove partition pruning.
- Separate warm-up and measured samples; predeclare sample count, percentile method, cache condition, repetition count, and rate. Record all runs, errors, and timeouts. Zero successful requests or zero heartbeat samples must fail, not produce a zero-millisecond pass. Do not rerun until a lucky pass and discard failures.
- Generate an isolated historical database compatible with the four existing resampling tests, then set `PERFORMANCE_HISTORICAL_DB_PATH` explicitly. Keep the safe skip for ordinary developer runs, but the qualification command must fail if the dataset is missing or any required benchmark skips. If those benchmarks are outside the released scope, explicitly explain that exclusion rather than marking them passed.

**Gates retained from the original requirements/plan**

| Measurement | Required outcome |
|---|---|
| Data parity, footer validity, tick-DB lock errors | Exact multiset parity; zero invalid finalized files; zero lock errors |
| Writer CPU seconds per million ticks | At least 50% reduction against documented comparable baseline |
| Event-loop scheduling lag | p99 <20 ms at declared peak input rate |
| Warm one-symbol/session 1m and 5m queries; existing dashboard/Repo B gates | p95 <100 ms |
| Warm one-symbol/month daily candles | p95 <250 ms |
| Healthy-load visibility freshness | p99 <= configured flush interval +1 second |
| Backpressure and memory | No sustained healthy-load backlog; bounded queue and aggregate RSS within predeclared host budget |

The original design called some targets proposed; archived v4.0 requirements promoted CPU, lag, and chart targets into release requirements. Resolve any target-status discrepancy before benchmarking. Preserve the already enforced limits. State measured throughput, host budgets, and sample sizes before accepting performance results.

**Implementation direction:** Extend the validator's configuration and structured metrics; do not add test-dependent production branches. Profile failures before optimizing. Possible fixes include file discovery, query predicates, connection concurrency, batching, and bounded immutable metadata caches. Preserve current DST and response-isolation regressions when changing continuity caching.

**Exit evidence:** Raw measurements, summarized percentiles, baseline comparison, and test results on the named reference machine and actual intended storage medium using isolated scratch data.

## 6. Q04 — Real endurance and sustained recovery

**Gap:** A test named soak that finishes after 10k ticks is not the original requirement of at least 24 hours under sustained load. Short RSS tests do not prove long-term resource behavior.

**Extend:** The existing chaos/soak tests and concurrency tool. Add a duration-based runner and a short deterministic harness test; mark expensive tests separately, such as `endurance`, and register the marker before use.

**Required scenarios**

- Run at least **24 continuous hours** on the candidate with writer, dashboard, independent reader, and registry activity. Predeclare normal/peak rates, query mix, fault schedule, and recovery deadlines. Include long quiet intervals and a burst so batching/file churn are exercised.
- Sample per-process and aggregate CPU/RSS, file descriptors or platform-equivalent handles, threads, queue depth, oldest pending age, file/receipt/intent counts, free space, and latency windows throughout the run. Warm-up memory bounds and post-warm-up growth limits must be explicit. Aggregate p95 alone must not hide failing windows.
- Kill/restart writer, dashboard, and supervisor at controlled points; exercise provider disconnect/reconnect, transient storage errors, registry changes, failed drain, and recovery. Use real runner processes for lifecycle claims. Keep errors expected during a declared fault separate from healthy operation; assert recovery within the declared deadline and preserved durable records.
- Reconcile durable/committed IDs throughout the run and at final drain. Keep an independent input ledger outside the killed process. Do not confuse producer-emitted, RAM-admitted, durable, published, and rejected records.
- Abort the harness itself in a short test and ensure the result is `INCOMPLETE`/failed qualification, never a green report. An unexpected child death or absent telemetry must fail the run.

**Implementation direction:** Stream metrics to bounded/rotated artifacts; retain summaries and fault timestamps. Prevent test instrumentation from accumulating all ticks or samples in RAM. Implement deterministic barriers and readiness checks rather than arbitrary sleeps for fault placement.

**Exit evidence:** Complete 24-hour report, all fault/recovery outcomes, stable resource measurements, exact reconciliation, and no orphaned services after cleanup. A shortened smoke run validates the harness only.

## 7. Q05 — Durability boundary and capture-gap contract

**Gap:** Historical migration zero loss, graceful drain, retry idempotence, and survival of RAM-only ticks after a hard kill are different guarantees. F01–F11 fixes do not make RAM durable. A supervised migration pause can also leave a provider capture gap.

**Extend:** Writer/runner audit regressions, storage publication/recovery tests, and real-process chaos tests. Preserve passing F01–F03 and F11 coverage.

**Required tests**

1. Identify barriers for queue admission, intent persistence, staged file persistence, final publication, receipt persistence, and queue acknowledgment. Kill the writer at each applicable barrier; restart in a fresh process and compare IDs against an independent durability ledger.
2. Fault file writes, file fsync, final promotion, directory fsync, receipt/status writes, and recovery itself. Assert no false committed counts, no corrupt visible files, no duplicate retry, and explicit errors when durability cannot be established. Include a batch spanning multiple partitions: per-file atomic visibility does not imply atomic visibility of the entire batch.
3. For the RAM-only interval, demonstrate and document the recoverable boundary honestly. A graceful drain or readable file after process termination is not a power-loss test. Separately validate supported filesystem durability behavior in scratch space; never unplug or damage a production volume for a test.
4. Inject a provider disconnect during queue saturation and during handoff. Use a fake provider that records sequence IDs independently and supports controlled replay/non-replay behavior. Verify exact recovery when replay is supported, and a visible capture-gap interval/counter when it is not.
5. Assert status and exit codes distinguish healthy stop, failed drain, durable pending recovery, and unrecoverable RAM-only pending work. Confirm operators can find the affected interval and recovery instructions.

**Implementation decision:** For the original append-only release, explicitly scope zero loss to verified frozen-source migration and the documented durable boundary. If the product requires every acknowledged live tick to survive abrupt termination, implement a checksummed durable inbox with group fsync before durable acknowledgment, or a verified provider acknowledgment/replay protocol. This is an additional capability, not a documentation fix.

For a durable inbox, add tests for truncated final frames, corrupt nonfinal frames, disk full, acknowledgment-before-fsync prevention, replay after publication-before-checkpoint failure, duplicate delivery, stable IDs, and safe segment reclamation only after publication receipts cover every record. Measure its CPU/freshness cost in Q03. Provider replay needs retention/sequence-gap and duplicate-multiplicity tests; do not deduplicate distinct equal-valued observations.

**Exit evidence:** Explicit guarantee and loss boundary, crash matrix, provider gap/replay results, and durability-mode performance results if that mode is introduced. An unconditional live zero-loss claim remains blocked without the stronger capability.

## 8. Q06 — Actual Repo B contract and integration

**Gap:** The validator's standalone DuckDB subprocess is a useful reference consumer. It does not prove that the actual analytics/rewind repository stopped opening the legacy tick database or obeys maintenance rules.

**Extend:** `docs/contracts/repo_b_tick_lake_contract.md` and standalone consumer tests. Add portable golden fixtures and executable contract tests under `tests/contracts/`. Inventory Repo B before naming its adapter files; record its repository and commit as an execution prerequisite.

**Required tests**

- Run a consumer in a separate process with no `src` imports. Verify physical schema/types/nullability, encoded symbols, UTC partition selection, empty lake behavior, invalid schema, timestamp ties, duplicate multiplicity, and in-memory connection use.
- Execute contract examples as tests. Compare 1m/5m/1d candles with an independent oracle, including UTC/exchange date boundaries, DST, null/zero volume, and late data. Preserve existing endpoint inclusivity where promised; new range APIs must specify boundaries.
- Keep a dummy legacy tick database exclusively locked while real Repo B opens charts and switches symbols. Verify actual consumer behavior and exact results, not only absence of a thrown exception. Inspect its configuration/runtime database opens for accidental legacy attachment.
- Verify a fresh request observes newly finalized files and excludes staging/migration/retired files. Document that a captured file list is a query snapshot and that separate files from one batch may become visible at different times.
- Test cancellation, connection cleanup, concurrent readers, unavailable/missing roots, maintenance pause/resume, and stale snapshot handling. Plain external globs do not participate in an advisory maintenance lock automatically.

**Implementation direction:** Update the actual Repo B adapter to the shared contract, preserving API behavior. Keep portable consumer tests independent of writer internals. Record actual end-to-end command, Repo A/B SHAs, data fixture hash, and results. If Repo B is unavailable, complete local contract work and leave external integration `BLOCKED`; do not substitute synthetic evidence.

**Exit evidence:** Portable contract passes and the real Repo B integration passes on the candidate lake. Full rewind semantics are separately covered by Q10.

## 9. Q07 — Production-shaped migration, backup and restore rehearsal

**Gap:** Fault regressions validate migration code but do not prove that the actual historical inventory, inactive symbols, capacity, backup, and operational cutover have been reconciled successfully.

**Extend:** `tests/storage/test_migration_tool.py`, `test_migration_stress.py`, `test_tick_lake_audit_migration_regressions.py`, and F11 supervised handoff tests.

**Required tests and rehearsal**

- Generate a large frozen source containing inactive symbols, duplicate rows, timestamp ties, nullable values, floating-point edge values within the supported schema, and late events. Cover supported source schema variants and explicit rejection of unsupported mappings.
- Compare source and final published files using bidirectional `EXCEPT ALL` on all mapped fields, plus per-symbol/date counts and registry mapping. Verification must target the final published inventory, not only staging. Separate migrated provenance from concurrent live provenance; event timestamps alone cannot distinguish them.
- Crash between export/checkpoint/verify/publish/receipt steps and restart a fresh CLI process. Re-run the same migration and append a different source. Preserve exact multiplicity and all immutable prior files. Retain F04/F05/F10 tampering and dry-run cases unchanged in intent.
- Rehearse stop/drain, frozen-source capture, ownership release, migration publish, restart, and reconciliation. Inject a stalled drain and failed restart; assert honest state, no competing writer, and retained recovery information. Include Q05's feed-gap accounting.
- Restore a retained backup into a new scratch destination and prove it can be queried and reconciled. Test insufficient space and interrupted restore without overwriting the only backup. Rehearse rollback while preserving new live Parquet data; simply restarting the old database writer is not a complete rollback.
- For operational evidence, use a verified consistent copy of the intended historical database and the actual destination filesystem in a dedicated scratch directory. Do not copy an actively changing DuckDB database without a supported consistent snapshot procedure. Record source identity, schema, inventory, checksums, storage headroom, and final reconciliation report.

**Implementation direction:** Add missing inventory/reporting and recovery steps where rehearsal reveals gaps. Preserve the legacy source and backups; no automatic deletion belongs in qualification. If real source access is unavailable, synthetic migration can pass while operational migration remains `BLOCKED`.

**Exit evidence:** Final-file reconciliation, source/registry inventory, restart and repeat-migration tests, backup restore report, and a repeatable cutover/rollback runbook.

## 10. Q08 — Append-only capacity and maintenance safety

**Gap:** Micro-batching accumulates files and metadata. An append-only first release needs measured capacity and an actionable response before performance degrades. Maintenance fencing does not stop unmanaged glob readers.

**Required tests**

- Simulate realistic quiet/busy schedules and estimate files/day by symbol and date, receipt/intent growth, disk usage, and projected query discovery cost. Include old partitions receiving late ticks.
- Test low-space/file-count thresholds with injected filesystem statistics. Require actionable status before capacity exhaustion, rate-limited reporting, and safe retention of pending work on ENOSPC. Define thresholds from Q03/Q04 measurements before signoff.
- Confirm administrative symbol deletion reports pending purge honestly and does not remove active files. Confirm no startup/cleanup path accidentally implements uncoordinated retention.
- Verify maintenance fences writer, migration, recovery, and service restart paths already covered by F06. Add consumer-drain tests for any supported maintenance operation. An external unmanaged reader must either be demonstrably stopped or replacement must remain disabled.
- Test stale maintenance markers and interrupted operations fail closed with a documented recovery path. Do not clear a marker merely because its process exited.

**Implementation direction:** Deliver capacity/status reporting and a concrete maintenance follow-up trigger. Keep physical replacement disabled until Q10 maintenance passes. A fixed clock time or market close is insufficient for crypto, replay, extended sessions, and late writes.

**Exit evidence:** Measured capacity budget, tested alert/action behavior, pending-purge contract, and explicit enabled/disabled maintenance modes.

## 11. Q09 — Repair contract and completion documentation

**Discrepancies observed at planning time, and their resolution in Phase 36:**

| # | Discrepancy | Resolution |
|---|---|---|
| 1 | Archived v4.0 text called the schema **eight** columns while the reader contract lists **nine** including `ingest_id` | Corrected in `docs/plans/partitioned-parquet-tick-lake.md` and `.planning/milestones/v4.0-REQUIREMENTS.md`; `tests/docs/test_documentation_contract.py::test_no_document_claims_an_eight_column_schema` now fails if any document reintroduces it |
| 2 | The Repo B contract listed `symbol` as Arrow `string` | Corrected to dictionary-encoded; found and fixed by executing the published examples in Phase 33 |
| 3 | The operations guide documented the runner flush interval as `2.0s` | Corrected to `5.0s` (`DEFAULT_STREAM_FLUSH_INTERVAL_SECONDS`); now asserted against the code |
| 4 | Backend fail-closed behaviour was implemented and tested (v4.1, F07) but undocumented for operators | Documented as §2.6 of the operations guide, and asserted by test |
| 5 | The v4.1 audit mixes current and withdrawn historical verification claims | Not copied into this signoff; the signoff cites only artifacts produced in v4.2 |

The eight-column count remains correct **only** for the legacy `streaming.duckdb` schema, which has no `ingest_id`; it is never correct for the tick lake.

**Tests and implementation**

- Compare schema documentation to `src/storage/schema.py`; test public examples against generated fixtures. Clarify data-file visibility, writer ownership locks, maintenance coordination, and hardware/filesystem limits. Avoid suggesting that every kind of lock or storage corruption is impossible.
- Verify configuration precedence, actual flush defaults, data-root selection, empty registry startup, legacy compatibility selection, and service restart behavior against existing F07–F09 tests. Update README/runbooks/examples where they disagree.
- Map all supported runtime streaming entry points, including tools and services, to their intended backend. Tests must ensure lake-selected paths fail closed and do not silently reopen the legacy tick DB; retain explicitly supported historical/legacy paths.
- Publish v4.2 execution and audit reports with the requirement matrix, CI evidence, benchmark artifacts, migration/restore reports, Repo B result, durability contract, and explicit deferred scope. Link from prior audit documents without erasing historical failures.

**Exit evidence:** Documentation examples execute successfully, no contradictory current completion claims remain, and every signed requirement has attributable evidence.

## 12. Q10 — Explicitly deferred capabilities

These were identified in the original architecture as follow-ups, rather than completed M4.1 features. At this baseline there is no `src/storage/replay.py` or `src/storage/compaction.py`. Do not imply that chart pagination implements rewind or that a maintenance lock implements compaction.

### Q10a: Full rewind/replay — required for full original rewind objective

Implement bounded snapshot replay with an explicit immutable file list, deterministic total ordering, a documented multi-symbol tie-breaker, and a resumable cursor tied to the snapshot. Avoid whole-history materialization and large OFFSET iteration. Capture late-arrival/snapshot semantics explicitly.

Add `tests/replay/` cases for overlapping files, timestamp ties, page size one, cancellation, process restart/resume, invalid/stale cursors, new files arriving during replay, empty intervals, and symbol/date boundaries. Compare concatenated results with an independent sorted multiset. Verify actual Repo B playback uses this API. At 1M/10M rows, require warm time-to-first-batch <250 ms and memory bounded by a predeclared process budget rather than archive size. Test resource cleanup on early iterator close. Maintenance must wait for active replay or explicitly invalidate unsupported resume snapshots.

### Q10b: Offline compaction and physical purge — required before enabling replacement

Implement only after all affected readers can be stopped/drained and writers fenced. Prebuild outputs in excluded staging, verify full multiset equivalence, and use unique replacement names and a recoverable journal. Recovery must finish or roll back before any reader restarts. Keep online catalogs/leases outside this milestone unless offline windows demonstrably cannot satisfy operations.

Test repeat compaction, late arrivals, partition boundaries, large split outputs, inactive-symbol purge, disk full, competing owners, and a crash after every retirement/promotion/journal step. Assert the resumed reader sees exactly the expected multiset, with neither duplicate old/new inputs nor missing data. Test unmanaged-reader refusal, stale replay references, and attempted service auto-restart during maintenance. Backups must survive failed replacement. Measure compaction time and headroom against the tested pause budget.

**Scope rule:** Q10 can remain deferred for a scoped append-only signoff only with capacity controls and explicit exclusions. An unqualified claim that every original rewind/maintenance objective is complete requires these gates too.

## 13. Updating previous tests without weakening them

| Existing area | Required update |
|---|---|
| Four `test_tick_lake_audit_*_regressions.py` modules | Retain F01–F11 assertions and defect traceability; add uncovered fault boundaries rather than replacing precise assertions with broad success checks. |
| `tests/integration/test_supervisor_chaos_soak.py` | Keep fast 10k regression tests; add a separate real-duration qualification path. Label short tests accurately. Add independent input/durability ledgers and telemetry validity checks. |
| `tools/validate_concurrency.py` | Retain existing six gates; add configurable workloads and missing scale/CPU/freshness/resource measurements, fail-closed result handling, and artifact metadata. |
| `tests/database/test_resampling.py` | Preserve explicit isolated dataset selection; supply reproducible fixtures for qualification and reject required-test skips there. |
| Reader stress/continuity tests | Preserve DST, cache response isolation, pruning, and memory regressions; add scale and aggregate process measurements separately. |
| Migration tests | Preserve corruption, source-bound resume, immutable publication, and non-mutating dry runs; add final-inventory and backup/restore rehearsal. |
| Isolation/support tests | Cover every new process/tool/output path before enabling volume or large-data qualification. |

For a discovered bug, record a failing assertion before the fix and a passing run afterward. For missing qualification of working code, a new test may pass immediately; record that honestly. Avoid requiring artificial red tests for correct behavior. Never remove a regression, widen a threshold, ignore errors, add unconditional skips, or turn a partial run green to meet a test-count target.

## 14. Execution commands and evidence package

Existing baseline commands (run inside the isolated test environment):

```sh
python -m pytest tests --collect-only -q
python -m pytest tests/test_isolation_guard.py -q
python -m pytest tests -m 'not live and not performance' -q
python -m pytest tests -m performance -q -s --tb=short
git diff --check
```

Once `endurance` tests are introduced, update offline selection everywhere to exclude `endurance` as well, and give endurance its own explicit command. Register any new marker and document exact supported commands in the execution report. New benchmark, generator, and endurance CLI flags are implementation work; do not present them as existing commands until implemented and tested. Do not run live/provider tests implicitly.

Create `docs/plans/milestone-4.2-execution.md` and `.planning/v4.2-MILESTONE-AUDIT.md` during execution. Store large/raw artifacts in an appropriate artifact location and link them with checksums rather than committing large datasets. Include:

- Repo A and Repo B commits; dirty-tree status; OS/CPU/RAM/filesystem/storage details; Python and dependency versions; resolved nonsecret configuration.
- Fixture manifest, generator seed/version, row/partition/file counts, provenance, and independent oracle definitions.
- Exact commands, start/end times, JUnit/logs, CI URLs, raw latency/resource samples, percentile method, failures/skips, and recovery outcomes.
- A row for every original requirement and Q01–Q10 gate, with status, artifact reference, and any remaining blocker or deferred capability.

## 15. Final signoff checklist

- [ ] Q01: Candidate-specific hosted CI and complete requirement traceability.
- [ ] Q02: New tests/tools remain isolated; independent oracles detect injected corruption.
- [ ] Q03: Required large-scale and historical benchmarks run with no unexplained skips; CPU, latency, lag, freshness, and memory gates satisfied.
- [ ] Q04: At least 24 hours of sustained concurrent load and scheduled faults completed with exact reconciliation and bounded resources.
- [ ] Q05: Durability boundary, acknowledgment semantics, and provider interruption behavior verified and documented.
- [ ] Q06: Shared contract and actual Repo B integration verified, with both commits recorded.
- [ ] Q07: Intended historical inventory reconciled through final publication; backup restore and rollback rehearsed safely.
- [ ] Q08: Append-only capacity controls and maintenance boundaries tested.
- [ ] Q09: Documentation and current audit claims agree with executable evidence.
- [ ] Q10: Replay/compaction either qualified for their released scope or explicitly deferred and disabled where appropriate.
- [ ] Final offline and focused regression runs pass on the release candidate; required hosted checks pass on its code. Material changes after endurance/performance qualification invalidate affected results and require rerunning those gates.

**Permitted conclusion:** “Milestones 4.0/4.1 are verified for the documented append-only production scope” only after Q01–Q09 pass. List deferred capabilities alongside that conclusion. “All original ingestion, historical migration, analytics, and rewind objectives are complete” additionally requires actual replay integration and its evidence. No test count or historical shipped label substitutes for these gates.
