# Tick lake remediation: tests first, production fixes later

> **📌 STATUS: EXECUTED — the test-first remediation described here was completed across Milestones v4.0 (Phases 15–21) and v4.1 (Phases 22–27).**
> This document is retained as the historical strategy record and is **not** a pending work item. The 122 tests added in v4.1 completed the adversarial coverage this plan called for; see [.planning/MILESTONES.md](../../.planning/MILESTONES.md) and [.planning/milestones/v4.1-ROADMAP.md](../../.planning/milestones/v4.1-ROADMAP.md).
> The reference-machine qualification for timing gates and the 24-hour endurance run noted below remain operational follow-ups (see [.planning/ROADMAP.md](../../.planning/ROADMAP.md) backlog).

Status: ~~test implementation plan only. No tests or application changes are implemented by this document.~~ **Executed** (see banner above).
Reviewed baseline: commit `624b90d3`, 2026-10-03.
Companion architecture: `docs/plans/partitioned-parquet-tick-lake.md`.

## 1. Scope and stop boundary

The next implementation agent must build a trustworthy regression suite before fixing the application. This document authorizes a future tests-only phase: test files, test fixtures/helpers, pytest configuration, and test-result documentation. Do not modify `src/`, migration/validator/service tools, launch scripts, dependency versions, or production data to make tests pass. In particular, do not repair the writer while writing its tests. After tests are collected, exercised, and classified, stop and present the failing regression inventory for a later production-fix phase.

This document itself is only a plan. Writing it does not execute that tests-only phase.

A successful tests-only phase is not a green suite. It is a reproducible set of correct assertions that exposes the unsafe behavior, preserves existing valid coverage, and clearly separates application defects from fixture defects and environmental restrictions.

Required deliverables for that future phase:

1. Isolated fixtures and child-process harnesses, with cleanup on every exit path.
2. Regression tests for R01–R10 below and the additional applicable A-series contracts.
3. Corrected existing tests; no assertions weakened merely to accommodate the implementation.
4. A coverage matrix linking requirement -> test node IDs -> observed result -> production code implicated.
5. A baseline report containing exact commands, package versions, OS, seed, failures, and unavailable checks.
6. No production fixes, migration runs on real data, or claims that failed acceptance gates are complete.

## 2. What the previous iteration missed

The review reproduced these behaviors against temporary data:

| ID | Observation | Why existing tests did not protect it |
|---|---|---|
| R01 | Default writer restart reuses sequence/batch identity; a different new row returns `ALREADY_PUBLISHED` and is omitted | Tests retry identical contents or restart with a different writer ID; they do not write new data after a default restart |
| R02 | Exhausted publication retries lose the buffer; runner clears its batch before success | Retry test injects two failures then success within the configured three attempts |
| R03 | Full queue raises `QueueFull`; provider callback logs the exception and loses the tick | Current “backpressure” test explicitly expects `QueueFull` from the queue, not successful provider admission |
| R04 | Fresh lake startup raises missing-registry `RegistryError`; migration does not import the legacy inventory | Integration fixtures manually initialize the registry and bypass actual bootstrap |
| R05 | `publish()` trusts an old `PASSED` report even if the staged data changes | Tests cover missing/failed reports and corruption before verification, not after it or mismatched scope |
| R06 | A second migration to the same partition overwrites generic chunk filenames | Only one migration source/run is tested against an empty destination |
| R07 | Writer starts and publishes while the maintenance guard exists | Guard tests call the metadata loader; the real writer does not use that guard |
| R08 | Configured lake initialization failure silently selects streaming DuckDB | Normal initialization and direct publisher-lock tests do not exercise runner fallback |
| R09 | Runner remains at 2 seconds/1,000 ticks and ignores documented configuration | Writer-class defaults are tested separately from the production runner entry point |
| R10 | Historical publication cannot proceed while the live writer owns the publisher lock | Migration and concurrency are tested separately, not in the intended cutover sequence |

Prior review results: 97 targeted tests passed after allowing local test servers. The full suite with an explicit temporary `TICK_LAKE_ROOT` returned 558 passed, 10 failed, 6 deselected. Nine failures reflected legacy DuckDB fixtures being bypassed in lake mode; one performance gate measured 35.25 ms p99 event-loop lag against 20 ms. Those are observations, not a fresh run or proof of all production behavior. Do not classify the nine fixture-routing failures as nine proven production-query bugs.

P5 full replay and P7 online/offline compaction were deferred in the revised architecture. Missing implementations of those features are not regressions in this first release. Their prerequisites, maintenance guard, read contract, and truthful pending-purge behavior are in scope now.

## 3. Test design rules

### 3.1 Isolate before importing the application

Update `tests/conftest.py` before running broad suites:

- Allocate a temporary test data root and temporary tick lake before application imports. Set `DATA_DIR` and `TICK_LAKE_ROOT` explicitly. Existing isolation sets only `DATA_DIR`; a real `TICK_LAKE_ROOT` or `.env` can still select a different root.
- Override or clear every relevant storage/stream setting; prevent `.env` loading from selecting real storage or contacting providers. Do not print secret environment values.
- Provide explicit `lake_backend` and `legacy_backend` fixtures. Lake tests must exercise configured lake mode; legacy-only tests must deliberately clear the lake selection and use temporary DuckDB paths. Do not globally force legacy mode just to make old tests green.
- Recreate/reset caches, imported path constants, status files, and registry state per test as necessary. Shared seeded session databases must not make a test depend on execution order.
- Guard Parquet and ordinary filesystem writes as well as DuckDB connections. Restrict test destinations to registered temporary roots; test guard behavior against a fake protected directory, never by attempting a destructive operation on the actual external drive.
- Application calls that attempt production reads/writes, disk DuckDB connections in lake-only tests, provider authentication, or non-loopback network access must fail with a clear test error. Apply equivalent restrictions/env to child processes; parent monkeypatches do not cross process boundaries.
- Fixtures that intentionally test path failure must stay inside temporary directories. Never simulate an unplugged mount by changing the real external drive.
- A localhost socket denial is an environment limitation, not an application failure. Report it explicitly; use an approved local-server environment or leave that check unverified. Do not replace a failing integration test with a mock and claim equivalence.

### 3.2 Independent fixtures and data oracles

Suggested helpers in `tests/support/`:

| Helper | Purpose |
|---|---|
| `lake_factory.py` | Distinct fresh-root, initialized-empty-lake, initialized-registry and populated-lake factories; no universal fixture that hides bootstrap omissions |
| `quote_cases.py` | Raw tuple/dict records with repeated timestamps, exact duplicate rows, nulls, offset timestamps and distinct provenance |
| `lake_assertions.py` | Read final Parquet directly with `pyarrow.parquet.ParquetFile` or a private raw DuckDB connection; do not use the application reader as the only writer oracle |
| `faults.py` | Scoped fault injection for stage-write, fsync, rename, receipt and status operations; retain original callables for test handshakes |
| `process_harness.py` | Spawned writer/reader/runner workers, pipe/event barriers, bounded waits and unconditional cleanup |
| `migration_factory.py` | Closed temporary legacy DuckDB snapshots, base tables/views, registry records, and distinct source identities |

Use the eight physical legacy columns as the row-value oracle. Compare multisets, not sets: two identical ticks must remain two rows. For assigned IDs assert uniqueness, stability across retry and association with the original row; do not hardcode an ID string format. Different accepted rows at the same timestamp need different IDs.

For normal orderly completion assert:

```text
accepted rows = published distinct ingest IDs + still-owned pending rows
```

After complete successful drain, pending is zero and every accepted valid record appears once. Quarantined/rejected input is accounted separately and must not increment persisted counters. During partial multi-file publication, a row may remain in an owned retry batch while already visible; use the union by ingest ID for conservation, not an arithmetic sum that double-counts it. Validate receipts against physical output separately.

Distinguish receipt acknowledgment from mere file visibility. If a process dies after final rename but before receipt, recovery must reconcile the visible file once. Do not require one atomic transaction across several Parquet files.

### 3.3 Faults and synchronization

- Use real Parquet files and the real publisher for integration checks; replace only the operation being failed or blocked.
- Prefer named barriers such as `entered_publish`, `staged`, `renamed`, `receipt_written`, `reader_started`, and `release`. Arbitrary sleeps are not proof of an interleaving.
- Inject errors at all configured attempts, not just an error count that guarantees recovery on the final retry.
- Use scoped module-local patches. Patching the shared global `time.monotonic` or `os.replace` indiscriminately can break asyncio or the test harness itself.
- Where no clock injection exists, a test-side clock wrapper and controlled wakeups may be patched into the specific module; do not add a production clock abstraction during this tests-only phase. Keep a small real-clock smoke test with a generous scheduling allowance.
- Crash tests launch a real child with test-side operation wrappers, wait for its stage announcement, terminate only that child, then reopen from a new process. Cover orderly exit and abrupt termination separately.
- Always close writers/executors/connections, shut down HTTP servers, join threads, and terminate/wait for child processes in `finally`. Injected failure must not strand a lock-holder. Use ephemeral ports bound by the server itself where possible.
- Hardware power-loss/fsync durability cannot be proven by mocking calls or killing a process. Test ordering and process recovery; record power-loss durability as an operational validation requirement.

## 4. Mandatory regression specifications

Test names below are proposed stable behavioral names. Adapt calls to existing public interfaces. A failure must reach the behavior under test; an import error, wrong fixture, or unsupported keyword is not a successful regression reproduction. When an interface does not exist yet, record a pending contract rather than making an empty test or a fake passing implementation.

### R01 — Stable identities and restart correctness

Files: extend `tests/storage/test_crash_recovery.py`; add `tests/storage/test_writer_restart.py`.

1. `test_default_writer_restart_preserves_old_and_new_rows`: write A with the default writer, close; create another default writer at the same root; write different B. Assert A and B are both present, IDs differ, no old file changed, new accepted/published counts refer to B rather than the old receipt. The current implementation should fail on the missing B.
2. Repeat the test across separate spawned processes and across UTC dates/symbols. Receipt identity must not collide even when the destination partition changes.
3. `test_same_batch_id_different_payload_is_rejected`: publish A, then pass B using A's explicit batch ID. Expect a specific collision/identity error; all prior checksums unchanged. A receipt existing is not sufficient proof that the payload matches.
4. Keep the exact-repeat test: same batch identity plus same records returns an idempotent outcome and does not rewrite files or count new unique published rows.
5. Remove/truncate a file referenced by a receipt, then retry. Require corruption/missing-data reporting or explicit validated recovery; never blind `ALREADY_PUBLISHED` success.
6. Feed identical-time records without supplied IDs through tuple/dict and `QuoteTick` entry points. Assert unique IDs for distinct admissions. A symbol/timestamp-generated ID must not collapse ties.

Do not “fix” the test by supplying a different writer ID on every restart. Exercise actual defaults, then parameterize explicit identity configurations separately.

### R02 — Exhausted retries, pending ownership and honest acknowledgment

Files: extend `tests/storage/test_parquet_writer.py`, `tests/stream/test_lake_runner_integration.py`; add `tests/storage/test_writer_failure_retention.py`.

- `test_flush_retains_batch_after_all_retries_fail`: accept a valid tick, force every publisher attempt to raise disk-full `OSError`, call flush, then restore I/O. Assert no successful acknowledgment or saved-counter increment during failure; the accepted batch remains recoverable through the owner's supported retry/flush flow without resubmitting it as a new tick. After recovery, assert one output row with its original ID. Current writer loses this batch.
- `test_runner_retains_failed_batch_until_retry`: enqueue via the real provider callback; block/fail publication; let the worker process another scheduling cycle. Assert queue completion is still pending and the batch is still owned. Restore I/O; await durable acknowledgment; assert exact rows and IDs. Do not infer success merely from an empty input queue.
- Repeat at staging write, fsync, first final rename of a multi-partition batch, and receipt creation. A partial publication must be completed without duplicating already visible rows.
- `test_invalid_rows_are_not_counted_as_persisted`: mixed valid/malformed batch; compare receipt row count and physical rows with `total_ticks_persisted`. Test quarantining diagnostics and retention policy, not only a counter. The runner currently counts input batch length.
- `test_close_does_not_report_clean_success_with_pending_failed_writes`: force final flush failure. Require explicit unsuccessful drain/health or a durable pending record; a clean STOPPED/success report with forgotten rows is forbidden.

Do not require RAM-only unpublished data to survive SIGKILL. In crash tests, assert retention only for data that reached the documented durable stage. Optional spool durability remains separate until implemented.

### R03 — Actual callback backpressure and bounded memory

Files: replace the behavioral claim in `test_bounded_queue_backpressure`; add provider callback cases in the same runner test module.

1. Set queue capacity to one; block the disk worker with an event. Admit one tick, start admission of the next tick as a task.
2. Use a handshake and bounded wait to establish that the second admission is pending, with no `QueueFull` exception escaping, while an independent event-loop task continues.
3. Release capacity. Both admissions complete; drain; assert both exact records are present once and counters agree.
4. Parameterize Capital and enabled Binance callbacks. Mock authentication/WebSocket transport, not the callback/queue/publisher chain. Check the provider error-catching layer cannot swallow overflow into a “successful” test.
5. Exercise sustained bounded producer pressure: input queue, current batch, executor backlog and any retry batch must have a finite documented bound. `max_queue_size` existing as an attribute is not proof of enforcement. Byte-budget tests must use large allowed records in addition to row counts; if no supported byte-budget interface exists, record that contract as missing rather than invent an API.
6. Cancel a blocked producer and initiate shutdown: no phantom acknowledgment, deadlock, or falsely accepted record. Assert accepted/published conservation under the chosen admission boundary.

Keep a pure `asyncio.Queue` unit check only if useful, but rename it so it does not claim application backpressure.

### R04 — Bootstrap, registry import and subscription safety

Files: add `tests/stream/test_lake_bootstrap.py`; extend migration and registry tests.

- `test_fresh_configured_lake_bootstraps_without_duckdb`: construct/start the real engine on a nonexistent temporary lake, without calling `init_registry` in the fixture. With fake provider transport, expect a valid empty registry, no legacy DB calls, and an idle healthy engine. No default subscriptions. This is the fresh-install contract; do not manually seed around the failure.
- `test_existing_lake_missing_or_corrupt_registry_fails_explicitly`: distinguish fresh initialization from damaged existing control data. Preserve existing files; do not silently replace a lost registry with an empty inventory or fall back to DuckDB.
- `test_legacy_registry_import_preserves_inventory`: migration/bootstrap must preserve display/provider mappings and inactive entries; historical symbols missing from the active registry remain archived, not auto-subscribed. Exercise the actual documented operator entry point once available; until then, record the absent import entry point as an implementation gap with a runnable cold-start regression.
- `test_registry_import_does_not_overwrite_later_admin_edits`: import, make a legitimate new edit, complete/retry historical export. Existing version and edit survive; repeat import is idempotent for the same source.
- `test_empty_registry_start_never_subscribes_defaults`: test `start()`, not only `reload_symbols()`. Capture initial provider epics and any subscription calls. Empty initial registry and removal of the final active symbol both remain empty.
- Malformed registry/startup failure must release locks/workers and surface an error exit status. No connection to the real provider is allowed.

### R05 — Verification must authorize the exact published files

Files: extend `tests/storage/test_migration_tool.py`; optionally split into `test_migration_verification_binding.py`.

Use a real source, actual export and actual successful verification for the setup. Then parameterize:

- Change one price after verify without changing row count.
- Replace a verified file with a valid empty Parquet file.
- Remove/truncate one file; add an unverified extra chunk.
- Verify only AAPL, then attempt unfiltered/all-symbol publication including NVDA.
- Re-export after a successful verification without re-verifying.
- Change source identity/plan, schema, or destination lake identity after verification.

For each, `publish()` must refuse before moving any unverified output, leave existing final files unchanged, and never mark migration `PUBLISHED`. Compare before/after tree inventories and checksums. Verify reports must be tied to exact source identity, selection, inventory, content checksums and schema; assert behavior, not a preferred JSON representation.

Keep positive controls: unchanged verified export publishes; a fresh verification after an authorized re-export permits the newly verified set. Report scope cannot be inferred from the string `PASSED` alone.

### R06 — Collision-free, immutable and restartable migration

Files: extend migration tests; add `tests/storage/test_migration_resume_safety.py`.

- `test_second_source_cannot_overwrite_prior_migration`: migrate source A; migrate source B with different rows into the same symbol/date. Default behavior must reject the incompatible resume/source with existing data untouched. A future explicit independent migration may append unique provenance, but must not overwrite A. Record that alternative as a separate contract, not a permissive “reject or lose data” test.
- `test_same_source_rerun_is_idempotent`: identical source and run identity, including a CLI restart, leave the final multiset/IDs/checksums unchanged.
- `test_resume_validates_completed_chunks`: valid completed chunks can be reused; corrupted/missing/mismatched chunks cannot be trusted merely because state says COMPLETED. The default safe expectation is explicit rejection without publication, followed by documented repair/re-export in a separate test.
- `test_changed_source_invalidates_saved_plan`: add a row in an already planned partition and in a new partition; change schema or replace the DB file. Resume must reject changed source identity rather than validate only the old partition list.
- Crash after exporting a chunk, after completing a partition, after first historical rename, and after all renames/before receipt/state update. Recover from a new process; no row duplication, overwrite, lost receipt membership, or false completion. Include mixed staged and already-published files.
- Change chunk size on a deliberate rerun; stale trailing chunks must not silently join the next export. Stable ingest IDs must survive equal-timestamp rows, restart and chunk boundaries; test ID-to-row association, not a regex.
- Preserve identical duplicate rows, null values, microseconds and floating-point values exactly. Distinguish schema-representable historical values from live-validation rules: non-positive or unusual legacy numeric values must be preserved by an explicit historical path or rejected with a complete blocking report, never dropped or silently coerced. Do not claim full historical migration succeeds if supported source values remain blocked.
- Source table detection must distinguish views from independent base tables; do not export a table plus its alias twice. Ambiguous multiple base tables require explicit selection. Unknown tables cannot be selected arbitrarily.
- Reconciliation after publication must select only that migration's provenance/files; new live files in the same event-time partition must not create false mismatches.

### R07 — Maintenance guard at real entry points

Files: extend `test_storage_config.py`; add `tests/storage/test_maintenance_entrypoints.py` and supervisor subprocess tests.

Create a valid lake and `_maintenance/in_progress.json`, then try actual writer, publisher recovery, runner and migration publication entry points. Assert each mutating entry refuses before initialization/recovery/final-file writes; compare the tree before/after. Readers return an explicit maintenance response rather than empty success or legacy fallback. Test malformed/stale guard as fail-closed until an operator recovery path resolves it.

Test managed service startup/automatic restart with the guard present using fake child commands and a temporary root. Merely testing `load_lake_metadata` is insufficient. Also test a guard appearing after process initialization: no new publication starts after the coordinated fence has been acknowledged; an already-in-flight publication is drained according to the documented maintenance protocol.

Do not pretend that a guard file alone stops arbitrary raw-glob readers or atomically fences an in-flight write. The deferred maintenance coordinator must stop/drain affected readers and fence the publisher before replacement. During this phase, test existing guard entry points and record absent coordinator tests as future P7a specifications.

### R08 — Explicit backend choice and storage failure behavior

Files: add `tests/stream/test_lake_backend_selection.py`; extend dashboard isolation tests.

Parameterize configured lake failures: held publisher lock, inaccessible/missing configured storage, malformed `lake.json`, unsupported schema version, corrupt registry, maintenance active, and recovery corruption. Spy on disk DuckDB opens and legacy save functions. Require explicit failure and zero legacy fallback; preserve the configured root and existing data.

Test both explicit `TICK_LAKE_ROOT` and the supported `DATA_DIR`-derived lake. Empty initialized lake, unknown symbol and empty date range must return typed empty lake results with zero streaming DB opens. Missing/inaccessible/corrupt lake is an error, not indistinguishable empty data. No silent alternative root creation on simulated mount disappearance. Test metadata identity validation, including a same-path replacement lake with a different `lake_id`.

Keep explicitly selected temporary legacy-mode tests separate. Compatibility cannot be inferred merely because the new backend threw an exception.

### R09 — Production configuration and real flush triggers

Files: add `tests/stream/test_lake_runner_configuration.py`; extend writer flush tests.

- Instantiate through the same entry path as the service, with threshold variables absent: assert 5 seconds/5,000 rows.
- Set `STREAM_FLUSH_INTERVAL`, `STREAM_MAX_BATCH_ROWS` and queue limits, then inspect actual emitted batches/triggers, not just constructor attributes. Test explicit argument versus environment precedence once documented; proposed precedence is explicit argument > environment > default.
- Zero, negative, NaN/infinite, nonnumeric intervals and invalid row/queue capacities must fail clearly before starting threads/services.
- Test below-threshold, exact-threshold, age, byte-cap, quiet-period, and shutdown flushes. Measure age from the oldest pending tick. No empty Parquet files; a hot symbol cannot bypass overall memory bounds.
- Verify one logical batching policy across runner and writer; demonstrate that runner does not publish at 1,000 rows despite a configured 5,000-row threshold. Test actual engine callbacks.
- Separate the deterministic off-loop test from the 20 ms benchmark: block encoding in the worker, then prove another asyncio task runs before releasing it. Also cover `write_tick_async` when it triggers a flush.
- Document observed freshness and RAM-only exposure; do not assert “zero visible latency” or crash-proof buffering from a flush interval alone.

### R10 — Migration publication during new live capture

Files: add `tests/integration/test_migration_live_cutover.py`.

Use a closed synthetic legacy DB and an independently running new lake writer. Establish that the writer owns its lock and has published live data. Export/verify history while live ticks continue; request publication through the existing migration entry point. The current ownership conflict should be recorded as a real failing acceptance test, not hidden by closing the writer first.

Required end-to-end result after the later fix: all historical rows plus all accepted live rows appear once, writer ownership remains exclusive, and migration does not overwrite live files. The implementation may later serialize migration through the existing owner or implement a bounded cooperative publication handoff. Do not patch lock acquisition to succeed or allow two uncoordinated publishers in order to satisfy this test. Capture accepted versus published live IDs throughout the handoff and test late event timestamps overlapping legacy dates.

If the eventual operational choice instead requires stopping capture, that is a changed acceptance contract requiring an explicit plan decision and measured gap; do not silently alter this regression to accept it.

## 5. Additional coverage beyond the reported blockers

These are inspection-driven test targets, not claims that every scenario was reproduced in the prior review. Add runnable tests for present interfaces now; keep absent P5/P7 features explicitly deferred.

| ID / area | Test construction and required assertion | Suggested file |
|---|---|---|
| A01 Shutdown ordering | Stop a real runner with callbacks, queued data and blocked in-flight publication; graceful signal stops admissions then drains and closes once. Error drain must not report clean success. Use a fake provider; test cancellation separately from SIGTERM. | `tests/stream/test_lake_shutdown.py` |
| A02 Supervisor/platform shutdown | Real POSIX signal test; Windows cooperative stop tested on Windows CI. Force timeout reports failure and preserves completed files; never pretend abrupt Windows termination drained RAM. | `tests/integration/test_lake_service_lifecycle.py` |
| A03 Corrupt/incomplete recovery | Invalid intent JSON, wrong checksum, missing staged/final file, wrong row count, forged receipt, partially recovered partition. Surface unhealthy state; never quietly skip and announce healthy recovery. Cleanup must retain staging referenced by live/recoverable intents. | `tests/storage/test_crash_recovery.py` |
| A04 Real status producer/consumer | Generate status with the real writer, then read it; timestamp format must roundtrip. Simulate stale status/dead process using injected liveness/clock: no perpetual LIVE. Differentiate latest event from latest publication, session counts from archive total, and last batch from ticks/minute. | `tests/storage/test_lake_status_contract.py` |
| A05 Subscription lifecycle | Startup/reload/provider alias mapping, inactive entries, remove last symbol, pending purge, denied re-add, admin changes during reload. Actual separate process registry edits must be observed. Buffered rows from a retired generation cannot reappear after physical purge/re-add; maintenance portion deferred to P7a. | `tests/stream/test_registry_reload.py` |
| A06 Timestamp/query boundaries | Parameterize naive UTC, aware UTC, nonzero offsets crossing UTC midnight, string `Z`, NYSE DST and regular/extended boundaries; compare public candle and tick queries to independent expectations. Preserve existing inclusive legacy API ends; specify new half-open contracts separately. Test `start/end` without the `date` argument so parameter-name shadowing/pruning failures are exposed. | `tests/storage/test_lake_query_boundaries.py` |
| A07 Reader error distinction | Empty valid lake versus missing root, unreadable partition, corrupt Parquet/schema, unsupported metadata. Only valid absence yields empty success. Connection closes on query error; resource settings applied per connection. | `tests/storage/test_lake_reader.py` |
| A08 Pruning and freshness | Populate many irrelevant symbols/dates; assert selected paths AND query scan evidence exclude them. Append a late file after a first query; next refreshed query reflects it. Preserve microsecond precision in physical data even if inspector intentionally rounds display. | `tests/storage/test_lake_reader.py` |
| A09 Independent oracle/package behavior | Run schema/writer import in a subprocess where `tests` is unavailable; behavior must not depend on production importing `tests.fixtures.QuoteTick`. Golden expected values use test-local plain records, not the application aggregation implementation. | `tests/storage/test_runtime_independence.py` |
| A10 Multi-process correctness | Actual runner callbacks -> writer -> separate dashboard/raw DuckDB reader; hold the exact configured temporary legacy DB locked. Prove overlapping execution with barriers, then compare every row and ID, not just counts. Include restart/fault cases, not just one healthy run. | `tests/integration/test_lake_multi_process_concurrency.py` |
| A11 Test harness cleanup | Fail before HTTP bind, after starting a lock guard, during writer startup and during query; every spawned process/connection closes and temporary locks can be reacquired. Caller gets failure details. | `tests/test_process_harness.py` |
| A12 Performance/scalability | Actual runner at representative rate, 1M/10M rows, skewed 19-symbol distribution, realistic small-file counts, cold/warm runs; CPU seconds/million, RSS, p99 lag, p95 reads, first-result latency. Compare fixed workloads to the old writer baseline. | `tests/performance/test_tick_lake_benchmarks.py` |
| A13 Durability boundaries | Test receipt/rename/fsync order and acknowledged-file recovery after child termination. Unpublished RAM rows may be lost on abrupt death; optional spool tests are deferred until such a feature exists. | publication/recovery tests |
| A14 Filesystem portability | Unicode/special symbols, encoded partition equality, traversal/symlink escape, path length/case behavior, permissions and disk-full faults on temporary roots; platform-specific checks on their real supported OS. | symbol/config tests |
| A15 Operator contracts | Run documented CLI modes against temporary fixtures: exact paths/report filenames, exit codes, dry-run immutability, retry/recovery. Document nonexistent APIs such as a named purge method rather than “testing” a fabricated implementation. No destructive runbook command executes outside fixtures. | `tests/integration/test_lake_operator_contracts.py` |

P5 follow-up specifications: immutable snapshot replay, total ordering at ties, keyset resume, overlapping files, cancellation and bounded query/output memory. Do not call the existing LIMIT/OFFSET inspector a completed replay engine.

P7a follow-up specifications: verified captured-input set, preserved late files, stopped-reader replacement, publisher fence, durable maintenance journal, crash after each retire/promote step, safe purge/re-add, unique compacted filenames, no resume against changed contents. P7b online catalogs remain optional; tests are not first-release blockers until that feature is selected.

## 6. Update existing tests instead of preserving false confidence

| Existing test/suite | Required change |
|---|---|
| `test_writer_initialization_and_lock` | Keep lock coverage, add writing after default-identity restart; acquire/release alone proves no restart integrity |
| `test_idempotent_duplicate_publish`, `test_collision_detection` | Keep exact-repeat/collision controls; add same batch ID with different payload and stale/missing receipt targets |
| `test_transient_io_retry_and_honest_counters` | Retain the transient success case; add beyond-budget failure, later recovery and partial publication; assert physical rows/IDs and pending ownership |
| `test_bounded_queue_backpressure` | Replace the success criterion of `QueueFull` with blocked callback admission followed by successful bounded drain |
| `test_honest_task_done_acknowledgment` | Retain in-flight check; add rejected rows, partial receipt failure and final failed drain |
| `test_batch_flush_on_monotonic_age` | Use controlled time/wakeups where possible; keep one real-clock smoke case; test age of oldest tick, not just a sleep longer than interval |
| `test_off_loop_event_loop_responsiveness` | Replace ordinary-CI hard timing with a deterministic worker-block/loop-progress assertion; retain timing as a separate performance gate |
| `test_export_mode_synthesizes_deterministic_ingest_id` | Remove rigid `mig_<symbol>_<date>_<index>` regex/order assumption; assert stable unique IDs, source namespace isolation, and ID-to-row mapping across restart |
| `test_export_mode_resume_skips_completed` | Replace `dummy_completed_content_aapl` with valid Parquet plus actual checkpoint evidence. Positive test reuses it; negative test with invalid bytes must reject/repair explicitly, not preserve corruption as success |
| `test_publish_mode_refuses_without_passed_verification` | Use `pytest.raises` with the intended exception/message; add valid-report-but-wrong-files/scope cases. Broad exception handling is not an assertion of the right failure |
| `test_publish_mode_promotes_staged_files_and_writes_receipts` | Build real export/verification instead of hand-authoring permissive success metadata; test destination collision, restart and live append preservation |
| `test_stream_status_reads_writer_status_file` | Use the actual writer-produced ISO status before testing compatibility fixtures; current hand-written numeric timestamp masks the format mismatch |
| `test_maintenance_guard_blocks` | Keep loader unit coverage; add real writer, runner, recovery, migration and managed-service entry-point tests |
| `test_runner_empty_registry_unsubscribes_all` | Keep reload test; add initial `start()` with empty registry and no default provider subscriptions |
| `test_runner_cross_process_version_polling` | Confirm an actual second process edits the registry; task/thread simulation alone is not cross-process evidence |
| New lake dashboard tests | Add error/empty/new-symbol cases, actual writer status, and positive proof no disk streaming DB access; do not only seed an already working lake |
| `test_lake_multi_process_concurrency` and validator CLI test | Separate functional row/ID/lock assertions from timing gates. Add actual runner coverage: current validator writes with `TickLakeWriter` directly and misses runner batching/admission defects. Keep existing CLI checks as separate tests; test-side harness may replace their use for deterministic correctness, without editing the production validator now |
| `tests/test_isolation_guard.py` | Extend to TICK_LAKE_ROOT, `.env`, Parquet/filesystem writes, imported constants and child-process inheritance; validate against temporary fake protected paths |

Migrate the legacy backend-dependent suites deliberately:

- `tests/dashboard/test_chart_timezone_determinism.py`: keep timestamp conversion unit tests; run public streaming-query regressions against real lake files, and keep explicit legacy adapter cases separately. Do not bypass the new reader by mocking the old connection factory.
- `tests/dashboard/test_observatories.py`, `tests/test_dashboard_segregation.py`: preserve response and historical/streaming separation assertions with lake fixtures; forbid disk tick DB access in lake mode.
- `tests/test_streaming_redesign.py`, `tests/test_streaming_week_and_day_drilldown.py`, other chart/gap/continuity suites: retain full-week count, session counts, gaps, boundaries and pagination expectations. Route them through the real lake path. A backend change is not permission to relax the expected candle counts.
- `tests/utils/test_integrity.py`: compare a temporary historical DuckDB against real lake ticks. Include overlapping drift, no overlap, and historical-side lock/error; do not mistake empty lake data for reconciliation success.
- `tests/database/test_dual_storage.py`, `tests/test_database_exclusivity.py`, `tests/test_symbol_maps_separation.py`: separate historical database contracts from streaming lake/control contracts. Preserve legacy-only coverage under explicit selection.
- `tests/stream/test_live_engine.py`, `test_dynamic_reload.py`, old end-to-end suites: retain parser/provider rules and add lake backend cases; replace cancellation-after-sleep as proof of successful drain with explicit completion receipts and row checks.

Do not delete original expected outputs to reduce failure counts. Record which tests were backend fixtures versus enduring user-visible contracts.

## 7. Ordering and gates for the tests-only implementation

### T0 — Establish safe, reproducible execution

Read the actual code at implementation time; record its commit. Implement isolation/helpers and verify guard self-tests. Build explicit backend fixtures and process cleanup. Capture the existing suite baseline before changing its assertions. Do not run broad suites against default external-volume paths.

Gate: all storage paths demonstrably temporary, providers blocked, child cleanup verified, commands/version metadata saved.

### T1 — Add minimal regressions for R01–R03 and R08

Start with tiny raw records and real files. Obtain the expected missing-row, lost-pending-batch, QueueFull and silent-fallback failures. Add positive controls. Confirm failures are assertion/behavior failures, not test setup/import errors.

Gate: each confirmed bug has a reproducible failing node ID and an independent output inventory; no production edits.

### T2 — Add R04–R07 migration/bootstrap/maintenance tests

Create valid source/registry fixtures; add verification binding, source identity, collision and resume faults. Update misleading migration tests at the same time so they no longer endorse unsafe behavior. Add actual guard entry points and clean startup cases.

Gate: no test equates a hand-written PASSED flag, invalid “completed” chunk, or manually seeded registry with an end-to-end guarantee.

### T3 — Add R09–R10 and applicable A-series cases

Exercise service-equivalent configuration and separate-process cutover. Add status, lifecycle, error/empty, boundary and package-independence tests. Define performance tests separately. Missing interfaces are recorded as pending contracts rather than mocked into existence.

Gate: requirements matrix distinguishes executable red tests, passing contracts, environmental blocks and deferred features.

### T4 — Update legacy suites and run the complete matrix

Run isolated legacy-adapter and lake-mode tests, deterministic integration tests, and performance tests in their respective environments. Correct fixture routing; preserve substantive assertions. Run core deterministic cases individually and in suite order to detect state leakage. Ensure tests clean up even when failing.

Gate: reviewed result report and tests-only diff; stop. Do not proceed to production implementation because tests are now red.

## 8. Failure handling, commands and completion evidence

Register a `remediation` marker for the new/rewritten regressions. Keep marked tests in ordinary collection: markers are for selection, not hiding failure. Do not add `skip`, broad `xfail`, or success-on-exception wrappers to manufacture a green run. If the repository requires a green main branch, retain the tests on a review branch and document the red baseline; do not silently remove them from CI. Future fixes must make the same tests pass without weakening them.

For a not-yet-supported interface, write a concrete specification and record `PENDING_INTERFACE` in the coverage matrix. Do not claim a test that fails at import/collection is protecting production behavior. Prefer a current-interface regression whenever possible.

Proposed commands after tests are authored (run only with T0 isolation in place; use the repository's Python environment):

```sh
.venv/bin/python -m pytest tests -m 'remediation and not performance and not live' -ra
.venv/bin/python -m pytest tests/storage tests/stream -m 'not performance and not live' -ra
.venv/bin/python -m pytest tests/dashboard tests/utils tests/integration -m 'not performance and not live' -ra
.venv/bin/python -m pytest tests -m 'not performance and not live' -ra
.venv/bin/python -m pytest tests/performance -m performance -ra
```

On Windows use the environment's Windows Python executable. `tests/performance` is proposed; create it in the tests phase before invoking that path. Fixtures must select lake versus legacy mode explicitly, not depend on the caller's shell configuration.

Performance acceptance remains on a named reference machine with fixed package versions, dataset/seed, storage filesystem, warmup and sample counts. Record all runs rather than rerunning until one passes. Keep p99 <20 ms and p95 <100 ms as the existing proposed targets pending hardware calibration; do not silently increase thresholds. CPU reduction and endurance were not established by a short 6,000-tick test. A 24-hour run and actual Repo B application integration remain operational gates; a standalone raw-DuckDB consumer is a valuable contract test, not proof that Repo B's application was modified.

The result report should have one row per contract:

```text
Requirement ID | test node ID | setup/fault | invariant | current result
              | intended failure evidence | later fix area | deferred reason, if any
```

Use result categories `PASS`, `FAIL_EXPECTED_APPLICATION`, `FAIL_TEST_HARNESS`, `BLOCKED_ENVIRONMENT`, `PENDING_INTERFACE`, and `DEFERRED_FEATURE`. These are report labels, not pytest outcomes or excuses to swallow failures. Every confirmed R-series issue needs an executable regression where the present interface permits it; otherwise include the concrete missing prerequisite and a current-interface test showing the externally observable failure.

Completion checklist for the tests phase:

- [ ] R01–R10 mapped to executable tests and any explicitly absent integration interfaces.
- [ ] Misleading prior assertions replaced; valid positive controls retained.
- [ ] Additional A-series tests authored for implemented behavior; future P5/P7 work clearly distinguished.
- [ ] Exact rows, multiplicity, IDs and file checksums used where counts alone could hide loss.
- [ ] All failure paths release test-owned threads, subprocesses, locks and connections.
- [ ] No production path/provider accessed; no application or tool code changed.
- [ ] Deterministic correctness separated from timing benchmarks without dropping either requirement.
- [ ] Failures reviewed for the intended reason; unresolved fixture/environment errors not counted as bug coverage.
- [ ] Full command/result/coverage report delivered, including red tests and remaining operational checks.
- [ ] Agent stops for review before implementing production fixes.
