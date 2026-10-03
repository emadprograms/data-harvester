# Tick lake audit remediation: regression tests and implementation

Status: **F01-F11 implementation and offline qualification passed; separate performance gates unresolved**. Baseline: `280d5b2a`, audited 2026-10-03. Execution evidence: [`tick-lake-audit-remediation-execution.md`](tick-lake-audit-remediation-execution.md).

This is the current remediation plan for the failed acceptance criteria in the latest audit. It supplements [the original architecture](partitioned-parquet-tick-lake.md) and supersedes the completion claim in [the previous test-first plan](tick-lake-test-first-remediation.md) for the issues below. Preserve useful existing coverage. Do not treat milestone completion banners or a green stress suite as evidence that these failures are resolved.

## 1. Objective, scope and execution boundary

Deliver concurrent ingestion and analytics over immutable Parquet files without silently losing, duplicating or replacing data during normal restart, retry, migration or shutdown. Keep in-memory DuckDB readers and finalized-file discovery. Do not introduce online compaction manifests or reader leases as part of this remediation; offline maintenance is sufficient when all affected readers and writers are quiescent.

Execution authority and status: the user authorized implementation after test preparation. The isolated regression tests were run against the baseline first; the 40-failure/8-pass evidence is recorded in the execution report. Application changes followed in the dependency order below. Preserve this test-first boundary for any further changes: prepare or identify the failing regression before a new application fix, then demonstrate the same acceptance test passing. Do not rewrite expected outcomes to match defective behavior.

The audit ran storage/stream tests: 213 passed, two localhost-dependent setup errors, one deselected; the two affected tests subsequently passed with localhost access. Thus 215 tests passed across those runs. Independent temporary-data reproducers still exposed the failures below. This was not a full-repository or endurance qualification. Recreate the reproducers as committed tests; temporary audit scripts are not lasting coverage.

## 2. Evidence and acceptance map

| ID | Priority | Current behavior reproduced or inspected | Acceptance condition |
|---|---|---|---|
| F01 / R01 | P1 | Default writer writes price 100; after restart, writing 200 returns `ALREADY_PUBLISHED`, leaving only 100 | Both records exist exactly once with distinct identities; old records and receipts remain unchanged |
| F02 / R02–R03 | P1 | Runner drops a failed batch and calls `task_done`; a full callback queue drops the next tick | Await capacity; retain accepted work across transient failure; acknowledge only terminal outcomes defined below |
| F03 / R02 | P1 | Receipt-write failure after file publication, then another flush, produces two copies of one tick | Retries and recovery reuse one immutable batch identity and converge to one copy |
| F04 / R05 | P1 | A verified staged file replaced with an empty file still publishes successfully | Publication authorizes exactly the verified source, scope, schema and file contents |
| F05 / R06 | P1 | A second source migrated to the same partition overwrites the first source's generic chunk | Distinct migrations cannot overwrite prior data; resuming the same migration is idempotent |
| F06 / R07 | P1 | Writer starts and publishes with `_maintenance/in_progress.json` present | Real mutation entry points are fenced; maintenance cannot race with active publication |
| F07 / R08 | P1 | Environment-selected lake initialization failure silently selects streaming DuckDB | Lake errors propagate as unhealthy/unavailable; legacy mode requires explicit selection |
| F08 / R04 | P2 | An empty registry starts six default subscriptions | Empty means no subscriptions; corrupt or missing inventory never activates defaults |
| F09 / R09 | P2 | Runner uses 2 seconds / 1,000 rows despite writer-class defaults | Actual service uses 5 seconds / 5,000 rows by default and honors validated overrides |
| F10 | P2 | `export(dry_run=True)` deletes a prior verification report | Every dry-run mode leaves existing artifacts byte-for-byte unchanged and creates no artifacts |
| F11 / R10 | Additional unresolved contract | Migration takes the same lifetime publisher lock held by ingestion | A documented, tested writer handoff safely publishes history; never bypass the ownership lock |

F01–F08 and F10 include direct fault reproductions. F09 and F11 are code/contract findings that need executable acceptance tests. Additional cases below are required coverage, not claims that every possible defect was reproduced.

## 3. Shared correctness contracts

### 3.1 Admission, durability and acknowledgment

Define these states explicitly: offered, rejected-before-admission, accepted-and-pending, durably-staged, committed, quarantined, and terminal failure. A tick rejected by a deliberate subscription fence is different from an accepted tick lost because storage is unavailable.

- An awaited callback must not return success while silently dropping a valid in-scope tick. On capacity exhaustion, await capacity with cancellation support. If an upstream provider cannot tolerate this, document and test its disconnect/replay policy; bounded RAM, unlimited outage tolerance and unconditional nonblocking admission cannot all be promised.
- A publication retry must not increment input/accepted counts again. Committed counts must derive from verified receipt results, not input batch length.
- Call queue `task_done()` after commitment, or an explicitly tested terminal validation/quarantine outcome. A storage error alone is not completion. Avoid double acknowledgment on cancellation.
- Represent each accepted tick in exactly one pending/committed/quarantined ownership state; compare row multisets and identities, not only totals. Document metric units where one queue item represents multiple records.
- With the present RAM buffer, an abrupt kill before durable staging can lose RAM-only ticks. Tests must distinguish graceful shutdown from hard-crash recovery. Do not claim crash-proof admission unless a durable inbox or replayable upstream acknowledgment protocol is separately implemented and tested. A hard-kill test must identify the exact durability boundary before killing the child.
- When a shutdown deadline expires, report unsuccessful drain, pending counts and recovery information. Do not report healthy `STOPPED` or successful service exit while unsaved work remains. RAM-only pending work cannot be recovered after exit; durable spooling or upstream replay is necessary if that guarantee is required.

### 3.2 Test isolation before application imports

Update `tests/conftest.py` first. Allocate temporary `DATA_DIR` and `TICK_LAKE_ROOT` before importing application configuration. Prevent `.env` and inherited environment from selecting production storage. Provide explicit lake and legacy fixtures; legacy tests must deliberately select their backend rather than accidentally passing through a fallback.

Guard filesystem/Parquet writes as well as DuckDB connections. Exercise guards against a fake protected temporary directory, never the real external drive. Every subprocess inherits isolated paths and uses fake providers; disable external networking for non-live tests. Register cleanup for processes, worker threads, locks, connections and temporary state, including assertion failures. Tests that need local HTTP servers may use loopback only.

### 3.3 Independent oracles and deterministic faults

Add reusable helpers under `tests/support/`:

- `lake_assertions.py`: read finalized files using `pyarrow.parquet.ParquetFile` or raw `duckdb.connect(':memory:')`; compare full-row multisets including duplicate multiplicity and separately validate ingest-ID uniqueness/mapping. Do not use the application reader as the sole writer oracle.
- `faults.py`: scoped stage-write, file-fsync, rename, directory-fsync, intent-write, receipt-write and status-write failures. Match the exact target operation/path, retain original functions, and count injected failures. A status-write error must not masquerade as a data-publication failure.
- `process_harness.py`: spawned-process ready/release/committed barriers, bounded joins and unconditional cleanup. Use events/pipes rather than sleeps to decide when to cancel or kill.
- `migration_factory.py`: frozen temporary sources with repeated timestamps, exact duplicate rows, multiple symbols/dates, nulls, and independently distinct source namespaces.
- `tree_snapshot.py`: relative paths, file hashes, sizes and modification times for proving dry-run and immutable-file behavior. Do not assert access times, which reads may change.

For each case below, add a named test, fixture description, independent expected result and fault point. Record its actual pytest node ID in a traceability report. Proposed test names in this document are specifications, not claims that those tests already exist.

## 4. Issue-by-issue tests and changes

### F01 — Restart identities and receipt idempotency

Files: `src/storage/parquet_writer.py`, `src/storage/publication.py`; tests: extend `test_parquet_writer.py`, `test_crash_recovery.py`, `test_atomic_publication.py`.

**Tests first**

1. `test_default_writer_restart_preserves_new_rows`: publish price 100 using default settings, close, construct another default writer at the same root, publish 200. Assert two rows, distinct generated IDs, distinct new-batch receipt, unchanged original file hashes. Repeat with spawned processes, same timestamp, same symbol, and a new date.
2. `test_receipt_replay_checks_payload`: repeat the identical batch identity and contents; assert no new rows. Reuse that identity with changed price, symbol, row multiplicity or schema; require a specific collision error and no modifications.
3. `test_receipt_replay_checks_final_files`: remove or corrupt a receipt-referenced final file. Replaying must not claim healthy `ALREADY_PUBLISHED`; require verified recovery from valid staging or an explicit integrity error.
4. Restart with pending intents; prove recovery keeps their IDs while newly admitted ticks use a new namespace. Preserve legitimate identical market observations rather than deduplicating by timestamp/price.

**Implementation**

Introduce a durable publication identity distinct from the operator's `writer_id`. A persisted per-run UUID plus sequence is sufficient for new batches; existing pending intents must retain their original IDs after restart. Generate ingest IDs from that namespace and a counter, or persist a monotonic allocation scheme under ownership. Never reset into an existing identity namespace. Keep old receipt formats readable and fail explicitly when their integrity cannot be established.

Bind receipts to a canonical logical payload fingerprint, schema version, row count and final-file checksums. Compare supplied contents on identity replay and verify final files before declaring success. Canonicalization must preserve duplicate multiplicity and normalized timestamp/value semantics. File checksum and logical payload fingerprint serve different purposes; serializer byte differences must not turn an identical logical retry into new data.

### F02 — Backpressure, retained batches and honest shutdown

Files: `src/stream/runner.py`, writer admission methods and status handling; tests: `test_lake_runner_integration.py`, `test_lake_runner_stress.py`, provider callback tests.

**Tests first**

1. `test_callback_waits_for_capacity_without_loss`: stop the consumer at a barrier, fill a capacity-one queue and launch a second real provider callback. Assert it remains pending and the event loop still advances. Release the consumer; both callbacks complete and both rows are persisted once. Parameterize Capital/Binance and cancellation while waiting.
2. `test_exhausted_retries_keep_runner_batch`: inject more failures than the retry limit. Assert the original accepted batch remains owned, committed count stays zero and queue join has not falsely completed. Restore storage and explicitly resume/retry; assert complete row multiset and one acknowledgment per item.
3. `test_shutdown_reports_pending_storage_failure`: fail persistence during shutdown. Require a bounded failure response, truthful unhealthy/pending status and no successful drain claim. Then test recovery while the process is alive; if durable shutdown spooling is implemented, restart and recover it too.
4. Mix valid and malformed ticks; assert receipt-based committed counts, separate quarantine/rejection counts and valid-row preservation. Check cancellation before admission, after dequeue and during publication.
5. Run bounded producer bursts using actual awaited callback APIs. All successfully admitted valid ticks must eventually persist under recoverable storage faults; `received == committed + dropped` alone is insufficient evidence.

**Implementation**

Make asynchronous admission use `await queue.put(...)`, carrying that await through provider callbacks. Define a separate synchronous API with explicit blocking or explicit pre-admission rejection semantics; never silently succeed on a full buffer. Do not block the event loop through `write_tick_async` calling a synchronous flush.

Make the worker own a pending batch until verified publication succeeds. Exhausted internal retries transition it to a recoverable paused/error state, retain ownership and apply backpressure; retry on a bounded schedule or explicit recovery signal without a CPU spin. Do not clear the only copy or acknowledge in the storage-exception handler. Store prepared normalized data/identity once so a later retry cannot generate new IDs or double-count admission. Use the publication receipt for committed counts. Coordinate worker shutdown/cancellation rather than hiding errors in `close()`.

### F03 — Partial publication must converge without duplicates

Files: writer batch lifecycle and `publication.py` recovery; tests: `test_crash_recovery.py` and runner integration.

**Tests first**

Create one batch spanning two symbols. Parameterize faults before staging completes, after intent durability, after the first final rename, after all renames but before receipt, and after receipt before acknowledgment. Exhaust all configured attempts, remove the fault, and retry in the same process and after restart. Assert one copy per input observation, one stable batch identity, intact prior files and correct recovery bookkeeping. The minimal receipt-failure case must catch the currently reproduced duplicate. Retain exact duplicate input rows twice, with distinct ingest identities.

**Implementation**

Represent a prepared batch as immutable normalized rows/IDs plus a stable batch ID and publication progress. Allocate identity at preparation, not on every flush attempt. Retry the same intent. If a target exists, verify it against the intended hash; never overwrite conflicting contents. Recovery verifies every file before writing the receipt, with durable ordering of staged data, intent, promoted files and receipt (including supported parent-directory fsync). A receipt durable before an acknowledgment should make a subsequent retry safely idempotent.

File-level rename does not make a multi-file batch atomically visible to raw glob readers. Readers may observe a growing committed set while publication is in progress; test that every visible file is complete and that final recovery converges, not an unsupported all-or-nothing snapshot across partitions.

### F04 — Verification must bind the actual migration publication

Files: `tools/migrate_streaming_to_parquet.py`; tests: `test_migration_tool.py`, `test_migration_stress.py`.

**Tests first**

After a real successful verification, parameterize: replace a chunk with an empty valid file; change a price without changing row count; remove/add a chunk; change schema; change source identity, filter scope or migration ID. Publishing must reject each case before any new final file appears. Existing final hashes must remain unchanged. Add a barrier-controlled mutation attempt between validation and promotion to test the chosen staging-ownership protocol. Verify a normal unmodified export publishes successfully.

**Implementation**

Persist a verification artifact containing migration/source identity, normalized symbol/date scope, source schema/projection version, exact staged relative-file inventory, schema, per-file checksum/size/rows and reconciliation evidence. Bind state, plan and verification to the same migration. Under exclusive migration control, revalidate this authorization before publication and reject missing, additional, changed or out-of-scope files. Re-export must invalidate old authorization before mutating real staging, and failure to invalidate must abort the export; dry-run is exempt because it cannot mutate anything.

Prevent cooperating exporters/publishers from changing staging during validation/promotion through migration ownership. Specify behavior for externally modified files; a checksum check followed by an unprotected rename is not a complete concurrency protocol. Restrict staged paths to the migration directory and reject symlink/path escapes. Source reads must use an immutable or consistent snapshot; a read-only connection alone is not proof that an independently changing source is frozen.

### F05 — Immutable migration identities, provenance and safe resume

**Tests first**

1. Migrate source A and distinct source B into one symbol/day. Require both full multisets, distinct identity namespaces, and unchanged A-file hashes. Rerun A with the same migration identity; require no additional rows.
2. Resume after each chunk export, rename and receipt boundary. A completed checkpoint must be checked against its file inventory/hashes. Corrupt or missing completed chunks must fail or be explicitly rebuilt and reverified; do not skip blindly.
3. Change source, filters or schema while requesting resume; require an explicit identity mismatch, not reuse of stale state. Include equal timestamps and exact duplicate source records across chunk boundaries.
4. Seed live batch files in the destination. A migration receipt must reference only that migration's files, never all Parquet files in the partition. Verify live hashes remain unchanged.
5. Introduce a conflicting destination file at the planned name. Require safe refusal, never replacement. With chunk sizes of 1 and larger values, test the declared identity stability rules and resume constraints.

**Implementation**

Persist a migration UUID with a source snapshot fingerprint, projection/schema version and normalized scope; reuse it only for the same migration. A path or source mtime alone is not adequate identity. Use migration-namespaced final paths/ingest IDs. Associate every chunk with exact source provenance; repeated market rows are not automatically duplicates across independent sources. Define overlapping-snapshot handling explicitly: either reject ambiguous overlap or use stable source-record identities; never guess from equal prices/timestamps.

Replace unconditional destination `os.replace` with collision detection and idempotent verification under exclusive ownership. Track a publish journal so partial promotion resumes without relying on remaining staging files alone. Receipts list only journal-owned files. Deterministic source ordering needs all relevant value fields plus a stable duplicate occurrence identity, or a persisted immutable exported snapshot; ordering only by timestamp is insufficient. Resume must validate source identity and chunk evidence before trusting `COMPLETED`.

### F06 — Fence maintenance at real entry points

Files: `config.py`, `publication.py`, writer startup/recovery, migration and managed service orchestration.

**Tests first**

With a maintenance marker present, call writer construction, actual runner startup, direct publication, recovery and migration publication; require specific maintenance errors before data mutation. Test an already-running writer when maintenance is requested. Use two processes and barriers to prove maintenance cannot acquire exclusive ownership during an in-flight publication and that publication cannot start after maintenance owns the lake. Test crash/stale-marker handling conservatively; do not automatically delete an unexplained marker. Retain reader rejection tests.

**Implementation**

Use a shared exclusive ownership protocol for publishers and maintenance. Acquire ownership and check the marker before initialization/recovery/publication mutations; a pre-lock marker check alone has a race. Maintenance acquires ownership before announcing and modifying the lake. Normal service orchestration must drain/stop the writer and stop or drain managed reader queries before offline compaction. Arbitrary external glob readers cannot be fenced by a JSON marker: document their required quiescence and do not claim safe concurrent compaction. On maintenance completion, clear the marker only after durable successful completion; leave failures explicitly recoverable.

### F07 — Explicit backend and failure propagation

**Tests first**

For both constructor-supplied and environment-selected roots, inject publisher lock conflict, permission error, invalid metadata/schema, maintenance state and unavailable storage. Patch disk streaming-DuckDB connection factories to raise a sentinel if called. Assert the specific lake failure reaches service health/startup and the sentinel is never triggered. Exercise dashboard lake errors separately: genuine empty data may return empty results; corruption/unavailability must not masquerade as empty or trigger legacy fallback. Positive tests must prove explicitly selected legacy mode still works.

**Implementation**

Resolve backend once from explicit configuration. Remove broad exception-to-DuckDB fallback in runner construction and equivalent dashboard selection. Release partially acquired resources on constructor failure. Raise actionable typed errors and preserve their causes. For configured mounted storage, validate the intended root/metadata rather than silently initializing an empty replacement directory after a mount disappears; test only with fake temporary mount layouts. Keep historical DuckDB access separate from forbidden disk tick-DB access.

### F08 — Registry bootstrap, empty inventory and provider safety

**Tests first**

Use fake providers and invoke actual `start()`, not only reload. Cover initialized empty, all inactive, populated, corrupt, missing and pending-purge registries. Empty/all-inactive means zero subscriptions and no provider authentication. Corrupt state must fail closed; missing state must follow documented bootstrap behavior with no defaults. Test a temporary legacy symbol map import with provider mappings, inactive flags and idempotent re-import. Ensure startup and reload share subscription rules and removed symbols cannot be admitted after their fence takes effect.

**Implementation**

Remove fallback ticker activation in lake mode. Add explicit bootstrap that initializes a genuinely fresh empty registry, or imports a specified legacy inventory under control ownership; do not infer desired subscriptions from hard-coded tickers. Missing registry in an established lake should be treated as an integrity/configuration error, not silently erased/recreated. Preserve corruption errors. Keep the service capable of waiting for later registry activation with no active provider connection. Do not resurrect deliberately purged or disabled entries on repeated import.

### F09 — Real service flush configuration

**Tests first**

Assert defaults through actual CLI/config resolution and runner batching: 5 seconds and 5,000 rows. Test explicit CLI overrides over environment overrides over defaults; validate zero, negative, NaN and infinite values. Use a controllable monotonic clock/barriers to prove both age and row triggers, quiet-period flush, independent queue capacity, and correct reset after successful flush. Test the service supervisor's effective child command/environment. A writer constructor-default assertion alone is insufficient.

**Implementation**

Centralize validated configuration. Use `STREAM_FLUSH_INTERVAL_SECONDS` and `STREAM_MAX_BATCH_ROWS` unless an existing documented equivalent is retained consistently. Add matching CLI options, propagate the effective values to both runner and writer, and remove hard-coded 1,000-row batching. Set 5 seconds / 5,000 rows as defaults. Keep row capacity and queue capacity distinct. Test event-loop progress separately from reference-machine performance budgets.

### F10 — Dry-run must be read-only

**Tests first**

Snapshot a lake containing a plan, state, staging, final files, receipts and a successful verification report. Parameterize every CLI mode with dry-run, resume/force and filters. Require identical tree snapshots afterward. Repeat with a nonexistent destination and require it remain absent. Specifically prove `export(dry_run=True)` retains the existing verification report. Include validation failure paths and any source-side artifacts; use a closed immutable source fixture.

**Implementation**

Place dry-run handling before all mutations, including constructor initialization, directory creation, locks that create files, verification invalidation, state writes and cleanup. Separate read-only planning from effectful execution and report proposed changes without applying them. Do not broadly suppress permission errors. Inspect the entire call chain, not only the obvious `unlink` in `export()`.

### F11 — Historical cutover while capture is running

**Tests first**

Run a real writer process and prepare a verified historical migration. Assert direct competing publication is rejected by ownership; then exercise the supported orchestrated handoff. Fence admission, drain committed live data, close the writer, publish history, restart ingestion with safe new identities and resume provider delivery. Compare historical plus pre/post-handoff live multisets, including overlap policy. Inject migration failure and interruption at each handoff phase; no two publishers may own the lake, no queued accepted ticks may be silently abandoned, and the service must expose the failed phase.

**Implementation**

Prefer a supervised bounded cutover with a persisted handoff state over bypassing the lifetime writer lock. Complete expensive export/verification before the pause, then transfer exclusive ownership for final validation/publication and restart the writer. Keep readers on finalized files unless a maintenance operation requires quiescence. If uninterrupted feed coverage is mandatory, provide durable buffering or upstream replay across this pause; stop/start alone cannot guarantee ticks that the provider emits while disconnected. Treat that as a separate required capability before claiming uninterrupted zero-loss cutover.

## 5. Correct existing tests instead of accepting defects

| Existing test / area | Required change |
|---|---|
| `test_tick_lake_writer_bounded_buffer_backpressure` | Remove the success requirement `total_dropped > 0`. Assert bounded admission, documented wait/rejection semantics and preservation of all accepted valid rows |
| `test_streaming_engine_concurrent_producers_surge` | Drive awaited public callbacks and compare exact input/output identities; counting drops is telemetry, not the backpressure acceptance condition |
| `test_bounded_queue_backpressure` | Replace direct `put_nowait`/`QueueFull` expectations as product proof with the blocked-producer/released-consumer test |
| `test_honest_task_done_acknowledgment`, `test_honest_task_done_accounting_under_partial_flushes` | Include permanent publication failure, recovery and cancellation; join must not complete falsely |
| `test_disk_full_in_runner_pipeline_prevents_queue_hang` | Require an explicit bounded error/recovery path rather than draining the queue by discarding the batch |
| `test_exhausted_retries_io_error_telemetry` | Keep telemetry assertions and add ownership/retention, immutable identity and eventual exact persistence |
| `test_transient_io_retry_and_honest_counters` | Keep within-budget success; add exhausted-budget recovery and verify counters are not incremented per retry |
| `test_idempotent_duplicate_publish`, `test_collision_detection` | Add existing-receipt collisions, file corruption and default-writer restart, not only same-process/same-payload retries |
| `test_export_mode_resume_skips_completed` | Use valid Parquet and real checkpoint checksums; corrupted completed output must not count as successful resume |
| `test_export_mode_synthesizes_deterministic_ingest_id` | Assert stable ID-to-row mapping and separate source/run namespaces, not the old collision-prone filename/ID format |
| `test_publish_mode_refuses_without_passed_verification` | Add real successful verification followed by mutations, changed scope/source and extra files |
| `test_publish_mode_promotes_staged_files_and_writes_receipts` | Add preexisting live files, second migration, destination collision and interrupted resume; inspect receipt ownership |
| `test_dry_run_leaves_disk_unmodified` | Seed prior verification/state/final data and cover every mode plus nonexistent destination |
| `test_runner_empty_registry_unsubscribes_all` | Preserve reload coverage and add initial startup with no symbols and no authentication |
| Maintenance loader tests | Keep unit tests, but also call actual mutation/service entry points under contention |
| Supervisor kill/restart tests | Write new data after restart using production defaults; test against known durable boundaries, not merely process liveness |

Retain schema validation, symbol encoding, candle/session behavior and existing correctness expectations. Update backend-dependent dashboard/integrity fixtures to explicitly use lake or legacy mode. Do not lower candle counts, disable failures, broadly catch exceptions, or mark confirmed regressions `xfail` to advertise a green suite. If a temporary expected-failure mechanism is needed for CI, keep strict tracking and report those acceptance gates as unresolved.

## 6. Additional required coverage

1. **Status truthfulness:** receipt-based committed counts; accepted/pending/quarantined distinctions; real writer timestamp format consumed by dashboard; heartbeat freshness; failed drain and recovery states. Status-file failure must not duplicate a successfully committed batch.
2. **Read concurrency:** spawned writer and independent in-memory DuckDB readers on disjoint/overlapping partitions. Ignore staging; every visible file is valid. Assert final row sets and no disk tick DB access. Do not claim a stable raw-glob snapshot while files are being added.
3. **Reader semantics:** tied timestamps with deterministic open/close order, late ticks, UTC offsets and DST/session/day boundaries, empty/new symbols, corrupt files, invalid schema and date pruning. Preserve legitimate duplicate tick volume.
4. **Resource boundaries:** bounded queue plus in-flight batch memory, event-loop responsiveness while storage is blocked, retry backoff and cleanup of threads/FDs/locks on failure. Separate functional barriers from optional CPU/RSS/latency benchmarks on a recorded reference machine.
5. **Migration source coverage:** tables/views and supported legacy aliases, missing optional columns, timestamp precision/timezone, nulls and duplicate multiplicity. Verify full data comparison, not just row count/min/max. Snapshot acquisition from a locked source requires a supported writer-stop/export procedure, not copying a live DuckDB file unsafely.
6. **Recovery compatibility:** existing lake metadata, receipts and intents; corrupt/truncated control files must not be ignored as healthy. Orphan cleanup must preserve files still referenced by pending intents or migrations.
7. **Path and ownership safety:** encoded symbols, symlinks/path escapes, same-root resolution, publisher contention, stale markers and restart after failure. All tests use temporary data.
8. **Repo B contract:** independently open finalized Parquet with supported schema and resource limits; document timestamp/tie-order/provenance rules. If replay and compaction remain deferred, retain that status rather than claiming their unimplemented features are tested.

## 7. Dependency order and release gates

| Stage | Work | Gate |
|---|---|---|
| T0 | Isolation, fixtures, independent oracles and baseline inventory | Test processes cannot select production paths/providers; existing suite classified |
| T1 | Minimal F01–F10 regressions; F11 cutover contract tests; correct permissive assertions | Reproduced defects fail for the intended reason; report node IDs and baseline evidence before fixes |
| I1 | F01 stable identities and F03 prepared-batch recovery | Restart, every publication boundary and receipt replay preserve exact rows |
| I2 | F02 admission/worker/shutdown and status accounting | Accepted valid ticks survive recoverable faults; no silent queue drain or event-loop blocking |
| I3 | F06 ownership/fencing, F07 backend choice, F08 registry, F09 configuration | Real service startup/failure and effective settings meet contracts |
| I4 | F04/F05 migration integrity/provenance and F10 dry-run | Tampering rejected before promotion; repeated migrations immutable; dry-run unchanged |
| I5 | F11 supervised cutover and independent reader integration | Lock transfer and fault recovery verified; any feed-gap limitation explicit |
| Q | Complete regression matrix, compatibility and separately scoped operational qualification | All mandatory functional gates green; unresolved operational claims remain pending |

For every fix, run its targeted tests and affected adjacent suites before broader qualification. Add targeted mutation checks: demonstrate the test fails if stale receipt acceptance, failed-batch acknowledgment, unconditional destination replacement or default subscription fallback is reintroduced. These can be temporary local mutations reverted immediately; do not ship broken code or confuse deliberate mutation failures with baseline results.

After isolation changes are in place, use the repository interpreter:

```sh
.venv/bin/python -m pytest tests/storage tests/stream -m 'not live and not performance' -q
.venv/bin/python -m pytest tests -m 'not live and not performance' -q
```

Before isolation is fixed, explicitly supply a temporary `TICK_LAKE_ROOT` to every run. Run localhost integration tests in an environment that permits loopback binding; report sandbox restrictions separately. Do not enable external/live tests merely to make a test count complete. Run performance/endurance qualification separately with recorded hardware, dataset size, rates, memory budget, duration and thresholds; a short stress test is not a 24-hour endurance test.

## 8. Completion evidence and documentation repair

Create an execution report alongside this plan with: baseline and final commit IDs; Python/DuckDB/PyArrow versions; OS; exact commands; collected/passed/failed/deselected/skipped counts; F01–F11 and additional-contract mappings to actual test node IDs; pre-fix failure and post-fix success evidence; remaining limitations. Distinguish a functional test failure, invalid fixture and environmental restriction.

The execution report and `.planning/v4.1-MILESTONE-AUDIT.md` record the current outcomes. The historical `Executed`/milestone `PASSED` claims remain labeled as historical evidence rather than reused for F01-F11. Current findings F01-F11 and the offline repository suite pass; the separately run dashboard latency performance gates remain unresolved as recorded in the execution report.

Release acceptance requires no silent restart loss, no accepted-batch loss on recoverable I/O failure, no duplicate replay after partial publication, immutable verified migration, real maintenance fencing, explicit backend selection, safe empty-registry behavior, effective configured batching and read-only dry-run. Any abrupt-crash/feed-gap durability promise must have its own implemented mechanism and evidence; it cannot be inferred from a green unit suite.
