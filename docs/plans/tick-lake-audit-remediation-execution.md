# Tick lake audit remediation execution report

**Plan:** [`tick-lake-audit-remediation.md`](tick-lake-audit-remediation.md)  
**Audit:** [`.planning/v4.1-MILESTONE-AUDIT.md`](../../.planning/v4.1-MILESTONE-AUDIT.md)  
**Status:** F01-F11 and offline qualification passed. Post-merge local performance qualification passed on the macOS host described below; PR #7 had no GitHub checks. The earlier latency failures remain recorded as historical evidence.

## Environment and baseline

- Baseline checkout: `c55350e5da4c2c6f2c9db5aba7f72f39c287a9c5`, branch `arena/01a10288-data-harvester` (shallow checkout in this workspace).
- Plan-stated baseline: `280d5b2a`; it is not the current checkout SHA, so observed results are attributed to the actual checkout above.
- OS: Linux `6.1.158+`, x86_64, glibc 2.36.
- Python: 3.11.2; DuckDB: 1.5.5; PyArrow: 22.0.0; pytest: 9.1.1.
- Test interpreter: repository `.venv/bin/python`.
- Storage isolation: conftest sets `DATA_DIR` and `TICK_LAKE_ROOT` to a session-temporary directory before application imports and initializes valid empty lake metadata; per-test roots use `tmp_path`. Legacy DuckDB tests explicitly inject a client/path. Non-live external socket connects remain guarded; subprocess fixtures inject isolated paths.

### Baseline commands and results

| Command | Result |
|---|---|
| `.venv/bin/python -m pytest tests/test_isolation_guard.py -q` | 22 passed (after T0 isolation guard setup) |
| `.venv/bin/python -m pytest tests/storage tests/stream -m 'not live and not performance' -q` | 215 passed, 1 deselected, 80.26 s (pre-remediation existing suites; localhost available) |
| Four audit regression modules, before application changes | **40 failed, 8 passed, 48 total**. The failing-node details are recorded below; this was the intended test-first defect baseline. |
| Initial all-suite collection, before optional Databento handling | Blocked by 2 `ModuleNotFoundError: databento` collection errors in `tests/data/test_databento.py` and `tests/test_symbol_maps_separation.py`; no suite result was claimed. |

## Execution stages

| Stage | Status | Evidence / next action |
|---|---|---|
| T0: isolation, fixtures, independent oracles | complete | `tests/conftest.py` pins temporary `DATA_DIR`/`TICK_LAKE_ROOT`, initializes valid empty lake metadata, provides explicit lake/legacy/subprocess fixtures, and guards DuckDB/filesystem/PyArrow writes. Independent Parquet/fault/process/migration/tree-snapshot helpers are present. |
| T1: F01-F10 regressions and F11 contract tests | complete | Pre-fix suite: **40 failed, 8 passed**. Final T1 regression suites: **51 passed**. The real supervised runner handoff is covered and passes. Node IDs and baseline defect evidence are listed below. |
| I1: identities and publication recovery | complete | F01/F03 and adjacent writer/publication suites pass. |
| I2: admission, worker ownership, shutdown, status | complete | F02 callback admission, retained retries, receipt-based accounting and failed-drain behavior pass in targeted and full offline runs. |
| I3: maintenance, backend, registry, effective batching | complete | F06-F09 focused storage/runner regressions pass; lake initialization serializes boundedly against active publishers. |
| I4: migration authorization/provenance and dry-run | complete | F04/F05/F10 migration regressions and stress suites pass; immutable publication and non-mutating dry-runs verified. |
| I5: supervised historical cutover | complete | F11 coordinator drains the managed runner, suspends supervisor restart, publishes while exclusive owner is released, then restarts capture. Real child-process regression passes. |
| Q: offline repository qualification | passed | PR #7: **742 passed, 10 deselected**. Post-merge macOS rerun: **746 passed, 11 deselected** in 117.91 s. |
| Q: performance qualification | passed locally after follow-up | The prior run failed two dashboard latency gates. The post-merge run passed **5**, skipped **4** optional historical-dataset benchmarks; measured values and host are below. |

## T1 pre-fix regression baseline

Command:

```sh
.venv/bin/python -m pytest \
  tests/storage/test_tick_lake_audit_writer_regressions.py \
  tests/storage/test_tick_lake_audit_migration_regressions.py \
  tests/storage/test_tick_lake_audit_storage_regressions.py \
  tests/stream/test_tick_lake_audit_runner_regressions.py \
  -q --tb=no
```

Result: **40 failed, 8 passed, 48 total** in 5.40 s. These failures occurred before any application-code changes. The 8 passing cases include unmodified migration flows, lock exclusion, dry-run modes that did not mutate the seeded tree, explicit legacy selection and the manually bounded writer handoff.

| Finding | Actual regression node IDs (baseline) | Observed baseline failure |
|---|---|---|
| F01 | `test_default_writer_restart_publishes_new_observation_once`; `test_receipt_identity_replay_rejects_changed_payload_and_corrupt_target` | Second default writer reused batch/ingest identity; changed-payload replay returned success. |
| F02 | `test_real_callback_waits_for_queue_capacity_without_loss[capital]`; `[binance]`; `test_callback_waiting_for_capacity_can_be_cancelled_without_false_drop`; `test_exhausted_storage_retries_retain_batch_until_recovery`; `test_shutdown_reports_unsaved_accepted_batch_as_failed_drain`; `test_runner_committed_count_uses_verified_receipt_rows` | Full-queue callbacks returned after drop; accepted rows were acknowledged/dropped on write error; unsaved rows counted committed. |
| F03 | `test_receipt_failure_retry_reuses_prepared_batch_without_duplicate_rows` | One receipt replace failure followed by flush retry yielded two Parquet rows for one accepted observation. |
| F04 | `test_publish_rejects_staging_mutated_after_successful_verification[empty]`; `[price]`; `[extra_file]`; `[schema]`; `test_publish_verification_is_bound_to_source_scope_and_migration` | Tampered staged content and mismatched source/scope were not rejected after verification. |
| F05 | `test_distinct_migrations_append_immutably_and_same_migration_is_idempotent`; `test_resume_does_not_trust_corrupt_completed_chunk_checkpoint`; `test_resume_rejects_different_source_or_scope` | Second source replaced the first source's chunk; completed corrupt checkpoint was trusted; changed source resume was accepted. |
| F06 | `test_writer_startup_and_direct_publication_are_fenced_by_maintenance`; `test_pending_publication_recovery_is_fenced_by_maintenance`; `test_migration_publication_is_fenced_by_maintenance`; `test_maintenance_lock_serializes_with_publisher_and_fences_new_publishers` | Mutation entry points proceeded through a maintenance marker; shared maintenance ownership API was absent. |
| F07 | `test_environment_selected_lake_failure_never_falls_back_to_streaming_duckdb`; `test_dashboard_lake_error_does_not_fall_back_to_streaming_duckdb` | Configured lake errors were hidden by the legacy fallback path. |
| F08 | `test_empty_registry_start_does_not_subscribe_or_authenticate[fresh_empty]`; `[empty]`; `[all_inactive]`; `test_established_lake_registry_errors_fail_closed_before_provider_start[missing]`; `[corrupt]` | Empty/inactive inventory activated default tickers; missing/corrupt inventory did not stop provider startup. |
| F09 | `test_invalid_stream_flush_settings_are_rejected[0]`, `[-1]`, `[nan]`, `[inf]`; `test_invalid_stream_batch_settings_are_rejected[0]`, `[-1]`; `test_runner_defaults_and_environment_overrides_reach_real_writer`; `test_runner_cli_overrides_env_and_reaches_engine`; `test_configured_batch_row_trigger_flushes_before_timer` | Invalid values were accepted; actual service defaults/effective overrides and row-trigger batching were wrong or unavailable. |
| F10 | `test_each_dry_run_mode_preserves_existing_tree_byte_for_byte[export]`; `[all]` | Dry-run export/all modified the existing migration tree (including deleting the prior verification report). |
| F11 | `test_real_supervised_runner_cutover_drains_publishes_and_restarts` | Real runner process was started; migration correctly hit the live publisher lock. The test then failed because persisted supervised handoff orchestration was absent. |

Other T1/support nodes: `test_publish_ownership_blocks_competing_live_writer` passes (direct ownership enforcement already existed); `test_writer_handoff_contract_preserves_pre_and_post_cutover_rows` passes for a manually ordered same-process cutover; `test_dry_run_does_not_create_a_nonexistent_destination` passes. These do not substitute for F11's supervised process handoff or F10's existing-tree snapshot.

## Test traceability and post-fix outcomes

### I1 — F01/F03 identities, receipt integrity and retry

- `LakePublisher` validates safe batch IDs, records payload SHA-256 fingerprints in intents/receipts, compares logical row-multisets on replay, verifies receipt-owned final Parquet checksums/schema/counts/partition values, and rejects changed payloads or corrupt/missing targets.
- Recovery validates all targets before promotion, verifies logical payload and physical-file checksums, and writes a receipt only after complete batch verification.
- `TickLakeWriter` uses a run UUID namespace for generated ingest IDs, receipt batch IDs and final filenames. A prepared batch retains its sequence, ID and normalized rows after publication failure and retries unchanged.
- Writer, atomic-publication, crash-recovery and adjacent writer suite: **21 passed**. Full T1/offline results below also include these cases.

### I2 — F02 admission, durable ownership and shutdown

- Capital/Binance callbacks await bounded queue capacity; cancellation while waiting does not create an accepted tick or false drop. The synchronous best-effort helper reports pre-admission rejection explicitly.
- Lake worker retains failed batches and unfinished queue acknowledgments, pauses until storage recovery, then retries the same normalized batch. Committed counts come from `PublishReceipt.row_count`.
- Shutdown raises `DrainFailedError` on a drain deadline, reports pending work as `DRAIN_FAILED`, and never converts unsaved accepted items into drops or healthy `STOPPED` state.
- F02 regression cases plus `tests/stream/test_lake_runner_integration.py` and `tests/stream/test_lake_runner_stress.py`: **26 passed**. Updated legacy tests are `test_runner_drain_timeout_reports_pending_work_without_false_drops` and `test_disk_full_in_runner_pipeline_retains_batch_until_recovery`.

### I3 — F06-F09 ownership fences, backend choice, registry and batching

- Publisher and maintenance share an OS ownership lock and durable maintenance marker; writer, direct publication, recovery, migration and initialization honor the fence. `init_tick_lake` waits up to 30 seconds for a current owner before taking its idempotent initialization lock; fail-fast publisher acquisition remains the default.
- Environment-selected lake errors propagate instead of selecting DuckDB. Explicitly injected legacy clients/paths remain available.
- Fresh registries are empty; empty inventory subscribes to nothing; missing/corrupt established registries fail before provider startup.
- Validated 5-second/5,000-row defaults, environment values and CLI/constructor overrides reach the actual writer and row-trigger batching.
- Focused storage/runner regression command: **28 passed**. Lock-contention/initialization tests also pass in the final combined 102-test run.

### I4 — F04/F05/F10 migration identity, verification and dry-run safety

`tools/migrate_streaming_to_parquet.py` now scopes deterministic output identities by migration and source; opens a consistent read-only source snapshot; assigns stable source ordinals; validates source/checkpoint hashes; reconciles staged inventory against the verified source/scope/schema; publishes migration-prefixed immutable files with no-clobber links; and recovers through migration journal/receipt state. Dry-run plan/export/verify/publish/all flows do not change existing files or create artifacts. Legacy migration tests were updated to assert migration-scoped IDs, checkpoint-based safe resume and immutable promotion rather than old generic chunk overwrite semantics.

Defect-specific final outcomes:

| Finding | Post-fix verification |
|---|---|
| F04 | Tampered/empty/extra/wrong-schema staging, changed source/scope, and promotion interruption/recovery cases pass in `test_tick_lake_audit_migration_regressions.py`. |
| F05 | Two-source append, same-migration idempotency, invalid/corrupt checkpoint, source identity and stress/resume cases pass in the migration audit, `test_migration_tool.py`, and `test_migration_stress.py` suites. |
| F10 | Existing-tree snapshots remain byte-for-byte unchanged for every dry-run mode; absent destinations remain absent. |

Migration/tool/stress plus migration audit regression command previously returned **66 passed**. The final combined migration/ownership/audit/debounce command returned **102 passed**.

### I5 — F11 supervised historical handoff

`MigrationHandoffCoordinator` persists the handoff lifecycle, suspends automatic supervisor restart/reload, requests graceful child drain, waits for publisher ownership to be released, performs verified migration publication while ingestion is quiescent, records completion/recovery state, and resumes capture. The runner awaits its engine's one shutdown/drain owner rather than closing the same writer twice. The real subprocess regression verifies pre/post-cutover rows, ownership exclusion, supervisor suspension and resumed capture. F11 regression passes in the final T1 and offline runs. Existing real signal/chaos tests now explicitly seed a registry instrument, since F08 correctly forbids default subscriptions from an empty registry.

### PR #7 targeted and offline qualification

T1 audit regression command:

```sh
.venv/bin/python -m pytest \
  tests/storage/test_tick_lake_audit_writer_regressions.py \
  tests/storage/test_tick_lake_audit_migration_regressions.py \
  tests/storage/test_tick_lake_audit_storage_regressions.py \
  tests/stream/test_tick_lake_audit_runner_regressions.py \
  -q --tb=short
```

Result: **51 passed** in 9.39 s.

Combined focused command covering migration tool/stress, audit regressions, lake initialization/lock contention and signal-storm debounce: **102 passed** in 22.27 s. `git diff --check` and Python compilation of the changed runtime modules passed.

Full offline qualification:

```sh
.venv/bin/python -m pytest tests -m 'not live and not performance' -q
```

Result: **742 passed, 10 deselected** in 243.92 s. This also confirms test collection succeeds without the optional `databento` SDK. Offline-only Databento helpers import without it; an attempted live client raises a clear `RuntimeError` requesting the optional package.

### Previous separately selected performance tests — unresolved at PR #7 merge

The two latency-gated multi-process soak tests are correctly marked `performance` and are excluded by the offline qualification command. They were run separately:

```sh
.venv/bin/python -m pytest tests -m performance -q --tb=short
```

Result at PR #7 merge: **2 failed, 2 passed, 4 skipped, 744 deselected** in 13.31 s. The four database performance cases were skipped by their fixture conditions. All data-parity, corruption/lock, writer-lag and Repo B latency gates in the 10k-tick soak passed; the dashboard gate measured **102.518 ms p95** against `<100 ms`. The concurrent multi-wave test's first two waves passed, but continuity wave 3 measured **110.735 ms p95** against `<100 ms`. Earlier reruns also exceeded those limits (102.818 ms and 121.311 ms, respectively), so these were recorded as unresolved performance failures at merge time, not green qualification or waived checks. No latency threshold was relaxed.

## Repository hygiene

- `git diff --check`: passed.
- `py_compile` for the modified migration tool, supervisor, runner, storage ownership/configuration, integrity module and optional Databento backfill module: passed.
- At this earlier stage no commit or push was made; those changes were later merged in PR #7.

## Post-merge performance and CI follow-up (2026-10-03)

**Starting commit:** `1c9af7667d97bb3bd9020301bae0465e5e5fee35` on `main` (merged PR #7). **Local host:** macOS Darwin 25.6.0, arm64; Python 3.12.13, DuckDB 1.5.5, PyArrow 22.0.0, pytest 9.0.2. Tests used temporary lake and data roots. The historical benchmark suite now requires an explicit `PERFORMANCE_HISTORICAL_DB_PATH` pointing to an isolated benchmark dataset; none was supplied, so those four tests were skipped. They are not claimed as qualified.

GitHub's [PR #7 Checks tab](https://github.com/emadprograms/data-harvester/pull/7/checks) reports **“There are no checks for this commit”** for `869a15c`. Its empty rollup was not a passing CI result. A new `.github/workflows/offline-tests.yml` runs the offline suite on future PRs and pushes to `main`; it cannot retroactively add checks to merged PR #7. A hosted CI result for this follow-up remains pending until that workflow runs on GitHub.

Profiling `TickLakeReader.get_streaming_continuity_analysis` with a temporary published AAPL partition showed repeated rebuilding of the same US holiday dates and 960 exchange-minute labels/epochs per request. A 20-call local sample had p95 **61.541 ms** before and **17.002 ms** after caching immutable holiday and date/session templates. The cache contains no response dictionaries or Parquet data; every request still reads current finalized files and constructs a fresh response. A new regression checks regular/extended sessions, spring/fall DST epochs and response isolation.

The first post-merge full performance selection revealed that a relative `data/market_data.duckdb` existed on this machine: four optional historical benchmarks attempted to access it despite the test isolation guard. They now select only an explicitly supplied isolated benchmark file. That run also saw one writer p99 lag of 20.964 ms. Separately, an offline run saw a 21.07 ms maximum lag in a host-sensitive writer test. Its `<20 ms` threshold remains unchanged in the `performance` selection; a deterministic offline test now proves that a blocked publication worker leaves the event loop responsive.

The first post-merge offline run returned **744 passed, 2 failed, 10 deselected**. The failures were a macOS `/var` versus `/private/var` temporary-path assertion and a mixed read-only/read-write DuckDB connection race. Both were repaired: the assertion resolves both paths, and the legacy connection wrapper serializes mode negotiation and close per database file. A second offline run returned **745 passed, 1 failed, 10 deselected**; the remaining failure was the host-sensitive writer lag test described above. The final offline run passed:

```sh
.venv/bin/python -m pytest tests -m 'not live and not performance' -q
# 746 passed, 11 deselected in 117.91 s
```

Final separate performance qualification, with the original `<100 ms` dashboard p95 and `<20 ms` writer lag thresholds:

```sh
.venv/bin/python -m pytest tests -m performance -q -s --tb=short
# 5 passed, 4 skipped, 748 deselected in 10.20 s
```

| Gate | Final measured result | Target |
|---|---:|---:|
| 6k-tick dashboard p95 / writer p99 | 20.12 ms / 13.99 ms | <100 ms / <20 ms |
| 10k-tick dashboard p95 / writer p99 | 13.24 ms / 14.23 ms | <100 ms / <20 ms |
| 100-candle request wave p95 | 3.098 ms | <100 ms |
| 100-tape request wave p95 | 23.828 ms | <100 ms |
| 50-continuity request wave p95 | 24.660 ms | <100 ms |
| Repo B p95 at 6k / 10k | 2.08 ms / 2.35 ms | <100 ms |

Both concurrency cases reported zero DuckDB lock exceptions, zero Parquet footer errors, zero HTTP errors and exact tick parity. `git diff --check`, Python compilation and workflow YAML parsing passed. These are local measurements on the named host. Long-duration endurance and the optional historical dataset benchmarks remain separate qualifications.
