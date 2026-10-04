# Milestone v4.2 — Requirement Traceability Matrix

- Requirements tracked: **51**
- Mapped to executable nodes: **48**
- Declared coverage gaps: **3**
- Collected pytest nodes: **771**

Mapping granularity: `test` = a named test function; `module` = covered somewhere in that module
(a weaker claim, labelled as such); `none` = declared gap, not passing evidence.

| Requirement | Description | Nodes | Granularity | Status |
|---|---|---|---|---|
| LAKE-P0-01 | Test isolation routes default paths to tmp_path and blocks production mutations | `tests/test_isolation_guard.py` | module | Mapped |
| LAKE-P0-02 | Deterministic quote fixtures with repeats, ties, late arrivals, nulls, session boundaries | `tests/test_quote_fixtures.py` | module | Mapped |
| LAKE-P0-03 | Baseline characterization of legacy DuckDB CPU seconds, latency percentiles, event-loop lag | — | none | GAP (declared) |
| LAKE-P1-01 | Lake layout, TICK_LAKE_ROOT resolution, format versioning and safe symbol encoding | `tests/storage/test_storage_config.py`, `tests/storage/test_symbol_encoding.py` | module | Mapped |
| LAKE-P1-02 | Typed Schema v1 with microsecond timestamp and stable unique ingest_id | `tests/storage/test_schema_v1.py` | module | Mapped |
| LAKE-P1-03 | Atomic file staging via .tmp files and atomic rename | `tests/storage/test_atomic_publication.py` | module | Mapped |
| LAKE-P1-04 | Publication state machine and idempotent receipts | `tests/storage/test_atomic_publication.py`, `tests/storage/test_crash_recovery.py` | module | Mapped |
| LAKE-P2-01 | Micro-batch writer with configurable flush thresholds and bounded queue | `tests/storage/test_parquet_writer.py` | module | Mapped |
| LAKE-P2-02 | Off-loop PyArrow worker keeping event-loop scheduling lag bounded | `tests/storage/test_parquet_writer.py` | module | Mapped |
| LAKE-P2-03 | Runner lifecycle integration with honest counters and cooperative drain | `tests/stream/test_lake_runner_stress.py` | module | Mapped |
| LAKE-P3-01 | Atomic JSON symbol registry managed by a single control owner | `tests/storage/test_registry.py` | module | Mapped |
| LAKE-P3-02 | Cross-process registry polling and dynamic reload without DB locks | `tests/storage/test_registry_stress.py` | module | Mapped |
| LAKE-P3-03 | Pending purge semantics and subscription fencing | `tests/storage/test_registry_stress.py` | module | Mapped |
| LAKE-P4-01 | TickLakeReader engine with private in-memory DuckDB and partition pruning | `tests/storage/test_lake_reader.py` | module | Mapped |
| LAKE-P4-02 | Deterministic OHLCV resampling via time_bucket/arg_min/arg_max | `tests/storage/test_lake_reader.py` | module | Mapped |
| LAKE-P4-03 | Dashboard analytics migrated off legacy DuckDB files | `tests/dashboard/test_lake_dashboard_integration.py` | module | Mapped |
| LAKE-P4-04 | External consumer (Repo B) reader contract: schema docs, examples, connection patterns | — | none | GAP (declared) |
| LAKE-P6-01 | Migration CLI with plan/export/verify/publish modes | `tests/storage/test_migration_tool.py` | module | Mapped |
| LAKE-P6-02 | Chunked export and checkpointed progress | `tests/storage/test_migration_tool.py` | module | Mapped |
| LAKE-P6-03 | Two-way EXCEPT ALL reconciliation with zero row loss | `tests/storage/test_migration_tool.py`, `tests/storage/test_migration_stress.py` | module | Mapped |
| LAKE-P8-01 | Coordinated writer cutover from legacy writer to live Parquet lake | `tests/storage/test_tick_lake_audit_migration_regressions.py` | module | Mapped |
| LAKE-P8-02 | Multi-process concurrency without lock errors | `tests/integration/test_lake_multi_process_concurrency.py` | module | Mapped |
| LAKE-P8-03 | Documentation and service configuration updates | — | none | GAP (declared) |
| TEST-P22-01 | Storage foundation edge cases: paths, encoding, corrupted metadata | `tests/storage/test_storage_edge_cases.py` | module | Mapped |
| TEST-P22-02 | Schema coercion and extreme numeric handling | `tests/storage/test_storage_edge_cases.py` | module | Mapped |
| TEST-P22-03 | Atomic publication collisions, intent recovery, lock serialization | `tests/storage/test_storage_edge_cases.py` | module | Mapped |
| TEST-P23-01 | High-throughput micro-batching under memory pressure | `tests/stream/test_lake_runner_stress.py` | module | Mapped |
| TEST-P23-02 | Bounded queue backpressure and honest shedding | `tests/stream/test_lake_runner_stress.py` | module | Mapped |
| TEST-P23-03 | Runner lifecycle: shutdown mid-flush, drain, acknowledgment after durability | `tests/stream/test_lake_runner_stress.py` | module | Mapped |
| TEST-P24-01 | Cross-process concurrent registry CRUD serialization | `tests/storage/test_registry_stress.py` | module | Mapped |
| TEST-P24-02 | Monotonic versioning, torn-write rejection, pending purge fencing | `tests/storage/test_registry_stress.py` | module | Mapped |
| TEST-P24-03 | Reload signal debouncing and reload latency under polling | `tests/storage/test_registry_stress.py` | module | Mapped |
| TEST-P25-01 | Concurrent in-memory reader connections without leaks | `tests/storage/test_lake_reader_stress.py` | module | Mapped |
| TEST-P25-02 | Resampling edge cases: sparse partitions, roll-overs, DST, leap years | `tests/storage/test_lake_reader_stress.py` | module | Mapped |
| TEST-P25-03 | Reverse-chronological tape pagination and symbol pruning | `tests/storage/test_lake_reader_stress.py` | module | Mapped |
| TEST-P26-01 | Migration of corrupt/partial legacy sources and schema drift | `tests/storage/test_migration_stress.py` | module | Mapped |
| TEST-P26-02 | Crash interruption across migration modes and resumable checkpoints | `tests/storage/test_migration_stress.py` | module | Mapped |
| TEST-P26-03 | Two-way EXCEPT ALL fuzz reconciliation preserving multiplicity and precision | `tests/storage/test_migration_stress.py` | module | Mapped |
| TEST-P27-01 | Sustained multi-process soak with concurrent analytical readers | `tests/integration/test_supervisor_chaos_soak.py` | module | Mapped |
| TEST-P27-02 | Chaos monkey termination and supervisor self-healing to steady state | `tests/integration/test_supervisor_chaos_soak.py` | module | Mapped |
| F01 | Restart defaults; payload replay and corrupt receipt target | `tests/storage/test_tick_lake_audit_writer_regressions.py::test_default_writer_restart_publishes_new_observation_once`, `tests/storage/test_tick_lake_audit_writer_regressions.py::test_receipt_identity_replay_rejects_changed_payload_and_corrupt_target` | test | Mapped |
| F02 | Callback capacity/cancellation; retry retention; failed drain; receipt counts | `tests/stream/test_tick_lake_audit_runner_regressions.py::test_real_callback_waits_for_queue_capacity_without_loss`, `tests/stream/test_tick_lake_audit_runner_regressions.py::test_callback_waiting_for_capacity_can_be_cancelled_without_false_drop`, `tests/stream/test_tick_lake_audit_runner_regressions.py::test_exhausted_storage_retries_retain_batch_until_recovery`, `tests/stream/test_tick_lake_audit_runner_regressions.py::test_shutdown_reports_unsaved_accepted_batch_as_failed_drain`, `tests/stream/test_tick_lake_audit_runner_regressions.py::test_runner_committed_count_uses_verified_receipt_rows` | test | Mapped |
| F03 | Receipt failure after rename; retry publishes exactly once | `tests/storage/test_tick_lake_audit_writer_regressions.py::test_receipt_failure_retry_reuses_prepared_batch_without_duplicate_rows` | test | Mapped |
| F04 | Staged file replacement, wrong schema/extra file, source/scope mismatch at publication | `tests/storage/test_tick_lake_audit_migration_regressions.py::test_publish_rejects_staging_mutated_after_successful_verification`, `tests/storage/test_tick_lake_audit_migration_regressions.py::test_publish_verification_is_bound_to_source_scope_and_migration` | test | Mapped |
| F05 | Distinct-source migrations, corrupt checkpoint, source mismatch on resume | `tests/storage/test_tick_lake_audit_migration_regressions.py::test_distinct_migrations_append_immutably_and_same_migration_is_idempotent`, `tests/storage/test_tick_lake_audit_migration_regressions.py::test_resume_does_not_trust_corrupt_completed_chunk_checkpoint`, `tests/storage/test_tick_lake_audit_migration_regressions.py::test_resume_rejects_different_source_or_scope` | test | Mapped |
| F06 | Maintenance marker fences writer, recovery and migration; shared ownership gate | `tests/storage/test_tick_lake_audit_storage_regressions.py::test_writer_startup_and_direct_publication_are_fenced_by_maintenance`, `tests/storage/test_tick_lake_audit_storage_regressions.py::test_pending_publication_recovery_is_fenced_by_maintenance`, `tests/storage/test_tick_lake_audit_storage_regressions.py::test_migration_publication_is_fenced_by_maintenance`, `tests/storage/test_tick_lake_audit_storage_regressions.py::test_maintenance_lock_serializes_with_publisher_and_fences_new_publishers`, `tests/storage/test_tick_lake_audit_storage_regressions.py::test_maintenance_and_publisher_ownership_cross_process_barriers` | test | Mapped |
| F07 | Configured lake initialization and dashboard errors must not fall back to legacy DuckDB | `tests/storage/test_tick_lake_audit_storage_regressions.py::test_environment_selected_lake_failure_never_falls_back_to_streaming_duckdb`, `tests/storage/test_tick_lake_audit_storage_regressions.py::test_dashboard_lake_error_does_not_fall_back_to_streaming_duckdb`, `tests/storage/test_tick_lake_audit_storage_regressions.py::test_explicit_legacy_backend_remains_available` | test | Mapped |
| F08 | Empty/inactive registry and missing/corrupt registry startup | `tests/stream/test_tick_lake_audit_runner_regressions.py::test_empty_registry_start_does_not_subscribe_or_authenticate`, `tests/stream/test_tick_lake_audit_runner_regressions.py::test_established_lake_registry_errors_fail_closed_before_provider_start` | test | Mapped |
| F09 | Invalid values, effective defaults/env/CLI precedence, row trigger, child config | `tests/stream/test_tick_lake_audit_runner_regressions.py::test_invalid_stream_flush_settings_are_rejected`, `tests/stream/test_tick_lake_audit_runner_regressions.py::test_invalid_stream_batch_settings_are_rejected`, `tests/stream/test_tick_lake_audit_runner_regressions.py::test_runner_defaults_and_environment_overrides_reach_real_writer`, `tests/stream/test_tick_lake_audit_runner_regressions.py::test_runner_cli_overrides_env_and_reaches_engine`, `tests/stream/test_tick_lake_audit_runner_regressions.py::test_configured_batch_row_trigger_flushes_before_timer`, `tests/stream/test_tick_lake_audit_runner_regressions.py::test_supervisor_passes_effective_stream_settings_to_child` | test | Mapped |
| F10 | All dry-run modes are non-mutating, including an absent destination | `tests/storage/test_tick_lake_audit_migration_regressions.py::test_each_dry_run_mode_preserves_existing_tree_byte_for_byte`, `tests/storage/test_tick_lake_audit_migration_regressions.py::test_dry_run_does_not_create_a_nonexistent_destination` | test | Mapped |
| F11 | Real runner child, competing owner, supervised stop/publish/restart handoff | `tests/storage/test_tick_lake_audit_migration_regressions.py::test_publish_ownership_blocks_competing_live_writer`, `tests/storage/test_tick_lake_audit_migration_regressions.py::test_publish_holds_migration_ownership_during_promotion`, `tests/storage/test_tick_lake_audit_migration_regressions.py::test_writer_handoff_contract_preserves_pre_and_post_cutover_rows`, `tests/storage/test_tick_lake_audit_migration_regressions.py::test_real_supervised_runner_cutover_drains_publishes_and_restarts`, `tests/storage/test_tick_lake_audit_migration_regressions.py::test_supervised_handoff_persists_publish_failure_and_restarts_capture` | test | Mapped |

## Declared coverage gaps

- **LAKE-P0-03**: No automated node. Only the manual script tools/benchmark_baseline.py exists; no test asserts baseline CPU/latency numbers.
- **LAKE-P4-04**: No executable node. The contract is documentation only; Phase 33 (REPB-01) adds executable contract tests. Planned: Phase 33.
- **LAKE-P8-03**: Documentation-only requirement; no automated node. Phase 36 (DOCS-01..03) verifies docs against code. Planned: Phase 36.

---

*Generated by `tools/requirement_traceability.py`.*
