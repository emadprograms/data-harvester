# Milestone 5.0 — Phase 48 Report

**Phase:** 48 — Schedule, Off-Hours Compaction, Notifications & Test Disposition
**Date:** 2026-10-05 · **Status:** ✅ Complete
**Commits:** `d8258cd` (SCHED-01/03, NOTIF-01) · `1a62ae6` (SCHED-02/04/05) · `137f0bd` (SYMB-01, CO-01, CO-02)
**Branch:** `arena/01a10aa6-data-harvester` (PR #9) · **Working method:** test-driven per the owner directive
(failing tests → implement → verify → re-implement and re-verify on any failure)

---

## 1. What the phase had to deliver

Nine requirements, all now checked in `.planning/REQUIREMENTS.md`:

| ID | Requirement (short form) |
|---|---|
| SCHED-01 | One timezone policy module: eligibility in `America/New_York`, `[04:00, 20:00)`, injectable clock, DST-correct, host-timezone-independent |
| SCHED-02 | Supervisor owns lifecycle states (`WAITING_FOR_WINDOW`, `STARTING`, `INGESTING`, `DRAINING`, `MAINTENANCE`, `ERROR`); no provider auth outside the window; an intentional off-hours stop is not a crash |
| SCHED-03 | The direct runner entry point enforces the same window (a manual start cannot bypass the schedule) |
| SCHED-04 | At 20:00 admission stops, accepted ticks drain exactly once, a failed drain blocks compaction and is reported honestly |
| SCHED-05 | Compaction runs unattended once per closed interval, idempotently, after confirmed drain; a no-op interval counts as success; one maintenance lease prevents duplicate launchers |
| SYMB-01 | `_control/registry.json` is the single symbol authority (exactly the 19 approved equities); unsolicited symbols rejected at the callback boundary and cannot be added through the UI |
| NOTIF-01 | Discord notifications for session started/stopped/**failed to start**, supervisor restart, drain/compaction failure — best-effort, never blocking ingestion |
| CO-01 | Every test that v5.0 touched is dispositioned retain/retarget/delete with a reason; the retained suite passes with no required xfails |
| CO-02 | Candle behaviour verified unchanged against the Phase 46 baseline (the query engine was not modified) |

## 2. What was built

| File | Lines | Role |
|---|---|---|
| `src/utils/session_window.py` | 119 | SCHED-01: `WINDOW_START/END`, `OPEN_WEEKDAYS`, `now_et(clock=None)`, `is_eligible`, `next_open`, `window_close`, `seconds_until_open`, `describe`; `STREAM_NOW_OVERRIDE` test seam. No holiday calendar by design — holidays simply produce no ticks |
| `src/utils/lifecycle.py` | 75 | SCHED-02: `LifecycleState` + `LifecycleRecorder` writing `<log_dir>/<name>.state.json` atomically for the status script |
| `src/storage/offhours.py` | 284 | SCHED-05: `run_scheduled_compaction(...)` — per-interval ledger (`_maintenance/compaction_ledger.json`), `O_EXCL` lease with stale reclaim, drain gate over `_control/writer_status.json`, statuses `ALREADY_COMPLETED / COMPLETED / FAILED / LEASE_HELD / BLOCKED_DRAIN_FAILED / NOT_ELIGIBLE` |
| `src/utils/notifications.py` | 153 | NOTIF-01: `EVENTS`, `build_embed`, `notify(...) -> bool` (never raises), `notify_detached` (<0.5 s, skipped when no webhook or `SKIP_DISCORD`) |
| `src/stream/runner.py` | edited | SCHED-03/04: window gate before connecting (exit `3` outside), `admission_open`/`close_admission`/`WINDOW_CLOSED` drops, `window_watchdog(stop_event, clock, poll_seconds)`, session start/stop/drain-failure/start-failure notifications; mock mode exempt |
| `src/dashboard/server.py` | edited | SYMB-01: `POST /api/streaming/symbols` → HTTP 400 outside `APPROVED_EQUITY_SYMBOLS` unless `ALLOW_UNSCOPED_SYMBOLS=1` |
| `tools/service_supervisor.py` | edited | SCHED-02/05: `--enforce-window` / `--maintenance`, `_windowed_tick`, dispatched clock, maintenance hook → `run_scheduled_compaction(notifier=notify_detached)` |
| `tools/mac/{start_streamer,start_services,install_startup}` | edited | The launchd streamer agent passes both flags; the dashboard stays 24/7 |
| `tests/conftest.py` | edited | Sets `ALLOW_UNSCOPED_SYMBOLS=1` so synthetic-name CRUD suites keep working; the production refusal is asserted with the override removed |

## 3. Evidence — requirement, status, test

Every count below is from `pytest <file> -q -p no:randomly` on this branch tip; all green.

| Requirement | Status | Test evidence | Result |
|---|---|---|---|
| SCHED-01 | Complete | `tests/utils/test_session_window.py` — half-open boundaries, weekends, both DST transitions, host-timezone independence, `next_open`/`seconds_until_open`, clock + env override | 34 passed |
| SCHED-02 | Complete | `tests/support/test_supervisor_lifecycle.py` — state set, no child outside the window, off-hours stop ≠ crash, crash backoff, `max_restarts` → ERROR, state file published, maintenance ownership | 15 passed |
| SCHED-03 | Complete | `tests/stream/test_runner_schedule_and_notifications.py` — gate allows/refuses, CLI exit `3` off-hours, mock exempt | 19 passed (file) |
| SCHED-04 | Complete | same file — watchdog stops at close, admission closes, close path drains exactly once, manual stop drains once | 19 passed (file) |
| SCHED-05 | Complete | `tests/storage/test_offhours_compaction.py` — once per interval, no-op = success, NOT_ELIGIBLE in-window, lease held/stale-reclaim/released, `DRAIN_FAILED` blocks, live-writer blocks (120 s heartbeat), real `LakeCompactor` integration | 14 passed |
| SYMB-01 | Complete | `tests/test_symbol_authority.py` — registry equals the ticket, boundary rejection, capital-ticker mapping, one log per symbol, UI refuses/accepts correctly | 11 passed |
| NOTIF-01 | Complete | `tests/utils/test_notifications.py` (19) + the runner file (19) — every event titled, unknown refused, `SKIP_DISCORD`/no-webhook/transport-error silent `False`, detached dispatch non-blocking | 19 + 19 passed |
| CO-01 | Complete | `.planning/v5.0-TEST-DISPOSITION.md` (Phase 48 block: every added/retargeted/edited test + reason); `grep -rn xfail tests/` finds only the validator's input fixtures | 0 xfails in 988 |
| CO-02 | Complete | `tests/storage/test_candle_baseline.py` — golden values for 7 timeframes + gap windows, entry-point signatures pinned, dashboard layer proven to be a pass-through | 12 passed |

**Independent confirmation that the engine was untouched (CO-02):** `src/storage/reader.py`'s only
diff since the branch base is the RMV-08 replay-method deletion (49 lines, no candle code);
`src/dashboard/analytics.py` only lost the dead disk-database seam.

## 4. Verification runs at the close of the phase

| Run | Result |
|---|---|
| Six Phase 48 suites, run individually | 34 + 15 + 19 + 19 + 14 + 11 = **112 passed** |
| Combined regression: `tests/support tests/integration tests/storage/test_offhours_compaction.py tests/stream` | **173 passed** (50.72 s) |
| SYMB-01 regression: `tests/stream tests/storage/test_registry_stress.py tests/dashboard tests/e2e tests/test_streaming_symbols_management.py` | **205 passed** (44.87 s) |
| Full offline suite (`pytest tests/ -q`) | **988 passed** in 320.5 s — 0 failures, 0 xfails |
| Requirement-evidence sweep (14 files cited above) | **108 passed** |

Two host-load-sensitive tests (`test_validate_concurrency_cli_execution`, the supervisor soak)
reproduce green in isolation; they are load artefacts, not regressions.

## 5. Honest limitations and residual risks

1. **The window has never run against the live Capital feed for a full day.** All scheduling
   evidence is test-driven with an injected clock; the first real 04:00–20:00 weekday cycle is
   the owner's first production observation.
2. **Missed first launch is possible.** The ledger makes compaction idempotent, but if the Mac
   is asleep at 04:00 the session simply does not start — the failure notification is the
   designed signal, not a retry.
3. **`barriers.py`'s test hook remains in the production path** (pre-existing; documented risk).
4. **RAM-only loss window on kill** is unchanged: ticks acknowledged into the batch buffer are
   lost if the process is killed before flush (pre-existing, accepted).
5. **CO-01 is a document plus a green suite**, not an automated guard against future xfails —
   the release validator already rejects `xfailed_required_test` when reports are validated.

## 6. Milestone 5.0 at a glance — is Phase 49 the only thing left?

**Yes.** Phase 49 is the last phase. Everything in Phases 46–48 is complete and pushed;
nothing else in the milestone is open. Phase 49 itself is three owner actions, not only the
migration: the **migration gate** (MIG-01/02/03), the **deletion** of the two `.duckdb` files
(RMV-05), and **one completion report** (CO-03). The full requirement table follows.

| Requirement | Phase | Status | Test exists & passes |
|---|---|---|---|
| BASE-01 | 46 | Complete | `tests/storage/test_candle_baseline.py` (12) |
| STOR-01 | 46 | Complete | `tests/stream/test_no_disk_db_backend.py` (8) |
| STOR-02 | 46 | Complete | same (8) + `tests/storage/test_tick_lake_audit_storage_regressions.py` (8) |
| STOR-03 | 46 | Complete | `tests/test_symbol_maps_separation.py` (3) + `tests/dashboard/test_registry_endpoints.py` (6) |
| STOR-04 | 46 | Complete | `tests/utils/test_integrity_lake.py` (5) |
| STOR-05 | 46 | Complete | `tests/test_disk_database_layer_removed.py` (4) |
| DASH-01 | 46 | Complete | `tests/dashboard/test_parquet_only_dashboard.py` (7) |
| DASH-02 | 46 | Complete | `tests/dashboard/test_single_view_frontend.py` (4) |
| DASH-03 | 46 | Complete | `tests/utils/test_integrity_lake.py` (5) |
| GAP-01 | 46 | Complete | `tests/stream/test_gap_ledger.py` (6) + `test_symbol_maps_separation.py::test_databento_backfill_targets_lake_registry` |
| GAP-02 | 46 | Complete | `tests/data/test_databento.py` (5) + `test_bar_era_removal.py::test_retained_dependencies_survive` |
| RMV-01 | 47 | Complete | `tests/test_disk_database_layer_removed.py` (4), `test_bar_era_removal.py` (20), `tests/docs/test_documentation_contract.py` (15) |
| RMV-02 | 47 | Complete | `tests/test_bar_era_removal.py` (20) + `tests/dashboard/test_gap_visualisation_survives.py` (5) |
| RMV-03 | 47 | Complete | `tests/test_bar_era_removal.py` (20) + `test_single_view_frontend.py` (4) |
| RMV-04 | 47 | Complete | `tests/test_bar_era_removal.py` (20) |
| RMV-06 | 47 | Complete | `tests/docs/test_documentation_contract.py` (15) + `test_bar_era_removal.py` (20) |
| RMV-07 | 47 | Complete | `tests/test_bar_era_removal.py` (20) |
| RMV-08 | 47 | Complete | `tests/test_bar_era_removal.py` (20) |
| RMV-09 | 47 | Complete | `tests/test_bar_era_removal.py` (20) + `tests/utils/test_notifications.py` (19) |
| RMV-10 *(close-out residue, not in the traceability table)* | 47 | Complete | `tests/test_bar_era_removal.py` (20) |
| SCHED-01 | 48 | Complete | `tests/utils/test_session_window.py` (34) |
| SCHED-02 | 48 | Complete | `tests/support/test_supervisor_lifecycle.py` (15) |
| SCHED-03 | 48 | Complete | `tests/stream/test_runner_schedule_and_notifications.py` (19) |
| SCHED-04 | 48 | Complete | same (19) |
| SCHED-05 | 48 | Complete | `tests/storage/test_offhours_compaction.py` (14) |
| SYMB-01 | 48 | Complete | `tests/test_symbol_authority.py` (11) |
| NOTIF-01 | 48 | Complete | `tests/utils/test_notifications.py` (19) + the runner file (19) |
| CO-01 | 48 | Complete | `.planning/v5.0-TEST-DISPOSITION.md`; suite has 0 xfails |
| CO-02 | 48 | Complete | `tests/storage/test_candle_baseline.py` (12) |
| MIG-01 | 49 | **Pending — owner** | Runbook §1 acceptance table; rehearsed against a synthetic source |
| MIG-02 | 49 | **Pending — owner** | Runbook §1 → §3 ordering rule |
| MIG-03 | 49 | **Pending — owner** | Runbook §2; re-run idempotence rehearsed (9 files / 360 rows unchanged) |
| RMV-05 | 49 | **Pending — owner** | Runbook §3 (deletion; `src/` has zero `.duckdb` references) |
| CO-03 | 49 | **Pending — owner** | Runbook §5 completion table |

**Traceability:** 33 requirements, 33 mapped to tests or an owner procedure, 0 unmapped.
The migration tool those Phase 49 rows rely on is itself covered by
`tests/storage/test_migration_tool.py`, `test_migration_stress.py`,
`test_tick_lake_audit_migration_regressions.py` and the rehearsal described in
`docs/operations/phase49_migration_runbook.md`.
