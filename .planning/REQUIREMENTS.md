# Requirements: Data Harvester

**Defined:** 2026-10-05
**Core Value:** Zero-cloud, zero-quota persistent market data ingestion and storage — capture ticks reliably and serve them concurrently, with no locks and no quotas.
**Milestone:** v5.0 Parquet-Only Storage (DuckDB retained as query engine)

**Source of truth for scope:** [`PLAN-MILESTONE-5.0.md`](../PLAN-MILESTONE-5.0.md) — four phases (46–49).

> **Scope decision (owner, 2026-10-05):** remove DuckDB as **storage** only. No `.duckdb` files may exist; all persisted data is Parquet. DuckDB remains as the **in-memory query engine** reading Parquet. The larger alternative — removing the DuckDB library and reimplementing resampling in PyArrow — was considered and **rejected**. `src/storage/reader.py`, `compaction.py` and `replay.py` are therefore **out of scope for modification**.

A requirement is **Complete** only when its stated evidence exists. Shortfalls are recorded honestly, never rounded up.

---

## v5.0 Requirements by Phase

### Phase 46: Rewire Off the Disk Databases

- [ ] **BASE-01**: The lake's current candle output is recorded once as the reference behaviour **before** any code change, so an accidental change during the refactor is detectable.
- [x] **STOR-01**: No code path opens or creates a disk-backed DuckDB database during normal runtime. `tests/stream/test_no_disk_db_backend.py` proves it and would fail if one were reintroduced. `src/dashboard/analytics.py` no longer imports `src.database` and its six silent disk fallbacks (`get_streaming_db_connection(read_only=True)`) are gone — an unavailable lake now raises. *Residual:* the file keeps a test-only `client=` injection seam that runs DuckDB SQL against a caller-supplied client; production never passes one, and STOR-05 removes the seam with the legacy tests.
- [ ] **STOR-02**: `src/stream/runner.py` is lake-only — the DuckDB writer fallback is **removed**, not merely disabled, and `init_streaming_db` / `save_ticks_to_storage` are gone.
- [ ] **STOR-03**: The streaming symbol inventory is read from `_control/registry.json`; the legacy database lookups are removed.
- [x] **STOR-04**: Satisfied without a new helper — DASH-03 made `src/utils/integrity.py` open no DuckDB connection of its own; it queries through `TickLakeReader`, which owns the in-memory engine. There is nothing to relocate when `src/database` is deleted.
- [ ] **STOR-05**: `src/database/{connection,schema,operations}.py` are deleted with every importer resolved.
- [ ] **DASH-01**: `/api/historical/*` routes and their analytics functions are removed; retained tick routes serve the lake.
- [ ] **DASH-02**: The dashboard is a **single Parquet-only view**. The Historical Dashboard (nav, chart container, symbol list, source selector, 'Historical DB' stat) is deleted. Charts render tick-derived candles from the lake; dates predating tick capture return an honest empty state and never imply bars were converted.
- [ ] **DASH-03**: `src/utils/integrity.py` tick-health checks read the lake; cross-store drift analysis is removed.
- [ ] **GAP-01**: Databento gap-fill publishes ticks into the Parquet lake, reads symbols from `_control/registry.json`, performs its already-backfilled check against the lake (never a database), and honours maintenance/publisher fences.
- [ ] **GAP-02**: `databento` is declared in `requirements.txt` and its operational symbol scope is restricted to the 19 approved symbols.

### Phase 47: Remove the Bar Subsystem & Dead Providers

- [ ] **RMV-01**: `grep -ri "historical.duckdb\|streaming.duckdb"` returns nothing in `src/`, `tools/`, `tests/`, `main.py` or current user-facing docs.
- [ ] **RMV-02**: The historical **1-minute bar archive pipeline** is deleted — the harvesters that downloaded pre-aggregated bars into `minute_data`: `main.py` harvest CLI, `src/data/harvester.py`, `src/data/normalizer.py`, `src/api/massive.py`, `src/api/yahoo.py`, `src/api/binance.py`, `tools/backfill_massive.py`, `tools/benchmark_baseline.py`, `tools/audit_database_integrity.py`, `src/dashboard/harvester_job.py`. **Not in scope — explicitly retained:** the tick-lake gap visualisation (shaded missing-data regions, continuity ribbons, `detect_stream_quiet_intervals`) and everything that feeds it. Guarded by `tests/dashboard/test_gap_visualisation_survives.py`.
- [ ] **RMV-03**: Bar-era frontend surfaces are removed — historical navigation, data-source selector, harvester controls, stale database labels. The streaming chart, its gap shading and the continuity ribbons stay.
- [ ] **RMV-04**: `yfinance` and `polygon-api-client` are removed from `requirements.txt`. `DISCORD_WEBHOOK_URL` is **retained** in `.env.example`. **`duckdb` stays** (in-memory engine). Retained: `pyarrow`, `pandas`, `pytz`/`tzdata`, `websockets`, `requests`, `python-dotenv`, `psutil`, `pytest`.
- [ ] **RMV-06**: Reference cleanup covers code and current user-facing docs; `.planning/` archives are left intact.
- [ ] **RMV-07**: `src/config.py`'s dead bar constants are removed, and Capital credentials are retained (live auth depends on them).
- [ ] **RMV-08**: The unused replay subsystem is removed — `src/storage/replay.py`, `tests/storage/test_replay.py`, its exports in `src/storage/__init__.py`, and the `create_replay_iterator`/`create_replay_snapshot` methods on `TickLakeReader`. `PROJECT.md` places market-rewind out of scope; it was built during v4.3 against that decision and is reachable only programmatically.
- [ ] **RMV-09**: `src/utils/discord.py` is **rewritten, not deleted** — bar-era functions (`build_health_alerts`, `build_database_health_grid`, `send_discord_harvest_report`) are removed; the webhook plumbing (`_post_embed`, `_post_file`) and `get_discord_webhook_url()` are retained for streamer notifications.

### Phase 48: Schedule, Off-Hours Compaction, Notifications & Test Disposition

- [ ] **SCHED-01**: A single timezone policy module computes ingestion eligibility in `ZoneInfo("America/New_York")` over `[04:00, 20:00)`, using an injectable clock. Behaviour is identical under DST transitions and when the host is not in ET.
- [ ] **SCHED-02**: The supervisor owns lifecycle states (`WAITING_FOR_WINDOW`, `STARTING`, `INGESTING`, `DRAINING`, `MAINTENANCE`, `ERROR`). No provider authentication or subscription occurs outside the window, and an intentional off-hours stop is never treated as a crash.
- [ ] **SCHED-03**: The direct runner entry point enforces the same window, so a manual start cannot bypass the schedule.
- [ ] **SCHED-04**: At 20:00 admission stops, accepted ticks drain exactly once, and a failed drain blocks compaction and is reported honestly.
- [ ] **SCHED-05**: Compaction runs unattended once per eligible closed interval, idempotently, after confirmed drain. A no-op interval counts as success, and a single maintenance lease prevents duplicate launchers.
- [ ] **SYMB-01**: `_control/registry.json` is the single symbol authority containing exactly the 19 approved equities. Unsolicited and out-of-scope symbols are rejected at the callback boundary and cannot be added through the UI.
- [ ] **NOTIF-01**: Operational notifications reach Discord for: session started, session stopped, **session failed to start**, supervisor restart/crash, and drain or compaction failure. Notifications are best-effort — a webhook failure must never block or crash ingestion.
- [ ] **CO-01**: Tests are dispositioned retain / retarget / delete with a one-line reason each; the retained offline suite passes with no required xfails.
- [ ] **CO-02**: Candle behaviour is verified unchanged against the Phase 46 baseline — the query engine was not modified.

### Phase 49: Final Gate, Deletion & Closure *(owner's machine)*

- [ ] **MIG-01**: The legacy tick source is migrated into the Parquet lake with `plan → export → verify → verify-published → audit-lake` all completing, and row counts reconciled per symbol and per date.
- [ ] **MIG-02**: Migration is confirmed **before** any `.duckdb` file is deleted. The agent never copies, exports, archives or deletes the owner's database files — deletion is an owner action.
- [ ] **MIG-03**: Re-running the migration over an overlapping or broader scope publishes no duplicate rows (source-coverage ledger).
- [ ] **RMV-05**: The owner deletes `data/historical.duckdb` and `data/streaming.duckdb` **immediately after Phase 49 verification** — no retention or confirmation period. Permanent loss of the bar history was accepted by the owner on 2026-10-05.
- [ ] **CO-03**: The milestone closes with one completion report and **stops** — no new phases, audits, or follow-up programme.

---

## Out of Scope

| Item | Reason |
|---|---|
| Removing the DuckDB **library** / reimplementing resampling in PyArrow | Considered and rejected by owner 2026-10-05. `reader.py`, `compaction.py`, `replay.py` unmodified. |
| 24-hour endurance run, hosted CI log verification | Waived in v4.3; not revived. |
| Postgres / SQLite migration | Rejected — Parquet stays. |
| Editing `.planning/` historical archives | Falsifying past audit records is not permitted. |
| Agent deletion or archival of the owner's `.duckdb` files | Owner action only, in Phase 49. |

---

## Traceability Mapping

| Requirement | Phase | Status |
|-------------|-------|--------|
| BASE-01 | 46 | Complete |
| STOR-01 | 46 | Complete |
| STOR-02 | 46 | Pending |
| STOR-03 | 46 | Pending |
| STOR-04 | 46 | Complete |
| STOR-05 | 46 | Pending |
| DASH-01 | 46 | Pending |
| DASH-02 | 46 | Pending |
| DASH-03 | 46 | Pending |
| GAP-01 | 46 | Pending |
| GAP-02 | 46 | Pending |
| RMV-01 | 47 | Pending |
| RMV-02 | 47 | Pending |
| RMV-03 | 47 | Pending |
| RMV-04 | 47 | Pending |
| RMV-06 | 47 | Pending |
| RMV-07 | 47 | Pending |
| RMV-08 | 47 | Pending |
| RMV-09 | 47 | Pending |
| SCHED-01 | 48 | Pending |
| SCHED-02 | 48 | Pending |
| SCHED-03 | 48 | Pending |
| SCHED-04 | 48 | Pending |
| SCHED-05 | 48 | Pending |
| SYMB-01 | 48 | Pending |
| NOTIF-01 | 48 | Pending |
| CO-01 | 48 | Pending |
| CO-02 | 48 | Pending |
| MIG-01 | 49 | Pending |
| MIG-02 | 49 | Pending |
| MIG-03 | 49 | Pending |
| RMV-05 | 49 | Pending |
| CO-03 | 49 | Pending |

**Coverage:** 33 requirements, 33 mapped, 0 unmapped ✓  
**Phase order equals execution order** (renumbered 2026-10-05): code phases 46–48 first, owner-machine gate 49 last.

**Working method (owner directive, 2026-10-05):** test-driven — research, failing tests, implement, verify, re-implement and re-verify on failure.

---

*Requirements defined: 2026-10-05*
*Derived from: `PLAN-MILESTONE-5.0.md` (Option A — Parquet-only storage, DuckDB retained as query engine)*
