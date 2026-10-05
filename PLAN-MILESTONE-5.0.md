# Milestone 5.0 — Parquet-only storage (DuckDB retained as query engine)

**Date:** 2026-10-05
**Status:** Plan for execution.
**Scope:** One bounded removal/refactor. Four phases (46–49). No sub-programmes, no audits of the work itself.

---

## 1. What the owner asked for

> Remove DuckDB as **storage**. No `.duckdb` files exist. All persisted data is Parquet. The app may still use DuckDB as an **in-memory query engine** over those Parquet files.

This is deliberately the smaller of two options. The alternative — removing the DuckDB library entirely and reimplementing resampling in PyArrow — was considered and rejected by the owner: it would rewrite `reader.py`, rebuild `time_bucket`/`arg_min`/`arg_max` grouping (concentrated DST risk), replace `EXCEPT ALL` reconciliation, and invalidate roughly 30 of 93 test files.

**Option A keeps all of that.** `src/storage/reader.py`, `compaction.py` and `replay.py` are **not modified** by this milestone.

## 1a. Governing principle: maximum declutter

The owner's instruction (2026-10-05): **reduce clutter in this repo as much as possible.** Where a choice exists between keeping something "just in case" and removing it, remove it. The dashboard becomes a single Parquet-only view; the ~2 years of bar history is permanently dropped rather than retained; unused subsystems are deleted rather than left dormant. Nothing is kept for potential future use.

## 2. End state

- No `data/historical.duckdb`, no `data/streaming.duckdb`. No code can open or create a disk-backed DuckDB database.
- All persisted market data is Parquet ticks in the lake.
- DuckDB remains in `requirements.txt` and continues to serve as the ephemeral in-memory engine that reads Parquet and computes candles.
- Ticks only — no 1-minute bars anywhere.
- 19 approved equities: AAPL, ADBE, AMD, AMZN, APP, AVGO, BABA, GOOGL, META, MSFT, MU, NDAQ, NVDA, ORCL, PANW, QCOM, SHOP, TSLA, TSM.
- Live source Capital.com; gap repair Databento (writing to the lake).
- Ingestion 04:00–20:00 ET, **weekdays only**; compaction unattended in the closed window.
- **Platform: macOS.** Only the launchd-based scheduler is built; Windows scripts are removed or marked unsupported.

## 3. Definition of done

1. `data/historical.duckdb` and `data/streaming.duckdb` deleted by the owner immediately after the Phase 49 final gate; no code path can recreate or open one.
2. No reference to either file, or to the bar-archive pipeline, remains in `src/`, `tools/`, `tests/`, `main.py` or the docs.
3. Candles still compute correctly (behaviour unchanged — the engine was never touched).
4. All 19 symbols collected and readable from the lake.
5. Streamer runs 04:00–20:00 ET only; compaction runs unattended outside it.
6. Tests are dispositioned retain / retarget / delete with a reason each; the retained suite passes.
7. **Then stop.**

## 4. What stays / what goes

**Stays (untouched):** `src/storage/reader.py` · `compaction.py` · `replay.py` · `publication.py` · `parquet_writer.py` · `registry.py` · `capacity.py` · the DuckDB in-memory query engine.

**Deleted:** `data/historical.duckdb` + `data/streaming.duckdb` (owner action) · `src/database/{connection,schema,operations}.py` (disk-backed DB layer) · `src/storage/replay.py` + `tests/storage/test_replay.py` + replay exports/methods (unused; owner did not request it) · `main.py` harvest CLI · `src/data/harvester.py`, `normalizer.py` · `src/api/massive.py`, `yahoo.py`, `binance.py` · `tools/backfill_massive.py`, `benchmark_baseline.py`, `audit_database_integrity.py` · `src/dashboard/harvester_job.py` · `/api/historical/*` and their frontend callers · `yfinance`, `polygon-api-client` dependencies.

**Rewritten:** `src/utils/discord.py` — bar-era alert builders removed; webhook plumbing kept and redirected to operational events (session start/stop/failure, crash, maintenance failure).

**Rewired:** `src/stream/runner.py` (drop the DuckDB writer fallback; symbols from the registry) · `src/dashboard/{server,analytics}.py` (lake-only) · `src/utils/integrity.py` (tick checks against the lake; drop cross-store drift) · `src/data/databento_backfill.py` (write to the lake).

**One helper must survive the `src/database` deletion:** `src/utils/integrity.py` uses `get_duckdb_connection` for in-memory work. It moves to the storage layer rather than being deleted.

## 5. Hard sequencing constraint

The legacy tick store is read only by the migration tool, which the owner runs. Therefore:

1. **Code work first** — Phases 46–48 need no `data/` directory and no owner machine.
2. **Verification and deletion last** — Phase 49 runs on the owner's machine while the legacy file still exists. On a clean verify the owner deletes the files; **no retention or confirmation period**.
3. **The agent never copies, exports, archives or deletes the owner's database files** — that is an owner action.

Nothing in Phases 46–48 depends on the migration having run.

## 6. Phases

**Execution order equals phase order.** Renumbered 2026-10-05: the former Phase 46 (migration gate) is now **Phase 49**; the former Phase 47 (rewiring) is now **Phase 46**. Code phases come first because they are the only ones this checkout can execute.

**Working method (owner directive, 2026-10-05): test-driven.** For every phase: research → write failing tests → implement → verify → re-implement and re-verify on failure. A phase is not complete until its tests pass.

### Phase 46 — Rewire Off the Disk Databases
Record the lake's current candle output as the reference baseline (BASE-01) **before** touching code — characterization tests first. Then: `runner.py` becomes lake-only (the DuckDB writer fallback is removed, not merely disabled). Symbol inventory comes from `_control/registry.json`. Dashboard and analytics drop historical routes/functions and serve the lake as a single Parquet-only view. `integrity.py` tick checks read the lake; the in-memory helper is relocated out of `src/database`. `databento_backfill.py` publishes to the lake, reads the registry, and honours maintenance fences.

**Exit:** no runtime import of a disk-backed DB factory; a startup regression proves it; the dashboard works with both `.duckdb` files absent.

### Phase 47 — Remove the Historical Bar Archive & Dead Providers

Removes the 1-minute bar *archive pipeline* (harvesters → `minute_data`), not the chart's missing-data visualisation: gap shading, continuity ribbons and `detect_stream_quiet_intervals` are retained and guarded by `tests/dashboard/test_gap_visualisation_survives.py`.
Delete the harvest CLI, bar pipeline, provider clients, the harvester dashboard job, the dead tools and the historical endpoints — and the unused replay subsystem. `src/utils/discord.py` is **rewritten, not deleted**: bar-era alert builders go, webhook plumbing stays for streamer notifications. Remove `yfinance` and `polygon-api-client`; keep `duckdb`. Clean imports, `.env.example`, README and operations guide.

**Exit:** `grep -ri "historical.duckdb\|streaming.duckdb"` returns nothing in code or current docs; the app runs with no bar code present; the replay subsystem is gone.

### Phase 48 — Schedule, Off-Hours Compaction, Notifications & Test Disposition
Timezone policy module (`ZoneInfo("America/New_York")`, `[04:00, 20:00)`, weekdays, injectable clock), supervisor lifecycle states, direct-runner guard so a manual start cannot bypass the window, admission stop at 20:00 with a single drain, and unattended idempotent compaction under one maintenance lease. Discord notifications for session start/stop, **failed start**, restart and maintenance failure. Registry restricted to the 19 symbols with out-of-scope rejection. Every surviving test dispositioned retain/retarget/delete; candle behaviour re-verified against the Phase 46 baseline.

### Phase 49 — Final Gate, Deletion & Closure *(runs on the owner's machine)*
Run `plan → export → verify → verify-published → audit-lake` against the real legacy tick store; reconcile row counts per symbol and date; confirm that re-running publishes no duplicates. On a clean verify, the owner deletes both `.duckdb` files. One completion report, then **stop**.

**Requires:** access to the real data — this checkout has no `data/` directory.

## 7. Risks

| # | Risk | Mitigation |
|---|---|---|
| R1 | Deleting the legacy file before the migration is verified | Phase 49 is a hard gate; owner deletes, not the agent |
| R2 | `src/database` deletion breaks an unnoticed import | Map every importer first; a startup regression proves normal paths need no disk DB |
| R3 | `integrity.py` and `databento_backfill.py` are easy to miss — both read the legacy DB | Both are explicit Phase 46 items, not afterthoughts |
| R4 | Tests that construct disk DuckDBs | Disposition each: retarget to the lake, or delete with the feature |
| R5 | Frontend changes are unverifiable here (8 tests skip without Node) | Keep frontend changes minimal; flag anything unverified |
| R6 | Scope creep back toward full DuckDB removal | The owner rejected it; engine files are explicitly out of scope |
| R7 | Removing replay breaks `reader.py` | Only two thin wrapper methods reference it; both are deleted with the module, and `reader.py` is otherwise untouched |

## 8. Effort

Roughly **3–5 focused days** of code work across Phases 46–48, plus one sitting on the owner's machine for Phase 49. Phase 47 is largely mechanical; Phase 46 carries the real risk; Phase 48 is new code but small.

---

**Success is a smaller, simpler system that passes its retained tests and then stops changing.**
