# Milestone 5.0 — Parquet-only storage (DuckDB retained as query engine)

**Date:** 2026-10-05
**Status:** Plan for execution.
**Scope:** One bounded removal/refactor. Four phases (46–49). No sub-programmes, no audits of the work itself.

---

## 1. What the owner asked for

> Remove DuckDB as **storage**. No `.duckdb` files exist. All persisted data is Parquet. The app may still use DuckDB as an **in-memory query engine** over those Parquet files.

This is deliberately the smaller of two options. The alternative — removing the DuckDB library entirely and reimplementing resampling in PyArrow — was considered and rejected by the owner: it would rewrite `reader.py`, rebuild `time_bucket`/`arg_min`/`arg_max` grouping (concentrated DST risk), replace `EXCEPT ALL` reconciliation, and invalidate roughly 30 of 93 test files.

**Option A keeps all of that.** `src/storage/reader.py`, `compaction.py` and `replay.py` are **not modified** by this milestone.

## 2. End state

- No `data/historical.duckdb`, no `data/streaming.duckdb`. No code can open or create a disk-backed DuckDB database.
- All persisted market data is Parquet ticks in the lake.
- DuckDB remains in `requirements.txt` and continues to serve as the ephemeral in-memory engine that reads Parquet and computes candles.
- Ticks only — no 1-minute bars anywhere.
- 19 approved equities: AAPL, ADBE, AMD, AMZN, APP, AVGO, BABA, GOOGL, META, MSFT, MU, NDAQ, NVDA, ORCL, PANW, QCOM, SHOP, TSLA, TSM.
- Live source Capital.com; gap repair Databento (writing to the lake).
- Ingestion 04:00–20:00 ET; compaction unattended in the closed window.

## 3. Definition of done

1. `data/historical.duckdb` and `data/streaming.duckdb` deleted by the owner; no code path can recreate or open one.
2. No reference to either file, or to the bar subsystem, remains in `src/`, `tools/`, `tests/`, `main.py` or the docs.
3. Candles still compute correctly (behaviour unchanged — the engine was never touched).
4. All 19 symbols collected and readable from the lake.
5. Streamer runs 04:00–20:00 ET only; compaction runs unattended outside it.
6. Tests are dispositioned retain / retarget / delete with a reason each; the retained suite passes.
7. **Then stop.**

## 4. What stays / what goes

**Stays (untouched):** `src/storage/reader.py` · `compaction.py` · `replay.py` · `publication.py` · `parquet_writer.py` · `registry.py` · `capacity.py` · the DuckDB in-memory query engine.

**Deleted:** `data/historical.duckdb` + `data/streaming.duckdb` (owner action) · `src/database/{connection,schema,operations}.py` (disk-backed DB layer) · `main.py` harvest CLI · `src/data/harvester.py`, `normalizer.py` · `src/api/massive.py`, `yahoo.py`, `binance.py` · `tools/backfill_massive.py`, `benchmark_baseline.py`, `audit_database_integrity.py` · `src/utils/discord.py` · `src/dashboard/harvester_job.py` · `/api/historical/*` and their frontend callers · `yfinance`, `polygon-api-client` dependencies.

**Rewired:** `src/stream/runner.py` (drop the DuckDB writer fallback; symbols from the registry) · `src/dashboard/{server,analytics}.py` (lake-only) · `src/utils/integrity.py` (tick checks against the lake; drop cross-store drift) · `src/data/databento_backfill.py` (write to the lake).

**One helper must survive the `src/database` deletion:** `src/utils/integrity.py` uses `get_duckdb_connection` for in-memory work. It moves to the storage layer rather than being deleted.

## 5. Hard sequencing constraint

The migration tool uses DuckDB to **read** the legacy `streaming.duckdb`. Therefore:

1. **First** — migrate and verify (Phase 46), while the legacy file still exists.
2. **Only then** — delete the files (owner action).

Deleting before verifying destroys anything unmigrated. **The agent never copies, exports, archives or deletes the owner's `.duckdb` files** — that is an owner action.

## 6. Phases

### Phase 46 — Baseline & Legacy Tick Migration Verification *(runs on the owner's machine)*
Confirm the lake's current candle output as the reference. Run `plan → export → verify → verify-published → audit-lake` against the real `streaming.duckdb`; reconcile row counts per symbol and date. Owner confirms, then deletes both `.duckdb` files.

**Requires:** access to the real data — this checkout has no `data/` directory.

### Phase 47 — Rewire Off the Disk Databases
`runner.py` becomes lake-only (the DuckDB writer fallback is removed, not merely disabled). Symbol inventory comes from `_control/registry.json`. Dashboard and analytics drop historical routes/functions and serve the lake. `integrity.py` tick checks read the lake; the in-memory helper is relocated. `databento_backfill.py` publishes to the lake and reads the registry, honouring maintenance fences.

**Exit:** no runtime import of a disk-backed DB factory; the dashboard works with both `.duckdb` files absent.

### Phase 48 — Remove the Bar Subsystem & Dead Providers
Delete the harvest CLI, bar pipeline, provider clients, Discord, the harvester dashboard job, the dead tools, and the historical endpoints. Remove `yfinance` and `polygon-api-client`. Clean imports, `.env.example`, README and operations guide.

**Exit:** `grep -ri "historical.duckdb\|streaming.duckdb"` returns nothing in code or current docs; the app runs with no bar code present.

### Phase 49 — Schedule, Off-Hours Compaction & Closure
Timezone policy module (`ZoneInfo("America/New_York")`, `[04:00, 20:00)`, injectable clock), supervisor lifecycle states, direct-runner guard so a manual start cannot bypass the window, admission stop at 20:00 with a single drain, and unattended idempotent compaction under one maintenance lease. Registry restricted to the 19 symbols with out-of-scope rejection. Test disposition, one completion report, stop.

## 7. Risks

| # | Risk | Mitigation |
|---|---|---|
| R1 | Deleting the legacy file before the migration is verified | Phase 46 is a hard gate; owner deletes, not the agent |
| R2 | `src/database` deletion breaks an unnoticed import | Map every importer first; a startup regression proves normal paths need no disk DB |
| R3 | `integrity.py` and `databento_backfill.py` are easy to miss — both read the legacy DB | Both are explicit Phase 47 items, not afterthoughts |
| R4 | Tests that construct disk DuckDBs | Disposition each: retarget to the lake, or delete with the feature |
| R5 | Frontend changes are unverifiable here (8 tests skip without Node) | Keep frontend changes minimal; flag anything unverified |
| R6 | Scope creep back toward full DuckDB removal | The owner rejected it; engine files are explicitly out of scope |

## 8. Effort

Roughly **3–5 focused days** of code work, plus one sitting on the owner's machine for Phase 46. Phase 48 is largely mechanical; Phase 47 carries the real risk; Phase 49 is new code but small.

---

**Success is a smaller, simpler system that passes its retained tests and then stops changing.**
