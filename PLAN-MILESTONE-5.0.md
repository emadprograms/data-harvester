# Milestone 5.0 — DuckDB-free, tick-only Parquet system

**Date:** 2026-10-05
**Requested by:** repo owner
**Status:** Plan for execution. Not yet started.
**Scope:** One bounded refactor that ends. No sub-phases, no audit-of-the-audit, no follow-up programme.

> **Naming note.** This is called "5.0" as requested. Functionally it is a single refactor, not a milestone programme. It has one finish line and stops there. Nothing in this plan authorises a subsequent verification milestone.

---

## 1. End state

The application records and serves **ticks only**, in **Parquet only**, with **no DuckDB dependency at all** — not as a database, not as a query engine, not as a library.

- Live source: **Capital.com** (19 equity symbols, 04:00–20:00 ET).
- Gap repair: **Databento**, writing into the same Parquet lake.
- Storage: append-only Parquet tick lake, atomically published.
- Reading: **PyArrow** (plus pandas where convenient), in-process, no SQL engine.
- Candles: computed from ticks at read time. Never stored.
- Maintenance: compaction in the closed window; capacity monitoring.

## 2. Definition of done

1. `grep -r "duckdb"` returns nothing in `src/`, `tools/`, `main.py`, `tests/`, `requirements.txt`.
2. `data/historical.duckdb` and `data/streaming.duckdb` deleted by the owner; nothing can open them.
3. All 19 symbols' ticks written to and readable from the lake.
4. Candle output matches the independent oracle on the full edge-case matrix.
5. Streamer runs 04:00–20:00 ET only; compaction runs unattended outside it.
6. Retained tests pass; deleted tests justified individually.
7. **Then stop.** No new phases.

## 3. What stays / what goes

**Stays:** Parquet lake format and Hive layout · atomic publication + receipts + recovery · `TickLakeWriter` · symbol registry (`_control/registry.json`) · compaction concept (journal, drain, lineage) · capacity monitor · supervisor · dashboard · schedule policy · 19-symbol inventory · Capital live auth · Databento gap-fill (rewired).

**Goes:** DuckDB (library and all usage) · `historical.duckdb` and every bar path · `streaming.duckdb` · `main.py` harvest CLI · `src/data/harvester.py`, `normalizer.py` · `src/api/massive.py`, `yahoo.py`, `binance.py` · `tools/backfill_massive.py`, `benchmark_baseline.py`, `audit_database_integrity.py` · `src/utils/discord.py` · `src/dashboard/harvester_job.py` · `yfinance`, `polygon-api-client` dependencies · historical chart endpoints and their frontend callers.

**Replaced (the actual work):** every DuckDB SQL construct with PyArrow equivalents:

| DuckDB construct | Used by | Replacement |
|---|---|---|
| `read_parquet` + partition pruning | reader, replay, compaction | `pyarrow.parquet` + path resolution (already proven in `tests/support/lake_assertions.py`) |
| `time_bucket()` DST-aware bucketing | reader, analytics | Arrow `floor_temporal` on ET-converted timestamps |
| `arg_min(price,(timestamp,ingest_id))` / `arg_max` | reader | sort by `(timestamp, ingest_id)` then group-first/group-last |
| `EXCEPT ALL` bidirectional reconciliation | compaction, migration verify | multiset comparison via `Counter` (already proven in the oracle) |
| In-memory `:memory:` connections | reader, integrity, tests | direct Arrow tables |

## 4. Critical sequencing constraint — read this first

**The migration must complete and verify BEFORE DuckDB is removed.**

`tools/migrate_streaming_to_parquet.py` uses DuckDB to *read* the legacy `.duckdb` source file. Once the dependency is gone, that tool cannot run and `streaming.duckdb` becomes unreadable by this application.

Therefore:

1. **First** — run the existing migration against the real legacy file and verify it (`--mode verify-published`, `--mode audit-lake`), while DuckDB still exists.
2. **Only then** — proceed to remove DuckDB.

If the legacy file is deleted without this step, anything unmigrated is gone permanently. This step is not optional and must not be reordered.

## 5. Workstreams

### W0 — Baseline capture (do first, small)
Record current oracle-verified behaviour and latency on a fixed synthetic dataset: candle outputs for the full edge-case matrix, and query/tape latencies. This is the reference the new implementation must match. Recorded once; **not** a re-qualification campaign.

### W1 — Complete the legacy tick migration (DuckDB still present)
Run `plan → export → verify → verify-published → audit-lake` against the real `streaming.duckdb`. Reconcile row counts per symbol/date. Fix any gap found. Owner confirms; only then may the file be deleted. (Owner deletes the file, not the agent.)

### W2 — PyArrow reader core
Rewrite `src/storage/reader.py` internals off DuckDB, keeping its public method names and return shapes so callers do not change:
- `resolve_partition_files` — already path-based, keep
- `query_ticks` / `get_tape` / `get_latest_tick` — Arrow filter + slice
- `get_stream_status`, `read_gaps`, `discover_available_weeks` — Arrow/pandas aggregates
- structured lake errors (`LakeUnavailableError`, etc.) preserved exactly
- `validate_lake` unchanged

Constraint: process per-symbol and per-day partitions rather than materialising the whole lake, to bound memory.

### W3 — Candle resampling in Arrow (highest-risk item)
Port `query_candles` / `get_candles`:
- group by ET-aligned time bucket
- open/close via sort `(timestamp, ingest_id)` then first/last (preserves the current deterministic tie-break)
- high/low via max/min
- null and zero volume semantics unchanged
- DST transitions (March/November) and leap years
- half-open intervals and timezone-aware inputs preserved

**Spec:** the existing candle oracle. Port must pass the same matrix. This is the single item most likely to hide a defect — treat the oracle as the contract, not as a formality.

### W4 — Multiset verification off DuckDB
Replace `EXCEPT ALL` in `src/storage/compaction.py` and the migration tool with the Counter-based comparison already implemented in `tests/support/lake_assertions.py`. Must preserve multiplicity (duplicates matter), float precision, and null handling. Chunk comparisons per partition to bound memory.

### W5 — Dashboard and analytics rewiring
- Remove `/api/historical/*`, harvester endpoints, dual-source dispatch, historical overview, cross-store drift.
- Retained routes (`/api/stream/*`, symbols, coverage, continuity, integrity) call the PyArrow reader.
- Tick health checks preserved; bar-era checks removed.
- Frontend: remove historical nav, source selector, harvester controls, stale labels; chart reads the lake only and shows an honest empty state for dates predating tick capture.

### W6 — Repo B contract v2.0
Rewrite `docs/contracts/repo_b_tick_lake_contract.md` for **pyarrow-only**: no DuckDB in any example. Re-execute the published examples in an isolated subprocess with zero `src` imports. Remove DuckDB from the stated dependencies.

**Gate:** if Repo B currently consumes these files *through DuckDB SQL*, this is a breaking change for that repo and must be coordinated before W6 lands. Confirm with the owner.

### W7 — Removal
Delete the bar subsystem, dead providers, Discord, harvester CLI, and the tools listed in §3. Remove `duckdb`, `yfinance`, `polygon-api-client` from `requirements.txt`. Clean imports, `.env.example` keys, README, operations guide. Keep `pyarrow`, `pandas`, `pytz`/`tzdata`, `websockets`, `requests`, `python-dotenv`, `psutil`, `pytest`.

Repository-wide reference cleanup = **code + current user-facing docs only**. Leave `.planning/` archives untouched (editing past audit records falsifies the trail).

### W8 — Schedule and unattended maintenance
- One timezone policy module using `ZoneInfo("America/New_York")`, injectable clock, `[04:00, 20:00)` ET.
- Day policy: **Mon–Fri**, no exchange calendar (holidays simply produce no ticks). Revisit only if a real need appears.
- Supervisor owns lifecycle: `WAITING_FOR_WINDOW → STARTING → INGESTING → DRAINING → MAINTENANCE → ERROR`.
- Same guard inside the direct runner entry point, so a manual start cannot bypass the window.
- 20:00: stop admission, unsubscribe, drain accepted ticks exactly once. Failed drain blocks compaction and reports honestly. No restart loop that mistakes an intentional stop for a crash.
- Compaction runs once per eligible closed interval, idempotent, after confirmed drain; no-op is success. Single maintenance lease prevents duplicate launchers.

### W9 — Databento gap-fill rewiring
Currently writes to `streaming.duckdb` and reads symbol lists from the legacy databases. Required:
- publish ticks into the Parquet lake via the existing publisher
- replace the "already backfilled?" DB query with a lake-based check (coverage ledger or receipts)
- read symbols from `_control/registry.json`
- honour the maintenance/publisher fences so a manual backfill cannot race the compactor
- pin `databento` in `requirements.txt`

### W10 — Tests, verification, closure
- Retarget shared tests to the lake; delete tests whose sole subject is removed functionality. Record a retain / retarget / delete disposition with a one-line reason each.
- Keep: publication, registry, migration/provenance, compaction/lineage, replay, durability, symbol codec, process isolation, and the oracles.
- Keep the oracle independent: it must never import the application reader.
- One performance measurement on the reference workload, recorded once and compared against W0. Report the delta honestly; no re-tuning to hit a target.
- Update `README.md` and the operations runbook; produce one completion report; stop.

## 6. Risks

| # | Risk | Mitigation |
|---|---|---|
| R1 | **Losing the query engine's correctness edge cases** (DST, ties, nulls, duplicates) | The oracle already encodes these; port against it and mutation-test it once |
| R2 | **Performance regression** — candles now computed in Python/Arrow instead of DuckDB's vectorised engine | Measure in W0 and after W3; process per-partition; if the delta is unacceptable, report it rather than hiding it |
| R3 | **Memory** — sorting large partitions without DuckDB's spilling | Bound work per symbol/day; chunk multiset comparisons |
| R4 | **Repo B breaking change** if it consumes via DuckDB SQL | Confirm before W6; coordinate the switch |
| R5 | **Migration ordering** — removing DuckDB before migrating | §4 is a hard constraint, enforced as a gate |
| R6 | **Hand-rolled engine drift** — custom resampling code accumulates its own bugs over years | Keep the oracle as a permanent regression test; keep interface signatures identical so a future engine swap stays cheap |
| R7 | **Scope creep** — this refactor spawning audits | §2.7 stopping rule; no phases |

## 7. Sequence

```
W0 baseline
  └─ W1 migrate + verify  ← DuckDB still present, HARD GATE
       └─ W2 reader core
            ├─ W3 candles
            └─ W4 multiset verification
                 └─ W5 dashboard/analytics
                      ├─ W6 Repo B contract
                      └─ W9 Databento gap-fill
                           └─ W7 remove DuckDB + bars + dead providers
                                └─ W8 schedule + compaction
                                     └─ W10 tests, verify, close
```

W5 and W9 may proceed in parallel once W2–W4 land. W7 may only start after W1 is confirmed.

## 8. Gates requiring an owner decision

| Gate | Question | Recommended |
|---|---|---|
| G1 | Does Repo B read these files through DuckDB today? | Confirm before W6; if yes, coordinate the breaking change |
| G2 | Accept that candle computation moves out of DuckDB, with a measured latency change reported honestly? | Yes; report the delta, do not silently re-tune targets |
| G3 | Accept that legacy `.duckdb` files become unreadable by this application after W7? | Yes, given W1 passed; a dev-only copy of DuckDB can read them offline if ever needed |
| G4 | Confirm deletion of `historical.duckdb` **and** `streaming.duckdb` by the owner after W1? | Yes |

## 9. Effort

Roughly **2–3 focused weeks**, dominated by W3 (candles) and W5 (dashboard/frontend). W1 is hours on the owner's machine. W7 is largely mechanical once W2–W6 land.

This is the largest change proposed so far — larger than the bar removal — because the resampling and verification engines are being rebuilt. It is a deliberate trade: one fewer dependency and one mental model, in exchange for owning the query logic.

## 10. What this is not

Not a new programme · not an opportunity to re-qualify old milestones · not a reason to revisit the Parquet decision · not a licence to add replay features, durable spools, or benchmarks · not permission to touch `.planning/` archives.

**Success is a smaller, simpler system that passes its retained tests and then stops changing.**
