# Project Retrospective

*A living document updated after each milestone. Lessons feed forward into future planning.*

## Milestone: v4.1 — Partitioned Parquet Lake Deep Testing & Hardening

**Shipped:** 2026-10-03
**Phases:** 6 (Phases 22–27) | **Plans:** 6 | **Tests added:** 122 (offline suite: 688 passing at closeout)

### What Was Built
- Adversarial test coverage for the storage foundation: path traversal, unicode/special-symbol encoding, corrupted metadata, PyArrow schema coercion, float extremes, and null bitmasks (`tests/storage/test_storage_edge_cases.py`, 36 tests).
- Writer/runner stress and lifecycle coverage: 100k+ tick micro-batching, bounded-queue backpressure, abrupt shutdown mid-flush, drain timeouts, disk-full backoff, and malformed-tick quarantine (`tests/stream/test_lake_runner_stress.py`, 14 tests).
- Registry concurrency coverage: cross-process CRUD serialization, monotonic versioning, `PENDING_PURGE` fencing, 500-touch signal storms, and reload latency under polling (`tests/storage/test_registry_stress.py`, 13 tests).
- Reader/analytics edge coverage: 30+ concurrent in-memory DuckDB readers with 1,000+ sequential queries, DST/leap-year resampling, sparse partitions, high-offset tape pagination (`tests/storage/test_lake_reader_stress.py`, 17 tests).
- Migration fuzzing: corrupt/partial legacy sources, schema drift, crash interruption in every mode, and two-way `EXCEPT ALL` precision/multiplicity reconciliation (`tests/storage/test_migration_stress.py`, 32 tests).
- Multi-process soak and chaos: sustained writer + reader load, chaos-monkey termination of streamer/dashboard/supervisor, and supervised self-healing (`tests/integration/test_supervisor_chaos_soak.py`, 10 tests).

### What Worked
- The strict two-stage loop per phase (researcher spec → implementer tests) produced focused, non-overlapping test files with clear traceability from requirement IDs to test nodes.
- Freezing application code for the whole milestone meant every observed failure was either a genuine defect or a test/fixture issue — no ambiguity from concurrent refactors. None required an application fix in this milestone.
- Deterministic quote fixtures and `tmp_path`-isolated lake roots (introduced in v4.0 Phase 15) made adversarial scenarios like crash-recovery and fuzz reconciliation reproducible.
- Chaos tests around the existing supervisor proved the auto-restart/backoff contract without any changes to `tools/service_supervisor.py`.

### What Was Inefficient
- The v4.0-era `docs/plans/tick-lake-test-first-remediation.md` remained the only place that documented the test-first strategy; nothing rolled the executed phases back into the operations/contract docs until this closeout, leaving docs lagging a full milestone behind the code.
- Historical milestone entries drifted (paths such as `data/lake/...` and `symbol_registry.json`, and an integer `ingest_id` claim) because docs were written from plan intent rather than post-ship reality.
- **No verification artifacts were emitted per phase.** The researcher→implementer cycle produced excellent tests but no `VERIFICATION.md`, so the independent audit returned `gaps_found` despite 17/17 requirements having green tests. The next test-heavy milestone should require a verification artifact as a phase exit criterion, not a closeout afterthought — and the researcher/implementer reports should be checked against the delivered files (the Phase 25 report misdescribed its own test file).

### Patterns Established
- Requirement-ID → test-file → test-count traceability tables in the milestone requirements archive.
- Test-only milestones: freeze `src/` and tools, add adversarial coverage, and report any application defect as a blocking finding rather than patching it in-flight.
- Documentation truth pass at milestone closeout: every path, filename, and configuration default in user-facing docs is verified against code before shipping.

### Key Lessons
1. An append-only, atomically-published lake is verifiable by construction — fuzzing and crash injection around `_staging/` and publication intents found no data-loss path, confirming the v4.0 design.
2. Concurrency correctness (registry versioning, writer ownership, reader scaling) is best proven with real multi-process tests and signal storms, not same-process mocks.
3. Timing-sensitive performance gates (p95 latency, debounce coalescing) are environment-dependent; they must be run on a named reference machine with recorded packages/hardware, never silently re-tuned to pass.
4. Green tests are not a verified milestone: without per-phase verification artifacts, an independent audit correctly returns `gaps_found`. Test *substitutes* also need scrutiny — chaos-testing a stand-in (`synthetic_streamer.py`) proves the supervisor, not the real streamer's recovery.

---

## Milestone: v4.0 — Partitioned Parquet Tick Lake (Decoupled High-Concurrency Storage)

**Shipped:** 2026-10-03
**Phases:** 7 (Phases 15–21) | **Plans:** 7 | **Requirements:** 19/19 verified

### What Was Built
- Replacement of the single-writer `streaming.duckdb` tick store with an append-only, Hive-partitioned Parquet lake (`data/tick_lake/ticks/symbol=<SYMBOL>/date=<YYYY-MM-DD>/batch_<writer>_<seq>.parquet`).
- Lake Schema v1 with microsecond UTC timestamps, a stable string `ingest_id`, and deterministic `arg_min`/`arg_max` OHLCV resampling.
- `TickLakeWriter` with an off-loop PyArrow worker thread, bounded queue, retry/backoff, and malformed-tick quarantine.
- Atomic publication protocol: `.tmp` staging, checksum-verified atomic rename, publication receipts, and crash-intent recovery.
- Versioned JSON symbol registry with cross-process reload signalling and `PENDING_PURGE` fencing.
- Zero-loss migration CLI (`plan` / `export` / `verify` / `publish` / `all`) with chunked export and two-way `EXCEPT ALL` reconciliation.
- Production cutover and multi-process concurrency validation with the dashboard, writer, and an independent Repo B-style reader running concurrently.

### What Worked
- Decoupling ingestion from reads via immutable Parquet files eliminated the DuckDB file-lock failures that dominated v1–v3 operations.
- In-memory DuckDB per query (`:memory:`, `threads=4`, `max_memory=2GB`) kept readers lock-free and fast while preserving DuckDB's vectorized engine and `time_bucket()`.
- Atomic filesystem renames plus receipt idempotency made crash recovery deterministic and testable.

### What Was Inefficient
- Early documentation used the plan's provisional paths (`data/lake/...`) after implementation settled on `data/tick_lake` — corrected in the v4.1 closeout.
- The `ingest_id` type shifted from a provisional 64-bit integer claim to the shipped string form; only the contract table captured that correctly at first.

### Patterns Established
- Additive compatibility: legacy `streaming.duckdb` is frozen and never deleted; the lake is the sole live write target.
- Publication intents + receipts as the audit trail for every durable batch.
- Service supervision via `tools/service_supervisor.py` for both streamer and dashboard, with code-change and git auto-reload.

### Key Lessons
1. Decoupling storage from process ownership removes an entire class of operational failures in single-file embedded databases.
2. Publication protocols need first-class crash semantics (intent files, idempotent receipts) or recovery becomes guesswork.
3. Promising performance in a plan is not delivering it: gates (event-loop lag < 20ms p99, reader p95 < 100ms) need measured evidence and named reference hardware.

---

## Milestone: v3.0 — Observability Command Center, Interactive Financial Charts & Live Telemetry Dashboard

**Shipped:** 2026-09-26
**Phases:** 5 | **Plans:** 5 | **Sessions:** 1

### What Was Built
- Interactive financial charts powered by TradingView Lightweight Charts (v4.1.3) with sub-25ms dynamic rendering across 7.99M candles.
- High-performance analytical query engine in DuckDB with native `time_bucket()` resampling (`1m` to `1D`).
- Live Ticker Wall and quote tape with visual flash animations, bid/ask spreads, and streamer process telemetry.
- Real-time market session clock with countdowns to regular trading hours and 8:00 PM ET cutoff.
- Background harvester job runner with real-time log streaming in a web drawer.
- Searchable, actionable Symbol Coverage Matrix and context-aware data integrity auditor.

### What Worked
- Vanilla ES6 + Tailwind CSS CDN + TradingView Lightweight Charts CDN eliminated all build-step overhead while providing native-grade performance.
- DuckDB's in-engine `time_bucket()` execution allowed instant multi-timeframe aggregation without transferring raw bars into Python memory.
- Symbol-specific date range discovery avoided false gap alerts during historical audits.
- Dedicated tests for each feature domain enabled fast feedback loops.

### What Was Inefficient
- Re-architecting symbol maps late in the cycle (historical vs streaming) required quick tasks to resolve schema overlaps. Defining dual symbol tables earlier would have avoided migrations.

### Patterns Established
- Separation of concerns between static historical analytical storage and live tick streaming buffers.
- In-process adaptive configuration matching for concurrent DuckDB connections.
- NYSE Eastern time normalization for financial UI display.

### Key Lessons
1. Native database aggregation outperforms application-layer resampling by orders of magnitude for large datasets (>1M rows).
2. Clean separation of analytical queries and live write loops prevents lock contention completely in embedded databases.

### Cost Observations
- Model mix: Claude Opus / Gemini Flash
- Sessions: 1
- Notable: Comprehensive automated testing (182 tests) enabled zero-regression iteration.

---

## Milestone: v5.0 — DuckDB-Free Tick-Only Parquet

**Shipped:** 2026-10-06
**Phases:** 4 (Phases 46–49) | **Plans:** 4 | **Requirements:** 33/33

### What Was Built
- Total removal of disk-backed DuckDB database files (`data/streaming.duckdb`, `data/historical.duckdb`, `data/market_data.duckdb`).
- Reconciled and published 105,894,626 legacy ticks across 7,355 partitions into 7,367 Parquet files with zero discrepancies via two-way `EXCEPT ALL` mathematical verification.
- Stateless in-memory DuckDB analytical engine querying partitioned Parquet files directly with zero disk lock contention.
- Complete purge of the 1-minute historical bar pipeline, harvesters, dead REST providers, and dead dependencies (`yfinance`, `polygon-api-client`), reducing codebase size by over 10,400 LOC.
- Single-view Parquet dashboard with live tape, TradingView candlestick charts, and integrity health checks reading purely from Parquet.
- Weekdays-only 04:00–20:00 ET ingestion schedule with supervisor lifecycle states, close-time queue drain, and unattended off-hours compaction under single-lease fencing.
- Strict 19-symbol authority enforced by `_control/registry.json`.

### What Worked
- **Test-Driven Refactoring:** Establishing golden reference baselines (`tests/storage/test_candle_baseline.py`) and strict negative guardrails (`tests/stream/test_no_disk_db_backend.py`, `tests/test_disk_database_layer_removed.py`, `tests/test_bar_era_removal.py`) ensured zero regressions while deleting thousands of lines of legacy code.
- **Two-Way Mathematical Reconciliation:** Bidirectional `EXCEPT ALL` partition-level verification guaranteed that the 105.8M ticks in Parquet had 100% duplicate multiplicity and floating-point precision parity with the legacy store before database deletion.
- **In-Memory DuckDB on Parquet:** Ephemeral DuckDB connections query the Parquet lake with sub-100ms multi-day latencies, completely eliminating OS-level file lock contention between writers and readers.

### What Was Inefficient
- **Algorithmic Redundancy in Migration Hashing:** `self._source_fingerprint()` was initially invoked inside the 7,355-partition publish loop rather than once per run, causing redundant disk hashing of the 15.5GB database file until terminated and bypassed. Pre-calculating global invariant fingerprints outside loop scopes is essential for high-partition migrations.

### Patterns Established
- Pure Parquet tick lake with in-memory DuckDB query engine as the sole data architecture.
- Single JSON registry (`_control/registry.json`) as the sole cross-process symbol authority.
- Hardened macOS launchd/supervisor lifecycle states (session window enforcement, close-time drain, off-hours compaction).

### Key Lessons
1. When performing large-scale data migrations, compute source-level checksums once and pass the memoized hash to partition publishers.
2. In-memory query engines over partitioned columnar storage provide the query power of SQL without the write-lock bottlenecks of embedded databases.
3. Maximum declutter (removing dead bar pipelines, unused dependencies, dormant subsystems) drastically simplifies operational maintenance and reduces cognitive overhead.

---

## Cross-Milestone Trends

### Process Evolution

| Milestone | Sessions | Phases | Key Change |
|-----------|----------|--------|------------|
| v1.0 | 1 | 4 | Migration from Turso SQLite to local DuckDB and 24/7 WebSockets |
| v2.0 | 1 | 5 | Decoupled dual DuckDB files (`historical.duckdb` & `streaming.duckdb`) and web dashboard |
| v3.0 | 1 | 5 | Observability Command Center with TradingView charts, live telemetry, and automated job drawer |
| v4.0 | 1 | 7 | Partitioned Parquet tick lake, atomic publication, in-memory DuckDB readers, zero-loss migration |
| v4.1 | 1 | 6 | Tests-only hardening: edge cases, stress, fuzzing, chaos, multi-process soak |
| v4.2 | 1 | 9 | Tick lake qualification, CI evidence, and scoped release signoff |
| v4.3 | 1 | 7 | Hardening, leak-free reader contracts, offline compaction, and benchmark qualification |
| v5.0 | 1 | 4 | Complete removal of disk DuckDB files; 105.8M tick Parquet migration; bar era purge; terminal milestone |

### Cumulative Quality

| Milestone | Tests | Pass Rate | Total Records |
|-----------|-------|-----------|---------------|
| v1.0 | 127 | 100% | 3.9M bars |
| v2.0 | 163 | 100% | 7.99M bars |
| v3.0 | 182 | 100% | 7.99M bars + live tick stream |
| v4.0 | 566 | 100% | ~8.9M bars + multi-million migrated ticks in the Parquet lake |
| v4.1 | 688 | 100% at closeout | ~8.9M bars + continuing Parquet lake capture |
| v4.2 | 823 | 100% offline | Lake qualification and migration rehearsal |
| v4.3 | 1,045 | 100% offline | Durable maintenance journal, replay iterator, benchmark harness |
| v5.0 | 988 | 100% offline | 105,894,626 verified lake ticks across 7,367 Parquet files (0 .duckdb files) |

### Top Lessons (Verified Across Milestones)

1. Zero-dependency single-file frontend delivery (vanilla JS + CDN libraries) provides high velocity and zero build drift for local developer tools.
2. Embedded-database concurrency is a process-ownership problem, not a retry problem: decoupling writers from readers via immutable, atomically published files is the durable fix.
3. Verify documentation against shipped code at every closeout — paths, filenames, env knobs, and data types drift otherwise.
4. Eliminate clutter aggressively: preserving unused legacy storage formats or dead endpoints creates maintenance drag and confusion.

