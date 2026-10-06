# Data Harvester — Local DuckDB & Live Streaming Engine

## What This Is
A high-performance, 100% local market data harvesting and streaming engine running on macOS (Mac Mini). It captures live market quotes from Capital.com via persistent WebSockets, storing real-time tick-by-tick quotes exclusively in an append-only partitioned Parquet tick lake (`data/tick_lake/ticks`), with sub-millisecond in-memory DuckDB dynamic OHLCV candlestick resampling, deep data integrity validation, unattended off-hours compaction, and an interactive local web dashboard (`http://localhost:8420`). All disk-backed `.duckdb` files have been permanently eliminated.

## Core Value
Zero-cloud, zero-quota persistent market data ingestion and storage: capture real-time market data reliably and provide sub-millisecond OHLCV querying without hitting API limits or heating up hardware.

## Current Milestone: v6.0 Bid and Ask Prices (checkout complete)

**Goal:** Store only the bid price and the ask price, rewrite the existing lake to that shape, and fill computer-offline holes from Databento without a midpoint or a volume column.

**Target features:**
- New writes store `bid_price` and `ask_price` only. Capital.com maps `bid` and `ofr`. Databento maps `bid_px_00` and `ask_px_00`. No midpoint, no `price`, no `volume`, no sizes.
- The inspection chart draws candles from `bid_price`. The volume histogram is removed.
- The existing lake is rewritten in place: stored `bid` becomes `bid_price`, stored `ask` becomes `ask_price`. The old midpoint and trade price are not copied. Missing bid or ask is quarantined, not invented.
- Gap fill takes one day. A hole counts only when every registry symbol is silent together: 15 minutes in the pre-market and post-market, 2 minutes in regular hours, inside 04:00–20:00 ET. Weekends, full holidays, and time after an early close are skipped.

The v5.0 stop rule is lifted by the owner on 2026-10-06. Research for this milestone was skipped: the column set, the rewrite rule, and the gap rule were locked in conversation.

## Shipped Milestone (v5.0) — Complete

**v5.0: DuckDB-Free Tick-Only Parquet** — Phases 46–49 (shipped 2026-10-06)

**Goal:** Remove DuckDB as **storage**. No `.duckdb` files exist; every persisted market datum is a Parquet tick. DuckDB remains exclusively as the in-memory query engine reading those files.

**Delivered:**
- `streaming.duckdb`, `historical.duckdb`, and `market_data.duckdb` deleted; zero runtime code paths open or create a disk DuckDB file.
- 105,894,626 legacy tick rows across 7,355 partitions migrated and mathematically reconciled (`EXCEPT ALL`) with zero discrepancies into 7,367 Parquet files in `data/tick_lake/ticks/`.
- Ticks only — the 1-minute historical bar pipeline and harvesters removed entirely (net −10,415 LOC dead code).
- Capital.com is the sole live source; Databento is the gap-repair source writing into the lake.
- Symbols strictly restricted to 19 approved equities via `_control/registry.json`.
- Ingestion restricted to 04:00–20:00 ET weekdays; unattended compaction in off-hours under single lease.
- Single Parquet-only dashboard view (`http://localhost:8420`).
- Terminal milestone reached; development closed per owner directive.

**Artifacts:** [.planning/milestones/v5.0-REQUIREMENTS.md](milestones/v5.0-REQUIREMENTS.md) (33 requirements verified) · [.planning/milestones/v5.0-ROADMAP.md](milestones/v5.0-ROADMAP.md) (Phases 46–49) · [.planning/milestones/v5.0-MILESTONE-AUDIT.md](milestones/v5.0-MILESTONE-AUDIT.md) · [reports/v5.0_completion_report.md](../reports/v5.0_completion_report.md)

## Current State (Shipped v5.0)
- **Partitioned Parquet Tick Lake (`data/tick_lake/ticks`)**: Fully decoupled append-only storage organized by Hive two-level partitioning (`symbol=<ENCODED_SYMBOL>/date=<YYYY-MM-DD>/*.parquet`). 105,894,626 legacy and live ticks stored across 7,367 files for 19 approved equities. Live ticks never touch a disk-backed DuckDB database, completely eliminating write locks between ingestion and analytical readers.
- **Pure In-Memory DuckDB Lake Reader (`TickLakeReader`)**: Ephemeral private in-memory DuckDB connections per query (default `threads = 4`, `max_memory = '2GB'`) with Hive partition pruning and deterministic OHLCV resampling via `arg_min(price, (timestamp, ingest_id))` / `arg_max`.
- **Micro-Batch Lake Writer (`TickLakeWriter`)**: Asynchronous worker thread writing Snappy Parquet batches with backpressure, retries with exponential backoff, malformed-tick quarantine, and graceful drain.
- **Sole Symbol Authority (`_control/registry.json`)**: Versioned JSON registry enforcing strictly 19 approved US equities, filtering unsolicited symbols at the WebSocket callback boundary.
- **Automated Operations & Off-Hours Compaction**: Weekday 04:00–20:00 ET session window enforcement, close-time queue drain, unattended off-hours partition consolidation, and operational alerts via Discord.
- **Observability Command Center**: Interactive TradingView Lightweight Charts (v4.1.3), raw candle inspector, live tick tape, market session clock, and Parquet data integrity auditing on port 8420.
- **Documentation**: `README.md`, `docs/operations/tick_lake_operations_guide.md`, `docs/contracts/repo_b_tick_lake_contract.md`, and `reports/v5.0_completion_report.md` aligned with the final Parquet-only architecture.

## Requirements

### Validated
- [x] **EXTR-01**: One-time zero-read extraction of historical data from Turso Archive into local DuckDB — v1.0
- [x] **PURG-01**: Purged legacy Turso, Docker (`.devcontainer`), instruction (`.gemini`), and GitHub Actions files — v1.0
- [x] **DUCK-01**: Implemented native DuckDB database layer (`market_data.duckdb`) with optimized schema and batch ingestion — v1.0
- [x] **DUCK-02**: Implemented high-speed `time_bucket()` resampling queries for OHLCV candlesticks — v1.0
- [x] **STRM-01**: Implemented Capital.com live WebSocket streaming with automatic session authentication and heartbeats — v1.0
- [x] **STRM-02**: Implemented Binance 24/7 live WebSocket streaming for crypto and gold — v1.0
- [x] **STRM-03**: Continuous background ingestion pipeline buffering ticks and writing cleanly to DuckDB — v1.0
- [x] **STRM-04**: Dedicated pure tick-by-tick storage via Binance `@trade` and Capital quotes — v1.0
- [x] **QUAL-01**: Comprehensive integration and acceptance tests verifying live streaming write and DuckDB query performance — v1.0
- [x] **DUAL-01**: Decoupled database into 100% separate dedicated DuckDB files (`historical.duckdb` and `streaming.duckdb`) — v2.0
- [x] **DUAL-02**: Implemented isolated connection factories with read-only concurrency support and cross-database `ATTACH` capabilities — v2.0
- [x] **DUAL-03**: Created optimized schema and indexes for raw tick quotes in `data/streaming.duckdb` — v2.0
- [x] **STRM-05**: Deactivated Binance from live streaming runner; stream exclusively from Capital.com — v2.0
- [x] **STRM-06**: Ingest Capital.com raw tick quotes directly into `data/streaming.duckdb` using batched async writer — v2.0
- [x] **STRM-07**: Implemented dynamic live reload of symbol subscriptions without restarting daemon — v2.0
- [x] **INTG-01**: Automated gap and continuity detection for historical 1-minute bars during market trading hours — v2.0
- [x] **INTG-02**: Stream continuity and quiet interval detector for streaming ticks — v2.0
- [x] **INTG-03**: OHLCV sanity and anomaly detection (negative/zero prices, `high < low`, price spikes, nulls) — v2.0
- [x] **INTG-04**: Cross-database drift analyzer comparing historical REST candle closes against streaming tick prices — v2.0
- [x] **DASH-01**: Lightweight local Python backend server running on `http://localhost:8420` exposing REST endpoints — v2.0 (originally port 8000)
- [x] **DASH-02**: Responsive single-page JavaScript/Tailwind web dashboard displaying live streamer status, tick rates, and storage — v2.0
- [x] **DASH-03**: Interactive Data Integrity visual audit view with one-click test execution and pass/warn/fail matrix — v2.0
- [x] **DASH-04**: Symbol Management interface in the web dashboard allowing users to add/remove symbols with dynamic live reload — v2.0
- [x] **QUAL-02**: Comprehensive automated test suite validating dual DuckDB files, streaming raw ticks, live symbol reload, and integrity — v2.0
- [x] **QUAL-03**: End-to-end API tests validating dashboard endpoints, symbol lifecycle, and concurrency stress testing — v2.0
- [x] **CHRT-01**: High-performance backend DuckDB candle query API (`/api/candles`) with time bucketing — v3.0
- [x] **CHRT-02**: Interactive Candlestick & Volume Chart powered by TradingView Lightweight Charts — v3.0
- [x] **CHRT-03**: Interactive Raw Candle Inspector & Exporter with search, pagination, and CSV download — v3.0
- [x] **STRM-08**: Live Stream Telemetry & Ticker Tape API (`/api/stream/tape`, `/api/stream/status`) — v3.0
- [x] **STRM-09**: Real-time Ticker Wall & Streamer Control UI with flash feedback — v3.0
- [x] **MKT-01**: Market Session Status API (`/api/market/session`) with session phase calculations — v3.0
- [x] **MKT-02**: Live Market Clock & Session Bar in dashboard header — v3.0
- [x] **HRVST-01**: Harvester Runner API (`/api/harvester/run`, `/api/harvester/status`) — v3.0
- [x] **HRVST-02**: Real-time Log Console Drawer in web UI — v3.0
- [x] **SYMB-01**: Symbol Coverage & Health API (`/api/symbols/coverage`) — v3.0
- [x] **SYMB-02**: Rich Per-Symbol Data Matrix UI with search and quick actions — v3.0
- [x] **INTG-05**: Context-aware Gap & Integrity Auditor with market-hours awareness — v3.0
- [x] **QUAL-04**: Comprehensive automated test suite in `tests/test_dashboard_v3.py` (182 total passing tests) — v3.0
- [x] **LAKE-P0-01**: Safe test isolation routing paths to `tmp_path`; block production volume mutations — v4.0 (shipped 2026-10-03)
- [x] **LAKE-P0-02**: Deterministic quote fixtures (repeats, identical timestamps, late arrivals, nulls) — v4.0 (shipped 2026-10-03)
- [x] **LAKE-P0-03**: Baseline characterization of legacy DuckDB write/query CPU seconds & latency — v4.0 (shipped 2026-10-03)
- [x] **LAKE-P1-01**: Lake layout & explicit `TICK_LAKE_ROOT` configuration — v4.0 (shipped 2026-10-03)
- [x] **LAKE-P1-02**: Typed Schema v1 with microsecond UTC timestamp and stable unique `ingest_id` (string) — v4.0 (shipped 2026-10-03)
- [x] **LAKE-P1-03**: Atomic file staging (`.tmp` -> atomic rename) — v4.0 (shipped 2026-10-03)
- [x] **LAKE-P1-04**: Publication state machine & receipts — v4.0 (shipped 2026-10-03)
- [x] **LAKE-P2-01**: Micro-batch writer `TickLakeWriter` with configurable time/count thresholds — v4.0 (shipped 2026-10-03)
- [x] **LAKE-P2-02**: Off-loop PyArrow worker thread preserving <20ms event loop lag — v4.0 (shipped 2026-10-03)
- [x] **LAKE-P2-03**: Runner lifecycle integration with cooperative shutdown drain — v4.0 (shipped 2026-10-03)
- [x] **LAKE-P3-01**: Atomic JSON symbol registry (`_control/registry.json`) — v4.0 (shipped 2026-10-03)
- [x] **LAKE-P3-02**: Cross-process registry version polling & dynamic reload — v4.0 (shipped 2026-10-03)
- [x] **LAKE-P3-03**: Pending purge semantics & subscription fencing — v4.0 (shipped 2026-10-03)
- [x] **LAKE-P4-01**: In-memory DuckDB `TickLakeReader` engine with Hive partition pruning — v4.0 (shipped 2026-10-03)
- [x] **LAKE-P4-02**: Deterministic OHLCV resampling via `arg_min(price, (timestamp, ingest_id))` — v4.0 (shipped 2026-10-03)
- [x] **LAKE-P4-03**: Dashboard analytics integration migrated off legacy DuckDB — v4.0 (shipped 2026-10-03)
- [x] **LAKE-P4-04**: Minimal Repo B reader contract and usage examples — v4.0 (shipped 2026-10-03)
- [x] **LAKE-P6-01**: Migration CLI tool `tools/migrate_streaming_to_parquet.py` — v4.0 (shipped 2026-10-03)
- [x] **LAKE-P6-02**: Chunked export & checkpointing of legacy `streaming.duckdb` — v4.0 (shipped 2026-10-03)
- [x] **LAKE-P6-03**: Two-way `EXCEPT ALL` reconciliation verifying zero row loss — v4.0 (shipped 2026-10-03)
- [x] **LAKE-P8-01**: Coordinated writer cutover from legacy DB to Parquet lake — v4.0 (shipped 2026-10-03)
- [x] **LAKE-P8-02**: Multi-process concurrency verification under sustained live streaming — v4.0 (shipped 2026-10-03)
- [x] **LAKE-P8-03**: Documentation, service supervisor configs, and operational runbook — v4.0 (shipped 2026-10-03)
- [x] **TEST-P22-01 … TEST-P22-03**: Storage layout path traversal, unicode/special symbol encoding, corrupted metadata; PyArrow schema coercion, float extremes, null bitmasks; publication collisions, crashed intent recovery, lock serialization — v4.1 (shipped 2026-10-03)
- [x] **TEST-P23-01 … TEST-P23-03**: 100k+ tick micro-batching under memory pressure and bounded-queue backpressure; sudden shutdown mid-flush, drain timeouts, honest acknowledgments; disk-full / I/O-error backoff and quarantine — v4.1 (shipped 2026-10-03)
- [x] **TEST-P24-01 … TEST-P24-03**: Cross-process symbol CRUD lock serialization and monotonic versioning; toggle/delete flapping with `PENDING_PURGE` fences; signal debouncing and reload latency under heavy polling — v4.1 (shipped 2026-10-03)
- [x] **TEST-P25-01 … TEST-P25-03**: 30+ concurrent in-memory readers without leaks; sparse partitions, DST/leap-year resampling edges; reverse-chronological tape pagination and symbol pruning — v4.1 (shipped 2026-10-03)
- [x] **TEST-P26-01 … TEST-P26-03**: Corrupt/partial legacy sources and schema drift; crash interruption across all migration modes; two-way `EXCEPT ALL` fuzzing with precision-mismatch detection — v4.1 (shipped 2026-10-03)
- [x] **TEST-P27-01 … TEST-P27-02**: Multi-process soak under continuous ingestion and reads; chaos-monkey termination with supervisor self-healing — v4.1 (shipped 2026-10-03)
- [x] **BASE-01**: Lake candle output recorded as reference baseline before code changes — v5.0 (shipped 2026-10-06)
- [x] **STOR-01 … STOR-05**: Disk DuckDB removed from runtime; runner lake-only; symbols from registry; in-memory query helper relocated; `src/database/` deleted — v5.0 (shipped 2026-10-06)
- [x] **DASH-01 … DASH-03**: Historical routes removed; single Parquet dashboard view; integrity reads lake — v5.0 (shipped 2026-10-06)
- [x] **GAP-01 … GAP-02**: Databento backfill writes to Parquet lake for 19 symbols — v5.0 (shipped 2026-10-06)
- [x] **RMV-01 … RMV-04, RMV-06 … RMV-09**: Disk DB unreferenced; 1m bar pipeline & dead harvesters deleted; frontend purged; dead dependencies removed; discord rewritten for streamer operations — v5.0 (shipped 2026-10-06)
- [x] **SCHED-01 … SCHED-05**: America/New_York weekday 04:00–20:00 window; supervisor lifecycle; runner schedule gate; close-time drain; unattended off-hours compaction — v5.0 (shipped 2026-10-06)
- [x] **SYMB-01**: `_control/registry.json` sole symbol authority (19 symbols) — v5.0 (shipped 2026-10-06)
- [x] **NOTIF-01**: Discord operational alerts — v5.0 (shipped 2026-10-06)
- [x] **CO-01 … CO-02**: Full test disposition accounting; candle query behavior verified unchanged — v5.0 (shipped 2026-10-06)
- [x] **MIG-01 … MIG-03**: 105,894,626 legacy ticks migrated & verified with zero discrepancies; gate before deletion; idempotent re-runs — v5.0 (shipped 2026-10-06)
- [x] **RMV-05**: Legacy `.duckdb` files deleted immediately after verification — v5.0 (shipped 2026-10-06)
- [x] **CO-03**: Milestone closed with completion report — v5.0 (shipped 2026-10-06)

- [x] **QUOTE-01 … QUOTE-03**: New quotes store `bid_price` and `ask_price` only (Capital.com & Databento); inspection candles use `bid_price`, volume histogram removed — v6.0 (shipped 2026-10-06)
- [x] **QFILL-01 … QFILL-05**: Computer-offline gap fill for named days requests all-symbol silence only; holds lock; skips weekends/holidays — v6.0 (shipped 2026-10-06)
- [x] **DOCS-01**: README, operations guide, and Repo B contract updated for schema v2 and gap fill — v6.0 (shipped 2026-10-06)
- [x] **REWRITE-01 … REWRITE-05**: Offline schema rewrite tool with backup verification, resume, and receipts (verified in fixtures; production lake owner-run) — v6.0 (shipped 2026-10-06)

_Milestone requirement details are archived in [.planning/milestones/](milestones/)._

### Active
None. Milestone v6.0 complete (checkout complete; production lake rewrite owner-run).

### Out of Scope
- Bid quantity and ask quantity (owner, 2026-10-06). They were never stored, so the rewrite cannot recover them.
- A `volume` column, and any attempt to scale Databento size onto Capital.com size (owner, 2026-10-06).
- A stored midpoint (owner, 2026-10-06). The inspection chart uses the bid.
- Switching Databento from `tbbo` to `mbp-1`, or making the two feeds emit the same number of rows (owner, 2026-10-06).
- Historical 1-minute pre-aggregated bar storage and REST harvesters (permanently purged in v5.0)
- Removing DuckDB library / PyArrow resampling rewrite (considered and rejected in v5.0 Option A)
- Direct `market-rewind` frontend modifications (deferred per user instruction: focus on data harvesting, storage, and integrity dashboard)
- Cloudflare R2 / S3 remote object storage (preserving 100% local, zero-cloud architecture)
- Turso dual-replica synchronization and mirror database (deprecated and purged)
- GitHub Actions scheduled runs (deprecated in favor of persistent local daemon)
- Binance live streaming writes (streaming is dedicated exclusively to Capital.com)

## Context
- The previous implementation relied on Turso cloud SQLite with complex dual-replica mirror synchronization, which suffered from severe read/write quota exhaustion.
- In v2.0, storage was decoupled into separate `.duckdb` files to avoid single-writer lock contention between streaming and dashboard operations.
- In v3.0, the dashboard transformed into an interactive Observability Command Center with TradingView charts, telemetry tape, market clocks, and background job runners.
- In v4.0, live tick storage was migrated completely off DuckDB into an append-only, partitioned Parquet lake with atomic staging and in-memory DuckDB querying. This resolved OS-level write lock contention while the canonical 1-minute history remained in `historical.duckdb`.
- In v4.1, the engine underwent deep hardening and edge-case stress testing: 122 new edge-case, stress, fuzz, backpressure, and chaos tests across Phases 22–27, validating crash recovery, leak-free multi-threaded readers (30+ concurrent), migration fuzzing, and self-healing supervisor processes under chaos-monkey termination.
- In v5.0, the final architecture was realized: complete elimination of disk-backed DuckDB databases (`streaming.duckdb`, `historical.duckdb`, `market_data.duckdb`), zero-loss migration of 105,894,626 legacy ticks into 7,367 Parquet files, total purge of bar-era pipelines (net −10,415 LOC), restricted 19-symbol authority, and automated weekday 04:00–20:00 ET schedule with unattended compaction.
- On 2026-10-06 the owner reopened development as v6.0. The lake must store bid price and ask price only. The existing 7,367 files must be rewritten, not left on schema v1. Databento gap fill must detect computer-offline stretches, not per-symbol quiet periods.

## Constraints
- **Zero Cloud Limits**: No dependence on cloud database quotas.
- **100% Local**: All data stored locally on SSD in the Parquet lake (`data/tick_lake/`). Zero `.duckdb` files on disk.
- **Persistent Streams**: Real-time tick capture via Capital.com exclusively.
- **Mac Mini & Windows Portability**: Standard Python 3.12+ cross-platform execution with zero complex build chains.

## Key Decisions
| Decision | Rationale | Outcome |
|----------|-----------|---------|
| Migrate from Turso to DuckDB | Eliminates cloud read/write quotas, prevents CPU overheating, provides native `time_bucket` resampling | ✓ Good |
| Switch from 6x daily REST batch to 24/7 WebSockets | Captures real-time ticks, eliminates Capital.com 16-hour lookback clipping, avoids REST rate limit spikes | ✓ Good |
| Pure local storage on SSD | Zero egress costs, no network latency, works on Mac Mini and Windows | ✓ Good |
| 100% Separate Dedicated DuckDB Files | Decouples streaming writes from historical harvesting | ✓ Good |
| Partitioned Parquet Lake for Ticks | Completely eliminates DuckDB file write-lock contention; off-loads disk I/O from the asyncio loop | ✓ Good |
| Atomic Staging & Publication Protocol | Staging into hidden `.tmp` files with atomic filesystem rename prevents partial file visibility | ✓ Good |
| In-Memory DuckDB Connection per Request | Private in-memory DuckDB connections query the Parquet lake directly with zero file locks | ✓ Good |
| Versioned JSON Symbol Registry | Monotonic versioning and file-based signal notifications enable lock-free dynamic reload | ✓ Good |
| Vectorized Release Data Consolidation | Uses DuckDB's native SQLite scanner to merge, deduplicate, and tier 9.4M rows in 11.64s | ✓ Good |
| TradingView Lightweight Charts (v4.1.3) | Sub-25ms canvas rendering for 8M rows with zero frontend build dependencies | ✓ Good |
| DuckDB `time_bucket()` Server-Side Resampling | Native analytical grouping across multiple timeframes (`1m` to `1D`) without Python loop overhead | ✓ Good |
| Context-Aware Gap Range Discovery | Dynamic recorded date range targeting prevents false gap alarms on archived historical data | ✓ Good |
| Tests-only hardening milestone (v4.1) | Freezing application code while adding 122 adversarial tests proves the lake design rather than hiding defects behind concurrent refactors | ✓ Good |
| Option A: Parquet storage with DuckDB in-memory query engine | Eliminates file locks and storage fragmentation while preserving existing fast candle queries | ✓ Good |
| Maximum declutter principle | Deleted bar pipelines, unused harvesters, dead dependencies (yfinance, polygon-api-client), and replay subsystem | ✓ Good |
| Ingestion schedule window (04:00–20:00 ET weekdays) | Matches US market liquidity hours, protects hardware during nights/weekends | ✓ Good |
| Unattended off-hours compaction | Consolidates micro-batches during quiet hours under lease lock without contention | ✓ Good |
| Strict 19-symbol authority | Enforces canonical symbol inventory across runner, supervisor, gap-fill, and UI | ✓ Good |
| v6.0 stores bid price and ask price only | The chart is for inspection. A midpoint was redundant. Volume was not comparable across feeds, and sizes were never stored | ✓ Good |
| Existing-lake rewrite is the last phase | Tool and fixture tests shipped in Phase 53. Production rewrite stays owner-run. | ✓ Good (fixture only) |

## Evolution
This document evolves at phase transitions and milestone boundaries.

**After each phase transition** (via `/gsd-transition`):
1. Requirements invalidated? → Move to Out of Scope with reason
2. Requirements validated? → Move to Validated with phase reference
3. New requirements emerged? → Add to Active
4. Decisions to log? → Add to Key Decisions
5. "What This Is" still accurate? → Update if drifted

**After each milestone** (via `/gsd-complete-milestone`):
1. Full review of all sections
2. Core Value check — still the right priority?
3. Audit Out of Scope — reasons still valid?
4. Update Context with current state

---
*Last updated: 2026-10-06 — Milestone v6.0 checkout complete. Production lake rewrite remains owner-run.*
