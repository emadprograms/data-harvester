# Data Harvester — Local DuckDB & Live Streaming Engine

## What This Is
A high-performance, 100% local market data harvesting and streaming engine running on macOS (Mac Mini) and Windows. It captures live market quotes from Capital.com via persistent WebSockets, storing real-time tick-by-tick quotes in an append-only partitioned Parquet tick lake (`data/tick_lake/ticks`) and canonical 1-minute historical bars in `data/historical.duckdb` with sub-millisecond dynamic OHLCV candlestick resampling, deep data integrity validation, and an interactive local web dashboard (`http://localhost:8420`).

## Core Value
Zero-cloud, zero-quota persistent market data ingestion and storage: capture real-time market data reliably and provide sub-millisecond OHLCV querying without hitting API limits or heating up hardware.

## Current Milestone (v5.0) — In Progress

**v5.0: DuckDB-Free Tick-Only Parquet** — Phases 46–51 (started 2026-10-05)

**Goal:** Reduce the system to one storage format, one query engine, and one data type — ticks in Parquet, read with PyArrow. DuckDB is removed entirely: not only as a store, but as the analytical engine. The streamer ingests the 19 approved equity symbols during 04:00–20:00 ET only, and compaction runs unattended in the closed window.

**End state:**
- `historical.duckdb`, `streaming.duckdb`, and the entire 1-minute bar pipeline are deleted by the owner; no code can open or recreate a disk-backed DuckDB database.
- Every query previously served by DuckDB SQL (`time_bucket`, `arg_min`/`arg_max`, `read_parquet`, `EXCEPT ALL`) is served by PyArrow against the Parquet tick lake.
- Capital.com is the sole live source; Databento is the sole gap-repair source, writing into the same lake.
- Symbols are exactly: AAPL, ADBE, AMD, AMZN, APP, AVGO, BABA, GOOGL, META, MSFT, MU, NDAQ, NVDA, ORCL, PANW, QCOM, SHOP, TSLA, TSM.

**Hard sequencing constraint:** the legacy tick migration must complete and verify **before** DuckDB is removed — the migration tool uses DuckDB to read the legacy `.duckdb` source.

**Artifacts:** [REQUIREMENTS.md](REQUIREMENTS.md) (v5.0 requirements) · [ROADMAP.md](ROADMAP.md) (Phases 46–51) · [STATE.md](STATE.md) · [PLAN-MILESTONE-5.0.md](../PLAN-MILESTONE-5.0.md)

## Current State (Shipped v4.1)
- **Partitioned Parquet Tick Lake (`data/tick_lake/ticks`)**: Fully decoupled append-only storage organized by Hive two-level partitioning (`symbol=<ENCODED_SYMBOL>/date=<YYYY-MM-DD>/*.parquet`). Live ticks never touch a disk-backed DuckDB database, completely eliminating write locks between ingestion and analytical readers.
- **Micro-Batch Lake Writer (`TickLakeWriter`)**: Asynchronous worker thread writing Snappy Parquet batches (class defaults: flush every 5.0s or 5,000 rows; the streaming runner constructs it with a 2.0s flush interval and a 10,000-tick bounded queue) with backpressure, retries with exponential backoff, malformed-tick quarantine, and graceful drain.
- **In-Memory DuckDB Lake Reader (`TickLakeReader`)**: Ephemeral private in-memory DuckDB connections per query (default `threads = 4`, `max_memory = '2GB'`) with Hive partition pruning and deterministic OHLCV resampling via `arg_min(price, (timestamp, ingest_id))` / `arg_max`.
- **Versioned Symbol Registry (`_control/registry.json`)**: Atomic JSON registry with monotonic version tracking and cross-process file signal notifications (`.stream_reload.signal`) for dynamic symbol additions/removals without daemon restarts.
- **Zero-Loss Migration Tooling (`tools/migrate_streaming_to_parquet.py`)**: Chunked export (default 100,000 rows/chunk), idempotent checkpointing, and rigorous two-way `EXCEPT ALL` reconciliation guaranteeing 100% data fidelity.
- **Historical Storage (`data/historical.duckdb`)**: ~8.9M deduplicated 1-minute OHLCV bars across 40 symbols covering October 2024 to September 2026. Sub-10ms dynamic candlestick resampling via DuckDB native `time_bucket()`.
- **Observability Command Center**: Interactive TradingView Lightweight Charts (v4.1.3) with multi-timeframe analytical resampling (`1m`, `5m`, `15m`, `30m`, `1h`, `4h`, `1D`), raw OHLCV candle inspector, CSV export, live tick tape, market session clock, and integrity auditing.
- **Milestone v4.1 Shipped (2026-10-03, Closed: 2026-10-04)**: `Partitioned Parquet Lake Deep Testing & Hardening` (Phases 22–27) — 122 new comprehensive edge-case, stress, fuzz, backpressure, and chaos tests expanded to **746 passing offline tests** (0 failures, 0 errors) with 5 local performance gates passing (<100ms p95). All 17 requirements verified; all audit findings F01–F11 remediated, regression-tested, and offline qualified; formal audit verdict: `PASSED / QUALIFIED` ([.planning/milestones/v4.1-MILESTONE-AUDIT.md](milestones/v4.1-MILESTONE-AUDIT.md)).
- **Milestone v4.2 Closed (2026-10-04)**: `Tick Lake Qualification & Scoped Signoff` (Phases 28–36) — Scoped signoff with 30 of 45 requirements verified; 15 requirements explicitly excluded/deferred (24h endurance, capacity monitoring, Q10a replay, Q10b compaction) and two xfails pinned for remediation in v4.3 ([.planning/milestones/v4.2-MILESTONE-AUDIT.md](milestones/v4.2-MILESTONE-AUDIT.md)).
- **Documentation**: `README.md`, `docs/operations/tick_lake_operations_guide.md`, `docs/contracts/repo_b_tick_lake_contract.md`, `docs/windows_service_setup.md`, and `docs/plans/*` are aligned with the shipped architecture and paths.

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

_Phase-level requirement detail for v4.1 is archived at [.planning/milestones/v4.1-REQUIREMENTS.md](milestones/v4.1-REQUIREMENTS.md)._

### Active
v5.0 requirements are defined in [REQUIREMENTS.md](REQUIREMENTS.md): DuckDB-free tick-only Parquet storage; authoritative 19-symbol inventory; 04:00–20:00 ET ingestion window with unattended off-hours compaction; PyArrow-based reading and resampling.

### Out of Scope
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
- In v4.1, the engine underwent deep hardening and edge-case stress testing: 122 new edge-case, stress, fuzz, backpressure, and chaos tests across Phases 22–27, validating crash recovery, leak-free multi-threaded readers (30+ concurrent), migration fuzzing, and self-healing supervisor processes under chaos-monkey termination. The application code required no functional changes, confirming the v4.0 lake design.

## Constraints
- **Zero Cloud Limits**: No dependence on cloud database quotas.
- **100% Local**: All data stored locally on SSD in the Parquet lake (`data/tick_lake/`) and the DuckDB historical archive (`data/historical.duckdb`).
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
*Last updated: 2026-10-05 — Milestone v5.0 (DuckDB-Free Tick-Only Parquet) started; v4.3 closed with a documented waiver of phases 44–45.*
