# Milestones

## v4.0 Partitioned Parquet Tick Lake (Decoupled High-Concurrency Storage) (Shipped: 2026-10-03)

**Phases completed:** 7 phases (Phases 15–21), 7 plans, 19/19 requirements verified  
**Tests:** 200+ automated tests passing (0 failures, 0 warnings)  
**Dataset Scale:** Multi-million raw ticks stored in partitioned Parquet lake (`data/lake/ticks`) + 7,993,726 canonical 1-minute OHLCV candles (`data/historical.duckdb`)  
**Architecture:** Decoupled append-only Parquet storage, off-loop PyArrow worker thread, atomic staging & publication protocol, versioned JSON symbol registry, in-memory DuckDB analytical queries  

**Key accomplishments:**

- Completely decoupled high-frequency live tick streaming from analytical queries and dashboard reads by replacing the single-writer locked `data/streaming.duckdb` with an append-only, partitioned Parquet lake (`data/lake/ticks/symbol=.../year=.../month=.../*.parquet`).
- Implemented strongly-typed **Schema v1** featuring microsecond-precision UTC timestamps and a stable, unique 64-bit integer `ingest_id` to preserve duplicate quotes and handle high-frequency bursts without row collision.
- Built **Atomic Publication Protocol** (`src/storage/publication.py`) using hidden `.tmp` staging directories on the same physical filesystem and atomic filesystem renames, ensuring readers never observe uncommitted or partial Parquet files.
- Built **`TickLakeWriter`** (`src/storage/parquet_writer.py`) with configurable time/count flush triggers (5s / 5,000 ticks) and an off-loop PyArrow worker thread, maintaining asyncio event loop scheduling lag below 20ms during peak tick ingestion.
- Built **Versioned Symbol Registry** (`src/storage/registry.py`) providing atomic, lock-free symbol configuration (`data/lake/registry.json`) with cross-process dynamic reload signaling via file-touch events and `PENDING_PURGE` generation fences.
- Built **In-Memory DuckDB Lake Reader** (`src/storage/reader.py`), issuing ephemeral in-memory DuckDB connections per query with Hive partition pruning, sub-100ms multi-day querying, and deterministic OHLCV resampling via `arg_min(price, (timestamp, ingest_id))`.
- Built zero-loss **Migration CLI Tool** (`tools/migrate_streaming_to_parquet.py`) supporting `plan`, `export`, `verify`, and `publish` modes with checkpointed progress and two-way `EXCEPT ALL` reconciliation guaranteeing 100% duplicate multiplicity and precision preservation.
- Completed seamless production cutover and multi-process concurrency validation, demonstrating sustained real-time tick streaming, live dashboard chart updates, and independent reader queries without OS file lock collisions.

---

## v3.0 Observability Command Center, Interactive Financial Charts & Live Telemetry Dashboard (Shipped: 2026-09-26)

**Phases completed:** 5 phases (Phases 10–14), 5 plans, 13/13 requirements verified
**Tests:** 182 automated tests passing (0 failures, 0 warnings)
**Dataset Scale:** 7,993,726 canonical 1-minute OHLCV candles (October 2024 → July 2026) + Live Tick Stream
**Commits:** 21 commits | 91 files changed | +7,076 / −558 lines
**Closeout:** override_closeout
**Known verification overrides:** 3 newly acknowledged, 0 carried forward (see STATE.md Deferred Items)

**Key accomplishments:**

- Integrated **TradingView Lightweight Charts** (v4.1.3) into the web dashboard with responsive dark-slate design, providing sub-25ms dynamic rendering of candlesticks and volume histograms across all ~8M historical candles.
- Implemented multi-timeframe analytical resampling API (`/api/candles`) supporting `1m`, `5m`, `15m`, `30m`, `1h`, `4h`, `1D` intervals via native DuckDB `time_bucket()` with date filtering, limit clamping, and chronological ordering.
- Built **Raw OHLCV Candle Inspector** with interactive search filter, session badges, source tiering pills (`MASSIVE`, `CAPITAL`, `BINANCE`, `YAHOO`), and browser-side "Export CSV" functionality.
- Implemented **Live Price Tape Wall** with flashing uptick/downtick animations, real-time incoming tick feed table (last 50 quotes with bid/ask/spread), and streamer process telemetry (`/api/stream/tape`, `/api/stream/status`).
- Built **Market Session Clock & Telemetry Bar** in header calculating live US Eastern & UTC times, market phase pills (`REGULAR`, `PRE_MARKET`, `AFTER_HOURS`, `CLOSED`), and countdown to the 8:00 PM ET session cutoff.
- Implemented **Harvester Automation Console & Live Terminal** (`/api/harvester/run`, `/api/harvester/status`, `/api/harvester/logs`) enabling web-triggered execution of `main.py` with real-time stdout/stderr log streaming.
- Transformed symbol table into a comprehensive **Symbol Coverage Matrix** (`/api/symbols/coverage`) with search, asset classes, stored bar counts, date ranges, latest close prices, freshness status, and one-click chart/audit navigation.
- Upgraded the **Data Integrity Engine** with symbol-specific targeting and recorded-date discovery, eliminating false gap warnings on historical data.
- Expanded the automated test suite with 19 new integration and endpoint tests (`tests/test_dashboard_v3.py`), bringing total passing tests to 182/182 (100% pass rate).

---

## v1.0 Local DuckDB & 24/7 Live Streaming Engine (Shipped: 2026-09-25)

**Phases completed:** 4 phases, 4 plans, 13/13 requirements verified
**Tests:** 127 automated tests passing (0 failures, 0 warnings)

**Key accomplishments:**

- Migrated 3,949,885 historical 1-minute OHLCV bars across 40 symbols from Turso Archive into native local DuckDB (`data/market_data.duckdb`) without burning row read quotas.
- Implemented high-performance DuckDB data access layer with composite primary keys, Source-Tiering protection, and sub-10ms dynamic OHLCV candlestick resampling via `time_bucket()`.
- Implemented 24/7 live multi-broker streaming engine capturing tick-by-tick data to `data/streaming.db` from Binance (`@trade` stream for crypto and gold) and Capital.com (real-time quote stream for equities and indices).
- Purged legacy Turso cloud replicas, Docker devcontainer, GitHub Actions batch runners, and removed Infisical SDK in favor of local `.env` configuration.
- Upgraded test suite to 127 integration and acceptance tests running against isolated DuckDB environments with zero cloud dependencies.

---

## v2.0 Dedicated Dual-DuckDB Storage, Capital.com Tick Streamer & Data Integrity Web Dashboard (Shipped: 2026-09-25)

**Phases completed:** 5 phases (Phases 5–9), 5 plans, 16/16 requirements verified  
**Tests:** 163 automated tests passing (0 failures, 0 warnings)  
**Dataset Scale:** 7,993,726 canonical 1-minute OHLCV candles (October 2024 → July 2026)  

**Key accomplishments:**

- Decoupled storage into 100% separate dedicated DuckDB database files (`data/historical.duckdb` and `data/streaming.duckdb`), completely eliminating single-writer file lock contention between the 24/7 streaming daemon and REST harvesting or dashboard reads.
- Dedicated streaming engine exclusively to Capital.com (deactivated Binance), capturing granular raw tick quotes (`timestamp`, `symbol`, `price`, `volume`, `bid`, `ask`, `source`, `session`) with on-demand dynamic resampling via native DuckDB `time_bucket()`.
- Implemented dynamic live reload mechanism for symbol subscriptions, allowing users to add or remove tracked symbols on the fly without dropping WebSocket connections or restarting the streaming runner.
- Built comprehensive Data Integrity & Health Engine (`src/utils/integrity.py`) providing automated 1-minute historical gap detection during US market hours, stream continuity/staleness monitoring, OHLCV sanity/anomaly validation, and cross-database price drift reconciliation.
- Developed zero-dependency multi-threaded Python backend server on `http://localhost:8000` with a modern dark-mode single-page JavaScript/Tailwind web dashboard featuring real-time KPI metrics, on-demand integrity audit explorer, and interactive symbol management.
- Implemented in-process adaptive configuration matching and transient lock retries in `DuckDBClient`, ensuring high-concurrency burst traffic across all endpoints executes seamlessly.
- Consolidated multi-year historical dataset from `market-rewind` release assets (`archive_data.db` and `market_data.db`) into `data/historical.duckdb`, scaling canonical storage to 7,993,726 1-minute candles covering over 21 continuous months.

---
