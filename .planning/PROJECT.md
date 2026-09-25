# Data Harvester — Local DuckDB & Live Streaming Engine

## What This Is
A high-performance, 100% local market data harvesting and streaming engine running on macOS (Mac Mini) and Windows. It captures live market quotes from Capital.com via persistent WebSockets, storing real-time tick-by-tick quotes in `data/streaming.duckdb` and canonical 1-minute historical bars in `data/historical.duckdb` with sub-millisecond dynamic OHLCV candlestick resampling, deep data integrity validation, and an interactive local web dashboard (`http://localhost:8000`).

## Core Value
Zero-cloud, zero-quota persistent market data ingestion and storage: capture real-time market data reliably and provide sub-millisecond OHLCV querying without hitting API limits or heating up hardware.

## Current State (Shipped v2.0)
- **Historical Storage (`data/historical.duckdb`)**: 7,993,726 deduplicated 1-minute OHLCV bars across 40 symbols covering October 2024 to July 2026 (920 MB on disk). Sub-10ms dynamic candlestick resampling via DuckDB native `time_bucket()`.
- **Live Streaming Storage (`data/streaming.duckdb`)**: Dedicated tick quote storage (`ticks` table) capturing bid, ask, price, and volume exclusively from Capital.com.
- **Dynamic Subscription Hot-Reload**: Live symbol subscriptions update on the fly upon database/dashboard modification without dropping WebSocket connections.
- **Data Integrity & Health Engine**: Automated gap detection for regular market hours, stream quiet interval monitoring, OHLCV sanity/anomaly bounds validation, and cross-database price drift reconciliation.
- **Interactive Web Dashboard**: Zero-dependency multi-threaded Python server (`http://localhost:8000`) with modern dark-mode single-page JS/Tailwind UI for real-time KPIs, one-click integrity audits, and symbol management.
- **Test Automation**: 163 automated unit, integration, concurrency stress, and end-to-end tests passing cleanly (0 failures).

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
- [x] **DASH-01**: Lightweight local Python backend server running on `http://localhost:8000` exposing REST endpoints — v2.0
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

### Active
*(Ready for next milestone cycle — initialize via `/gsd-new-milestone`)*

### Out of Scope
- Direct `market-rewind` frontend modifications (deferred per user instruction: focus on data harvesting, storage, and integrity dashboard)
- Cloudflare R2 / S3 remote object storage (preserving 100% local, zero-cloud architecture)
- Turso dual-replica synchronization and mirror database (deprecated and purged)
- GitHub Actions scheduled runs (deprecated in favor of persistent local daemon)
- Binance live streaming writes (streaming table is dedicated exclusively to Capital.com)

## Context
- The previous implementation relied on Turso cloud SQLite with complex dual-replica mirror synchronization, which suffered from severe read/write quota exhaustion.
- In v2.0, the user specifically requested 100% separate dedicated `.duckdb` files to avoid DuckDB single-writer lock contention between the 24/7 streaming engine and historical batch harvesting or web dashboard queries.
- Raw tick quotes are stored for Capital.com to retain granular bid/ask/price precision without quantization.
- A modern local web dashboard provides immediate observability into database health, data integrity, and interactive symbol management.
- Multi-year historical data from release assets was consolidated into `historical.duckdb`, giving a unified continuous archive from October 2024 through July 2026.

## Constraints
- **Zero Cloud Limits**: No dependence on cloud database quotas.
- **100% Local**: All data stored locally in separate DuckDB files: `data/historical.duckdb` and `data/streaming.duckdb`.
- **Persistent Streams**: Real-time tick capture via Capital.com exclusively.
- **Mac Mini & Windows Portability**: Standard Python 3.12+ cross-platform execution with zero complex build chains.

## Key Decisions
| Decision | Rationale | Outcome |
|----------|-----------|---------|
| Migrate from Turso to DuckDB | Eliminates cloud read/write quotas, prevents CPU overheating, provides native `time_bucket` resampling | ✓ Good |
| Switch from 6x daily REST batch to 24/7 WebSockets | Captures real-time ticks, eliminates Capital.com 16-hour lookback clipping, avoids REST rate limit spikes | ✓ Good |
| Pure local storage on SSD | Zero egress costs, no network latency, works on Mac Mini and Windows | ✓ Good |
| 100% Separate Dedicated DuckDB Files | Completely avoids DuckDB file write-lock contention between 24/7 streamer and REST harvester/web dashboard | ✓ Good |
| Raw Tick Quotes for Capital.com Streaming | Retains full price/quote fidelity at tick level in dedicated `streaming.duckdb` | ✓ Good |
| Deactivate Binance from Streaming Runner | User specified webstreaming only saves data from Capital.com | ✓ Good |
| Dynamic Live Reload for Subscriptions | Enables adding/removing tracked symbols without stopping the always-on daemon | ✓ Good |
| Lightweight Python API + Vanilla JS/Tailwind Dashboard | Zero-build simplicity, instant browser access on localhost:8000, easy maintenance | ✓ Good |
| In-Process Adaptive Configuration Matching | Solves DuckDB's in-process `read_only` configuration conflict by adaptively matching existing open mode | ✓ Good |
| Intermittent Flush Locking in Runner | Opens connection only during ~5ms flush window, keeping file unlocked 99.5% of the time for readers | ✓ Good |
| Vectorized Release Data Consolidation | Uses DuckDB's native SQLite scanner to merge, deduplicate, and tier 9.4M rows in 11.64s | ✓ Good |

---
*Last updated: 2026-09-25 after v2.0 milestone completion*
