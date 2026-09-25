# Data Harvester — Local DuckDB & Live Streaming Engine

## What This Is
A high-performance, 100% local market data harvesting and streaming engine running on macOS (Mac Mini) and Windows. It captures live market ticks from Capital.com and Binance via persistent WebSockets, storing real-time tick-by-tick data in `streaming.db` and historical 1-minute bars in `market_data.duckdb` for instant analysis and candlestick charting.

## Core Value
Zero-cloud, zero-quota persistent market data ingestion and storage: capture real-time market data reliably and provide sub-millisecond OHLCV querying without hitting API limits or heating up hardware.

## Current Milestone: v2.0 Dedicated Dual-DuckDB Storage, Capital.com Tick Streamer & Data Integrity Web Dashboard

**Goal:** Decouple REST and Streaming into 100% separate dedicated DuckDB files, capture raw tick quotes exclusively from Capital.com with dynamic symbol reload, and provide an interactive JavaScript data integrity & symbol management web dashboard.

**Target features:**
- 100% separate dedicated `.duckdb` files (`data/historical.duckdb` for 1m REST bars and `data/streaming.duckdb` for raw streaming ticks) with zero write-lock contention.
- Dedicated Capital.com WebSocket streaming engine capturing raw tick quotes (`timestamp`, `symbol`, `bid`, `ask`, `price`, `volume`, `source`) with Binance deactivated from the streaming runner.
- Dynamic live reload of symbol subscriptions via database/API without process restarts.
- Data integrity test engine checking missing minutes/gaps, OHLCV sanity, and REST vs stream drift.
- Local JavaScript web dashboard for system health, integrity audits, and symbol management served on localhost:8000.

## Current State (Shipped v1.0)
- **Historical Database**: 3,949,885 rows across 40 symbols migrated into DuckDB. Sub-10ms dynamic OHLCV candlestick resampling via DuckDB `time_bucket()`.
- **Live Streaming**: Multi-broker WebSocket streamer implemented.
- **Test Suite**: 127 automated unit, integration, and acceptance tests passing cleanly with zero cloud dependencies.

## Requirements

### Validated
- [x] **EXTR-01**: One-time zero-read extraction of historical data from Turso Archive into local DuckDB (3.95M rows) — v1.0
- [x] **PURG-01**: Purged legacy Turso, Docker (`.devcontainer`), instruction (`.gemini`), and GitHub Actions files — v1.0
- [x] **DUCK-01**: Implemented native DuckDB database layer (`market_data.duckdb`) with optimized schema and batch ingestion — v1.0
- [x] **DUCK-02**: Implemented high-speed `time_bucket()` resampling queries for OHLCV candlesticks — v1.0
- [x] **STRM-01**: Implemented Capital.com live WebSocket streaming with automatic session authentication and heartbeats — v1.0
- [x] **STRM-02**: Implemented Binance 24/7 live WebSocket streaming for crypto and gold — v1.0
- [x] **STRM-03**: Continuous background ingestion pipeline buffering ticks and writing cleanly to DuckDB — v1.0
- [x] **STRM-04**: Dedicated pure tick-by-tick storage via Binance `@trade` and Capital quotes — v1.0
- [x] **QUAL-01**: Comprehensive integration and acceptance tests verifying live streaming write and DuckDB query performance (127 tests passing) — v1.0

### Active (v2.0)
- [ ] **DUAL-01**: 100% separate dedicated DuckDB files (`data/historical.duckdb` and `data/streaming.duckdb`) with isolated connection pools and `ATTACH` support.
- [ ] **DUAL-02**: Dedicated schema for `streaming.duckdb` storing raw tick quotes (`streaming_ticks` table).
- [ ] **STRM-05**: Capital.com exclusive streaming runner with Binance deactivated.
- [ ] **STRM-06**: Dynamic live reload of symbol subscriptions without restarting the streaming runner process.
- [ ] **INTG-01**: Automated gap and continuity detection for historical 1-minute bars during trading sessions.
- [ ] **INTG-02**: OHLCV data anomaly and sanity validator (negative prices, high < low, price spikes, nulls).
- [ ] **INTG-03**: REST vs WebSocket price drift and reconciliation analyzer.
- [ ] **DASH-01**: Lightweight local web server exposing REST endpoints for database health, integrity metrics, and symbol management.
- [ ] **DASH-02**: Interactive JavaScript single-page web dashboard displaying live heartbeat, integrity test results, and gap analysis.
- [ ] **DASH-03**: Full Symbol Management in web dashboard allowing users to add, remove, and trigger dynamic subscription reloads.
- [ ] **QUAL-02**: Comprehensive automated test suite validating dual DuckDB files, streaming raw ticks, live symbol reload, integrity checks, and dashboard APIs.

### Out of Scope
- Direct `market-rewind` frontend modifications (deferred per user instruction: focus on database, streamer, and dashboard first)
- Cloudflare R2 / S3 remote object storage (user decided on 100% local architecture)
- Turso dual-replica synchronization and mirror database (deprecated and purged)
- GitHub Actions scheduled runs (deprecated in favor of persistent local daemon)
- Binance live streaming writes (streaming table is dedicated exclusively to Capital.com)

## Context
- The previous implementation relied on Turso cloud SQLite with complex dual-replica mirror synchronization, which suffered from severe read/write quota exhaustion.
- In v2.0, the user specifically requested 100% separate dedicated `.duckdb` files to avoid DuckDB single-writer lock contention between the 24/7 streaming engine and historical batch harvesting or web dashboard queries.
- Raw tick quotes are stored for Capital.com to retain granular bid/ask/price precision without quantization.
- A modern local web dashboard provides immediate observability into database health, data integrity, and interactive symbol management.

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
| 100% Separate Dedicated DuckDB Files | Completely avoids DuckDB file write-lock contention between 24/7 streamer and REST harvester/web dashboard | Pending Phase 5 |
| Raw Tick Quotes for Capital.com Streaming | Retains full price/quote fidelity at tick level in dedicated `streaming.duckdb` | Pending Phase 6 |
| Deactivate Binance from Streaming Runner | User specified webstreaming only saves data from Capital.com | Pending Phase 6 |
| Dynamic Live Reload for Subscriptions | Enables adding/removing tracked symbols without stopping the always-on daemon | Pending Phase 6 |
| Lightweight Python API + Vanilla JS/Tailwind Dashboard | Zero-build simplicity, instant browser access on localhost:8000, easy maintenance | Pending Phase 8 |

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
*Last updated: 2026-09-25 after v2.0 milestone initialization*
