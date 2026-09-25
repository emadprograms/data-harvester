# Data Harvester — Local DuckDB & Live Streaming Engine

## What This Is
A high-performance, 100% local market data harvesting and streaming engine running on macOS (Mac Mini) and Windows. It captures live market ticks and 1-minute bars from Capital.com and Binance via persistent WebSockets, storing historical data in a local DuckDB columnar database for instant analysis and candlestick charting.

## Core Value
Zero-cloud, zero-quota persistent market data ingestion and storage: capture real-time market data reliably and provide sub-millisecond OHLCV querying without hitting API limits or heating up hardware.

## Requirements

### Validated
- [x] Initial market data harvesting logic and provider integrations (archived on `v1-archive` branch)

### Active
- [ ] **EXTR-01**: One-time zero-read extraction of historical data from Turso Archive into local SQLite/DuckDB
- [ ] **PURG-01**: Purge all legacy Turso, Docker (`.devcontainer`), instruction (`.gemini`), and GitHub Actions files
- [ ] **DUCK-01**: Implement native DuckDB database layer (`market_data.duckdb`) with optimized schema and batch ingestion
- [ ] **DUCK-02**: Implement high-speed `time_bucket()` resampling queries for OHLCV candlesticks
- [ ] **STRM-01**: Implement Capital.com live WebSocket streaming with automatic session authentication and heartbeats
- [ ] **STRM-02**: Implement Binance 24/7 live WebSocket streaming for crypto and gold
- [ ] **STRM-03**: Continuous background ingestion pipeline buffering ticks and writing cleanly to DuckDB
- [ ] **QUAL-01**: Comprehensive integration tests verifying live streaming write and DuckDB query performance

### Out of Scope
- Direct `market-rewind` frontend modifications (deferred per user instruction: focus on database & streaming first)
- Cloudflare R2 / S3 remote object storage (user decided on 100% local architecture)
- Turso dual-replica synchronization and mirror database (deprecated and purged)
- GitHub Actions scheduled runs (deprecated in favor of persistent local daemon)

## Context
- The previous implementation relied on Turso cloud SQLite with complex dual-replica mirror synchronization, which suffered from severe read/write quota exhaustion.
- The user is deploying this on an always-on Mac Mini (and later a Windows machine), making an always-on WebSocket stream with local storage the superior architecture.
- DuckDB provides columnar, vectorized C++ execution that prevents Mac CPU heating and delivers 10x-100x faster candlestick resampling.

## Constraints
- **Zero Cloud Limits**: No dependence on cloud database quotas (Turso, Supabase, etc.).
- **100% Local**: All data stored locally in `data/market_data.duckdb`.
- **Persistent Streams**: Real-time tick and 1-minute bar capture via Capital.com and Binance WebSockets.
- **Mac Mini & Windows Portability**: Standard Python 3.12+ cross-platform execution.

## Key Decisions
| Decision | Rationale | Outcome |
|----------|-----------|---------|
| Migrate from Turso to DuckDB | Eliminates cloud read/write quotas, prevents CPU overheating, provides native `time_bucket` resampling | ✓ Good |
| Switch from 6x daily REST batch to 24/7 WebSockets | Captures real-time ticks, eliminates Capital.com 16-hour lookback clipping, avoids REST rate limit spikes | ✓ Good |
| Pure local storage on SSD | Zero egress costs, no network latency, works on Mac Mini and Windows | ✓ Good |
| Defer `market-rewind` integration | Focus exclusively on robust data capture and database engine first | ✓ Good |

## Evolution
This document evolves at phase transitions and milestone boundaries.
