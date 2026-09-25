# Data Harvester — Local DuckDB & Live Streaming Engine

## What This Is
A high-performance, 100% local market data harvesting and streaming engine running on macOS (Mac Mini) and Windows. It captures live market ticks from Capital.com and Binance via persistent WebSockets, storing real-time tick-by-tick data in `streaming.db` and historical 1-minute bars in `market_data.duckdb` for instant analysis and candlestick charting.

## Core Value
Zero-cloud, zero-quota persistent market data ingestion and storage: capture real-time market data reliably and provide sub-millisecond OHLCV querying without hitting API limits or heating up hardware.

## Current State (Shipped v1.0)
- **Historical Database**: 3,949,885 rows across 40 symbols migrated into `data/market_data.duckdb`. Sub-10ms dynamic OHLCV candlestick resampling via DuckDB `time_bucket()`.
- **Live Streaming**: 24/7 multi-broker WebSocket streamer saving pure tick-by-tick data to `data/streaming.db` (Binance `@trade` trades for crypto/gold and Capital.com real-time quotes for equities/indices).
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
- [x] **STRM-04**: Dedicated pure tick-by-tick storage in `streaming.db` via Binance `@trade` and Capital quotes — v1.0
- [x] **QUAL-01**: Comprehensive integration and acceptance tests verifying live streaming write and DuckDB query performance (127 tests passing) — v1.0

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
- **100% Local**: All data stored locally in `data/market_data.duckdb` and `data/streaming.db`.
- **Persistent Streams**: Real-time tick capture via Capital.com and Binance WebSockets.
- **Mac Mini & Windows Portability**: Standard Python 3.12+ cross-platform execution.

## Key Decisions
| Decision | Rationale | Outcome |
|----------|-----------|---------|
| Migrate from Turso to DuckDB | Eliminates cloud read/write quotas, prevents CPU overheating, provides native `time_bucket` resampling | ✓ Good |
| Switch from 6x daily REST batch to 24/7 WebSockets | Captures real-time ticks, eliminates Capital.com 16-hour lookback clipping, avoids REST rate limit spikes | ✓ Good |
| Pure local storage on SSD | Zero egress costs, no network latency, works on Mac Mini and Windows | ✓ Good |
| Store tick-by-tick data in `streaming.db` | Captures microsecond/millisecond price action directly from Binance `@trade` and Capital quotes without minute quantization | ✓ Good |
| Defer `market-rewind` integration | Focus exclusively on robust data capture and database engine first | ✓ Good |

---
*Last updated: 2026-09-25 after v1.0 milestone*
