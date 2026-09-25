# Roadmap: Data Harvester (Local DuckDB & Live Streaming Engine)

## Overview
Transform the data-harvester from a cloud-constrained Turso/GitHub Actions batch runner into an always-on, high-performance local streaming and DuckDB engine. We first migrate existing Turso data to DuckDB and purge legacy cloud artifacts, build the native DuckDB storage layer, implement 24/7 WebSocket streaming for Capital.com and Binance, and verify everything with comprehensive tests.

## Phases

**Phase Numbering:**
- Integer phases (1, 2, 3): Planned milestone work
- Decimal phases (2.1, 2.2): Urgent insertions (marked with INSERTED)

- [ ] **Phase 1: Turso Data Migration & Legacy Purge** - Extract existing historical data into DuckDB, purge Turso and cloud artifacts.
- [ ] **Phase 2: DuckDB Storage Engine** - Build native DuckDB connection, time-series schema, batch inserts, and candle resampling.
- [ ] **Phase 3: 24/7 Live WebSocket Streaming** - Persistent real-time streaming for Capital.com and Binance with auto-reconnect.
- [ ] **Phase 4: Test Suite Migration & Verification** - End-to-end integration tests verifying live data write, DuckDB queries, and test suite.

## Phase Details

### Phase 1: Turso Data Migration & Legacy Purge
**Goal**: Pull all historical data from Turso Archive into a local DuckDB file without burning read quotas, then purge legacy cloud files and outdated dependencies.
**Depends on**: Nothing (first phase)
**Requirements**: MIGR-01, MIGR-02, MIGR-03
**Success Criteria**:
  1. Historical market data from Turso is extracted into `data/market_data.duckdb`.
  2. `.gemini/`, `.devcontainer/`, and `.github/workflows/harvest.yml` are removed.
  3. `requirements.txt` has `libsql` removed and `duckdb` + `websockets` added.
**Plans**: 1 plan

### Phase 2: DuckDB Storage Engine
**Goal**: Create a robust, high-performance DuckDB data access layer with optimized time-series schemas, batch upsert/insert operations, and fast `time_bucket` OHLCV aggregation.
**Depends on**: Phase 1
**Requirements**: DUCK-01, DUCK-02, DUCK-03, DUCK-04
**Success Criteria**:
  1. `src/database/duckdb_manager.py` manages connections and initializes the `market_data` table schema.
  2. Batch ingestion flushes buffered ticks/candles into DuckDB efficiently.
  3. `time_bucket()` resampling queries aggregate 1m bars into 5m/15m/1h/1d candles in <10ms.
**Plans**: 1 plan

### Phase 3: 24/7 Live WebSocket Streaming
**Goal**: Implement an always-on streaming runner that connects to Capital.com and Binance WebSockets, streaming real-time ticks and candles into DuckDB with automatic reconnection.
**Depends on**: Phase 2
**Requirements**: STRM-01, STRM-02, STRM-03, STRM-04
**Success Criteria**:
  1. Capital.com WebSocket client authenticates, manages heartbeats, and streams prices.
  2. Binance WebSocket client connects to 24/7 streams for crypto and gold.
  3. Ingestion pipeline buffers incoming ticks and writes cleanly to DuckDB without CPU spikes.
  4. Connection auto-reconnects with backoff if network drops.
**Plans**: 1 plan

### Phase 4: Test Suite Migration & Verification
**Goal**: Update the entire test suite to validate DuckDB operations, streaming ingestion, and query performance.
**Depends on**: Phase 3
**Requirements**: TEST-01, TEST-02
**Success Criteria**:
  1. All test fixtures run against temporary/in-memory DuckDB instances.
  2. Automated tests verify end-to-end ingestion and candlestick generation.
  3. Pytest suite passes 100%.
**Plans**: 1 plan
