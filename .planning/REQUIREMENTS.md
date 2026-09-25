# Requirements: Milestone v2.0
Dedicated Dual-DuckDB Storage, Capital.com Tick Streamer & Data Integrity Web Dashboard

**Defined:** 2026-09-25
**Core Value:** Zero-cloud, zero-quota persistent market data ingestion and storage: capture real-time market data reliably and provide sub-millisecond OHLCV querying without hitting API limits or heating up hardware.

## v2 Requirements

### Dedicated Dual-DuckDB Storage Layer

- [x] **DUAL-01**: Decouple database into 100% separate dedicated DuckDB files (`data/historical.duckdb` for 1m REST bars and `data/streaming.duckdb` for raw streaming ticks) with zero write-lock contention.
- [x] **DUAL-02**: Implement isolated connection factories (`get_historical_db_connection`, `get_streaming_db_connection`) with read-only concurrency support and multi-database `ATTACH` capabilities for cross-database queries.
- [x] **DUAL-03**: Create optimized schema and indexes for raw tick quotes in `data/streaming.duckdb` (`streaming_ticks` table: `timestamp`, `symbol`, `bid`, `ask`, `price`, `volume`, `source`).

### Capital.com Exclusive WebSocket Streaming & Dynamic Reload

- [x] **STRM-05**: Deactivate Binance from the live streaming runner; stream exclusively from Capital.com.
- [x] **STRM-06**: Ingest Capital.com raw tick quotes directly into `data/streaming.duckdb` using batched async writer.
- [x] **STRM-07**: Implement dynamic live reload of symbol subscriptions without restarting the streaming runner process.

### Data Integrity & Health Engine

- [x] **INTG-01**: Automated gap and continuity detection for historical 1-minute bars during market trading hours (09:30–16:00 ET).
- [x] **INTG-02**: Stream continuity and quiet interval detector for streaming ticks.
- [x] **INTG-03**: OHLCV sanity and anomaly detection (negative/zero prices, `high < low`, out-of-bounds open/close, nulls).
- [x] **INTG-04**: Cross-database drift analyzer comparing historical REST candle closes against streaming tick prices for overlapping intervals.

### Interactive JavaScript Web Dashboard & Symbol Management

- [x] **DASH-01**: Lightweight local Python backend server running on `http://localhost:8000` exposing REST endpoints for health, metrics, integrity audits, and symbol CRUD.
- [x] **DASH-02**: Responsive single-page JavaScript/Tailwind web dashboard displaying live streamer status, tick rates, latest prices, and storage sizes.
- [x] **DASH-03**: Interactive Data Integrity visual audit view with one-click test execution and pass/warn/fail matrix.
- [x] **DASH-04**: Symbol Management interface in the web dashboard allowing users to add/remove symbols with dynamic live streamer reload.

### Quality & Comprehensive Testing

- [x] **QUAL-02**: Comprehensive automated test suite validating dual DuckDB file isolation, streaming raw ticks, live symbol reload, and integrity algorithms.
- [x] **QUAL-03**: End-to-end API tests validating dashboard endpoints, symbol lifecycle, and live reload signals.

## Out of Scope

| Feature | Reason |
|---------|--------|
| Binance live streaming writes | Streaming table is dedicated exclusively to Capital.com |
| Cloud storage sync (S3/R2) | Preserving 100% local, zero-cloud architecture |
| Node.js build dependencies | Dashboard built with zero-build vanilla JS / Tailwind CDN for zero installation overhead |
| `market-rewind` frontend modifications | Focus exclusively on data harvesting, database engine, and integrity dashboard |

## Traceability

| Requirement | Phase | Status |
|-------------|-------|--------|
| DUAL-01 | Phase 5 | Completed |
| DUAL-02 | Phase 5 | Completed |
| DUAL-03 | Phase 5 | Completed |
| STRM-05 | Phase 6 | Completed |
| STRM-06 | Phase 6 | Completed |
| STRM-07 | Phase 6 | Completed |
| INTG-01 | Phase 7 | Completed |
| INTG-02 | Phase 7 | Completed |
| INTG-03 | Phase 7 | Completed |
| INTG-04 | Phase 7 | Completed |
| DASH-01 | Phase 8 | Completed |
| DASH-02 | Phase 8 | Completed |
| DASH-03 | Phase 8 | Completed |
| DASH-04 | Phase 8 | Completed |
| QUAL-02 | Phase 9 | Completed |
| QUAL-03 | Phase 9 | Completed |

**Coverage:**
- v2 requirements: 16 total
- Completed: 16 ✓
- Unmapped: 0 ✓
