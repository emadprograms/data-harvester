# Requirements: Data Harvester (Local DuckDB & Live Streaming)

**Defined:** 2026-09-25
**Core Value:** Zero-cloud, zero-quota persistent market data ingestion and storage: capture real-time market data reliably and provide sub-millisecond OHLCV querying without hitting API limits or heating up hardware.

## v1 Requirements

### Data Migration & Legacy Purge
- [ ] **MIGR-01**: One-time export of existing historical data from Turso Archive into local SQLite/DuckDB without burning row read quotas
- [ ] **MIGR-02**: Purge legacy Turso, Docker (`.devcontainer`), instruction (`.gemini`), and GitHub Actions workflow files
- [ ] **MIGR-03**: Update dependencies in `requirements.txt` to remove `libsql` and add `duckdb` and `websockets`

### DuckDB Storage Engine
- [ ] **DUCK-01**: Implement thread-safe DuckDB connection manager for `data/market_data.duckdb`
- [ ] **DUCK-02**: Initialize optimized time-series table schema (`timestamp`, `symbol`, `open`, `high`, `low`, `close`, `volume`, `session`, `source`) with indexes
- [ ] **DUCK-03**: Implement buffered batch upsert/insert operations in DuckDB
- [ ] **DUCK-04**: Implement high-performance `time_bucket()` resampling function to produce custom candlestick timeframes (1m, 5m, 15m, 1h, 1d) in single-digit milliseconds

### 24/7 Live WebSocket Streaming
- [ ] **STRM-01**: Implement Capital.com WebSocket client with authentication, token refresh, and heartbeat management
- [ ] **STRM-02**: Implement Binance 24/7 public WebSocket client for crypto (`*USDT`) and gold (`PAXGUSDT`)
- [ ] **STRM-03**: Implement persistent streaming runner with automatic reconnection and exponential backoff
- [ ] **STRM-04**: Ingest streaming events into DuckDB batch queue with minimal CPU footprint

### Verification & Testing
- [ ] **TEST-01**: Update test suite to run against DuckDB in-memory/temp databases
- [ ] **TEST-02**: Validate streaming ingestion and DuckDB OHLCV aggregation queries with automated integration tests

## Out of Scope
| Feature | Reason |
|---------|--------|
| `market-rewind` frontend modifications | Deferred per user instruction: focus on database building and streaming engine first |
| Cloudflare R2 / S3 storage | Switched to 100% local architecture |
| Turso dual-replica sync & mirror database | Deprecated and completely eliminated |
| GitHub Actions scheduling | Replaced with always-on local execution on Mac Mini |

## Traceability

| Requirement | Phase | Status |
|-------------|-------|--------|
| MIGR-01 | Phase 1 | Pending |
| MIGR-02 | Phase 1 | Pending |
| MIGR-03 | Phase 1 | Pending |
| DUCK-01 | Phase 2 | Pending |
| DUCK-02 | Phase 2 | Pending |
| DUCK-03 | Phase 2 | Pending |
| DUCK-04 | Phase 2 | Pending |
| STRM-01 | Phase 3 | Pending |
| STRM-02 | Phase 3 | Pending |
| STRM-03 | Phase 3 | Pending |
| STRM-04 | Phase 3 | Pending |
| TEST-01 | Phase 4 | Pending |
| TEST-02 | Phase 4 | Pending |

**Coverage:**
- v1 requirements: 13 total
- Mapped to phases: 13
- Unmapped: 0 ✓
