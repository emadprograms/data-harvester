# Roadmap: Milestone v2.0
Dedicated Dual-DuckDB Storage, Capital.com Tick Streamer & Data Integrity Web Dashboard

## Overview

Transform the data-harvester architecture to use 100% separate dedicated DuckDB database files to eliminate write-lock contention between the always-on streaming engine and the historical REST harvester. Refactor the streaming engine to stream raw tick quotes exclusively from Capital.com with dynamic subscription reload. Build a comprehensive data integrity test engine and a modern, responsive JavaScript web dashboard for real-time observability and interactive symbol management.

## Phases

**Phase Numbering:**
- Integer phases continuing from Milestone v1.0 (Phases 1–4 completed):
- Active phases: 5, 6, 7, 8, 9

- [x] **Phase 5: Dedicated Dual-DuckDB Storage Layer** - Separate DuckDB databases for historical 1m data and live streaming raw ticks with isolated connection pools and ATTACH capabilities.
- [x] **Phase 6: Capital.com Exclusive WebSocket Streamer & Dynamic Reload** - Stream raw tick quotes exclusively from Capital.com with dynamic subscription management without process restart.
- [x] **Phase 7: Data Integrity & Health Engine** - Implement gap detection, OHLCV sanity checks, quiet interval detection, and cross-database price drift reconciliation.
- [ ] **Phase 8: Interactive JavaScript Web Dashboard & Symbol Management** - Build a lightweight local web backend and responsive single-page web UI on localhost:8000 for health status, integrity audits, and symbol management.
- [ ] **Phase 9: End-to-End Verification & Full Test Suite** - Comprehensive testing and validation across all components, dynamic reload workflows, and regression prevention.

## Phase Details

### Phase 5: Dedicated Dual-DuckDB Storage Layer
**Goal**: Decouple storage into two 100% dedicated `.duckdb` files (`data/historical.duckdb` and `data/streaming.duckdb`) with zero lock contention, multi-database query attachment, and dedicated raw tick schema.
**Depends on**: Milestone v1.0
**Requirements**: DUAL-01, DUAL-02, DUAL-03
**Success Criteria**:
  1. `src/database/connection.py` provides independent connection factories (`get_historical_db_connection` and `get_streaming_db_connection`) and cross-database `ATTACH` support.
  2. `src/database/schema.py` initializes `streaming.duckdb` with the `streaming_ticks` table (`timestamp`, `symbol`, `bid`, `ask`, `price`, `volume`, `source`) and optimized time-series indexes.
  3. `historical.duckdb` maintains canonical 1-minute OHLCV candles (`market_data` table) and `symbol_map`.
  4. Concurrent writes to `streaming.duckdb` do NOT block reads or batch writes to `historical.duckdb`.

### Phase 6: Capital.com Exclusive WebSocket Streamer & Dynamic Reload
**Goal**: Refactor the streaming engine to stream raw tick quotes exclusively from Capital.com, deactivating Binance, and supporting dynamic symbol subscription reload without process restart.
**Depends on**: Phase 5
**Requirements**: STRM-05, STRM-06, STRM-07
**Success Criteria**:
  1. Binance streamer is deactivated from the live streaming runner; streaming runner connects exclusively to Capital.com.
  2. Streamer captures raw tick quotes (timestamp, epic/symbol, bid, ask, price, volume) and flushes them to `data/streaming.duckdb` via batched transactions.
  3. Dynamic symbol reload mechanism reloads active subscriptions upon event signal or database change without dropping the WebSocket session.
  4. Automatic reconnection with exponential backoff and heartbeat management operates reliably.

### Phase 7: Data Integrity & Health Engine
**Goal**: Build a rigorous data integrity auditing engine in `src/utils/integrity.py` to identify missing time intervals, abnormal price action, and cross-database discrepancies.
**Depends on**: Phase 5, Phase 6
**Requirements**: INTG-01, INTG-02, INTG-03, INTG-04
**Success Criteria**:
  1. Historical gap detection scans for missing 1-minute bars during US market hours (09:30–16:00 ET) and outputs structured gap intervals.
  2. Stream continuity monitor flags quiet intervals or stalled tick feeds per symbol.
  3. Anomaly detection identifies zero/negative prices, `high < low`, out-of-range open/close, and sudden extreme outlier spikes.
  4. Cross-database drift analyzer compares historical REST close prices against streaming tick quotes for overlapping time windows.

### Phase 8: Interactive JavaScript Web Dashboard & Symbol Management
**Goal**: Create a lightweight Python web backend and a modern single-page JavaScript/Tailwind web dashboard on `http://localhost:8000` for system observability, integrity test execution, and symbol management.
**Depends on**: Phase 7
**Requirements**: DASH-01, DASH-02, DASH-03, DASH-04
**Success Criteria**:
  1. Backend server starts cleanly on `localhost:8000` exposing REST endpoints (`/api/status`, `/api/integrity`, `/api/symbols`, `/api/streamer/reload`).
  2. Single-page UI renders system health, streamer status, latest prices/timestamps, and database storage sizes.
  3. Data Integrity view allows one-click audit execution with visual pass/warn/fail matrix and gap explorer.
  4. Symbol Management UI allows adding/removing tracked symbols and triggering live streamer reload directly from the browser.

### Phase 9: End-to-End Verification & Full Test Suite
**Goal**: Implement comprehensive automated tests across all new modules and verify end-to-end reliability of dual-database storage, streaming ingestion, dynamic reload, and dashboard APIs.
**Depends on**: Phase 5, Phase 6, Phase 7, Phase 8
**Requirements**: QUAL-02, QUAL-03
**Success Criteria**:
  1. Dual-DuckDB isolation tests verify zero lock contention under concurrent load.
  2. Streaming tick ingestion tests verify accurate quote capture and batch writing.
  3. Dashboard API test suite validates all endpoints, symbol lifecycle, and live reload signals.
  4. 100% of tests pass across the entire project test suite.
