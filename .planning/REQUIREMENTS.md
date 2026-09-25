# Requirements: Milestone v3.0
# Observability Command Center, Interactive Financial Charts & Live Telemetry Dashboard

## Milestone Goal
Transform the local web dashboard into an enterprise-grade Observability Command Center for the Data Harvester. Provide interactive financial candlestick charts across ~8M historical candles, real-time live tick streaming tape and process telemetry, a rich per-symbol coverage matrix, market session clock & automation triggers, and context-aware data integrity auditing with comprehensive automated tests.

---

## Requirements

### 1. Interactive Financial Charts & Data Explorer
- [x] **CHRT-01**: High-performance backend DuckDB candle query API (`GET /api/candles`) supporting symbol selection, native `time_bucket()` timeframe aggregation (`1m`, `5m`, `15m`, `1h`, `1D`), date range boundaries, and pagination/limits.
- [x] **CHRT-02**: Interactive Financial Candlestick & Volume Chart embedded in the web dashboard powered by TradingView Lightweight Charts (responsive canvas, sub-50ms render, crosshair tooltips).
- [x] **CHRT-03**: Interactive Raw Candle Inspector & Exporter allowing inspection of OHLCV bars with source badges, search, pagination, and one-click CSV download.

### 2. Live Stream Telemetry & Ticker Tape
- [x] **STRM-08**: Live Stream Telemetry & Ticker Tape API (`GET /api/stream/tape`, `GET /api/stream/status`) exposing incoming ticks, bid/ask spreads, streamer process liveness (PID/alive status), and tick throughput rate.
- [x] **STRM-09**: Real-time Ticker Wall & Streamer Control UI displaying live price movements with visual flash feedback, ticker tape, and verified stream reload/status indicators.

### 3. Market Session & Operational Command
- [x] **MKT-01**: Market Session Status API (`GET /api/market/session`) calculating current ET/UTC time, active session phase (Pre-market, Regular, After-hours, Closed), and time remaining until the 8:00 PM ET session boundary.
- [x] **MKT-02**: Live Market Clock & Session Bar in dashboard header showing session phase and countdowns.
- [x] **HRVST-01**: Harvester Runner API (`POST /api/harvester/run`, `GET /api/harvester/status`) enabling triggering of `main.py` harvests for specific trading dates directly from the web interface.
- [x] **HRVST-02**: Real-time Log Console Drawer in the web UI streaming stdout/stderr execution output of harvester and streamer tasks.

### 4. Rich Symbol Matrix & Context-Aware Integrity Engine
- [x] **SYMB-01**: Symbol Coverage & Health API (`GET /api/symbols/coverage`) providing aggregate bar counts, date ranges, latest close prices, and primary data sources per symbol.
- [x] **SYMB-02**: Rich Per-Symbol Data Matrix UI with search, sorting, source distribution badges, and one-click "View Chart" and "Audit Gaps" actions.
- [x] **INTG-05**: Context-aware Gap & Integrity Auditor allowing symbol-specific and date-range-specific scans with market-hours awareness (preventing false alerts on weekends/overnight closures).

### 5. Automated Testing & Verification
- [x] **QUAL-04**: Comprehensive automated test suite in `tests/test_dashboard_v3.py` covering all new REST APIs, query aggregations, session math, streaming endpoints, and end-to-end integration.

---

## Non-Functional Requirements
- **Sub-50ms API Latency**: Fast DuckDB analytical queries over 8M rows using native columnar aggregations.
- **Zero Build Complexity**: Vanilla ES6 JavaScript + Tailwind CSS CDN + TradingView Lightweight Charts CDN. Zero npm, webpack, or node builds required to run the dashboard.
- **Zero Downtime Streaming**: Web dashboard reads from `streaming.duckdb` and `historical.duckdb` using read-only connections without locking the persistent streaming runner.
- **Cross-Platform**: Runs seamlessly on macOS (Darwin) and Windows.
