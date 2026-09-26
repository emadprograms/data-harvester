# 🚀 Stock Data Harvester & Observability Engine (v3.0)

A high-performance market data harvesting, 24/7 live tick streaming, and telemetry observability engine powered by local **DuckDB** storage, interactive financial charting, and real-time WebSocket ingestion.

---

## 🏛 Architecture Overview

### 💾 Dual-DuckDB Storage Engine
- **`data/historical.duckdb`**: Over 7.99 million canonical 1-minute OHLCV candles (spanning Oct 2024 to Jul 2026 across 40 symbols) with composite primary keys (`timestamp`, `symbol`), Source-Tiering quality overrides, and sub-10ms dynamic resampling (`time_bucket()`).
- **`data/streaming.duckdb`**: 24/7 continuous raw tick quotes captured from Capital.com (`timestamp`, `symbol`, `price`, `volume`, `bid`, `ask`, `source`, `session`). Decoupled from historical storage to eliminate writer lock contention.
- **UTC Storage Mandate**: every stored timestamp is pure UTC, and every DuckDB connection pins the session `TimeZone` to `UTC`, so query semantics never depend on the host OS timezone. Exchange-local rendering happens only at query/UI time.

### 🌐 Observability Command Center Dashboard
- Zero-dependency local multi-threaded HTTP server (`http://localhost:8000`).
- **TradingView Lightweight Charts** (v4.1.3) with dark-slate UI, multi-timeframe analytical resampling (`1m`, `5m`, `15m`, `30m`, `1h`, `4h`, `1D`), volume histograms, and session breakdown.
- **NYSE Exchange-Time Rendering**: chart X-axis ticks, crosshair badge, hover legend, raw candle inspector and CSV export are all labelled in `America/New_York` (EST/EDT), so the 09:30 opening bell — and its volume spike — always render at 09:30 ET no matter which timezone the server or the browser runs in. Candle API contract: `time` is a true UTC epoch, `time_str` is the exchange-local label, `timezone`/`time_epoch_basis` declare it.
- **Live Stream Tape**: Real-time tick wall with bid/ask/spread, process telemetry, and dynamic symbol reload.
- **Data Integrity Engine**: Automated 1-minute historical gap detection during US trading hours, stream freshness audits, OHLCV anomaly validation, and cross-database price drift reconciliation.
- **Harvester Console**: Browser-triggered runs of `main.py` with real-time log streaming.

---

## 📁 Repository Structure

```
data-harvester/
├── main.py                     # Primary market session harvesting entrypoint
├── requirements.txt            # Python dependencies (DuckDB, requests, pandas, etc.)
├── MILESTONES.md               # Historical milestones & delivery log (v1.0, v2.0, v3.0)
│
├── src/
│   ├── api/                    # API clients (Massive/Polygon, Capital.com, Binance, Yahoo)
│   ├── config.py               # Shared system constants & symbol defaults
│   ├── credentials.py          # Environment credentials & API keys loader
│   ├── dashboard/              # Observability Command Center backend & static frontend
│   │   ├── server.py           # Multi-threaded HTTP server with REST APIs & static handler
│   │   ├── analytics.py        # High-performance DuckDB analytical candle resampling
│   │   └── static/             # Frontend UI (HTML, CSS, JavaScript modular controllers)
│   ├── data/                   # Session harvesters, normalizers & Databento backfillers
│   ├── database/               # Native DuckDB connection pool, CRUD & resampling engine
│   ├── stream/                 # 24/7 Capital.com live WebSocket streaming runner
│   └── utils/                  # Data integrity health engine & audit diagnostics
│
└── tests/                      # Comprehensive test suite (262 passing, 5 skipped without API credentials)
    ├── conftest.py             # Global fixtures & environment mocks
    ├── api/                    # Tests for src/api/ connectors
    ├── config/                 # Tests for src/config.py & credentials
    ├── dashboard/              # Tests for dashboard analytics, REST APIs & static server
    ├── data/                   # Tests for session harvesting & normalizers
    ├── database/               # Tests for DuckDB storage, concurrency & resampling
    ├── stream/                 # Tests for live WebSocket streaming & dynamic reload
    ├── utils/                  # Tests for data integrity & health engine
    └── e2e/                    # End-to-end milestone & CLI smoke tests
```

---

## 🚀 Quickstart & Usage

### 1. Run Tests
Run the entire test suite across all nested test packages:
```bash
PYTHONPATH=. pytest tests/ -v
```

Or run a targeted sub-suite:
```bash
PYTHONPATH=. pytest tests/database/ -v    # Database & resampling tests
PYTHONPATH=. pytest tests/stream/ -v      # Live streaming tests
PYTHONPATH=. pytest tests/dashboard/ -v   # Command Center & API tests
```

### 2. Launch the Observability Command Center
Start the web dashboard server:
```bash
python3 -m src.dashboard.server
```
Then navigate to **http://localhost:8000** in your browser.

### 3. Launch the 24/7 Capital.com Streamer
Start the continuous live tick ingestion engine:
```bash
python3 -m src.stream.runner
```

### 4. Manual Session Harvest
Run the primary harvester for the current/most recent market session:
```bash
python3 main.py
```
Or harvest a specific historical market date:
```bash
python3 main.py --date 2026-04-15
```

---

## 📜 Milestones & Roadmap
Full details on shipped milestones (v1.0, v2.0, v3.0) are tracked in [MILESTONES.md](MILESTONES.md).
