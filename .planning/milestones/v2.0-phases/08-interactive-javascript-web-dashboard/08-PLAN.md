# Phase 8 Plan: Interactive JavaScript Web Dashboard & Symbol Management

**Goal:** Build a lightweight local web backend and responsive single-page web dashboard on `http://localhost:8000` for system observability, live stream monitoring, interactive integrity audits, and dynamic symbol management.

**Requirements:** DASH-01, DASH-02, DASH-03, DASH-04

## Tasks

### Task 1: Python Web Backend Service (`src/dashboard/server.py`)
- Zero-dependency HTTP server with REST routing and CORS support.
- API Endpoints:
  - `GET /api/status`: Overall system health, physical DuckDB file sizes, row counts, latest timestamps.
  - `GET /api/symbols`: Full inventory of tracked symbols with ticker mappings.
  - `POST /api/symbols`: Add new symbol to `symbol_map`.
  - `DELETE /api/symbols/<symbol>`: Remove symbol from `symbol_map`.
  - `GET /api/integrity`: Run automated integrity checks on demand (1m gaps, OHLCV anomalies, price drift).
  - `POST /api/streamer/reload`: Trigger dynamic symbol reload on the live streaming runner.
  - Static file route `/` serving the single-page dashboard.

### Task 2: Interactive JavaScript Single-Page Dashboard (`src/dashboard/static/index.html`)
- Modern, clean, dark-mode financial terminal aesthetic using Tailwind CSS (via CDN) and vanilla JS (zero build step).
- **Tab 1: System Overview & Live Heartbeat**:
  - Health cards: `historical.duckdb` status & size, `streaming.duckdb` status & size, total rows.
  - Live quote ticker displaying latest recorded price and timestamp per symbol.
- **Tab 2: Data Integrity & Health Audits**:
  - Interactive "Run Integrity Audit" button.
  - Pass/Warn/Fail status matrix.
  - Visual Gap Explorer detailing missing intervals and duration.
  - Anomaly table highlighting suspicious prices or high < low inversions.
  - REST vs Streaming drift report.
- **Tab 3: Symbol Management**:
  - Interactive symbol table displaying display name, Capital ticker, Massive/Polygon ticker, Binance ticker.
  - "Add Symbol" modal/form with validation.
  - "Remove" action button with confirmation.
  - "Reload Streamer Now" button to dynamically sync subscriptions without daemon restarts.

### Task 3: Comprehensive Test Suite (`tests/test_dashboard_server_api.py`)
- Test all REST endpoints:
  - `GET /api/status` returns valid 200 JSON with database metrics.
  - `GET /api/symbols` returns symbol list.
  - `POST /api/symbols` adds new symbol.
  - `DELETE /api/symbols/<symbol>` removes symbol.
  - `GET /api/integrity` runs checks and returns audit structure.
  - `POST /api/streamer/reload` triggers reload successfully.
  - Static file handler serves index.html with 200 OK.

### Task 4: Verification & Regression Testing
- Run full pytest test suite to ensure 100% pass rate.
- Produce `08-SUMMARY.md`.
