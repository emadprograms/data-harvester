# Quick Task 260925-vqd: Refactor index.html and Expand Test Coverage

## Description
Refactor `src/dashboard/static/index.html` from a monolithic ~1,578-line file into modular, clean components (extracting styles into `css/dashboard.css` and JavaScript controller logic into organized modules under `js/`), implement robust static asset serving with security guarantees in `src/dashboard/server.py`, and expand comprehensive test coverage for static asset serving and dashboard API endpoints.

## User Requirements & Architecture Context
- **Refactoring Goals:**
  - `src/dashboard/static/index.html` previously embedded 960+ lines of JavaScript and inline CSS directly in the HTML document.
  - Break down frontend logic into clean, cohesive modules:
    - `css/dashboard.css`: Visual styling, keyframe animations, scrollbar styling.
    - `js/state.js`: Global configuration, UI state management, and toast notifications.
    - `js/chart.js`: TradingView Lightweight Charts setup, candle rendering, timescale formatting, timeframe/source toggling.
    - `js/tables.js`: Raw candle inspector table rendering/filtering, CSV export, symbol coverage matrix.
    - `js/telemetry.js`: Live stream tick tape, stream process status, market session clock & countdown, system health.
    - `js/harvester.js`: Harvester trigger execution, live log polling, symbol CRUD modal handlers, streamer reload.
    - `js/app.js`: Tab navigation router, master telemetry polling loop, and application boot sequence.
  - `index.html` becomes a clean, readable, semantic HTML layout (~550 lines) importing these modules.
- **Server Static Asset Serving:**
  - `src/dashboard/server.py` currently only serves `/` and `/index.html`.
  - Add static file handler serving `/static/...` (and root-relative assets) with exact MIME types (`text/css`, `application/javascript`, `image/x-icon`, etc.).
  - Security: Strong path traversal prevention (`os.path.commonpath`), ensuring no files outside `STATIC_DIR` can be accessed.
- **Comprehensive Testing:**
  - Verify static file serving (200 OK, MIME types, 404 for missing assets, 403/404 for path traversal).
  - Verify all dashboard REST API endpoints (status, symbols, coverage, market session, stream tape/status, historical/streaming candles, harvester endpoints).
  - Guarantee zero regressions across all 187 existing tests.

---

## Tasks

### Task 1: Add Robust Static Asset Serving to Dashboard Server
- **Files:** `src/dashboard/server.py`
- **Actions:**
  - Implement `_serve_static(path)` in `DashboardRequestHandler` to serve files from `STATIC_DIR`.
  - Handle MIME types: `.html`, `.css`, `.js`, `.json`, `.svg`, `.png`, `.ico`.
  - Add strict directory traversal security check: verify canonical path stays strictly inside `STATIC_DIR`.
  - Handle `GET /static/<rel_path>` and direct requests.
- **Verification:**
  - Test static file resolution and directory traversal rejection.

### Task 2: Modularize Dashboard Frontend (CSS & JavaScript)
- **Files:**
  - `src/dashboard/static/css/dashboard.css`
  - `src/dashboard/static/js/state.js`
  - `src/dashboard/static/js/chart.js`
  - `src/dashboard/static/js/tables.js`
  - `src/dashboard/static/js/telemetry.js`
  - `src/dashboard/static/js/harvester.js`
  - `src/dashboard/static/js/app.js`
  - `src/dashboard/static/index.html`
- **Actions:**
  - Extract CSS into `src/dashboard/static/css/dashboard.css`.
  - Extract JS functions into their respective modular files under `src/dashboard/static/js/`.
  - Update `src/dashboard/static/index.html` to reference `<link rel="stylesheet" href="/static/css/dashboard.css">` and script tags.
  - Retain 100% of existing behavior and styling without regression.
- **Verification:**
  - Validate all static files exist and load correctly via curl / HTTP requests.

### Task 3: Expand Automated Test Coverage & Verify Zero Regressions
- **Files:**
  - `tests/test_dashboard_static_server.py`
  - `tests/test_dashboard_api_comprehensive.py`
- **Actions:**
  - Write test suite for static server: `/`, `/index.html`, `/static/css/dashboard.css`, `/static/js/app.js`, path traversal attacks (`/static/../../server.py`), 404 handling.
  - Write test suite for dashboard API endpoints: symbol inventory, coverage, market session, stream status, candles querying, error conditions.
  - Run full test suite (`pytest tests/ -v`) to verify all existing and new tests pass.
- **Verification:**
  - Confirm 200+ tests pass with zero failures.
