# Quick Task 260925-vhi: Separate Historical and Streaming Databases on Dashboard

## Description
Separate the historical database (`historical.duckdb`) and streaming database (`streaming.duckdb`) across backend APIs and frontend dashboard, removing artificial blending of streaming ticks into historical candles so each database is observed and analyzed independently.

## User Requirements & Architecture Context
- **Why Separate:** Streaming data is ephemeral and cannot be backfilled if the server was offline or during network dropouts. Historical data is canonical, backfillable via REST (Massive/Polygon, Capital, Binance) and permanent. Stitching them together creates false artifacts, confuses users, and makes data integrity auditing impossible.
- **REST Backward Harvester Architecture:**
  - Capital.com REST provides intraday/recent 1m bars as Tier 2 data.
  - Massive (Polygon.io) publishes canonical market data with a delay (after market close). Massive 1m bars are ingested as Tier 1 data and overwrite Capital.com provisional bars via DuckDB Source-Tiering Protection.
- **Dashboard Separation:**
  - Provide distinct views and controls for the **Historical Archive** and the **Live Stream Buffer**.
  - Remove silent blending in `get_candles` in `analytics.py`.
  - Provide explicit database source selection on charts (`source=historical` vs `source=streaming`).
  - Isolate Historical Gap Audits from Live Stream Quiet/Dropout Audits.

---

## Tasks

### Task 1: Decouple Backend Candle Queries & APIs
- **Files:** `src/dashboard/analytics.py`, `src/dashboard/server.py`
- **Actions:**
  - In `get_candles()`, remove automatic appending of ticks from `streaming.duckdb`.
  - Add `db_source: str = "historical"` parameter (accepts `"historical"` or `"streaming"`).
  - When `db_source == "historical"`, query exclusively `historical.duckdb`.
  - When `db_source == "streaming"`, query exclusively `streaming.duckdb` resampled ticks.
  - In `server.py`:
    - Accept `?source=historical` or `?source=streaming` on `/api/candles`.
    - Provide `/api/historical/candles` and `/api/streaming/candles`.
    - Provide `/api/historical/stats` and `/api/streaming/stats`.
- **Verification:**
  - Unit tests verifying `/api/candles?source=historical` contains zero streaming ticks and `/api/candles?source=streaming` queries `streaming.duckdb`.

### Task 2: Redesign Dashboard Frontend into Dedicated Observatories
- **Files:** `src/dashboard/static/index.html`
- **Actions:**
  - Update top KPI strip with clear visual separation between Historical Storage and Live Stream Buffer.
  - Add explicit database source toggle to the Chart toolbar:
    - `[ 🏛️ Historical DB (historical.duckdb) ]` (default)
    - `[ ⚡ Streaming DB (streaming.duckdb) ]`
  - Reorganize main tabs:
    1. **Charts & Telemetry**: with explicit database toggle and source badges.
    2. **Streaming Buffer Tape**: dedicated live tick feed, process status, and tick rates.
    3. **Historical Archive & Inventory**: symbol coverage table, date ranges, total bars, source breakdown.
    4. **Integrity & Audits**: separated into Historical Gaps vs Streaming Dropouts.
    5. **Harvester**: REST harvester runner.
  - Update JavaScript fetchers (`loadChartData`, etc.) to pass the chosen database source.
- **Verification:**
  - Check browser rendering, toggle between Historical and Streaming, verify legend and raw candle inspector update accurately.

### Task 3: Automated Testing & Verification
- **Files:** `tests/test_dashboard_separated_observatories.py`
- **Actions:**
  - Write unit and integration tests verifying clean separation of historical vs streaming APIs.
  - Run `pytest tests/` to verify all existing and new tests pass.
  - Verify live HTTP responses from `http://localhost:8000`.
