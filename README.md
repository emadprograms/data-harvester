# 🚀 Stock Data Harvester (v7.3)

A high-performance, stateless data harvesting engine designed for scheduled daily automation. Optimized for resilience, integrity, and strict adherence to Turso DB write quotas.

## 🏛 Market Session Architecture

The harvester operates on a strict **Market Session** basis rather than calendar days.
- **Session Definition**: **8:00 PM ET (Previous Trading Day)** to **8:00 PM ET (Target Trading Day)**.
- **Weekend/Holiday Awareness**: If a target date falls on a weekend or holiday, the session automatically **rolls forward** to the next valid trading day (e.g., a Saturday run targets the Monday session, spanning from Friday 8 PM to Monday 8 PM).
- **Strict UTC**: All internal logic, API requests, and database storage use pure UTC.

## 🛠 Key Features

- **Session-Based Sourcing**:
    - **Active Session (Current)**: Uses **Capital.com** for live, minute-by-minute data (no volume). Implements an **Append/Update** strategy to preserve early-session data. Includes a 16-hour lookback auto-clamping mechanism.
    - **Completed Session (Past)**: Uses **Massive (Polygon.io)** for high-fidelity institutional OHLCV data. Implements a **Targeted Clean & Replace** strategy to ensure a perfect historical record.
- **Universal Sources**:
    - **Binance**: Primary source for Crypto (`*USDT`) and Gold (`PAXGUSDT`).
    - **Yahoo Finance**: Primary for specialized indices (`VIX`, `CL=F`) and fallbacks.
- **Source-Tiering Protection**: The database enforces a "Quality-First" overwrite policy via the `source` column. Data from **High-Quality** sources (Massive/Binance) can never be overwritten by lower-tier sources (Yahoo/Capital) for the same timestamp. This is enforced via an `ON CONFLICT DO UPDATE` clause in `_save_to_client`.
- **Targeted Cleaning**: Surgical symbol-specific deletion before commits. Only symbols arriving with high-quality data (Massive/Binance) trigger a range wipe; specialized Capital-only assets (e.g., `UUP`, `XLC`) are preserved.

## 💾 Database Architecture & Write Optimization

The system uses two Turso databases (**Archive** and **Mirror**) but is strictly optimized to minimize writes and stay within free-tier quotas.

- **Single Primary DB (Archive)**: The daily harvester (`main.py`) runs 6x daily and writes **only** to the Archive database. It ignores the Mirror completely.
- **Native Double-Replica Caching Sync**: Synchronization to the Mirror DB is handled exclusively by a weekly GitHub Actions workflow (`sync_mirror.yml`). 
- **Surgical Sync Strategy**:
    - The sync workflow restores local embedded replicas of both databases from a **stable cache key** (`turso-replicas-v1`), ensuring the replicas start fully populated.
    - It performs a binary `.sync()` to download only incremental changes (KB instead of MB).
    - It uses **`INSERT OR IGNORE`** (`_mirror_insert`) to sync rows to the Mirror. Existing rows generate **0 writes**, dramatically reducing the write load from millions to tens.
- **Manual Indexing**: The `market_data` table requires an index on `timestamp` for fast range deletes. To prevent massive write spikes on application startup, **this index is NOT created automatically in code**. It must be created manually via the Turso CLI.

## 📁 Repository Structure

- `main.py`: The session-aware entry point for automated harvesting.
- `src/api/`: Optimized clients for Massive, Binance, Yahoo, and Capital.com.
- `src/data/`: Parallel session harvesting engine (`harvester.py`).
- `src/database/operations.py`: Core logic for Source-Tiering (Archive) and Surgical Sync (Mirror).
- `tools/sync_mirror.py`: The weekly synchronization script.

## 🚀 Usage

### 1. Manual Harvest
Run the primary harvester for the current/most recent market session:
```bash
python3 main.py
```
Run for a specific market date:
```bash
python3 main.py --date 2024-05-15
```

### 2. Manual Mirror Sync
Force a synchronization from the Archive to the Mirror DB locally:
```bash
PYTHONPATH=. python3 tools/sync_mirror.py
```

### 3. Run Tests
Execute the comprehensive test suite (including Write Optimization checks):
```bash
python3 -m pytest tests/ -v
```

### 4. Database Setup (One-Time)
When deploying a fresh Archive database, you must manually create the timestamp index:
```bash
turso db shell <your-archive-db-name>
sqlite> CREATE INDEX IF NOT EXISTS idx_market_data_timestamp ON market_data (timestamp);
```

---
*Maintained by emadprograms & Gemini*
