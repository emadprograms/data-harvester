# 🚀 Stock Data Harvester (v7.2)

A high-performance, stateless data harvesting engine designed for scheduled daily automation. Optimized for resilience, integrity, and absolute database parity.

## 🏛 Market Session Architecture

The harvester operates on a strict **Market Session** basis rather than calendar days.
- **Session Definition**: **8:00 PM ET (Previous Trading Day)** to **8:00 PM ET (Target Trading Day)**.
- **Weekend/Holiday Awareness**: If a target date falls on a weekend or holiday, the session automatically **rolls forward** to the next valid trading day (e.g., a Saturday run targets the Monday session, spanning from Friday 8 PM to Monday 8 PM).
- **Strict UTC**: All internal logic and database storage use pure UTC.

## 🛠 Key Features

- **Session-Based Sourcing**:
    - **Active Session (Current)**: Uses **Capital.com** for live, minute-by-minute data (no volume). Implements an **Append/Update** strategy to preserve early-session data.
    - **Completed Session (Past)**: Uses **Massive (Polygon.io)** for high-fidelity institutional OHLCV data. Implements a **Surgical Clean & Replace** strategy to ensure a perfect historical record.
- **Universal Sources**:
    - **Binance**: Primary source for Crypto and Gold (`PAXGUSDT`).
    - **Yahoo Finance**: Primary for specialized indices (`VIX`, `CL=F`).
- **Source-Tiering Protection**: The database enforces a "Quality-First" overwrite policy via the `source` column. Data from **High-Quality** sources (Massive/Binance) can never be overwritten by lower-tier sources (Yahoo/Capital) for the same timestamp.
- **Targeted Cleaning**: Surgical symbol-specific deletion before commits. Only symbols arriving with high-quality data (Massive/Binance) trigger a range wipe; specialized Capital-only assets (e.g., `UUP`, `XLC`) are preserved.
- **Dual-Write Integrity**: Absolute 1-on-1 parity between **Archive** and **Mirror** databases, enforced by MD5 fingerprinting and automatic self-healing.

## 📁 Repository Structure

- `main.py`: The session-aware entry point for automated harvesting.
- `src/api/`: Optimized clients for Massive, Binance, Yahoo, and Capital.com (with 16h auto-clamping).
- `src/data/`: Parallel session harvesting engine (`harvester.py`).
- `src/database/`: Implementation of schema migrations, Tier Protection, and Targeted Cleaning.
- `tools/`: Administrative tools for parity checks, syncing, and inspections.

## 🧪 CI/CD Workflow

The GitHub Actions workflow is strictly gated:
1.  **Test Execution**: All unit and integration tests must pass.
2.  **Market Check**: Skips execution on US Market Holidays and weekends.
3.  **Harvest**: Executes the harvesting engine only if tests are successful.

## 🚀 Usage

### Manual Harvest
Run for a specific market session:
```bash
python3 main.py --date YYYY-MM-DD
```

### Run Tests Manually
```bash
python3 -m pytest tests/
```

---
*Maintained by emadprograms & Gemini CLI*
