# 🚀 Stock Data Harvester (v7.0)

A high-performance, stateless data harvesting engine designed for scheduled daily automation. Optimized for resilience, integrity, and absolute database parity.

## 🏛 Market Session Architecture

The harvester operates on a **Market Session** basis rather than calendar days. 
- **Session Definition**: **8:01:00 PM ET (Previous Trading Day)** to **8:00:00 PM ET (Target Day)**. This ensures exactly 1,440 minutes per day for 24/7 assets and eliminates "Midnight Spillover" overlaps.
- **Strict UTC**: All data is processed and stored using precise UTC ranges to eliminate time-zone overlap.

## 🛠 Key Features

- **Quad-Source Strategy**: 
    - **Massive (Polygon.io)**: Primary source for high-fidelity institutional equities and ETFs.
    - **Binance**: Primary source for Crypto and Gold (`PAXGUSDT`).
    - **Yahoo Finance**: Primary for specialized indices (`VIX`, `CL=F`) and secondary fallback for all others.
    - **Capital.com**: Legacy fallback for specialized CFD-only assets.
- **Source-Tiering Protection**: The database enforces a "Quality-First" overwrite policy. Data from **Tier 1** (Massive/Binance) can never be overwritten by **Tier 2** (Yahoo/Capital) for the same timestamp.
- **Targeted Cleaning**: Surgical symbol-specific deletion before commits. Only multi-source symbols that were successfully re-harvested are cleared; single-source tickers (e.g., specialized Capital.com assets) are preserved to prevent data loss.
- **Dual-Write Integrity**: absolute 1-on-1 parity between **Archive** and **Mirror** databases, enforced by MD5 fingerprinting and automatic pre-harvest self-healing.

## 📁 Repository Structure

- `main.py`: The range-based entry point for automated harvesting.
- `src/api/`: Optimized clients for Massive, Binance, Yahoo, and Capital.com.
- `src/data/`: Parallel session harvesting engine (`harvester.py`).
- `src/database/`: Implementation of Source-Tiering and Targeted Cleaning logic.
- `tools/`: Administrative tools for parity checks, schema migration, and synchronization.
- `tests/`: Comprehensive test suite validating the v7.0 architecture.

## 🧪 Testing

To run the full suite:
```bash
PYTHONPATH=. python3 -m pytest tests/ -v
```

## 🚀 Usage

### Manual Harvest
Run for a specific market session:
```bash
python3 main.py --date YYYY-MM-DD
```

### Full Parity Verification
Confirm Archive and Mirror are 1-on-1 identical:
```bash
PYTHONPATH=. python3 tools/full_database_md5_check.py
```

---
*Maintained by emadprograms & Gemini CLI*
