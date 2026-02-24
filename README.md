# 🚀 Stock Data Harvester (v6.0)

A high-performance, stateless data harvesting engine designed for scheduled daily automation. Optimized for resilience, integrity, and absolute database parity.

## 🏛 Market Session Architecture

The harvester operates on a **Market Session** basis rather than calendar days. 
- **Session Definition**: 8:00 PM ET (Previous Trading Day) to 8:00 PM ET (Target Day).
- **Strict UTC**: All data is fetched, cleaned, and stored using precise UTC ranges to eliminate time-zone overlap and "rogue" data issues.
- **Continuous Crypto**: The Monday session automatically captures the entire weekend gap for 24/7 assets.

## 🛠 Key Features

- **Quad-Source Strategy**: 
    - **Massive (Polygon.io)**: institutional-grade equities data.
    - **Capital.com**: High-fidelity pre-market and ETF data (using optimized epics like `US30`, `US100`).
    - **Binance**: Primary source for crypto and gold (`PAXGUSDT`).
    - **Yahoo Finance**: Restricted to specialized indices (`VIX`, `CL=F`) and crypto fallbacks.
- **Clean-Before-Write**: Surgical UTC range deletion before every commit to prevent data splicing.
- **Dual-Write Integrity**: 1-on-1 parity between **Archive** and **Mirror** databases, verified by pre/post-harvest MD5 fingerprinting.
- **High-Frequency Automation**: GitHub Actions run **4 times per trading day** to stay within API lookback windows.

## 📁 Repository Structure

- `main.py`: The range-based entry point for automated harvesting.
- `src/api/`: Optimized clients for Massive, Capital.com, Yahoo, and Binance.
- `src/data/`: The parallel harvesting engine and session logic.
- `src/database/`: Dual-layer persistence logic with surgical range cleaning.
- `tools/`: Parity tools, including full DB MD5 checks and Archive-to-Mirror synchronization.
- `tests/`: Comprehensive test suite validating the range-based pipeline.

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
