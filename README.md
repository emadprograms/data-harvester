# 🚀 Stock Data Harvester (v4.0)

A high-performance, stateless data harvesting engine designed for scheduled daily automation. Optimized for resilience, integrity, and ephemeral execution environments (like GitHub Actions).

## 🏛 Stateless v4.0 Architecture

The harvester follows a strict **ephemeral run cycle** to ensure data integrity and avoid local state drift:
1. **Bootstrap**: Download the latest master database from **Google Drive**.
2. **Self-Heal**: Automatically repair any data gaps between the remote **Turso** instance and the local buffer.
3. **Harvest**: Parallel fetch of 1-minute candles from **Capital.com**, **Binance**, and **Yahoo Finance**.
4. **Dual-Commit**: Sync new data to both the cloud (Turso) and the local session buffer.
5. **Persist**: Verify integrity (Fingerprinting) and upload the updated master database back to **Google Drive**.
6. **Purge**: Completely wipe the local environment, leaving no persistent footprint.

## 🛠 Key Features

- **Triple-Source Strategy**: 
    - **Capital.com**: High-fidelity institutional CFD data.
    - **Binance**: Direct WebSocket/REST data for crypto assets.
    - **Yahoo Finance**: Universal fallback and primary source for specialized indices.
- **Parallel Engine**: `ThreadPoolExecutor` architecture for high-concurrency harvesting across 45+ symbols.
- **Centralized Secrets**: Fully integrated with **Infisical** for secure, cached credential management.
- **Discord Observability**: Automated health matrix and integrity alerts sent directly to Discord after every run.

## 📁 Repository Structure

- `harvest_cli.py`: The stateless entry point for the daily automated harvest.
- `src/api/`: Optimized clients for Capital.com, Yahoo, and Binance with robust retry logic.
- `src/data/`: Normalization logic and the parallel harvesting engine.
- `src/database/`: Dual-layer persistence logic (Turso + SQLite).
- `src/utils/`: Integrity fingerprinting, GDrive sync, and Discord alerting.
- `tests/`: Comprehensive test suite (58+ tests) validating the entire pipeline.

## 🧪 Testing

To run the full suite:
```bash
PYTHONPATH=. python3 -m pytest tests/ -v
```

## 🚀 Usage

The harvester is triggered automatically via GitHub Actions (`Daily Harvest`). For manual local runs:
1. Ensure your `.env` contains Infisical credentials.
2. Run:
```bash
python harvest_cli.py
```

---
*Maintained by Antigravity*

