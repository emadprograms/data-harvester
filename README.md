# 🚀 Stock Data Harvester (v5.0)

A high-performance, stateless data harvesting engine designed for scheduled daily automation. Optimized for resilience, integrity, and ephemeral execution environments (like GitHub Actions).

## 🏛 Architecture & Documentation

The harvester follows a strict **ephemeral run cycle** to ensure data integrity and avoid local state drift.

For a deep dive into the system's design, dual-layer persistence, and Google Drive authentication:
👉 **[Read the System Architecture Guide](docs/system_architecture.md)**
👉 **[Read the Google Drive Setup Guide](docs/GOOGLE_DRIVE_SETUP.md)**

## 🛠 Key Features

- **Triple-Source Strategy**: 
    - **Massive (Polygon.io)**: High-fidelity, paged institutional data (Primary Source for Equities).
    - **Binance**: Direct WebSocket/REST data for crypto assets.
    - **Yahoo Finance**: Universal fallback and primary source for specialized indices.
- **Parallel Engine**: `ThreadPoolExecutor` architecture for high-concurrency harvesting using 8 parallel workers for optimal throughput.
- **Dual-Write Persistence**: Data is committed to both **Archive** and **Mirror** Turso (libsql) databases with post-harvest MD5 integrity validation.
- **Centralized Secrets**: Fully integrated with the latest **Infisical SDK** (`infisicalsdk`) for secure, cached credential management.
- **Discord Observability**: Automated health matrix and integrity alerts sent directly to Discord after every run.

## 📁 Repository Structure

- `main.py`: The stateless entry point for the daily automated harvest.
- `src/api/`: Optimized clients for Massive (Polygon), Yahoo, and Binance with robust retry logic.
- `src/data/`: Normalization logic and the parallel harvesting engine.
- `src/database/`: Dual-layer persistence logic (Turso/libsql).
- `src/utils/`: Integrity fingerprinting, GDrive sync, and Discord alerting.
- `tests/`: Comprehensive test suite validating the entire pipeline.

## 🧪 Testing

To run the full suite:
```bash
PYTHONPATH=. python3 -m pytest tests/ -v
```

## 🚀 Usage

The harvester is triggered automatically via GitHub Actions (`Daily Harvest`). For manual local runs:
1. Ensure your `.env` contains Infisical credentials (`INFISICAL_CLIENT_ID`, `INFISICAL_CLIENT_SECRET`, `INFISICAL_PROJECT_ID`).
2. Run:
```bash
python3 main.py
```

---
*Maintained by Antigravity & Gemini CLI*

