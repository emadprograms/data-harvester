# 🚀 Stock Data Harvester

A high-performance, parallel data harvesting engine designed for scheduled daily runs via GitHub Workflows.

## 🛠 Features

- **Parallel Harvesting**: Utilizes `ThreadPoolExecutor` (9 workers) to parallelize API requests across 43+ symbols.
- **Massive API Rotation**: Automatically rotates between 9 different Massive API keys to maximize throughput.
- **Triple Fallback Logic**:
  1. **Massive**: Primary high-fidelity source.
  2. **Binance**: Dedicated source for crypto-related tickers.
  3. **Yahoo Finance**: Secondary fallback for all assets and standalone source for indices/ETFs.
- **Automated Workflow**: Scheduled to run every Tuesday-Saturday at **6:00 AM Bahrain Time** (targetting the previous day's market session).
- **Turso Database**: Efficient persistence using the "Stock Data Archive" hosted on Turso.

## 📁 Repository Structure

- `harvest_cli.py`: Main entry point for the daily automated harvest.
- `src/`:
  - `api/`: API client implementations (Massive, Yahoo, Binance).
  - `data/`: Core harvesting logic, normalization, and parallelization.
  - `database/`: Schema definitions and Turso operations.
  - `infisical_manager.py`: Singleton-based secret management with caching.

## 🚀 Usage (Automation)

The harvest is fully automated via GitHub Actions (`.github/workflows/harvest.yml`).

To run a manual harvest:

```bash
python harvest_cli.py
```

---
*Created with ❤️ by Antigravity*
