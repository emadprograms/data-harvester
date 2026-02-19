# 🚀 Stock Data Harvester

A high-performance, headless, and resilient data harvesting engine designed for scheduled daily runs via GitHub Workflows.

## 🛠 Features

- **Pure CLI Architecture**: Decoupled from any UI (Streamlit), optimized for fast, automated execution.
- **Parallel Harvesting**: Utilizes `ThreadPoolExecutor` (9 workers) to parallelize API requests across 43+ symbols.
- **Intelligent Key Rotation**: Rotates between 9+ Massive API keys with automatic failover and status-based detection.
- **Triple-Stage Resilience**:
  1. **Massive**: Primary high-fidelity source for US stocks.
  2. **Binance**: Dedicated source for crypto-related tickers with domain fallback.
  3. **Yahoo Finance**: Secondary fallback for all assets and standalone source for indices/ETFs.
- **Automated Workflow**: Scheduled to run every Tuesday-Saturday at **02:00 UTC** (5:00 AM Bahrain).
- **Turso Database**: Efficient persistence using a hosted libSQL (Turso) instance.

## 🧪 Testing Suite

The project includes a comprehensive test suite (65+ tests) covering all internal logic, API behavior, and database operations.

To run tests locally:
```bash
PYTHONPATH=. python3 -m pytest tests/ -v
```

## 📁 Repository Structure

- `harvest_cli.py`: Main entry point for the daily automated harvest.
- `src/`:
  - `api/`: API client implementations (Massive, Yahoo, Binance).
  - `data/`: Core harvesting logic, normalization, and parallelization.
  - `database/`: Schema definitions and Turso operations.
  - `infisical_manager.py`: Singleton-based secret management with caching.
- `tests/`: Full unit and integration test suite.
- `chaos_runner.py`: Stress-testing tool for simulating API failures.

## 🚀 Usage (Automation)

The harvest is fully automated via GitHub Actions (`daily-harvest`). Manual runs can be triggered from the "Actions" tab or locally:

```bash
python harvest_cli.py
```

---
*Created with ❤️ by Antigravity*
