# System Architecture: Data Harvester (Pure CLI)

## 1. High-Level Overview
The **Data Harvester** is a decoupled, headless Python service optimized for **Stateless/Ephemeral** execution (GitHub Actions). It maintains data parity across two masters (Turso and Google Drive) using a high-performance parallel fetch core.

Architecture Highlights:
- **Stateless Design**: Optimized for GitHub Actions; local storage is treated as a transient buffer.
- **Dual-Master strategy**: Data is synced to Turso (Remote SQL) and Google Drive (Master SQLite Backup).
- **Self-Healing**: Automatically repairs gaps in the local buffer from Turso before every harvest.
- **Integrity Fingerprinting**: Uses lightweight statistical fingerprints to verify sync between cloud masters.

## 2. Directory Structure

```text
├── main.py             # Main Entry Point & Orchestrator
├── .github/workflows/         # Automation Layer
├── scripts/                   # System Maintenance & Migration
├── tools/                     # Core Utility Tools (Migration, Repair)
├── src/
│   ├── api/                   # PROVIDER LAYER (Capital.com, Yahoo, Binance)
│   ├── data/                  # LOGIC LAYER (Harvester, Normalizer)
│   ├── database/              # STORAGE LAYER (LibSQL/Turso/SQLite)
│   └── utils/                 # CROSS-CUTTING (Discord, Integrity, GDrive)
└── requirements.txt           # Environment Dependencies
```

## 3. The Harvesting & Sync Pipeline

```mermaid
graph TD
    Trigger[GitHub Actions / CLI] --> Wipe[Pre-Run Local Wipe]
    Wipe --> Download[GDrive Download]
    Download --> Repair[Self-Healing: Fill Gaps from Turso]
    Repair --> Harvest[Parallel API Fetch]
    Harvest --> Storage[Dual Write: Turso + Local SQLite]
    Storage --> Integrity[Integrity Check: Turso vs Local]
    Integrity --> Upload[GDrive Master Upload]
    Upload --> Cleanup[Post-Run Local Wipe]
    Cleanup --> Notify[Discord Health & Sync Report]
```

## 4. Key Components

### A. Harvester Controller (`src/data/harvester.py`)
Orchestrates parallel workers via `ThreadPoolExecutor`. Manages provider-specific rate limiting and handles session roll-overs.

### B. Ephemeral Data Manager (`main.py`)
Manages the lifecycle of the transient database buffer. Ensures connections are explicitly closed to prevent session leaks and wipes local data at exit.

### C. Self-Healing Engine (`tools/migrate_historical_turso.py`)
Ensures no data gaps exist between runs by comparing row counts and timestamps between Turso and the local buffer, pulling missing data as needed.

### D. Integrity & Health (`src/utils/`)
- **Integrity**: Compares MD5-style fingerprints (statistical sums) to detect sync drift.
- **Discord**: Time-aware health dashboard flagging tickers with low candle counts based on the current market session (PRE/REG/POST).

---
*Updated for Data Harvester v4.0 (Stateless & Self-Healing Architecture)*
