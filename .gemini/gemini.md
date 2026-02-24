# Analyst Workbench: AI Instructions & System Architecture (v7.1)

This document serves as the "System Knowledge Base" for the AI Agent (Antigravity) and human developers. It defines the core philosophy, infrastructure, and analytical rules engine.

## 🚀 Project Overview
The **Stock Data Harvester** is a high-performance, stateless data harvesting engine designed for scheduled daily automation. It is optimized for resilience, integrity, and ephemeral execution environments.

### Architecture & Integrity Rules
- **Market Session Mandate**: Data is harvested and stored based on **Market Sessions**, defined as **8:01:00 PM ET (Previous Trading Day)** to **8:00:00 PM ET (Target Day)**. This ensures exactly 1,440 minutes of data for 24/7 assets and avoids "Midnight Spillover" overlaps.
- **Source-Tiering Protection**: The database enforces a "Quality-First" overwrite policy via a **9-column schema** (including `source`):
    - **Tier 1 (Authoritative)**: `MASSIVE`, `BINANCE`.
    - **Tier 2 (Fallback)**: `YAHOO`, `CAPITAL`.
    - **Rule**: Data from a Tier 1 source can NEVER be overwritten by data from a Tier 2 source for the same timestamp and symbol.
- **Tiered Deletion (Source-Aware Cleaning)**: Before committing, the system only performs a `DELETE` if the **new data** is high-fidelity (Tier 1). Tier 2 data (Capital/Yahoo) is stacked incrementally without wiping existing records, preserving historical "tails."
- **Adaptive Request Clamping**: The Capital.com client automatically clips requests to the last **16 hours** of minute data. If a full session is requested, it fetches only the available "tail" to prevent API errors and "wrong-date" pollution.
- **Database Parity Mandate**: **Maintaining absolute 1-on-1 consistency between the Archive and Mirror databases is foundational.** Any operation modifying the Archive (schema or metadata) must be immediately reflected in the Mirror.
- **Self-Healing**: The engine automatically repairs the Mirror database from the Archive for the target date before every harvest if a desync is detected.

---

## 🔐 1. Secrets Management (Infisical)
*   **Correct Package**: Use `infisical-sdk` (imported as `infisical_sdk`).
*   **Manager Pattern**: `src/infisical_manager.py` handles dynamic retrieval of all 9+ Massive (Polygon) API keys and Turso credentials.

---

## 📁 2. Repository Structure
- `main.py`: Entry point for the automated session harvest.
- `src/api/`: Provider layer (Massive, Binance, Yahoo, Capital) updated for range-based UTC fetching.
- `src/database/operations.py`: Implementation of Source-Tiering and Targeted Cleaning.
- `src/utils/integrity.py`: MD5 parity and auto-repair logic.

---

## ⚖️ 4. Development Conventions

### Sourcing Priority (Implemented in `src/data/harvester.py`)
1.  **Equities & ETFs**: Massive (Polygon) -> Fallback: Yahoo Finance.
2.  **Gold**: Binance (`PAXGUSDT`) -> Fallback: Yahoo Finance (`GC=F`).
3.  **Crypto**: Binance (`*USDT`) -> Fallback: Yahoo Finance.
4.  **Specials**: Yahoo Finance Only (e.g., `CL=F` Oil, `VIX`).

---

## 🤖 5. CLI Operational Mandates (Gemini CLI ONLY)

1.  **Automatic Pushing**: Execute `git push` immediately after completing verified changes.
2.  **Database Parity (Mirroring)**: Ensure the Archive and Mirror databases remain 1-on-1 identical for all metadata and schema changes.
3.  **Testing**: Fulfill the user's request thoroughly, including updating or adding tests. All changes must be verified through implementation and validation.
