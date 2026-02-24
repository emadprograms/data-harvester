# Analyst Workbench: AI Instructions & System Architecture (v7.2)

This document serves as the "System Knowledge Base" for the AI Agent (Antigravity) and human developers. It defines the core philosophy, infrastructure, and analytical rules engine.

## 🚀 Project Overview
The **Stock Data Harvester** is a high-performance, stateless data harvesting engine designed for scheduled daily automation. It is optimized for resilience, integrity, and ephemeral execution environments.

### Key Technologies
- **Language**: Python 3.12+
- **Data Processing**: `pandas`, `numpy`
- **Concurrency**: `ThreadPoolExecutor` (8 parallel workers)
- `USFederalHolidayCalendar`: For market holiday awareness
- **Database**: Turso (libsql) with a Dual-Write strategy (Archive + Mirror)
- **Secrets Management**: Infisical SDK (`infisicalsdk`)
- **APIs**: Polygon.io (Massive), Capital.com, Binance, Yahoo Finance
- **Observability**: Discord Webhooks for health reports and alerts

### Architecture
- **Market Session Mandate**: Data is harvested based on a strict **Market Session** definition:
    - **Start**: 8:00 PM ET of the *Previous Trading Day* (Exclusive).
    - **End**: 8:00 PM ET of the *Target Trading Day* (Inclusive).
    - **Scope**: Captures all Pre-Market, Regular, and Post-Market activity in one continuous block, regardless of calendar days.
- **Session-Based Sourcing**:
    - **Current Session (Active)**: Uses **Capital.com** (Live Data). No volume. **Append/Update Strategy** (No clear) to preserve early-session data.
    - **Previous Session (Completed)**: Uses **Massive** (Polygon.io). Full Data. **Surgical Clean & Replace Strategy** to ensure a perfect, non-spliced record.
- **Strict UTC Definition**: All internal logic, API requests, and database storage use pure **UTC**.
- **Database Parity Mandate**: **Maintaining absolute 1-on-1 consistency between the Archive and Mirror databases is the single most important objective.** Every operation must verify and enforce this parity.
- **Dual-Master Strategy**: Data is committed to two independent Turso databases (Archive and Mirror) to ensure high availability and data redundancy.
- **Self-Healing**: The engine automatically performs a surgical parity check/repair (MD5) for the target date before and after every harvest.

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
1.  **Equities & ETFs**: Massive (Polygon) -> Fallback: Capital.com (Active Session) / None (Completed).
2.  **Gold**: Binance (`PAXGUSDT`) -> Fallback: Yahoo Finance (`GC=F`).
3.  **Crypto**: Binance (`*USDT`) -> Fallback: Yahoo Finance.
4.  **Specials**: Yahoo Finance Only (e.g., `CL=F` Oil, `VIX`).

---

## 🤖 5. CLI Operational Mandates (Gemini CLI ONLY)

1.  **Automatic Pushing**: Execute `git push` immediately after completing verified changes.
2.  **Database Parity (Mirroring)**: Ensure the Archive and Mirror databases remain 1-on-1 identical for all metadata and schema changes.
3.  **Testing**: Fulfill the user's request thoroughly, including updating or adding tests. All changes must be verified through implementation and validation.
