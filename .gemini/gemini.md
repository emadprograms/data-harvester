# Analyst Workbench: AI Instructions & System Architecture (v6.0)

This document serves as the "System Knowledge Base" for the AI Agent (Antigravity) and human developers. It defines the core philosophy, infrastructure, and analytical rules engine.

## 🚀 Project Overview
The **Stock Data Harvester** is a high-performance, stateless data harvesting engine designed for scheduled daily automation. It is optimized for resilience, integrity, and ephemeral execution environments like GitHub Actions.

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
- **Market Session Mandate**: Data is harvested and stored based on **Market Sessions**, defined as **8:00 PM ET (Previous Trading Day)** to **8:00 PM ET (Target Day)**. This ensures all pre-market, regular, and post-market data for a specific trading day is captured in one continuous block.
- **Strict UTC Definition**: All internal logic, API requests, and database storage use pure **UTC**. The session range is calculated in ET and then surgically mapped to UTC (e.g., 01:00 UTC to 01:00 UTC during Standard Time).
- **Database Parity Mandate**: **Maintaining absolute 1-on-1 consistency between the Archive and Mirror databases is the single most important objective.** Every operation must verify and enforce this parity.
- **Clean-Before-Write (Surgical)**: To prevent data splicing, the system surgically deletes the exact UTC timestamp range of the session from the database before committing new results.
- **Dual-Master Strategy**: Data is committed to two independent Turso databases (Archive and Mirror) to ensure high availability and data redundancy.
- **Simplified Fetching**: Uses a strict **Primary -> Fallback** logic. No data splicing or hybrid merging.
- **Self-Healing**: The engine automatically performs a surgical parity check/repair for the target date before every harvest.

---

## 🔐 1. Secrets Management (Infisical)

The project uses **Infisical** as the single source of truth for secrets.

### A. Implementation
*   **Correct Package**: Use `infisical-sdk` (imported as `infisical_sdk`).
*   **Manager Pattern**: All logic is encapsulated in `src/infisical_manager.py`. It handles authentication and credential retrieval for Turso, Massive, and Capital.com.

---

## 📁 2. Repository Structure
- `main.py`: Entry point for the "Market Session" automated harvest.
- `src/api/`: Provider layer (Massive, Capital, Binance, Yahoo) updated for range-based UTC fetching.
- `src/data/`: Logic layer handling the parallel session harvesting engine (`harvester.py`).
- `src/database/`: Storage layer; includes range-based surgical cleaning logic.
- `src/utils/`: Discord notifications, MD5 integrity checks, and logging.
- `tools/`: Administrative utilities (Full DB MD5 check, Archive-to-Mirror sync, etc.).

## 🛠 3. Building and Running

### Key Commands
- **Run Harvest**: `python3 main.py` (Auto-targets today's market session)
- **Manual Session Harvest**: `python3 main.py --date YYYY-MM-DD`
- **Full Parity Check**: `PYTHONPATH=. python3 tools/full_database_md5_check.py`
- **Manual Sync**: `PYTHONPATH=. python3 tools/sync_from_archive.py`

---

## ⚖️ 4. Development Conventions

### Sourcing Priority (Implemented in `src/data/harvester.py`)
1.  **Equities & ETFs**: Massive (Polygon) -> Fallback: Capital.com (Using optimized Epics like `US30`, `US100`).
2.  **Crypto & Gold**: Binance (`*USDT`, `PAXGUSDT`) -> Fallback: Yahoo Finance.
3.  **Specials**: Yahoo Finance Only (e.g., `CL=F` Oil, `VIX`).
*   **Note**: Yahoo is completely removed from the Equity/ETF pipelines to ensure pre-market data quality.

### GitHub Automation
The harvester runs **4 times per trading day** to stay within Capital.com's 16-hour window:
1.  **8:30 AM ET**: Pre-Market capture.
2.  **12:30 PM ET**: Mid-Market update.
3.  **4:30 PM ET**: Market Close capture.
4.  **10:30 PM ET**: Final Session Wrap-up.
*   The workflow automatically skips weekends and US Market Holidays.

### Testing Practices
- **Mocking**: External APIs must be mocked in unit tests.
- **Validation**: Every harvest concludes with a surgical MD5 check for the target session.

---

## 🤖 5. CLI Operational Mandates (Gemini CLI ONLY)

1.  **Automatic Pushing**: Execute `git push` immediately after completing verified changes.
2.  **Database Parity (Mirroring)**: The **Archive** and **Mirror** databases must remain 1-on-1 identical. Use `tools/sync_from_archive.py` if a full sync is needed.
3.  **Mandatory Test-Driven Workflow**: Run `python3 -m pytest tests/` after every modification. All tests must pass before pushing.

---
*Updated: 2026-02-24 (v6.0 - Market Session Architecture)*
