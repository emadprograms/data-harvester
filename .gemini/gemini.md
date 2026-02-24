# Analyst Workbench: AI Instructions & System Architecture (v5.0)

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
- **APIs**: Polygon.io (Massive), Yahoo Finance, Binance
- **Observability**: Discord Webhooks for health reports and alerts

### Architecture
- **Database Parity Mandate**: **Maintaining absolute 1-on-1 consistency between the Archive and Mirror databases is the single most important objective of this application.** Every operation must be designed to verify and enforce this parity.
- **Stateless/Ephemeral**: Designed to run in environments where local state is transient.
- **Dual-Master Strategy**: Data is committed to two independent Turso databases (Archive and Mirror) to ensure high availability and data redundancy.
- **Simplified Fetching**: Uses a strict **Primary -> Fallback** logic. No data splicing or hybrid merging.
- **Code-Based Priority**: All data sourcing priorities are defined in `src/data/harvester.py`, not in the database.
- **Self-Healing**: The engine automatically repairs gaps in the local buffer from Turso before every harvest.
- **Integrity Fingerprinting**: Uses optimized MD5-style fingerprints to verify sync parity between Archive and Mirror databases.

---

## 🔐 1. Secrets Management (Infisical)

The project uses **Infisical** as the single source of truth for secrets (Turso URLs, API Keys, Webhooks).

### A. The SDK & Implementation
*   **Correct Package**: Always use `infisical-sdk` (imported as `infisical_sdk`). **DO NOT** use the deprecated `infisical-python` package.
*   **Manager Pattern**: All logic is encapsulated in `src/infisical_manager.py`. It initializes the client and handles authentication state.
*   **Usage**: `main.py` and other entry points initialize the manager and fetch secrets during application startup.

### B. Authentication Methods
The manager supports two distinct authentication flows via environment variables:
1.  **Service Token (Legacy/Simple)**:
    *   Requires: `INFISICAL_TOKEN`.
    *   Auth Call: `client.auth.login(token=INFISICAL_TOKEN)`.
2.  **Universal Auth (Machine Identity - Preferred)**:
    *   Requires: `INFISICAL_CLIENT_ID`, `INFISICAL_CLIENT_SECRET`.
    *   Auth Call: `client.auth.universal_auth.login(client_id=..., client_secret=...)`.
*   **Required for both**: `INFISICAL_PROJECT_ID`.

---

## 📁 2. Repository Structure
- `main.py`: Entry point for the daily automated harvest cycle.
- `src/api/`: Provider layer containing optimized clients for Massive, Yahoo, and Binance.
- `src/data/`: Logic layer handling the parallel harvesting engine (`harvester.py`) and normalization (`normalizer.py`).
- `src/database/`: Storage layer for Turso/libsql connection management and CRUD operations.
- `src/utils/`: Cross-cutting concerns including Discord notifications, MD5 integrity checks, and logging.
- `tools/`: Administrative utilities for database migrations, historical repairs, and secret management.
- `discord_bot/`: Independent bot for real-time market data monitoring.

## 🛠 3. Building and Running

### Environment Setup
1. **Dependencies**: `pip install -r requirements.txt`
2. **Secrets**: Ensure `.env` contains Infisical credentials (`INFISICAL_CLIENT_ID`, `INFISICAL_CLIENT_SECRET`, `INFISICAL_PROJECT_ID`).

### Key Commands
- **Run Harvest**: `python3 main.py`
- **Manual Date Harvest**: `python3 main.py --date YYYY-MM-DD`
- **Run Tests**: `PYTHONPATH=. python3 -m pytest tests/ -v`
- **DB Migration**: `python3 tools/migrate_db_v5.py`
- **Sync Mirror**: `python3 tools/sync_mirror_v5.py`

---

## ⚖️ 4. Development Conventions

### Coding Style
- **Type Safety**: Use type hints where possible for clarity.
- **Logging**: Use the centralized `CLILogger` for all session-based logging.
- **Timezones**: Strictly adhere to UTC for storage and US/Eastern for market session logic.
- **Holiday Awareness**: Use `pandas.tseries.holiday.USFederalHolidayCalendar` to correctly identify and skip US Market Holidays during date rollover logic.
- **Error Handling**: Implement robust retry logic (see `src/api/retry.py`) for all external API calls.

### Sourcing Priority (Implemented in `src/data/harvester.py`)
1.  **Gold (`GC=F`)**: Binance (`PAXGUSDT`) -> Fallback: Yahoo (`GC=F`).
2.  **Crypto (`*USDT`)**: Binance -> Fallback: Yahoo.
3.  **Equities/ETFs**: Massive (Polygon) -> Fallback: Yahoo.
4.  **Specialized**: Yahoo only (if no other source available).

### Testing Practices
- **Mocking**: External APIs (Polygon, Yahoo, Binance) should be mocked in unit tests.
- **Integration**: `tests/test_integration.py` validates the full pipeline.
- **Validation**: Every harvest must conclude with a "Clean MD5" integrity check across both databases.

### Contribution Guidelines
- **No Local State**: Never assume files persisted in one run will be available in the next.
- **Dual Write**: Any schema change or data update must be applied to both Archive and Mirror databases.
- **Security**: Never hardcode secrets; always retrieve them via `InfisicalManager`.

---

## 🤖 5. CLI Operational Mandates (Gemini CLI ONLY)

The following rules apply **EXCLUSIVELY** to the **Gemini CLI** agent (this interface). They do **NOT** apply to automated agents like Antigravity.

1.  **Automatic Pushing**: Because all actions in the Gemini CLI are directed and approved by the user in real-time, the agent must **always** execute a `git push` immediately after completing a code modification or bug fix. 
2.  **No Manual Staging Required**: The agent should assume that once a task is finished, the state is ready for the remote repository.
3.  **Database Parity (Mirroring)**: The **Archive** and **Mirror** databases must remain 1-on-1 identical at all times for metadata and schema changes. Any modification made to the Archive database (e.g., updating `symbol_map`, renaming columns, or altering schema) MUST be immediately reflected in the Mirror database, whether explicitly requested or not.
4.  **Mandatory Test-Driven Workflow**: After every code modification or bug fix, the agent MUST run the full test suite (`python3 -m pytest tests/`). If tests fail, the agent must fix the errors and re-run the tests until they pass before pushing the changes to GitHub.

---

## 📅 6. Discord Bot Design Pattern

When adding or updating commands to the Discord bot, always adhere to the **Interactive-First** design pattern:

### A. Command Flow Strategy
1.  **Argument Support**: Commands should accept an optional `date_indicator` string.
    *   `0` → Today
    *   `-1`, `-2`... → Relative days
    *   `YYYY-MM-DD` → Exact date
2.  **Interactive Fallback**: If no argument is provided, the command **MUST** present a `DateSelectionView` to the user.

### B. Reusable UI Components
- **`DateSelectionView`**: A view containing a dropdown (`DateDropdown`) with the last 14 days and a "Manual Date Entry" button.
- **`CustomDateModal`**: A modal triggered by the manual entry button that allows users to type a specific `YYYY-MM-DD` string.
- **Action Callback**: Both the dropdown and modal should trigger a unified `action_callback` (e.g., `trigger_github_harvest`) to ensure consistent behavior across all input methods.

### C. UX Guidelines
- **Suppress Link Previews**: Always wrap URLs in masked links with angle brackets (e.g., `[Monitor Progress](<URL>)`) to prevent Discord from generating large link preview banners (embeds) that clutter the channel.

---
*Updated: 2026-02-24*
