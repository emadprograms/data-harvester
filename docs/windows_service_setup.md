# 🪟 Windows 24/7 Services Setup & Auto-Reload Guide

**Document Version:** 1.1.0 · **Last reviewed:** 2026-10-03 (Milestone v4.1)

Data Harvester provides a zero-dependency, automated background service architecture for Windows that runs continuously, launches automatically on logon/startup, and auto-updates whenever code changes.

---

## 🏗 Architecture

```
[Windows Task Scheduler]
        │
        ├──> DataHarvester-Streamer  ──> [tools/service_supervisor.py] ──> python -m src.stream.runner
        │                                         ▲
        │                                         │ (Watches src/ & .git/HEAD)
        │                                         │ (Restarts child on code change)
        │
        └──> DataHarvester-Dashboard ──> [tools/service_supervisor.py] ──> python -m src.dashboard.server
                                                  ▲
                                                  │ (Port 8420)
                                                  │ (Watches src/ & .git/HEAD)
```

Each service is supervised by `tools/service_supervisor.py`:
1. **Always-On**: Automatically restarts if Python terminates or encounters an unexpected exception (exponential backoff, default factor 3.0, capped at 30s; the crash counter resets after 30s of healthy uptime).
2. **Auto-Reload on Code Change**: Scans `src/` every 2 seconds for modified files (`.py`, `.html`, `.js`, `.css`). When a change is detected, it terminates the child process cleanly and launches the new code without manual intervention.
3. **Auto-Reload on Git Pull**: Watches `.git/HEAD` and branch references. Any `git pull` instantly triggers an automated reload.
4. **Log Redirection & Rotation**: Logs are continuously written to `logs/streamer.log` and `logs/dashboard.log` (rotated to `*.old.log` when exceeding 20 MB).
5. **Graceful Shutdown**: On stop, children receive a drain period so the streamer's bounded write queue can flush before being killed.

> ℹ️ The supervisor's crash-recovery and self-healing behaviour (including chaos-monkey termination of the streamer/dashboard/supervisor) is covered by `tests/integration/test_supervisor_chaos_soak.py`, added in Milestone v4.1.

---

## ⚡ Quickstart

### 1. Install & Start Both Services
Open PowerShell in the repository root and run:
```powershell
powershell -ExecutionPolicy Bypass -File tools/windows/install_services.ps1
```
This registers and immediately launches:
- `DataHarvester-Streamer` (Capital.com 24/7 WebSocket tick ingestion)
- `DataHarvester-Dashboard` (Observability Command Center at `http://localhost:8420`)

The PowerShell installer uses `python.exe` from `PATH` and registers the tasks with restart-on-failure settings (`RestartCount 3`, `RestartInterval 1 minute`), no execution time limit, and an at-logon trigger.

> ℹ️ A legacy alternative, `tools\windows\INSTALL_STARTUP.bat`, launches the same supervisors through WScript. Prefer the PowerShell installer: the `.bat` path uses `pythonw.exe`, which suppresses console I/O and can cause silent termination when modules expect standard streams.

### 2. Check Service Status
```powershell
powershell -ExecutionPolicy Bypass -File tools/windows/status_services.ps1
```
Displays task run state, exit codes, and recent stdout/stderr output (`logs\streamer.log`, `logs\dashboard.log`).

### 3. Stop All Services
```powershell
powershell -ExecutionPolicy Bypass -File tools/windows/stop_services.ps1
```

### 4. Uninstall Services
```powershell
powershell -ExecutionPolicy Bypass -File tools/windows/uninstall_services.ps1
```

---

## 🔄 How Auto-Update Works

- **When editing code locally**: Save any file in `src/`. The supervisor logs `[Detected code change. Auto-restarting...]` and brings up the new code in under 2 seconds.
- **When pulling code from GitHub**: Run `git pull`. The change to `.git/HEAD` triggers an automated restart of both processes.
- **Live Symbol Changes**: Adding/removing tracked symbols via the dashboard or database does NOT even require a restart — the streamer hot-reloads dynamically via the `.stream_reload.signal` file in the tick lake root (and `_control/`) with a debounce of ~0.05s and a registry poll interval of ~1s.

---

## ⚙️ Configuration Notes

- The services inherit the environment of the user/session that launches them. Define `TICK_LAKE_ROOT` (e.g. `D:\data-harvester\data\tick_lake`) system-wide or in the repository `.env` so both the streamer and the dashboard resolve the same lake.
- `DASHBOARD_PORT` (or `PORT`) overrides the default dashboard port `8420`; ports `8421`, `8422`, and `8425` are tried automatically if `8420` is occupied.
- `STREAM_FLUSH_INTERVAL`, `STREAM_MAX_BATCH_ROWS`, `STREAM_MAX_QUEUE_SIZE`, and `STREAM_COMPRESSION` are documented in `.env.example` but are **not yet read by the runtime** (backlog item). The effective defaults are: runner flush `2.0s`, writer flush `5.0s` / `5,000` rows, queue `10,000`, Snappy compression — see `docs/operations/tick_lake_operations_guide.md` §2.2.

---

## 🧪 Post-Install Verification

```powershell
# Dashboard health
Invoke-RestMethod http://localhost:8420/api/status | ConvertTo-Json -Depth 4

# Streamer heartbeat (may be empty until the first ticks arrive)
Invoke-RestMethod http://localhost:8420/api/stream/status | ConvertTo-Json -Depth 4

# Offline suite (688 tests as of v4.1)
python -m pytest tests/ -m "not live and not performance" -q
```

---

## 📜 Revision History

| Version | Date | Milestone | Summary |
|---|---|---|---|
| 1.1.0 | 2026-10-03 | v4.1 | Fixed reload-signal filename; documented supervisor backoff/shutdown, installer differences, config/env state, and post-install verification. |
| 1.0.0 | 2026-10-03 | v4.0 | Initial Windows service setup guide. |
