# 🪟 Windows 24/7 Services Setup & Auto-Reload Guide

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
1. **Always-On**: Automatically restarts if Python terminates or encounters an unexpected exception.
2. **Auto-Reload on Code Change**: Scans `src/` every 2 seconds for modified files (`.py`, `.html`, `.js`, `.css`). When a change is detected, it terminates the child process cleanly and launches the new code without manual intervention.
3. **Auto-Reload on Git Pull**: Watches `.git/HEAD` and branch references. Any `git pull` instantly triggers an automated reload.
4. **Log Redirection & Rotation**: Logs are continuously written to `logs/streamer.log` and `logs/dashboard.log` (automatically rotated if exceeding 20 MB).

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

### 2. Check Service Status
```powershell
powershell -ExecutionPolicy Bypass -File tools/windows/status_services.ps1
```
Displays task run state, exit codes, and recent stdout/stderr output.

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
- **Live Symbol Changes**: Adding/removing tracked symbols via the dashboard or database does NOT even require a restart—it hot-reloads dynamically via the `.reload_streamer` signal.
