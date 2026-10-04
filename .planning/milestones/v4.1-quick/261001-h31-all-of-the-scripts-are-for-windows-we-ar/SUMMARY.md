---
quick_id: 261001-h31
slug: all-of-the-scripts-are-for-windows-we-ar
status: complete
date: 2026-10-01
---

# Quick Task Summary: Platform Separation & macOS Service Scripts

## Overview
Reorganized existing Windows scripts into a separate platform folder and created an equivalent, comprehensive suite of native bash management scripts for macOS with 24/7 background supervisors, live Capital.com tick streaming, and dashboard serving.

## Execution Details

1. **Windows Scripts Relocation**:
   - Moved all Windows `.bat` files from the repo root to `tools/windows/`:
     - `tools/windows/INSTALL_STARTUP.bat`
     - `tools/windows/STOP_SERVICES.bat`
     - `tools/windows/UNINSTALL_STARTUP.bat`
     - `tools/windows/VIEW_STATUS.bat`
   - Fixed path resolution inside the `.bat` files (`for %%I in ("%~dp0..\..") do set "DIR=%%~fI"`) so they operate properly from within `tools/windows/`.
   - Windows PowerShell scripts remain grouped in `tools/windows/`.

2. **Native macOS Service Scripts**:
   - Created `tools/mac/` with executable scripts:
     - `tools/mac/start_services.sh`: Launches both the 24/7 Capital.com live streamer and the Observability Command Center dashboard under `tools/service_supervisor.py` with automatic virtualenv Python resolution (`.venv`), auto-restart on code change, and log redirection.
     - `tools/mac/stop_services.sh`: Cleanly stops all supervisor, streamer, and dashboard processes (SIGTERM graceful wait with SIGKILL fallback).
     - `tools/mac/status_services.sh`: Displays active PIDs, RSS memory consumption, dashboard HTTP 200 health check, and tails `logs/streamer.log` and `logs/dashboard.log`.
     - `tools/mac/start_streamer.sh`: Standalone launcher for live tick streamer.
     - `tools/mac/start_dashboard.sh`: Standalone launcher for dashboard.
     - `tools/mac/install_startup.sh`: Sets up native macOS LaunchAgents (`~/Library/LaunchAgents/com.dataharvester.streamer.plist` and `com.dataharvester.dashboard.plist`) for automatic startup on user login.
     - `tools/mac/uninstall_startup.sh`: Unloads and removes LaunchAgents.
   - Added root-level convenience wrappers:
     - `START_SERVICES.sh` -> `tools/mac/start_services.sh`
     - `STOP_SERVICES.sh` -> `tools/mac/stop_services.sh`
     - `VIEW_STATUS.sh` -> `tools/mac/status_services.sh`

3. **Storage & Runtime Setup**:
   - Created repo-level symlink `data -> /Volumes/Micron-E 0256 A/data-harvester/data` to external storage.
   - Started both services using `./START_SERVICES.sh`.
   - Verified active WebSocket subscription to Capital.com with continuous tick commits to `streaming.duckdb`.
   - Verified dashboard responding HTTP 200 at `http://localhost:8420`.

4. **Testing & Verification**:
   - Executed full dashboard and streaming test suites (`PYTHONPATH=. pytest tests/dashboard/ tests/stream/ -v`): **126 passed, 0 failed**.
   - Verified `./VIEW_STATUS.sh` shows active supervisor and worker processes with streaming tick commits.
