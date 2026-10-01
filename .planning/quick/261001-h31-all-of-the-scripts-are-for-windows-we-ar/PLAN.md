# Quick Task: Organize Windows Scripts and Add macOS Service Management Scripts

## Objective
The repository currently contains Windows-only batch scripts (.bat) in the root directory and PowerShell scripts in `tools/windows/`.
This task:
1. Moves all Windows scripts into a dedicated folder `tools/windows/`.
2. Creates a dedicated folder `tools/mac/` with comprehensive, executable macOS shell scripts for starting, monitoring, stopping, and auto-starting Data Harvester services (Streamer and Observability Dashboard).
3. Verifies and starts both services (Capital.com 24/7 streamer and Port 8420 dashboard) on macOS.
4. Updates documentation to reflect the new Mac and Windows script locations.

## Tasks
1. Move `INSTALL_STARTUP.bat`, `STOP_SERVICES.bat`, `UNINSTALL_STARTUP.bat`, `VIEW_STATUS.bat` to `tools/windows/` and ensure relative path references work correctly.
2. Create `tools/mac/` containing:
   - `start_services.sh`: Starts both streamer and dashboard via `service_supervisor.py` using `.venv` Python with auto-restart and logging.
   - `stop_services.sh`: Stops all supervisor, streamer, and dashboard processes cleanly.
   - `status_services.sh`: Displays status, PIDs, port 8420 check, and tail logs.
   - `start_streamer.sh`: Starts only the live tick streamer under supervisor.
   - `start_dashboard.sh`: Starts only the dashboard under supervisor.
   - `install_startup.sh`: Configures macOS `launchd` plist (`~/Library/LaunchAgents/com.dataharvester.supervisor.plist`) for optional always-on boot/login startup.
   - `uninstall_startup.sh`: Unloads and removes the `launchd` plist.
3. Make all Mac scripts executable (`chmod +x`).
4. Update `README.md` with Mac and Windows quickstart instructions.
5. Launch both the streaming engine and the dashboard on the current Mac system and verify both are running, logging, and serving `http://localhost:8420`.
6. Write `SUMMARY.md`, update `STATE.md`, and commit changes.
