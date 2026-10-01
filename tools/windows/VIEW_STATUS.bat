@echo off
chcp 65001 >nul
title Data Harvester - Status
for %%I in ("%~dp0..\..") do set "DIR=%%~fI"
cd /d "%DIR%"

echo ====================================================
echo   Data Harvester - Background Services Status
echo ====================================================
echo.

python -c "import psutil, os; pids = [(p.pid, ' '.join(p.info['cmdline'])) for p in psutil.process_iter(['name', 'cmdline']) if p.info['name'] and 'python' in p.info['name'].lower() and p.pid != os.getpid() and any('service_supervisor.py' in arg for arg in (p.info['cmdline'] or [])) and not any('-c' == arg for arg in (p.info['cmdline'] or []))]; print('Active Supervisor Processes: ' + str(len(pids))); [print('  * PID ' + str(pid) + ': ' + cmd[:80] + '...') for pid, cmd in pids] if pids else print('  (No background supervisors running)')"

echo.
echo --- Recent Streamer Log (last 10 lines) ---
python -c "from pathlib import Path; p = Path('logs/streamer.log'); print(''.join(p.read_text(encoding='utf-8').splitlines(True)[-10:])) if p.exists() else print('(No streamer log yet)')"

echo.
echo --- Recent Dashboard Log (last 10 lines) ---
python -c "from pathlib import Path; p = Path('logs/dashboard.log'); print(''.join(p.read_text(encoding='utf-8').splitlines(True)[-10:])) if p.exists() else print('(No dashboard log yet)')"

echo.
echo Dashboard URL: http://localhost:8420
echo.
pause
