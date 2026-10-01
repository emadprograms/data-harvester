@echo off
title Data Harvester - Stop Services
for %%I in ("%~dp0..\..") do set "DIR=%%~fI"
cd /d "%DIR%"

echo Stopping Data Harvester background services...

python -c "import psutil, os; [p.kill() for p in psutil.process_iter(['name', 'cmdline']) if p.info['name'] and 'python' in p.info['name'].lower() and p.pid != os.getpid() and not any('-c' == arg for arg in (p.info['cmdline'] or [])) and any('service_supervisor.py' in arg or 'src.stream.runner' in arg or 'src.dashboard.server' in arg for arg in (p.info['cmdline'] or []))]" >nul 2>&1

echo.
echo [OK] All Data Harvester background services have been stopped.
echo.
pause
