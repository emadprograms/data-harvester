@echo off
title Data Harvester - Remove from Startup
for %%I in ("%~dp0..\..") do set "DIR=%%~fI"
cd /d "%DIR%"

set "STARTUP_FOLDER=%APPDATA%\Microsoft\Windows\Start Menu\Programs\Startup"
set "VBS_FILE=%STARTUP_FOLDER%\DataHarvester.vbs"

if exist "%VBS_FILE%" (
    del "%VBS_FILE%"
    echo [OK] Removed DataHarvester from Windows Startup folder.
) else (
    echo [INFO] Startup file was not present.
)

echo Stopping any active background services...
python -c "import psutil, os; [p.kill() for p in psutil.process_iter(['name', 'cmdline']) if p.info['name'] and 'python' in p.info['name'].lower() and p.pid != os.getpid() and not any('-c' == arg for arg in (p.info['cmdline'] or [])) and any('service_supervisor.py' in arg or 'src.stream.runner' in arg or 'src.dashboard.server' in arg for arg in (p.info['cmdline'] or []))]" >nul 2>&1

echo.
echo [OK] Data Harvester has been uninstalled from startup and stopped.
echo.
pause
