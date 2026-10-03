@echo off
title Data Harvester - 24/7 Startup Installer
for %%I in ("%~dp0..\..") do set "DIR=%%~fI"
cd /d "%DIR%"

echo ====================================================
echo   Data Harvester - 24/7 Windows Setup (Zero Config)
echo ====================================================
echo.

set "STARTUP_FOLDER=%APPDATA%\Microsoft\Windows\Start Menu\Programs\Startup"
set "VBS_FILE=%STARTUP_FOLDER%\DataHarvester.vbs"

set "PYTHONW=pythonw.exe"
for /f "delims=" %%I in ('where pythonw.exe 2^>nul') do set "PYTHONW=%%I"

set "SUPERVISOR=%DIR%\tools\service_supervisor.py"

:: Stop any old runs first
python -c "import psutil, os; [p.kill() for p in psutil.process_iter(['name', 'cmdline']) if p.info['name'] and 'python' in p.info['name'].lower() and p.pid != os.getpid() and not any('-c' == arg for arg in (p.info['cmdline'] or [])) and any('service_supervisor.py' in arg or 'src.stream.runner' in arg or 'src.dashboard.server' in arg for arg in (p.info['cmdline'] or []))]" >nul 2>&1

:: Create the silent VBScript launcher in the Windows Startup folder
(
echo Set WshShell = CreateObject("WScript.Shell"^)
echo WshShell.CurrentDirectory = "%DIR%"
echo WshShell.Run "pythonw.exe tools\service_supervisor.py --name streamer --module src.stream.runner", 0, False
echo WshShell.Run "pythonw.exe tools\service_supervisor.py --name dashboard --module src.dashboard.server", 0, False
) > "%VBS_FILE%"

echo [OK] Added to your Windows Startup folder!
echo      Starts automatically every time you log in to Windows.
echo.

:: Launch the services in the background immediately
wscript.exe "%VBS_FILE%"

echo [OK] Services started in the background (hidden):
echo      - Streamer:  Ingesting live ticks 24/7 into Partitioned Parquet Tick Lake (data\tick_lake)
echo      - Dashboard: Running at http://localhost:8420
echo      - Auto-Update: Detects code changes in src/ and git updates
echo.
echo Logs are saved to: %DIR%\logs\
echo.
echo Setup finished! You can close this window.
echo.
pause
