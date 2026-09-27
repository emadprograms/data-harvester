# Windows Task Scheduler Installation Script for Data Harvester
# Registers 24/7 background tasks for Streamer and Dashboard that auto-update on code change.

$ErrorActionPreference = "Stop"

$RepoRoot = (Get-Item "$PSScriptRoot\..\..").FullName
$PythonExe = (Get-Command python.exe -ErrorAction SilentlyContinue).Source

if (-not $PythonExe) {
    Write-Error "python.exe not found in PATH! Please ensure Python 3.12+ is installed."
    exit 1
}

Write-Host "============================================================" -ForegroundColor Cyan
Write-Host "  Data Harvester - 24/7 Windows Services Installer" -ForegroundColor Cyan
Write-Host "============================================================" -ForegroundColor Cyan
Write-Host "Repository: $RepoRoot"
Write-Host "Python:     $PythonExe"
Write-Host ""

$Tasks = @(
    @{
        Name        = "DataHarvester-Streamer"
        Module      = "src.stream.runner"
        Service     = "streamer"
        Description = "Data Harvester 24/7 Capital.com Live Tick Ingestion Streamer"
    },
    @{
        Name        = "DataHarvester-Dashboard"
        Module      = "src.dashboard.server"
        Service     = "dashboard"
        Description = "Data Harvester Observability Command Center Dashboard (Port 8420)"
    }
)

foreach ($task in $Tasks) {
    $taskName = $task.Name
    $service = $task.Service
    $module = $task.Module
    $desc = $task.Description

    Write-Host "Configuring task: $taskName..." -ForegroundColor Yellow

    # Stop and unregister existing task if it exists
    $existing = Get-ScheduledTask -TaskName $taskName -ErrorAction SilentlyContinue
    if ($existing) {
        Write-Host "  Stopping existing task $taskName..."
        Stop-ScheduledTask -TaskName $taskName -ErrorAction SilentlyContinue
        Start-Sleep -Seconds 1
        Unregister-ScheduledTask -TaskName $taskName -Confirm:$false
    }

    # Action: Run supervisor with python.exe (runs hidden when scheduled)
    $arguments = "tools/service_supervisor.py --name $service --module $module"
    $action = New-ScheduledTaskAction -Execute $PythonExe -Argument $arguments -WorkingDirectory $RepoRoot

    # Trigger: Run at user logon
    $trigger = New-ScheduledTaskTrigger -AtLogOn

    # Settings: Never stop, restart on failure, allow on battery
    $settings = New-ScheduledTaskSettingsSet `
        -AllowStartIfOnBatteries `
        -DontStopIfGoingOnBatteries `
        -ExecutionTimeLimit ([TimeSpan]::Zero) `
        -RestartCount 3 `
        -RestartInterval (New-TimeSpan -Minutes 1)

    # Register task for current user
    Register-ScheduledTask `
        -TaskName $taskName `
        -Action $action `
        -Trigger $trigger `
        -Settings $settings `
        -Description $desc | Out-Null

    # Start task immediately
    Start-ScheduledTask -TaskName $taskName
    Write-Host "  ✓ Task $taskName registered and started!" -ForegroundColor Green
}

Write-Host ""
Write-Host "============================================================" -ForegroundColor Cyan
Write-Host "  Installation Complete!" -ForegroundColor Green
Write-Host "============================================================" -ForegroundColor Cyan
Write-Host "Services are now running in the background:"
Write-Host "  • Streamer:  Ingesting ticks into data/streaming.duckdb"
Write-Host "  • Dashboard: Running at http://localhost:8420"
Write-Host ""
Write-Host "Logs are located in: $RepoRoot\logs\"
Write-Host "  • logs\streamer.log"
Write-Host "  • logs\dashboard.log"
Write-Host ""
Write-Host "Both services automatically detect code changes in src/ and git updates!" -ForegroundColor Yellow
