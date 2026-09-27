# Stop both Data Harvester Windows background tasks and any running python supervisor processes

$Tasks = @("DataHarvester-Streamer", "DataHarvester-Dashboard")

Write-Host "Stopping Data Harvester background tasks..." -ForegroundColor Yellow

foreach ($name in $Tasks) {
    $task = Get-ScheduledTask -TaskName $name -ErrorAction SilentlyContinue
    if ($task) {
        Stop-ScheduledTask -TaskName $name -ErrorAction SilentlyContinue
        Write-Host "  Stopped task: $name" -ForegroundColor Green
    }
}

# Also ensure any lingering python supervisors or children are terminated
$supervisors = Get-CimInstance Win32_Process | Where-Object { $_.CommandLine -like "*service_supervisor.py*" }
foreach ($p in $supervisors) {
    Write-Host "  Terminating supervisor process PID $($p.ProcessId)..."
    Stop-Process -Id $p.ProcessId -Force -ErrorAction SilentlyContinue
}

Write-Host "All Data Harvester services stopped." -ForegroundColor Green
