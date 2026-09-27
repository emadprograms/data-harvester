# Check status of Data Harvester Windows background tasks and inspect recent logs

$Tasks = @("DataHarvester-Streamer", "DataHarvester-Dashboard")
$RepoRoot = (Get-Item "$PSScriptRoot\..\..").FullName

Write-Host "============================================================" -ForegroundColor Cyan
Write-Host "  Data Harvester - Windows Services Status" -ForegroundColor Cyan
Write-Host "============================================================" -ForegroundColor Cyan

foreach ($name in $Tasks) {
    $task = Get-ScheduledTask -TaskName $name -ErrorAction SilentlyContinue
    if (-not $task) {
        Write-Host "[$name] NOT INSTALLED" -ForegroundColor Red
        continue
    }

    $info = Get-ScheduledTaskInfo -TaskName $name -ErrorAction SilentlyContinue
    $state = $task.State
    $color = if ($state -eq "Running") { "Green" } else { "Yellow" }
    Write-Host "[$name] State: $state" -ForegroundColor $color
    Write-Host "  Last Run Time:    $($info.LastRunTime)"
    Write-Host "  Last Task Result: $($info.LastTaskResult)"
}

Write-Host ""
Write-Host "--- Recent Streamer Log ---" -ForegroundColor Yellow
$streamerLog = "$RepoRoot\logs\streamer.log"
if (Test-Path $streamerLog) {
    Get-Content $streamerLog -Tail 10
} else {
    Write-Host "(No streamer log found yet)" -ForegroundColor Gray
}

Write-Host ""
Write-Host "--- Recent Dashboard Log ---" -ForegroundColor Yellow
$dashboardLog = "$RepoRoot\logs\dashboard.log"
if (Test-Path $dashboardLog) {
    Get-Content $dashboardLog -Tail 10
} else {
    Write-Host "(No dashboard log found yet)" -ForegroundColor Gray
}
Write-Host ""
