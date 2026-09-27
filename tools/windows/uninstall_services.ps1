# Uninstalls Data Harvester scheduled tasks cleanly

$Tasks = @("DataHarvester-Streamer", "DataHarvester-Dashboard")

Write-Host "Uninstalling Data Harvester Windows tasks..." -ForegroundColor Yellow

# Run stop script first
& "$PSScriptRoot\stop_services.ps1"

foreach ($name in $Tasks) {
    $task = Get-ScheduledTask -TaskName $name -ErrorAction SilentlyContinue
    if ($task) {
        Unregister-ScheduledTask -TaskName $name -Confirm:$false
        Write-Host "  ✓ Unregistered task: $name" -ForegroundColor Green
    }
}

Write-Host "Data Harvester services uninstalled successfully." -ForegroundColor Green
