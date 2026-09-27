# Sets up a git post-merge hook to notify upon git pull
# (The running service_supervisor already monitors .git/HEAD and src/ files to trigger reloads automatically)

$RepoRoot = (Get-Item "$PSScriptRoot\..\..").FullName
$HookPath = "$RepoRoot\.git\hooks\post-merge"

$HookContent = @"
#!/bin/sh
echo "--------------------------------------------------------"
echo "Git pull completed! Data Harvester services will reload"
echo "automatically within 2 seconds."
echo "--------------------------------------------------------"
"@

[System.IO.File]::WriteAllText($HookPath, $HookContent)

Write-Host "✓ Git post-merge hook installed at .git/hooks/post-merge" -ForegroundColor Green
Write-Host "Whenever 'git pull' is run, the supervisors will automatically detect the new code and reload!" -ForegroundColor Cyan
