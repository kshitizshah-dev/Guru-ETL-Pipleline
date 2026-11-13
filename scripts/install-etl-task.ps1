# Registers a Windows scheduled task that runs the ETL every N minutes.
# Usage:  .\scripts\install-etl-task.ps1                 (every 60 minutes)
#         .\scripts\install-etl-task.ps1 -EveryMinutes 15
param(
    [int]$EveryMinutes = 60,
    [string]$TaskName = "Ekal ETL"
)
$ErrorActionPreference = "Stop"
$ProjectRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$Runner = Join-Path $ProjectRoot "scripts\run-etl.bat"
if (-not (Test-Path $Runner)) { throw "Not found: $Runner" }
if ($EveryMinutes -lt 1) { throw "-EveryMinutes must be at least 1." }

$Action = New-ScheduledTaskAction -Execute $Runner -WorkingDirectory $ProjectRoot
$Trigger = New-ScheduledTaskTrigger -Once -At (Get-Date).AddMinutes(1) `
    -RepetitionInterval (New-TimeSpan -Minutes $EveryMinutes) `
    -RepetitionDuration (New-TimeSpan -Days 3650)
$Settings = New-ScheduledTaskSettingsSet `
    -StartWhenAvailable `
    -AllowStartIfOnBatteries `
    -DontStopIfGoingOnBatteries `
    -MultipleInstances IgnoreNew `
    -ExecutionTimeLimit (New-TimeSpan -Hours 3)

Register-ScheduledTask -TaskName $TaskName -Action $Action -Trigger $Trigger `
    -Settings $Settings -Description "Imports new Ekal Excel exports into the hosted PostgreSQL database." `
    -Force | Out-Null

Write-Host "Scheduled task '$TaskName' created: runs every $EveryMinutes minute(s)."
Write-Host "Run it now:  Start-ScheduledTask -TaskName '$TaskName'"
Write-Host "Remove it:   Unregister-ScheduledTask -TaskName '$TaskName' -Confirm:`$false"
Write-Host "Logs:        $ProjectRoot\logs\etl.log   and   logs\scheduler.log"
