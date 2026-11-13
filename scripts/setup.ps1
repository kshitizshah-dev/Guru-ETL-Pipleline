# One-time setup: virtual environment + dependencies + .env file.
$ErrorActionPreference = "Stop"
$ProjectRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
Set-Location $ProjectRoot

if (-not (Test-Path ".venv\Scripts\python.exe")) {
    Write-Host "Creating virtual environment..."
    if (Get-Command py -ErrorAction SilentlyContinue) { & py -3 -m venv .venv } else { & python -m venv .venv }
    if ($LASTEXITCODE -ne 0) { throw "Could not create the virtual environment. Install Python 3.11+ first." }
}

Write-Host "Installing dependencies..."
& ".\.venv\Scripts\python.exe" -m pip install --upgrade pip
& ".\.venv\Scripts\python.exe" -m pip install -r requirements.txt
& ".\.venv\Scripts\python.exe" -m pip install -e . --no-deps
if ($LASTEXITCODE -ne 0) { throw "Dependency installation failed." }

foreach ($dir in @("data\incoming", "logs", ".state")) {
    New-Item -ItemType Directory -Force -Path $dir | Out-Null
}

if (-not (Test-Path ".env")) {
    Copy-Item ".env.example" ".env"
    Write-Host ""
    Write-Host "Created .env - open it and set DATABASE_URL to your hosted database, then run:" -ForegroundColor Yellow
} else {
    Write-Host ".env already exists - left unchanged."
}
Write-Host "  .\.venv\Scripts\python.exe -m ekal_pipeline.run_once --init-db     (creates the tables)"
Write-Host "  .\scripts\install-etl-task.ps1                                    (schedules it)"
