$ErrorActionPreference = "Stop"

Write-Host "Running full data pipeline (Bronze -> Silver -> Gold -> Reports)..." -ForegroundColor Cyan

if (Test-Path "./venv/Scripts/python.exe") {
    $pythonExe = "./venv/Scripts/python.exe"
} else {
    $pythonExe = "python"
}

Write-Host "Using Python: $pythonExe" -ForegroundColor Yellow

Write-Host "[1/4] Bronze - Fetch holidays" -ForegroundColor Green
& $pythonExe "src/bronze/fetch_holidays.py"

Write-Host "[2/4] Silver - Transform issues" -ForegroundColor Green
& $pythonExe "src/silver/transform_silver.py"

Write-Host "[3/4] Gold - Build analytics" -ForegroundColor Green
& $pythonExe "src/gold/build_gold.py"

Write-Host "[4/4] Gold - Build SLA CSV reports" -ForegroundColor Green
& $pythonExe "src/gold/build_sla_reports.py"

Write-Host "Pipeline completed successfully." -ForegroundColor Cyan
