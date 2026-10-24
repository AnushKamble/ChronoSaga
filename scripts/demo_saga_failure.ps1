# ChronoSaga Saga Failure Demo (PowerShell)
# Demonstrates forward activity execution, downstream failure, and strict reverse LIFO compensation.
Write-Host "==========================================================" -ForegroundColor Cyan
Write-Host "     ChronoSaga Demo: Saga Reverse LIFO Compensation      " -ForegroundColor Cyan
Write-Host "==========================================================" -ForegroundColor Cyan

$binary = "bin\chronosaga.exe"
if (-not (Test-Path $binary)) {
    Write-Host "Building $binary..." -ForegroundColor Yellow
    go build -o $binary ./cmd/chronosaga
    if ($LASTEXITCODE -ne 0) {
        Write-Host "Build failed." -ForegroundColor Red
        exit 1
    }
}

& $binary demo saga-failure
