# ChronoSaga Crash Recovery Demo (PowerShell)
# Demonstrates process interruption mid-workflow, offline time elapsed, and deterministic replay resumption.
Write-Host "==========================================================" -ForegroundColor Cyan
Write-Host "  ChronoSaga Demo: Crash Recovery & Deterministic Replay  " -ForegroundColor Cyan
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

& $binary demo crash-recovery
