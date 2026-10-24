# ChronoSaga Benchmark Suite (PowerShell)
# Runs the full benchmark workload and records measurements into results/
Write-Host "==========================================================" -ForegroundColor Cyan
Write-Host "        ChronoSaga Benchmark Suite Protocol               " -ForegroundColor Cyan
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

& $binary benchmark @args
