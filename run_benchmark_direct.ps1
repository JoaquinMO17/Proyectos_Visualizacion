# Direct Benchmark Runner - Avoids Docker network issues
# This script runs the ETL benchmark directly using docker exec

Write-Host "ETL Performance Benchmark (Direct Mode)" -ForegroundColor Cyan
Write-Host "====================================" -ForegroundColor Gray

# Check if Docker is running
Write-Host "Checking Docker status..." -ForegroundColor Yellow
try {
    docker info | Out-Null
    Write-Host "Docker is running" -ForegroundColor Green
} catch {
    Write-Host "Docker is not running. Please start Docker Desktop." -ForegroundColor Red
    exit 1
}

# Start services if not running
Write-Host "Starting Docker services..." -ForegroundColor Yellow
docker-compose up -d

Write-Host "Waiting for services to be ready..." -ForegroundColor Yellow
Start-Sleep -Seconds 10

# Get container name
$webContainer = docker ps --filter "name=fastapi-app" --format "{{.Names}}"

if (-not $webContainer) {
    Write-Host "Web container not found. Please check Docker services." -ForegroundColor Red
    exit 1
}

Write-Host "Using container: $webContainer" -ForegroundColor Green

Write-Host ""
Write-Host "Installing benchmark dependencies in container..." -ForegroundColor Cyan
docker exec $webContainer pip install psutil matplotlib seaborn

Write-Host ""
Write-Host "Copying benchmark files to container..." -ForegroundColor Cyan

# Copy benchmark files directly to running container
docker cp "./benchmark_etl.py" "${webContainer}:/app/benchmark_etl.py"
docker cp "./benchmark_utils.py" "${webContainer}:/app/benchmark_utils.py"

Write-Host "Files copied successfully" -ForegroundColor Green

Write-Host ""
Write-Host "Executing ETL Benchmark..." -ForegroundColor Cyan
Write-Host "This will measure:" -ForegroundColor White
Write-Host "  - Processing time and speed" -ForegroundColor Gray
Write-Host "  - Memory usage patterns" -ForegroundColor Gray
Write-Host "  - Resource utilization" -ForegroundColor Gray
Write-Host "  - File generation metrics" -ForegroundColor Gray
Write-Host ""

# Execute benchmark directly
docker exec $webContainer python benchmark_etl.py

Write-Host ""
Write-Host "Generating detailed report..." -ForegroundColor Cyan
docker exec $webContainer python benchmark_utils.py

# Copy results back to host
Write-Host ""
Write-Host "Copying results to host..." -ForegroundColor Yellow

try {
    docker cp "${webContainer}:/app/benchmark_results.json" "./benchmark_results.json"
    Write-Host "benchmark_results.json copied" -ForegroundColor Green
} catch {
    Write-Host "benchmark_results.json not found" -ForegroundColor Yellow
}

try {
    docker cp "${webContainer}:/app/BENCHMARK_REPORT.md" "./BENCHMARK_REPORT.md"
    Write-Host "BENCHMARK_REPORT.md copied" -ForegroundColor Green
} catch {
    Write-Host "BENCHMARK_REPORT.md not found" -ForegroundColor Yellow
}

try {
    docker cp "${webContainer}:/app/benchmark_charts.png" "./benchmark_charts.png"
    Write-Host "benchmark_charts.png copied" -ForegroundColor Green
} catch {
    Write-Host "benchmark_charts.png not found" -ForegroundColor Yellow
}

Write-Host ""
Write-Host "ETL Benchmark Complete!" -ForegroundColor Green
Write-Host "====================================" -ForegroundColor Gray

# Display summary if results exist
if (Test-Path "benchmark_results.json") {
    Write-Host ""
    Write-Host "Quick Summary:" -ForegroundColor Cyan
    try {
        $results = Get-Content "benchmark_results.json" | ConvertFrom-Json
        Write-Host "  Movies Processed: $($results.total_records.ToString('N0'))" -ForegroundColor White
        Write-Host "  Total Time: $([math]::Round($results.total_duration_minutes, 2)) minutes" -ForegroundColor White
        Write-Host "  Speed: $([math]::Round($results.records_per_second, 0)) records/second" -ForegroundColor White
        Write-Host "  Peak Memory: $([math]::Round($results.peak_memory_mb, 1)) MB" -ForegroundColor White
        Write-Host "  Memory Delta: +$([math]::Round($results.total_memory_delta_mb, 1)) MB" -ForegroundColor White
        Write-Host "  Efficiency: $([math]::Round($results.memory_efficiency_mb_per_1k_records, 2)) MB per 1K records" -ForegroundColor White
    } catch {
        Write-Host "  Full results available in benchmark_results.json" -ForegroundColor Gray
    }

    Write-Host ""
    Write-Host "Generated Files:" -ForegroundColor White
    Write-Host "  - benchmark_results.json - Raw performance data" -ForegroundColor Gray
    Write-Host "  - BENCHMARK_REPORT.md - Detailed analysis report" -ForegroundColor Gray
    Write-Host "  - benchmark_charts.png - Performance visualizations" -ForegroundColor Gray
}

Write-Host ""
Write-Host "Benchmark completed successfully!" -ForegroundColor Green