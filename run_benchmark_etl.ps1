# ETL Benchmark Runner - Movie Data Processing Performance Analysis
# This script runs the ETL pipeline with comprehensive performance monitoring

Write-Host "ETL Performance Benchmark" -ForegroundColor Cyan
Write-Host "====================================" -ForegroundColor Gray
Write-Host "Analyzing performance of movie data ETL pipeline" -ForegroundColor White
Write-Host ""

# Check if Docker is running
Write-Host "Checking Docker status..." -ForegroundColor Yellow
$dockerRunning = $false
try {
    docker info | Out-Null
    $dockerRunning = $true
    Write-Host "Docker is running" -ForegroundColor Green
} catch {
    Write-Host "Docker is not running or not accessible" -ForegroundColor Red
    Write-Host "Please start Docker Desktop and try again." -ForegroundColor Yellow
    exit 1
}

# Check for database container
Write-Host "Checking database container..." -ForegroundColor Yellow
$dbContainer = docker ps -q --filter "name=fastapi-db"

if (-not $dbContainer) {
    Write-Host "Database container not found. Starting services..." -ForegroundColor Yellow
    docker-compose up -d
    Write-Host "Waiting for database to be ready..." -ForegroundColor Yellow
    Start-Sleep -Seconds 10
} else {
    Write-Host "Database container is running" -ForegroundColor Green
}

Write-Host ""
Write-Host "Starting ETL Performance Benchmark..." -ForegroundColor Cyan
Write-Host "This will measure:" -ForegroundColor White
Write-Host "  - Processing time and speed" -ForegroundColor Gray
Write-Host "  - Memory usage patterns" -ForegroundColor Gray
Write-Host "  - Resource utilization" -ForegroundColor Gray
Write-Host "  - File generation metrics" -ForegroundColor Gray
Write-Host ""

# Run the benchmark
Write-Host "Executing benchmark..." -ForegroundColor Yellow
try {
    docker-compose run --rm web python benchmark_etl.py
    Write-Host ""
    Write-Host "Benchmark execution completed!" -ForegroundColor Green
} catch {
    Write-Host "Benchmark execution failed: $_" -ForegroundColor Red
    exit 1
}

# Check if results file was generated
if (Test-Path "benchmark_results.json") {
    Write-Host "Benchmark results saved to: benchmark_results.json" -ForegroundColor Green

    Write-Host ""
    Write-Host "Generating detailed report and charts..." -ForegroundColor Cyan

    try {
        docker-compose run --rm web python benchmark_utils.py
        Write-Host "Detailed report generated!" -ForegroundColor Green

        Write-Host ""
        Write-Host "Generated Files:" -ForegroundColor White
        if (Test-Path "BENCHMARK_REPORT.md") {
            Write-Host "  BENCHMARK_REPORT.md - Detailed performance report" -ForegroundColor Gray
        }
        if (Test-Path "benchmark_charts.png") {
            Write-Host "  benchmark_charts.png - Performance visualizations" -ForegroundColor Gray
        }
        Write-Host "  benchmark_results.json - Raw benchmark data" -ForegroundColor Gray

    } catch {
        Write-Host "Report generation failed: $_" -ForegroundColor Yellow
        Write-Host "Raw results are still available in benchmark_results.json" -ForegroundColor Gray
    }
} else {
    Write-Host "Benchmark results file not found" -ForegroundColor Yellow
}

Write-Host ""
Write-Host "ETL Benchmark Complete!" -ForegroundColor Green
Write-Host "====================================" -ForegroundColor Gray

# Display quick summary if results exist
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
}

Write-Host ""
Write-Host "Next Steps:" -ForegroundColor Yellow
Write-Host "  - Review BENCHMARK_REPORT.md for detailed analysis" -ForegroundColor Gray
Write-Host "  - Check benchmark_charts.png for visual performance data" -ForegroundColor Gray
Write-Host "  - Share benchmark_results.json with your team" -ForegroundColor Gray
Write-Host ""