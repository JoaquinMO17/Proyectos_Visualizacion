# PowerShell script to run the ETL pipeline
# Usage: .\run_etl.ps1

Write-Host " Starting Movie Data ETL Pipeline..." -ForegroundColor Green

# Check if Docker is running
try {
    docker ps | Out-Null
    Write-Host " Docker is running" -ForegroundColor Green
} catch {
    Write-Host " Docker is not running. Please start Docker Desktop first." -ForegroundColor Red
    exit 1
}

# Check if containers are up
$dbContainer = docker ps --filter "name=fastapi-db" --format "{{.Names}}"
if (-not $dbContainer) {
    Write-Host " Database container not found. Starting services..." -ForegroundColor Yellow
    docker-compose up -d db
    Start-Sleep -Seconds 10
}

# Run the ETL pipeline
Write-Host " Executing ETL pipeline..." -ForegroundColor Cyan
try {
    docker-compose run --rm web python -m scripts.etl
    Write-Host " ETL pipeline completed successfully!" -ForegroundColor Green
} catch {
    Write-Host " ETL pipeline failed. Check logs for details." -ForegroundColor Red
    Write-Host " Check logs in the logs directory for more information." -ForegroundColor Yellow
    exit 1
}

Write-Host " ETL process finished!" -ForegroundColor Green