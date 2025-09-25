@echo off
REM Batch script to run the ETL pipeline
REM Usage: run_etl.bat

echo Starting Movie Data ETL Pipeline...

REM Check if Docker is running
docker ps >nul 2>&1
if %errorlevel% neq 0 (
    echo Docker is not running. Please start Docker Desktop first.
    pause
    exit /b 1
)

echo Docker is running

REM Check if database container is up
docker ps --filter "name=fastapi-db" --format "{{.Names}}" | findstr "fastapi-db" >nul
if %errorlevel% neq 0 (
    echo Database container not found. Starting services...
    docker-compose up -d db
    timeout /t 10 /nobreak >nul
)

REM Run the ETL pipeline
echo Executing ETL pipeline...
docker-compose run --rm web python -m scripts.etl
if %errorlevel% neq 0 (
    echo ETL pipeline failed. Check logs for details.
    echo Check logs in the 'logs' directory for more information.
    pause
    exit /b 1
)

echo ETL pipeline completed successfully!
echo ETL process finished!
pause