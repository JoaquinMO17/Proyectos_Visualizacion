import pandas as pd
import os
from datetime import datetime

def create_subset_datasets():
    """Create smaller datasets for scalability testing"""

    print("📊 Creating test datasets for scalability benchmarking...")

    # Load the full dataset
    full_csv_path = 'data/imdb_movies_final.csv'

    if not os.path.exists(full_csv_path):
        print(f"❌ Source file not found: {full_csv_path}")
        return

    print(f"📂 Loading full dataset from: {full_csv_path}")
    df_full = pd.read_csv(full_csv_path)
    total_records = len(df_full)
    print(f"📊 Full dataset contains: {total_records:,} records")

    # Create test data directory
    test_data_dir = 'data/test_datasets'
    os.makedirs(test_data_dir, exist_ok=True)

    # Define test dataset sizes
    test_sizes = [
        ('small', 1000),
        ('medium', 5000),
        ('large', 10000),
        ('xlarge', 25000)
    ]

    print("\n🎯 Creating test datasets:")

    for size_name, size_records in test_sizes:
        if size_records > total_records:
            print(f"⚠️  Skipping {size_name} ({size_records:,} records) - exceeds dataset size")
            continue

        # Take a representative sample
        df_subset = df_full.head(size_records).copy()

        # Create filename
        output_file = f"{test_data_dir}/imdb_movies_{size_name}_{size_records}.csv"

        # Save subset
        df_subset.to_csv(output_file, index=False)

        # Calculate file size
        file_size_mb = os.path.getsize(output_file) / 1024 / 1024

        print(f"✅ {size_name.upper()}: {size_records:,} records → {output_file} ({file_size_mb:.1f} MB)")

    print(f"\n📁 Test datasets created in: {test_data_dir}/")
    print("\n💡 Usage:")
    print("  • Copy any test dataset to 'data/imdb_movies_final.csv'")
    print("  • Run './run_benchmark_etl.ps1' to benchmark smaller datasets")
    print("  • Compare performance across different dataset sizes")


def create_scalability_benchmark_script():
    """Create a script to run benchmarks on different dataset sizes"""

    script_content = '''# Scalability Benchmark Script
# Tests ETL performance across different dataset sizes

Write-Host "🎯 ETL Scalability Benchmark" -ForegroundColor Cyan
Write-Host "====================================" -ForegroundColor Gray

$testDatasets = @(
    @{name="small"; size=1000; file="data/test_datasets/imdb_movies_small_1000.csv"},
    @{name="medium"; size=5000; file="data/test_datasets/imdb_movies_medium_5000.csv"},
    @{name="large"; size=10000; file="data/test_datasets/imdb_movies_large_10000.csv"},
    @{name="xlarge"; size=25000; file="data/test_datasets/imdb_movies_xlarge_25000.csv"}
)

$originalFile = "data/imdb_movies_final.csv"
$backupFile = "data/imdb_movies_final_backup.csv"

# Backup original file
if (Test-Path $originalFile) {
    Write-Host "💾 Backing up original dataset..." -ForegroundColor Yellow
    Copy-Item $originalFile $backupFile
}

$results = @()

foreach ($dataset in $testDatasets) {
    if (-not (Test-Path $dataset.file)) {
        Write-Host "⚠️  Skipping $($dataset.name): file not found" -ForegroundColor Yellow
        continue
    }

    Write-Host ""
    Write-Host "🎬 Testing $($dataset.name.ToUpper()) dataset ($($dataset.size) records)..." -ForegroundColor Cyan

    # Copy test dataset to main location
    Copy-Item $dataset.file $originalFile

    # Run benchmark
    $benchmarkFile = "benchmark_results_$($dataset.name).json"

    try {
        # Start Docker services
        docker-compose up -d
        Start-Sleep -Seconds 5

        # Run benchmark
        docker-compose run --rm web python benchmark_etl.py

        # Rename results file
        if (Test-Path "benchmark_results.json") {
            Move-Item "benchmark_results.json" $benchmarkFile
            Write-Host "✅ Results saved to: $benchmarkFile" -ForegroundColor Green
        }

        # Clean up database for next test
        docker-compose down

    } catch {
        Write-Host "❌ Benchmark failed for $($dataset.name): $_" -ForegroundColor Red
    }
}

# Restore original file
if (Test-Path $backupFile) {
    Write-Host ""
    Write-Host "🔄 Restoring original dataset..." -ForegroundColor Yellow
    Move-Item $backupFile $originalFile
}

Write-Host ""
Write-Host "🏁 Scalability benchmark complete!" -ForegroundColor Green
Write-Host "📊 Check benchmark_results_*.json files for detailed results" -ForegroundColor Gray
'''

    with open('run_scalability_benchmark.ps1', 'w', encoding='utf-8') as f:
        f.write(script_content)

    print("📄 Created scalability benchmark script: run_scalability_benchmark.ps1")


if __name__ == "__main__":
    print("🎯 ETL Test Dataset Generator")
    print("=" * 40)

    create_subset_datasets()
    print()
    create_scalability_benchmark_script()

    print("\n✅ Setup complete!")
    print("\n🚀 Quick Start:")
    print("  1. Run: python create_test_datasets.py")
    print("  2. Run: ./run_benchmark_etl.ps1 (for full dataset)")
    print("  3. Run: ./run_scalability_benchmark.ps1 (for all sizes)")