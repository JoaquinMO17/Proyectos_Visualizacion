import time
import psutil
import os
import json
from datetime import datetime
from scripts.etl import run_etl
from scripts.logging_conf import configure_logging
import logging

class ETLBenchmark:
    def __init__(self):
        self.process = psutil.Process()
        self.results = {}
        self.phase_results = []
        self.start_time = None
        self.logger = logging.getLogger(__name__)

    def get_memory_mb(self):
        """Get current memory usage in MB"""
        return self.process.memory_info().rss / 1024 / 1024

    def get_cpu_percent(self):
        """Get current CPU usage percentage"""
        return self.process.cpu_percent()

    def measure_system_baseline(self):
        """Measure baseline system resources before ETL"""
        baseline = {
            'memory_mb': self.get_memory_mb(),
            'cpu_percent': self.get_cpu_percent(),
            'timestamp': datetime.now()
        }
        print(f"📊 System Baseline: {baseline['memory_mb']:.1f} MB RAM, {baseline['cpu_percent']:.1f}% CPU")
        return baseline

    def measure_phase_start(self, phase_name):
        """Start measuring a specific ETL phase"""
        phase_data = {
            'name': phase_name,
            'start_time': time.time(),
            'start_memory_mb': self.get_memory_mb(),
            'start_cpu': self.get_cpu_percent()
        }
        print(f"🚀 Starting {phase_name}...")
        return phase_data

    def measure_phase_end(self, phase_data):
        """End measuring a specific ETL phase"""
        end_time = time.time()
        end_memory = self.get_memory_mb()

        phase_data.update({
            'end_time': end_time,
            'end_memory_mb': end_memory,
            'duration_seconds': end_time - phase_data['start_time'],
            'memory_delta_mb': end_memory - phase_data['start_memory_mb'],
            'peak_memory_mb': end_memory  # Simplified - could track actual peak
        })

        print(f"✅ {phase_data['name']} completed:")
        print(f"   ⏱️  Duration: {phase_data['duration_seconds']:.2f} seconds")
        print(f"   💾 Memory: {phase_data['start_memory_mb']:.1f} MB → {phase_data['end_memory_mb']:.1f} MB (Δ{phase_data['memory_delta_mb']:+.1f} MB)")

        self.phase_results.append(phase_data)
        return phase_data

    def run_full_benchmark(self):
        """Run complete ETL with benchmarking"""
        print("🏁 Starting ETL Benchmark")
        print("=" * 50)

        # Baseline measurement
        baseline = self.measure_system_baseline()

        # Start overall timing
        self.start_time = time.time()
        overall_start_memory = self.get_memory_mb()

        try:
            # Configure logging
            configure_logging()

            # Simulate the ETL phases by calling run_etl
            # Note: This will run the actual ETL, not individual phases
            print("\n🎬 Running Complete ETL Pipeline...")
            phase_data = self.measure_phase_start("Complete ETL")

            # Run the actual ETL
            run_etl()

            # End measurement
            self.measure_phase_end(phase_data)

            # Calculate overall results
            end_time = time.time()
            total_duration = end_time - self.start_time
            final_memory = self.get_memory_mb()

            # Estimate records processed (known value)
            total_records = 85736
            records_per_second = total_records / total_duration if total_duration > 0 else 0

            self.results = {
                'total_records': total_records,
                'total_duration_seconds': total_duration,
                'total_duration_minutes': total_duration / 60,
                'records_per_second': records_per_second,
                'baseline_memory_mb': baseline['memory_mb'],
                'peak_memory_mb': final_memory,
                'total_memory_delta_mb': final_memory - baseline['memory_mb'],
                'memory_efficiency_mb_per_1k_records': (final_memory - baseline['memory_mb']) / (total_records / 1000),
                'phases': self.phase_results,
                'timestamp': datetime.now().isoformat()
            }

            self.print_summary()
            self.save_results()

        except Exception as e:
            print(f"❌ ETL Benchmark failed: {str(e)}")
            self.logger.error(f"Benchmark failed: {str(e)}")
            raise

    def print_summary(self):
        """Print benchmark summary to console"""
        print("\n" + "=" * 50)
        print("🎯 ETL BENCHMARK RESULTS")
        print("=" * 50)
        print(f"📊 Dataset Size: {self.results['total_records']:,} movies")
        print(f"⏱️  Total Time: {self.results['total_duration_minutes']:.1f} minutes ({self.results['total_duration_seconds']:.1f} seconds)")
        print(f"🚀 Processing Speed: {self.results['records_per_second']:.0f} records/second")
        print(f"💾 Memory Usage:")
        print(f"   📈 Peak Memory: {self.results['peak_memory_mb']:.1f} MB")
        print(f"   📊 Memory Delta: +{self.results['total_memory_delta_mb']:.1f} MB")
        print(f"   🎯 Memory Efficiency: {self.results['memory_efficiency_mb_per_1k_records']:.2f} MB per 1K records")

        print(f"\n📁 Generated Files:")
        if os.path.exists('data/raw/imdb_movies_final.json'):
            size_mb = os.path.getsize('data/raw/imdb_movies_final.json') / 1024 / 1024
            print(f"   📄 JSON file: {size_mb:.1f} MB")
        if os.path.exists('data/processed/processed.csv'):
            size_mb = os.path.getsize('data/processed/processed.csv') / 1024 / 1024
            print(f"   📄 Processed CSV: {size_mb:.1f} MB")

    def save_results(self, filename='benchmark_results.json'):
        """Save benchmark results to JSON file"""
        try:
            with open(filename, 'w') as f:
                json.dump(self.results, f, indent=2, default=str)
            print(f"\n💾 Results saved to: {filename}")
        except Exception as e:
            print(f"⚠️  Could not save results: {str(e)}")

    def get_file_sizes(self):
        """Get sizes of generated files"""
        files = {
            'csv_source': 'data/imdb_movies_final.csv',
            'json_intermediate': 'data/raw/imdb_movies_final.json',
            'csv_back': 'data/raw/imdb_movies_back.csv',
            'processed_csv': 'data/processed/processed.csv'
        }

        sizes = {}
        for name, path in files.items():
            if os.path.exists(path):
                sizes[name] = {
                    'size_bytes': os.path.getsize(path),
                    'size_mb': os.path.getsize(path) / 1024 / 1024
                }

        return sizes


def main():
    """Main benchmark execution function"""
    print("🎯 ETL Performance Benchmark")
    print("Measuring time, memory, and throughput for movie data ETL pipeline")
    print()

    benchmark = ETLBenchmark()
    benchmark.run_full_benchmark()

    print("\n✅ Benchmark completed successfully!")
    print("Check 'benchmark_results.json' for detailed metrics")


if __name__ == "__main__":
    main()