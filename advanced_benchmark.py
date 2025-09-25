import time
import psutil
import threading
import os
import json
from datetime import datetime
from scripts.etl import run_etl
from scripts.logging_conf import configure_logging
import logging

class AdvancedETLBenchmark:
    """Advanced benchmark system with real-time monitoring"""

    def __init__(self):
        self.process = psutil.Process()
        self.results = {}
        self.monitoring_data = []
        self.start_time = None
        self.monitoring = False
        self.logger = logging.getLogger(__name__)

    def get_system_metrics(self):
        """Get comprehensive system metrics"""
        return {
            'memory_mb': self.process.memory_info().rss / 1024 / 1024,
            'memory_percent': self.process.memory_percent(),
            'cpu_percent': self.process.cpu_percent(),
            'cpu_times': self.process.cpu_times(),
            'io_counters': self.process.io_counters() if hasattr(self.process, 'io_counters') else None,
            'num_threads': self.process.num_threads(),
            'timestamp': time.time()
        }

    def monitor_system_continuously(self):
        """Continuously monitor system resources during ETL"""
        while self.monitoring:
            try:
                metrics = self.get_system_metrics()
                self.monitoring_data.append(metrics)
                time.sleep(1)  # Monitor every second
            except Exception as e:
                self.logger.warning(f"Monitoring error: {str(e)}")
                break

    def start_monitoring(self):
        """Start continuous system monitoring"""
        self.monitoring = True
        self.monitoring_thread = threading.Thread(target=self.monitor_system_continuously, daemon=True)
        self.monitoring_thread.start()
        print("📊 Real-time monitoring started...")

    def stop_monitoring(self):
        """Stop continuous system monitoring"""
        self.monitoring = False
        if hasattr(self, 'monitoring_thread'):
            self.monitoring_thread.join(timeout=2)
        print("📊 Monitoring stopped")

    def analyze_monitoring_data(self):
        """Analyze the collected monitoring data"""
        if not self.monitoring_data:
            return {}

        memory_values = [m['memory_mb'] for m in self.monitoring_data]
        cpu_values = [m['cpu_percent'] for m in self.monitoring_data]

        analysis = {
            'monitoring_duration_seconds': len(self.monitoring_data),
            'memory_stats': {
                'min_mb': min(memory_values),
                'max_mb': max(memory_values),
                'avg_mb': sum(memory_values) / len(memory_values),
                'peak_delta_mb': max(memory_values) - min(memory_values)
            },
            'cpu_stats': {
                'min_percent': min(cpu_values),
                'max_percent': max(cpu_values),
                'avg_percent': sum(cpu_values) / len(cpu_values)
            },
            'performance_profile': self.create_performance_profile()
        }

        return analysis

    def create_performance_profile(self):
        """Create a performance profile based on monitoring data"""
        if not self.monitoring_data:
            return {}

        # Divide into phases based on memory usage patterns
        memory_values = [m['memory_mb'] for m in self.monitoring_data]

        # Find major memory increases (likely phase transitions)
        phases = []
        current_phase_start = 0

        for i in range(1, len(memory_values)):
            memory_increase = memory_values[i] - memory_values[i-1]
            if memory_increase > 50:  # Significant memory increase (50MB+)
                phases.append({
                    'start_index': current_phase_start,
                    'end_index': i-1,
                    'duration_seconds': i - current_phase_start,
                    'memory_start_mb': memory_values[current_phase_start],
                    'memory_end_mb': memory_values[i-1]
                })
                current_phase_start = i

        # Add final phase
        if current_phase_start < len(memory_values) - 1:
            phases.append({
                'start_index': current_phase_start,
                'end_index': len(memory_values) - 1,
                'duration_seconds': len(memory_values) - current_phase_start,
                'memory_start_mb': memory_values[current_phase_start],
                'memory_end_mb': memory_values[-1]
            })

        return {
            'total_phases_detected': len(phases),
            'phases': phases[:5]  # Limit to first 5 phases for readability
        }

    def get_file_metrics(self):
        """Get detailed file metrics"""
        files = {
            'source_csv': 'data/imdb_movies_final.csv',
            'generated_json': 'data/raw/imdb_movies_final.json',
            'validation_csv': 'data/raw/imdb_movies_back.csv',
            'processed_csv': 'data/processed/processed.csv'
        }

        file_metrics = {}
        total_size = 0

        for name, path in files.items():
            if os.path.exists(path):
                size_bytes = os.path.getsize(path)
                total_size += size_bytes
                file_metrics[name] = {
                    'exists': True,
                    'size_bytes': size_bytes,
                    'size_mb': size_bytes / 1024 / 1024,
                    'size_kb': size_bytes / 1024
                }
            else:
                file_metrics[name] = {'exists': False}

        file_metrics['total_generated_mb'] = total_size / 1024 / 1024
        return file_metrics

    def run_comprehensive_benchmark(self):
        """Run comprehensive ETL benchmark with advanced monitoring"""
        print("🎯 Advanced ETL Performance Benchmark")
        print("=" * 50)

        # Get baseline metrics
        baseline = self.get_system_metrics()
        print(f"📊 Baseline: {baseline['memory_mb']:.1f} MB RAM, {baseline['cpu_percent']:.1f}% CPU")

        # Start monitoring
        self.start_time = time.time()
        self.start_monitoring()

        try:
            # Configure logging
            configure_logging()

            print("\n🚀 Running ETL with real-time monitoring...")

            # Run the ETL
            run_etl()

            # Stop monitoring
            self.stop_monitoring()

            # Calculate final metrics
            end_time = time.time()
            final_metrics = self.get_system_metrics()
            total_duration = end_time - self.start_time

            # Analyze monitoring data
            monitoring_analysis = self.analyze_monitoring_data()

            # Get file metrics
            file_metrics = self.get_file_metrics()

            # Estimate records (known from ETL)
            total_records = 85736

            # Compile comprehensive results
            self.results = {
                'benchmark_metadata': {
                    'timestamp': datetime.now().isoformat(),
                    'benchmark_type': 'advanced',
                    'monitoring_samples': len(self.monitoring_data)
                },
                'dataset_info': {
                    'total_records': total_records,
                    'source_file_mb': file_metrics.get('source_csv', {}).get('size_mb', 0)
                },
                'performance_metrics': {
                    'total_duration_seconds': total_duration,
                    'total_duration_minutes': total_duration / 60,
                    'records_per_second': total_records / total_duration if total_duration > 0 else 0,
                    'mb_processed_per_second': file_metrics.get('source_csv', {}).get('size_mb', 0) / total_duration if total_duration > 0 else 0
                },
                'memory_analysis': monitoring_analysis.get('memory_stats', {}),
                'cpu_analysis': monitoring_analysis.get('cpu_stats', {}),
                'resource_efficiency': {
                    'memory_per_1k_records_mb': monitoring_analysis.get('memory_stats', {}).get('peak_delta_mb', 0) / (total_records / 1000),
                    'cpu_efficiency_score': monitoring_analysis.get('cpu_stats', {}).get('avg_percent', 0) / monitoring_analysis.get('cpu_stats', {}).get('max_percent', 1) if monitoring_analysis.get('cpu_stats', {}).get('max_percent', 0) > 0 else 0
                },
                'file_metrics': file_metrics,
                'performance_profile': monitoring_analysis.get('performance_profile', {}),
                'raw_monitoring_data': self.monitoring_data[-100:]  # Keep last 100 samples for analysis
            }

            self.print_comprehensive_summary()
            self.save_advanced_results()

            print("✅ Advanced benchmark completed successfully!")

        except Exception as e:
            self.stop_monitoring()
            print(f"❌ Advanced benchmark failed: {str(e)}")
            self.logger.error(f"Advanced benchmark failed: {str(e)}")
            raise

    def print_comprehensive_summary(self):
        """Print comprehensive benchmark summary"""
        print("\n" + "=" * 60)
        print("🎯 ADVANCED ETL BENCHMARK RESULTS")
        print("=" * 60)

        perf = self.results['performance_metrics']
        mem = self.results['memory_analysis']
        cpu = self.results['cpu_analysis']
        files = self.results['file_metrics']
        efficiency = self.results['resource_efficiency']

        print(f"📊 Dataset: {self.results['dataset_info']['total_records']:,} movies ({self.results['dataset_info']['source_file_mb']:.1f} MB)")
        print(f"⏱️  Processing Time: {perf['total_duration_minutes']:.2f} minutes ({perf['total_duration_seconds']:.1f}s)")
        print(f"🚀 Throughput: {perf['records_per_second']:.0f} records/sec, {perf['mb_processed_per_second']:.2f} MB/sec")

        print(f"\n💾 Memory Analysis:")
        print(f"   📈 Peak Memory: {mem.get('max_mb', 0):.1f} MB")
        print(f"   📊 Average Memory: {mem.get('avg_mb', 0):.1f} MB")
        print(f"   📉 Memory Range: {mem.get('min_mb', 0):.1f} - {mem.get('max_mb', 0):.1f} MB")
        print(f"   🎯 Memory Efficiency: {efficiency['memory_per_1k_records_mb']:.2f} MB per 1K records")

        print(f"\n🖥️  CPU Analysis:")
        print(f"   📈 Peak CPU: {cpu.get('max_percent', 0):.1f}%")
        print(f"   📊 Average CPU: {cpu.get('avg_percent', 0):.1f}%")
        print(f"   🎯 CPU Efficiency Score: {efficiency['cpu_efficiency_score']:.2f}")

        print(f"\n📁 File Generation:")
        print(f"   📄 Total Files Generated: {files['total_generated_mb']:.1f} MB")
        for name, info in files.items():
            if name != 'total_generated_mb' and isinstance(info, dict) and info.get('exists'):
                print(f"   📄 {name.replace('_', ' ').title()}: {info['size_mb']:.1f} MB")

        profile = self.results['performance_profile']
        if profile.get('total_phases_detected', 0) > 0:
            print(f"\n🔍 Performance Profile: {profile['total_phases_detected']} processing phases detected")

        print(f"\n📊 Monitoring: {self.results['benchmark_metadata']['monitoring_samples']} data points collected")

    def save_advanced_results(self, filename='advanced_benchmark_results.json'):
        """Save advanced benchmark results"""
        try:
            with open(filename, 'w') as f:
                json.dump(self.results, f, indent=2, default=str)
            print(f"\n💾 Advanced results saved to: {filename}")
        except Exception as e:
            print(f"⚠️  Could not save advanced results: {str(e)}")

    def export_monitoring_data(self, filename='monitoring_data.csv'):
        """Export monitoring data to CSV for analysis"""
        if not self.monitoring_data:
            return

        try:
            import pandas as pd
            df = pd.DataFrame(self.monitoring_data)
            df['relative_time'] = df['timestamp'] - df['timestamp'].iloc[0]
            df.to_csv(filename, index=False)
            print(f"📊 Monitoring data exported to: {filename}")
        except ImportError:
            print("⚠️  pandas not available for CSV export")
        except Exception as e:
            print(f"⚠️  Could not export monitoring data: {str(e)}")


def main():
    """Main advanced benchmark execution"""
    print("🎯 Advanced ETL Performance Benchmark")
    print("Real-time monitoring with comprehensive analysis")
    print()

    benchmark = AdvancedETLBenchmark()
    benchmark.run_comprehensive_benchmark()

    # Export additional data
    benchmark.export_monitoring_data()

    print("\n✅ Advanced benchmark completed!")
    print("📄 Check 'advanced_benchmark_results.json' for detailed metrics")
    print("📊 Check 'monitoring_data.csv' for real-time monitoring data")


if __name__ == "__main__":
    main()