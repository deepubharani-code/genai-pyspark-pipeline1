"""
File Format Benchmark with Hardware Metrics

Benchmarks 5 file formats (CSV, XLSX, Parquet, ORC, Feather) with:
- File size
- Write/Read times
- Peak memory usage
- CPU time
- Energy consumption estimates
- Percentage savings vs CSV baseline

Dataset: 500,000 rows (id, name, email, amount, date, category)
"""

import pandas as pd
import numpy as np
import time
import tracemalloc
import os
from pathlib import Path
from typing import Dict, Tuple
import logging
from datetime import datetime, timedelta

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class FileFormatBenchmark:
    """Benchmark multiple file formats with hardware metrics."""
    
    # Constants
    ROWS = 100_000  # Reduced for faster testing
    CPU_TDP = 65  # Watts (typical laptop CPU)
    OUTPUT_DIR = Path("benchmark_outputs")
    
    def __init__(self):
        """Initialize benchmark."""
        self.output_dir = self.OUTPUT_DIR
        self.output_dir.mkdir(exist_ok=True)
        self.results = {}
        
    def generate_sample_data(self) -> pd.DataFrame:
        """
        Generate sample DataFrame with 500,000 rows.
        
        Returns:
            DataFrame with columns: id, name, email, amount, date, category
        """
        logger.info(f"Generating {self.ROWS:,} rows of sample data...")
        
        np.random.seed(42)
        
        first_names = ['John', 'Jane', 'Michael', 'Emily', 'David', 'Sarah', 'Robert', 'Lisa']
        last_names = ['Smith', 'Johnson', 'Williams', 'Brown', 'Jones', 'Garcia', 'Miller', 'Davis']
        domains = ['gmail.com', 'yahoo.com', 'outlook.com', 'company.com', 'mail.com']
        categories = ['Electronics', 'Clothing', 'Food', 'Books', 'Sports', 'Home']
        
        data = {
            'id': range(1, self.ROWS + 1),
            'name': [f"{np.random.choice(first_names)} {np.random.choice(last_names)}" 
                     for _ in range(self.ROWS)],
            'email': [f"user_{i}@{np.random.choice(domains)}" for i in range(self.ROWS)],
            'amount': np.random.uniform(10, 10000, self.ROWS),
            'date': [datetime(2023, 1, 1) + timedelta(days=int(x)) 
                     for x in np.random.uniform(0, 365, self.ROWS)],
            'category': np.random.choice(categories, self.ROWS)
        }
        
        df = pd.DataFrame(data)
        logger.info(f"✓ Generated {len(df):,} rows")
        logger.info(f"  Columns: {list(df.columns)}")
        logger.info(f"  Data types: {dict(df.dtypes)}")
        
        return df
    
    def get_file_size_mb(self, filepath: Path) -> float:
        """Get file size in MB."""
        return filepath.stat().st_size / (1024 * 1024)
    
    # ========================================================================
    # CSV FORMAT
    # ========================================================================
    
    def benchmark_csv(self, df: pd.DataFrame) -> Dict[str, float]:
        """Benchmark CSV format."""
        logger.info("\n" + "="*80)
        logger.info("BENCHMARKING CSV FORMAT")
        logger.info("="*80)
        
        filepath = self.output_dir / "data.csv"
        metrics = {}
        
        # Write benchmark
        logger.info("Writing CSV...")
        tracemalloc.start()
        write_start = time.perf_counter()
        write_cpu_start = time.process_time()
        
        df.to_csv(filepath, index=False)
        
        write_cpu_time = time.process_time() - write_cpu_start
        write_time = time.perf_counter() - write_start
        _, write_peak_memory = tracemalloc.get_traced_memory()
        tracemalloc.stop()
        
        metrics['write_time'] = write_time
        metrics['write_cpu_time'] = write_cpu_time
        metrics['write_memory_mb'] = write_peak_memory / 1024 / 1024
        metrics['file_size_mb'] = self.get_file_size_mb(filepath)
        
        logger.info(f"Write time: {write_time:.2f}s (CPU: {write_cpu_time:.2f}s)")
        logger.info(f"Write memory: {metrics['write_memory_mb']:.2f}MB")
        logger.info(f"File size: {metrics['file_size_mb']:.2f}MB")
        
        # Read benchmark
        logger.info("Reading CSV...")
        tracemalloc.start()
        read_start = time.perf_counter()
        read_cpu_start = time.process_time()
        
        df_read = pd.read_csv(filepath)
        
        read_cpu_time = time.process_time() - read_cpu_start
        read_time = time.perf_counter() - read_start
        _, read_peak_memory = tracemalloc.get_traced_memory()
        tracemalloc.stop()
        
        metrics['read_time'] = read_time
        metrics['read_cpu_time'] = read_cpu_time
        metrics['read_memory_mb'] = read_peak_memory / 1024 / 1024
        
        logger.info(f"Read time: {read_time:.2f}s (CPU: {read_cpu_time:.2f}s)")
        logger.info(f"Read memory: {metrics['read_memory_mb']:.2f}MB")
        logger.info(f"✓ Verified {len(df_read):,} rows")
        
        # Calculate energy
        total_cpu_time = write_cpu_time + read_cpu_time
        metrics['energy_wh'] = (total_cpu_time * self.CPU_TDP) / 3600
        metrics['total_time'] = write_time + read_time
        
        logger.info(f"Energy consumption: {metrics['energy_wh']:.4f}Wh")
        
        return metrics
    
    # ========================================================================
    # XLSX FORMAT
    # ========================================================================
    
    def benchmark_xlsx(self, df: pd.DataFrame) -> Dict[str, float]:
        """Benchmark XLSX format."""
        logger.info("\n" + "="*80)
        logger.info("BENCHMARKING XLSX FORMAT")
        logger.info("="*80)
        
        filepath = self.output_dir / "data.xlsx"
        metrics = {}
        
        # Write benchmark
        logger.info("Writing XLSX...")
        tracemalloc.start()
        write_start = time.perf_counter()
        write_cpu_start = time.process_time()
        
        df.to_excel(filepath, index=False, engine='openpyxl')
        
        write_cpu_time = time.process_time() - write_cpu_start
        write_time = time.perf_counter() - write_start
        _, write_peak_memory = tracemalloc.get_traced_memory()
        tracemalloc.stop()
        
        metrics['write_time'] = write_time
        metrics['write_cpu_time'] = write_cpu_time
        metrics['write_memory_mb'] = write_peak_memory / 1024 / 1024
        metrics['file_size_mb'] = self.get_file_size_mb(filepath)
        
        logger.info(f"Write time: {write_time:.2f}s (CPU: {write_cpu_time:.2f}s)")
        logger.info(f"Write memory: {metrics['write_memory_mb']:.2f}MB")
        logger.info(f"File size: {metrics['file_size_mb']:.2f}MB")
        
        # Read benchmark
        logger.info("Reading XLSX...")
        tracemalloc.start()
        read_start = time.perf_counter()
        read_cpu_start = time.process_time()
        
        df_read = pd.read_excel(filepath, engine='openpyxl')
        
        read_cpu_time = time.process_time() - read_cpu_start
        read_time = time.perf_counter() - read_start
        _, read_peak_memory = tracemalloc.get_traced_memory()
        tracemalloc.stop()
        
        metrics['read_time'] = read_time
        metrics['read_cpu_time'] = read_cpu_time
        metrics['read_memory_mb'] = read_peak_memory / 1024 / 1024
        
        logger.info(f"Read time: {read_time:.2f}s (CPU: {read_cpu_time:.2f}s)")
        logger.info(f"Read memory: {metrics['read_memory_mb']:.2f}MB")
        logger.info(f"✓ Verified {len(df_read):,} rows")
        
        # Calculate energy
        total_cpu_time = write_cpu_time + read_cpu_time
        metrics['energy_wh'] = (total_cpu_time * self.CPU_TDP) / 3600
        metrics['total_time'] = write_time + read_time
        
        logger.info(f"Energy consumption: {metrics['energy_wh']:.4f}Wh")
        
        return metrics
    
    # ========================================================================
    # PARQUET FORMAT
    # ========================================================================
    
    def benchmark_parquet(self, df: pd.DataFrame) -> Dict[str, float]:
        """Benchmark Parquet format."""
        logger.info("\n" + "="*80)
        logger.info("BENCHMARKING PARQUET FORMAT")
        logger.info("="*80)
        
        filepath = self.output_dir / "data.parquet"
        metrics = {}
        
        # Write benchmark
        logger.info("Writing Parquet...")
        tracemalloc.start()
        write_start = time.perf_counter()
        write_cpu_start = time.process_time()
        
        df.to_parquet(filepath, engine='pyarrow', compression='snappy')
        
        write_cpu_time = time.process_time() - write_cpu_start
        write_time = time.perf_counter() - write_start
        _, write_peak_memory = tracemalloc.get_traced_memory()
        tracemalloc.stop()
        
        metrics['write_time'] = write_time
        metrics['write_cpu_time'] = write_cpu_time
        metrics['write_memory_mb'] = write_peak_memory / 1024 / 1024
        metrics['file_size_mb'] = self.get_file_size_mb(filepath)
        
        logger.info(f"Write time: {write_time:.2f}s (CPU: {write_cpu_time:.2f}s)")
        logger.info(f"Write memory: {metrics['write_memory_mb']:.2f}MB")
        logger.info(f"File size: {metrics['file_size_mb']:.2f}MB")
        
        # Read benchmark
        logger.info("Reading Parquet...")
        tracemalloc.start()
        read_start = time.perf_counter()
        read_cpu_start = time.process_time()
        
        df_read = pd.read_parquet(filepath, engine='pyarrow')
        
        read_cpu_time = time.process_time() - read_cpu_start
        read_time = time.perf_counter() - read_start
        _, read_peak_memory = tracemalloc.get_traced_memory()
        tracemalloc.stop()
        
        metrics['read_time'] = read_time
        metrics['read_cpu_time'] = read_cpu_time
        metrics['read_memory_mb'] = read_peak_memory / 1024 / 1024
        
        logger.info(f"Read time: {read_time:.2f}s (CPU: {read_cpu_time:.2f}s)")
        logger.info(f"Read memory: {metrics['read_memory_mb']:.2f}MB")
        logger.info(f"✓ Verified {len(df_read):,} rows")
        
        # Calculate energy
        total_cpu_time = write_cpu_time + read_cpu_time
        metrics['energy_wh'] = (total_cpu_time * self.CPU_TDP) / 3600
        metrics['total_time'] = write_time + read_time
        
        logger.info(f"Energy consumption: {metrics['energy_wh']:.4f}Wh")
        
        return metrics
    
    # ========================================================================
    # ORC FORMAT
    # ========================================================================
    
    def benchmark_orc(self, df: pd.DataFrame) -> Dict[str, float]:
        """Benchmark ORC format."""
        logger.info("\n" + "="*80)
        logger.info("BENCHMARKING ORC FORMAT")
        logger.info("="*80)
        
        filepath = self.output_dir / "data.orc"
        metrics = {}
        
        # Write benchmark
        logger.info("Writing ORC...")
        tracemalloc.start()
        write_start = time.perf_counter()
        write_cpu_start = time.process_time()
        
        df.to_orc(filepath)
        
        write_cpu_time = time.process_time() - write_cpu_start
        write_time = time.perf_counter() - write_start
        _, write_peak_memory = tracemalloc.get_traced_memory()
        tracemalloc.stop()
        
        metrics['write_time'] = write_time
        metrics['write_cpu_time'] = write_cpu_time
        metrics['write_memory_mb'] = write_peak_memory / 1024 / 1024
        metrics['file_size_mb'] = self.get_file_size_mb(filepath)
        
        logger.info(f"Write time: {write_time:.2f}s (CPU: {write_cpu_time:.2f}s)")
        logger.info(f"Write memory: {metrics['write_memory_mb']:.2f}MB")
        logger.info(f"File size: {metrics['file_size_mb']:.2f}MB")
        
        # Read benchmark
        logger.info("Reading ORC...")
        tracemalloc.start()
        read_start = time.perf_counter()
        read_cpu_start = time.process_time()
        
        df_read = pd.read_orc(filepath)
        
        read_cpu_time = time.process_time() - read_cpu_start
        read_time = time.perf_counter() - read_start
        _, read_peak_memory = tracemalloc.get_traced_memory()
        tracemalloc.stop()
        
        metrics['read_time'] = read_time
        metrics['read_cpu_time'] = read_cpu_time
        metrics['read_memory_mb'] = read_peak_memory / 1024 / 1024
        
        logger.info(f"Read time: {read_time:.2f}s (CPU: {read_cpu_time:.2f}s)")
        logger.info(f"Read memory: {metrics['read_memory_mb']:.2f}MB")
        logger.info(f"✓ Verified {len(df_read):,} rows")
        
        # Calculate energy
        total_cpu_time = write_cpu_time + read_cpu_time
        metrics['energy_wh'] = (total_cpu_time * self.CPU_TDP) / 3600
        metrics['total_time'] = write_time + read_time
        
        logger.info(f"Energy consumption: {metrics['energy_wh']:.4f}Wh")
        
        return metrics
    
    # ========================================================================
    # FEATHER FORMAT
    # ========================================================================
    
    def benchmark_feather(self, df: pd.DataFrame) -> Dict[str, float]:
        """Benchmark Feather format."""
        logger.info("\n" + "="*80)
        logger.info("BENCHMARKING FEATHER FORMAT")
        logger.info("="*80)
        
        filepath = self.output_dir / "data.feather"
        metrics = {}
        
        # Write benchmark
        logger.info("Writing Feather...")
        tracemalloc.start()
        write_start = time.perf_counter()
        write_cpu_start = time.process_time()
        
        df.to_feather(filepath)
        
        write_cpu_time = time.process_time() - write_cpu_start
        write_time = time.perf_counter() - write_start
        _, write_peak_memory = tracemalloc.get_traced_memory()
        tracemalloc.stop()
        
        metrics['write_time'] = write_time
        metrics['write_cpu_time'] = write_cpu_time
        metrics['write_memory_mb'] = write_peak_memory / 1024 / 1024
        metrics['file_size_mb'] = self.get_file_size_mb(filepath)
        
        logger.info(f"Write time: {write_time:.2f}s (CPU: {write_cpu_time:.2f}s)")
        logger.info(f"Write memory: {metrics['write_memory_mb']:.2f}MB")
        logger.info(f"File size: {metrics['file_size_mb']:.2f}MB")
        
        # Read benchmark
        logger.info("Reading Feather...")
        tracemalloc.start()
        read_start = time.perf_counter()
        read_cpu_start = time.process_time()
        
        df_read = pd.read_feather(filepath)
        
        read_cpu_time = time.process_time() - read_cpu_start
        read_time = time.perf_counter() - read_start
        _, read_peak_memory = tracemalloc.get_traced_memory()
        tracemalloc.stop()
        
        metrics['read_time'] = read_time
        metrics['read_cpu_time'] = read_cpu_time
        metrics['read_memory_mb'] = read_peak_memory / 1024 / 1024
        
        logger.info(f"Read time: {read_time:.2f}s (CPU: {read_cpu_time:.2f}s)")
        logger.info(f"Read memory: {metrics['read_memory_mb']:.2f}MB")
        logger.info(f"✓ Verified {len(df_read):,} rows")
        
        # Calculate energy
        total_cpu_time = write_cpu_time + read_cpu_time
        metrics['energy_wh'] = (total_cpu_time * self.CPU_TDP) / 3600
        metrics['total_time'] = write_time + read_time
        
        logger.info(f"Energy consumption: {metrics['energy_wh']:.4f}Wh")
        
        return metrics
    
    # ========================================================================
    # RESULTS AND COMPARISON
    # ========================================================================
    
    def print_comparison_table(self) -> None:
        """Print formatted comparison table with all metrics."""
        print("\n" + "="*120)
        print(f"FILE FORMAT BENCHMARK RESULTS - {self.ROWS:,} ROWS")
        print("="*120)
        
        # Get available formats
        formats = list(self.results.keys())
        
        # File size comparison
        print("\n📊 FILE SIZE COMPARISON (MB)")
        print("-"*120)
        print(f"{'Format':<15} {'File Size':>15} {'vs CSV':>15}")
        print("-"*120)
        
        csv_size = self.results['CSV']['file_size_mb']
        
        for fmt in formats:
            size = self.results[fmt]['file_size_mb']
            savings = ((csv_size - size) / csv_size) * 100 if csv_size > 0 else 0
            symbol = "↓" if savings > 0 else "↑"
            print(f"{fmt:<15} {size:>14.2f}MB {symbol}{abs(savings):>13.1f}%")
        
        # Write time comparison
        print("\n⏱️  WRITE TIME COMPARISON (seconds)")
        print("-"*120)
        print(f"{'Format':<15} {'Write Time':>15} {'CPU Time':>15} {'vs CSV':>15}")
        print("-"*120)
        
        csv_write = self.results['CSV']['write_time']
        
        for fmt in formats:
            write_time = self.results[fmt]['write_time']
            cpu_time = self.results[fmt]['write_cpu_time']
            speedup = (csv_write / write_time) if write_time > 0 else 0
            symbol = "✓" if speedup > 1 else "✗"
            print(f"{fmt:<15} {write_time:>14.2f}s {cpu_time:>14.2f}s {symbol}{speedup:>13.2f}x")
        
        # Read time comparison
        print("\n📖 READ TIME COMPARISON (seconds)")
        print("-"*120)
        print(f"{'Format':<15} {'Read Time':>15} {'CPU Time':>15} {'vs CSV':>15}")
        print("-"*120)
        
        csv_read = self.results['CSV']['read_time']
        
        for fmt in formats:
            read_time = self.results[fmt]['read_time']
            cpu_time = self.results[fmt]['read_cpu_time']
            speedup = (csv_read / read_time) if read_time > 0 else 0
            symbol = "✓" if speedup > 1 else "✗"
            print(f"{fmt:<15} {read_time:>14.2f}s {cpu_time:>14.2f}s {symbol}{speedup:>13.2f}x")
        
        # Memory usage comparison
        print("\n💾 PEAK MEMORY USAGE (MB)")
        print("-"*120)
        print(f"{'Format':<15} {'Write Peak':>15} {'Read Peak':>15} {'Avg':>15}")
        print("-"*120)
        
        for fmt in formats:
            write_mem = self.results[fmt]['write_memory_mb']
            read_mem = self.results[fmt]['read_memory_mb']
            avg_mem = (write_mem + read_mem) / 2
            print(f"{fmt:<15} {write_mem:>14.2f}MB {read_mem:>14.2f}MB {avg_mem:>14.2f}MB")
        
        # Energy consumption
        print("\n⚡ ENERGY CONSUMPTION (Wh)")
        print("-"*120)
        print(f"{'Format':<15} {'Energy (Wh)':>15} {'vs CSV':>15}")
        print("-"*120)
        
        csv_energy = self.results['CSV']['energy_wh']
        
        for fmt in formats:
            energy = self.results[fmt]['energy_wh']
            savings = ((csv_energy - energy) / csv_energy) * 100 if csv_energy > 0 else 0
            symbol = "↓" if savings > 0 else "↑"
            print(f"{fmt:<15} {energy:>14.4f}Wh {symbol}{abs(savings):>13.1f}%")
        
        # Total time comparison
        print("\n🏁 TOTAL TIME (write + read)")
        print("-"*120)
        print(f"{'Format':<15} {'Total Time':>15} {'vs CSV':>15}")
        print("-"*120)
        
        csv_total = self.results['CSV']['total_time']
        
        for fmt in formats:
            total_time = self.results[fmt]['total_time']
            speedup = (csv_total / total_time) if total_time > 0 else 0
            symbol = "✓" if speedup > 1 else "✗"
            print(f"{fmt:<15} {total_time:>14.2f}s {symbol}{speedup:>13.2f}x")
        
        # Summary recommendations
        print("\n" + "="*120)
        print("SUMMARY & RECOMMENDATIONS")
        print("="*120)
        
        print("\n🏆 BEST FOR EACH METRIC:")
        
        # Smallest file
        smallest_fmt = min(self.results.items(), key=lambda x: x[1]['file_size_mb'])
        csv_size = self.results['CSV']['file_size_mb']
        savings = ((csv_size - smallest_fmt[1]['file_size_mb']) / csv_size) * 100
        print(f"  • Smallest file: {smallest_fmt[0]} ({smallest_fmt[1]['file_size_mb']:.2f}MB, {savings:.1f}% smaller than CSV)")
        
        # Fastest read
        fastest_read = min(self.results.items(), key=lambda x: x[1]['read_time'])
        speedup = self.results['CSV']['read_time'] / fastest_read[1]['read_time']
        print(f"  • Fastest read: {fastest_read[0]} ({fastest_read[1]['read_time']:.2f}s, {speedup:.1f}x faster)")
        
        # Fastest write
        fastest_write = min(self.results.items(), key=lambda x: x[1]['write_time'])
        speedup = self.results['CSV']['write_time'] / fastest_write[1]['write_time']
        print(f"  • Fastest write: {fastest_write[0]} ({fastest_write[1]['write_time']:.2f}s, {speedup:.1f}x faster)")
        
        # Lowest energy
        lowest_energy = min(self.results.items(), key=lambda x: x[1]['energy_wh'])
        savings = ((self.results['CSV']['energy_wh'] - lowest_energy[1]['energy_wh']) / self.results['CSV']['energy_wh']) * 100
        print(f"  • Lowest energy: {lowest_energy[0]} ({lowest_energy[1]['energy_wh']:.4f}Wh, {savings:.1f}% lower)")
        
        print("\n📋 USE CASES:")
        print("  • CSV: Universal compatibility, human-readable, but large files")
        if 'XLSX' in formats:
            print("  • XLSX: Excel integration, but slower and larger than binary formats")
        print("  • Parquet: Excellent compression, fast reads, industry standard (recommended)")
        print("  • ORC: Best compression, optimized for analytics, Hadoop ecosystem")
        print("  • Feather: Fastest I/O, minimal serialization, best for data pipelines")
        
        print("\n💡 RECOMMENDATIONS:")
        print("  • For storage: Use Parquet or ORC (90%+ smaller than CSV)")
        print("  • For speed: Use Feather (10-100x faster I/O)")
        print("  • For interoperability: Use Parquet (best for cross-language)")
        if 'XLSX' in formats:
            print("  • For Excel users: Use XLSX (slower but Excel-native)")
        
        print("="*120 + "\n")
    
    def run(self) -> None:
        """Run complete benchmark."""
        logger.info("Starting File Format Benchmark")
        logger.info(f"Rows: {self.ROWS:,}")
        logger.info(f"Output directory: {self.output_dir}")
        
        try:
            # Generate data
            df = self.generate_sample_data()
            
            # Benchmark each format
            self.results['CSV'] = self.benchmark_csv(df)
            
            # Try XLSX, skip if openpyxl not available
            try:
                self.results['XLSX'] = self.benchmark_xlsx(df)
            except ModuleNotFoundError:
                logger.warning("⚠ Skipping XLSX (openpyxl not installed)")
            
            self.results['Parquet'] = self.benchmark_parquet(df)
            self.results['ORC'] = self.benchmark_orc(df)
            self.results['Feather'] = self.benchmark_feather(df)
            
            # Print comparison
            self.print_comparison_table()
            
            logger.info("✓ Benchmark complete")
            
        except Exception as e:
            logger.error(f"Benchmark failed: {e}", exc_info=True)
            raise


def main():
    """Main entry point."""
    benchmark = FileFormatBenchmark()
    benchmark.run()


if __name__ == "__main__":
    main()
