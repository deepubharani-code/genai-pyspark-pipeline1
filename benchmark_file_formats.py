import os
import sys
import time
import tracemalloc
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
from pathlib import Path
import warnings

warnings.filterwarnings('ignore')

# Try to import optional dependencies
try:
    import pyarrow as pa
    import pyarrow.parquet as pq
    HAS_PYARROW = True
except ImportError:
    HAS_PYARROW = False
    print("Warning: pyarrow not installed. Parquet format will be skipped.")

try:
    import pyarrow.orc as orc
    HAS_ORC = True
except ImportError:
    HAS_ORC = False
    print("Warning: ORC support not available. ORC format will be skipped.")

try:
    import fastparquet
    HAS_FASTPARQUET = True
except ImportError:
    HAS_FASTPARQUET = False
    print("Warning: fastparquet not installed.")


class FileFormatBenchmark:
    """Benchmark different file formats with comprehensive hardware metrics."""
    
    def __init__(self, num_rows=500000):
        self.num_rows = num_rows
        self.results = {}
        self.output_dir = Path("benchmark_outputs")
        self.output_dir.mkdir(exist_ok=True)
        
    def create_sample_dataframe(self):
        """Create a sample DataFrame with specified number of rows."""
        np.random.seed(42)
        
        print(f"\n📊 Creating DataFrame with {self.num_rows:,} rows...")
        
        data = {
            'id': np.arange(1, self.num_rows + 1),
            'name': [f'User_{i}' for i in range(1, self.num_rows + 1)],
            'email': [f'user_{i}@example.com' for i in range(1, self.num_rows + 1)],
            'amount': np.random.uniform(100, 10000, self.num_rows),
            'date': pd.date_range('2023-01-01', periods=self.num_rows, freq='5min'),
            'category': np.random.choice(['A', 'B', 'C', 'D', 'E'], self.num_rows)
        }
        
        df = pd.DataFrame(data)
        print(f"✓ DataFrame created: {df.shape[0]:,} rows, {df.shape[1]} columns")
        print(f"  Memory usage: {df.memory_usage(deep=True).sum() / 1024**2:.2f} MB")
        
        return df
    
    def get_file_size_mb(self, filepath):
        """Get file size in MB."""
        if os.path.exists(filepath):
            return os.path.getsize(filepath) / (1024 ** 2)
        return 0
    
    def benchmark_csv(self, df):
        """Benchmark CSV format."""
        filepath = self.output_dir / "data.csv"
        format_name = "CSV"
        
        print(f"\n📝 Benchmarking {format_name}...")
        
        # Write benchmark
        tracemalloc.start()
        start_time = time.perf_counter()
        cpu_start = time.process_time()
        
        df.to_csv(filepath, index=False)
        
        cpu_time_write = time.process_time() - cpu_start
        write_time = time.perf_counter() - start_time
        current, peak = tracemalloc.get_traced_memory()
        peak_memory_write = peak / (1024 ** 2)
        tracemalloc.stop()
        
        file_size = self.get_file_size_mb(filepath)
        
        # Read benchmark
        tracemalloc.start()
        start_time = time.perf_counter()
        cpu_start = time.process_time()
        
        df_read = pd.read_csv(filepath)
        
        cpu_time_read = time.process_time() - cpu_start
        read_time = time.perf_counter() - start_time
        current, peak = tracemalloc.get_traced_memory()
        peak_memory_read = peak / (1024 ** 2)
        tracemalloc.stop()
        
        # Calculate metrics
        total_cpu_time = cpu_time_write + cpu_time_read
        energy_consumption = (total_cpu_time * 65) / 3600  # Wh with 65W TDP
        
        results = {
            'format': format_name,
            'file_size_mb': file_size,
            'write_time_s': write_time,
            'read_time_s': read_time,
            'total_time_s': write_time + read_time,
            'peak_memory_mb': max(peak_memory_write, peak_memory_read),
            'cpu_time_s': total_cpu_time,
            'energy_wh': energy_consumption,
            'rows': len(df_read)
        }
        
        print(f"✓ {format_name} benchmark complete")
        return results
    
    def benchmark_xlsx(self, df):
        """Benchmark XLSX format."""
        try:
            import openpyxl
        except ImportError:
            print("\n⚠️  openpyxl not installed. Skipping XLSX format.")
            return None
        
        filepath = self.output_dir / "data.xlsx"
        format_name = "XLSX"
        
        print(f"\n📝 Benchmarking {format_name}...")
        
        # Write benchmark
        tracemalloc.start()
        start_time = time.perf_counter()
        cpu_start = time.process_time()
        
        df.to_excel(filepath, index=False, sheet_name='Sheet1')
        
        cpu_time_write = time.process_time() - cpu_start
        write_time = time.perf_counter() - start_time
        current, peak = tracemalloc.get_traced_memory()
        peak_memory_write = peak / (1024 ** 2)
        tracemalloc.stop()
        
        file_size = self.get_file_size_mb(filepath)
        
        # Read benchmark
        tracemalloc.start()
        start_time = time.perf_counter()
        cpu_start = time.process_time()
        
        df_read = pd.read_excel(filepath, sheet_name='Sheet1')
        
        cpu_time_read = time.process_time() - cpu_start
        read_time = time.perf_counter() - start_time
        current, peak = tracemalloc.get_traced_memory()
        peak_memory_read = peak / (1024 ** 2)
        tracemalloc.stop()
        
        # Calculate metrics
        total_cpu_time = cpu_time_write + cpu_time_read
        energy_consumption = (total_cpu_time * 65) / 3600  # Wh with 65W TDP
        
        results = {
            'format': format_name,
            'file_size_mb': file_size,
            'write_time_s': write_time,
            'read_time_s': read_time,
            'total_time_s': write_time + read_time,
            'peak_memory_mb': max(peak_memory_write, peak_memory_read),
            'cpu_time_s': total_cpu_time,
            'energy_wh': energy_consumption,
            'rows': len(df_read)
        }
        
        print(f"✓ {format_name} benchmark complete")
        return results
    
    def benchmark_parquet(self, df):
        """Benchmark Parquet format."""
        if not HAS_PYARROW:
            print("\n⚠️  pyarrow not installed. Skipping Parquet format.")
            return None
        
        filepath = self.output_dir / "data.parquet"
        format_name = "Parquet"
        
        print(f"\n📝 Benchmarking {format_name}...")
        
        # Write benchmark
        tracemalloc.start()
        start_time = time.perf_counter()
        cpu_start = time.process_time()
        
        df.to_parquet(filepath, index=False, engine='pyarrow', compression='snappy')
        
        cpu_time_write = time.process_time() - cpu_start
        write_time = time.perf_counter() - start_time
        current, peak = tracemalloc.get_traced_memory()
        peak_memory_write = peak / (1024 ** 2)
        tracemalloc.stop()
        
        file_size = self.get_file_size_mb(filepath)
        
        # Read benchmark
        tracemalloc.start()
        start_time = time.perf_counter()
        cpu_start = time.process_time()
        
        df_read = pd.read_parquet(filepath, engine='pyarrow')
        
        cpu_time_read = time.process_time() - cpu_start
        read_time = time.perf_counter() - start_time
        current, peak = tracemalloc.get_traced_memory()
        peak_memory_read = peak / (1024 ** 2)
        tracemalloc.stop()
        
        # Calculate metrics
        total_cpu_time = cpu_time_write + cpu_time_read
        energy_consumption = (total_cpu_time * 65) / 3600  # Wh with 65W TDP
        
        results = {
            'format': format_name,
            'file_size_mb': file_size,
            'write_time_s': write_time,
            'read_time_s': read_time,
            'total_time_s': write_time + read_time,
            'peak_memory_mb': max(peak_memory_write, peak_memory_read),
            'cpu_time_s': total_cpu_time,
            'energy_wh': energy_consumption,
            'rows': len(df_read)
        }
        
        print(f"✓ {format_name} benchmark complete")
        return results
    
    def benchmark_orc(self, df):
        """Benchmark ORC format."""
        if not HAS_ORC:
            print("\n⚠️  ORC support not available. Skipping ORC format.")
            return None
        
        filepath = self.output_dir / "data.orc"
        format_name = "ORC"
        
        print(f"\n📝 Benchmarking {format_name}...")
        
        try:
            # Write benchmark
            tracemalloc.start()
            start_time = time.perf_counter()
            cpu_start = time.process_time()
            
            table = pa.Table.from_pandas(df)
            orc.write_table(table, filepath)
            
            cpu_time_write = time.process_time() - cpu_start
            write_time = time.perf_counter() - start_time
            current, peak = tracemalloc.get_traced_memory()
            peak_memory_write = peak / (1024 ** 2)
            tracemalloc.stop()
            
            file_size = self.get_file_size_mb(filepath)
            
            # Read benchmark
            tracemalloc.start()
            start_time = time.perf_counter()
            cpu_start = time.process_time()
            
            table_read = orc.read_table(filepath)
            df_read = table_read.to_pandas()
            
            cpu_time_read = time.process_time() - cpu_start
            read_time = time.perf_counter() - start_time
            current, peak = tracemalloc.get_traced_memory()
            peak_memory_read = peak / (1024 ** 2)
            tracemalloc.stop()
            
            # Calculate metrics
            total_cpu_time = cpu_time_write + cpu_time_read
            energy_consumption = (total_cpu_time * 65) / 3600  # Wh with 65W TDP
            
            results = {
                'format': format_name,
                'file_size_mb': file_size,
                'write_time_s': write_time,
                'read_time_s': read_time,
                'total_time_s': write_time + read_time,
                'peak_memory_mb': max(peak_memory_write, peak_memory_read),
                'cpu_time_s': total_cpu_time,
                'energy_wh': energy_consumption,
                'rows': len(df_read)
            }
            
            print(f"✓ {format_name} benchmark complete")
            return results
        except Exception as e:
            print(f"⚠️  Error benchmarking ORC: {e}")
            return None
    
    def benchmark_feather(self, df):
        """Benchmark Feather format."""
        if not HAS_PYARROW:
            print("\n⚠️  pyarrow not installed. Skipping Feather format.")
            return None
        
        filepath = self.output_dir / "data.feather"
        format_name = "Feather"
        
        print(f"\n📝 Benchmarking {format_name}...")
        
        # Write benchmark
        tracemalloc.start()
        start_time = time.perf_counter()
        cpu_start = time.process_time()
        
        df.to_feather(filepath)
        
        cpu_time_write = time.process_time() - cpu_start
        write_time = time.perf_counter() - start_time
        current, peak = tracemalloc.get_traced_memory()
        peak_memory_write = peak / (1024 ** 2)
        tracemalloc.stop()
        
        file_size = self.get_file_size_mb(filepath)
        
        # Read benchmark
        tracemalloc.start()
        start_time = time.perf_counter()
        cpu_start = time.process_time()
        
        df_read = pd.read_feather(filepath)
        
        cpu_time_read = time.process_time() - cpu_start
        read_time = time.perf_counter() - start_time
        current, peak = tracemalloc.get_traced_memory()
        peak_memory_read = peak / (1024 ** 2)
        tracemalloc.stop()
        
        # Calculate metrics
        total_cpu_time = cpu_time_write + cpu_time_read
        energy_consumption = (total_cpu_time * 65) / 3600  # Wh with 65W TDP
        
        results = {
            'format': format_name,
            'file_size_mb': file_size,
            'write_time_s': write_time,
            'read_time_s': read_time,
            'total_time_s': write_time + read_time,
            'peak_memory_mb': max(peak_memory_write, peak_memory_read),
            'cpu_time_s': total_cpu_time,
            'energy_wh': energy_consumption,
            'rows': len(df_read)
        }
        
        print(f"✓ {format_name} benchmark complete")
        return results
    
    def run_benchmarks(self, df):
        """Run all benchmarks."""
        print("\n" + "="*80)
        print("FILE FORMAT BENCHMARKING SUITE")
        print("="*80)
        
        all_results = []
        
        # Run benchmarks
        csv_results = self.benchmark_csv(df)
        if csv_results:
            all_results.append(csv_results)
            self.results['CSV'] = csv_results
        
        xlsx_results = self.benchmark_xlsx(df)
        if xlsx_results:
            all_results.append(xlsx_results)
            self.results['XLSX'] = xlsx_results
        
        parquet_results = self.benchmark_parquet(df)
        if parquet_results:
            all_results.append(parquet_results)
            self.results['Parquet'] = parquet_results
        
        orc_results = self.benchmark_orc(df)
        if orc_results:
            all_results.append(orc_results)
            self.results['ORC'] = orc_results
        
        feather_results = self.benchmark_feather(df)
        if feather_results:
            all_results.append(feather_results)
            self.results['Feather'] = feather_results
        
        return all_results
    
    def print_comparison_table(self, results_list):
        """Print a formatted comparison table."""
        if not results_list:
            print("No benchmarks completed.")
            return
        
        # Get CSV baseline for comparison
        csv_baseline = next((r for r in results_list if r['format'] == 'CSV'), None)
        if not csv_baseline:
            print("CSV baseline not found for comparison.")
            return
        
        print("\n" + "="*160)
        print("COMPREHENSIVE FILE FORMAT COMPARISON")
        print("="*160)
        
        # Sort by file size
        results_list = sorted(results_list, key=lambda x: x['file_size_mb'])
        
        # Print header
        print(f"{'Format':<12} | {'File Size':<12} | {'Size Savings':<14} | {'Write Time':<12} | {'Read Time':<12} | "
              f"{'Total Time':<12} | {'Peak Memory':<14} | {'CPU Time':<12} | {'Energy (Wh)':<12}")
        print("-"*160)
        
        # Print data rows
        for result in results_list:
            format_name = result['format']
            file_size = result['file_size_mb']
            size_saving = ((csv_baseline['file_size_mb'] - file_size) / csv_baseline['file_size_mb'] * 100)
            write_time = result['write_time_s']
            read_time = result['read_time_s']
            total_time = result['total_time_s']
            peak_memory = result['peak_memory_mb']
            cpu_time = result['cpu_time_s']
            energy = result['energy_wh']
            
            size_saving_str = f"{size_saving:+.1f}%"
            
            print(f"{format_name:<12} | {file_size:>10.2f} MB | {size_saving_str:>12} | {write_time:>10.4f}s | "
                  f"{read_time:>10.4f}s | {total_time:>10.4f}s | {peak_memory:>12.2f} MB | {cpu_time:>10.4f}s | "
                  f"{energy:>10.4f} Wh")
        
        print("="*160)
        print("\nNotes:")
        print("  - Size Savings: Percentage reduction compared to CSV")
        print("  - Peak Memory: Maximum memory usage during read/write")
        print("  - Energy: Estimated consumption at 65W TDP")
        print(f"  - Data: {results_list[0]['rows']:,} rows, 6 columns")
    
    def print_detailed_metrics(self, results_list):
        """Print detailed metrics for each format."""
        if not results_list:
            print("No benchmarks completed.")
            return
        
        print("\n" + "="*80)
        print("DETAILED METRICS PER FORMAT")
        print("="*80)
        
        for result in sorted(results_list, key=lambda x: x['file_size_mb']):
            format_name = result['format']
            print(f"\n📊 {format_name}")
            print("-"*80)
            print(f"  File Size:        {result['file_size_mb']:>12.2f} MB")
            print(f"  Write Time:       {result['write_time_s']:>12.4f} seconds")
            print(f"  Read Time:        {result['read_time_s']:>12.4f} seconds")
            print(f"  Total Time:       {result['total_time_s']:>12.4f} seconds")
            print(f"  Peak Memory:      {result['peak_memory_mb']:>12.2f} MB")
            print(f"  CPU Time:         {result['cpu_time_s']:>12.4f} seconds")
            print(f"  Energy (Wh):      {result['energy_wh']:>12.4f} Wh")
            print(f"  Rows Processed:   {result['rows']:>12,}")
    
    def print_savings_vs_csv(self, results_list):
        """Print savings comparison vs CSV baseline."""
        if not results_list:
            print("No benchmarks completed.")
            return
        
        csv_result = next((r for r in results_list if r['format'] == 'CSV'), None)
        if not csv_result:
            return
        
        print("\n" + "="*80)
        print("SAVINGS VS CSV BASELINE")
        print("="*80)
        
        for result in sorted(results_list, key=lambda x: x['file_size_mb']):
            if result['format'] == 'CSV':
                print(f"\n{result['format']} (Baseline)")
                print("-"*80)
                continue
            
            format_name = result['format']
            
            # Calculate savings
            size_saving = ((csv_result['file_size_mb'] - result['file_size_mb']) / csv_result['file_size_mb'] * 100)
            time_saving = ((csv_result['total_time_s'] - result['total_time_s']) / csv_result['total_time_s'] * 100)
            memory_saving = ((csv_result['peak_memory_mb'] - result['peak_memory_mb']) / csv_result['peak_memory_mb'] * 100)
            energy_saving = ((csv_result['energy_wh'] - result['energy_wh']) / csv_result['energy_wh'] * 100)
            
            print(f"\n{format_name}")
            print("-"*80)
            print(f"  File Size Reduction:    {size_saving:>+7.1f}%  ({csv_result['file_size_mb']:.2f} → {result['file_size_mb']:.2f} MB)")
            print(f"  Total Time Reduction:   {time_saving:>+7.1f}%  ({csv_result['total_time_s']:.4f} → {result['total_time_s']:.4f}s)")
            print(f"  Memory Reduction:       {memory_saving:>+7.1f}%  ({csv_result['peak_memory_mb']:.2f} → {result['peak_memory_mb']:.2f} MB)")
            print(f"  Energy Reduction:       {energy_saving:>+7.1f}%  ({csv_result['energy_wh']:.4f} → {result['energy_wh']:.4f} Wh)")


def main():
    """Main execution function."""
    try:
        benchmark = FileFormatBenchmark(num_rows=500000)
        
        # Create sample data
        df = benchmark.create_sample_dataframe()
        
        # Run all benchmarks
        results = benchmark.run_benchmarks(df)
        
        # Print results
        if results:
            benchmark.print_comparison_table(results)
            benchmark.print_detailed_metrics(results)
            benchmark.print_savings_vs_csv(results)
            
            print("\n✅ Benchmarking complete!")
            print(f"📁 Output files saved to: {benchmark.output_dir.absolute()}")
        else:
            print("\n❌ No benchmarks completed successfully.")
            
    except Exception as e:
        print(f"\n❌ Error during benchmarking: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()
