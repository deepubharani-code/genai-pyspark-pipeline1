"""
Pandas vs PySpark Performance Comparison on 500K Rows

Compares performance of Pandas and PySpark on identical operations:
1. Load orders.parquet and products.parquet 
2. Join on product_id
3. Calculate revenue (quantity * price)
4. Group by customer_id, sum revenue
5. Get top 10 customers by revenue
6. Display comprehensive timing comparison

Usage:
    python pandas_vs_pyspark_comparison.py                 # Both frameworks
    python pandas_vs_pyspark_comparison.py --pandas-only   # Pandas only
    python pandas_vs_pyspark_comparison.py --pyspark-only  # PySpark only
    python pandas_vs_pyspark_comparison.py --verbose       # Debug output
"""

import logging
import sys
import time
from pathlib import Path
from typing import Dict, Tuple, Optional
import tracemalloc

import pandas as pd
import numpy as np
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum, desc, count
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, DateType

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class PandasBenchmark:
    """Benchmark Pandas operations."""
    
    def __init__(self, data_dir: str = "data/raw"):
        """Initialize Pandas benchmark."""
        self.data_dir = Path(data_dir)
        self.timings: Dict[str, float] = {}
    
    def load_data(self) -> Tuple[pd.DataFrame, pd.DataFrame, float]:
        """
        Load Parquet files with Pandas.
        
        Returns:
            Tuple of (orders_df, products_df, load_time_seconds)
        """
        logger.info("Loading Parquet files with Pandas...")
        tracemalloc.start()
        
        start = time.perf_counter()
        
        orders_path = self.data_dir / "orders.parquet"
        products_path = self.data_dir / "products.parquet"
        
        orders_df = pd.read_parquet(str(orders_path))
        products_df = pd.read_parquet(str(products_path))
        
        load_time = time.perf_counter() - start
        _, peak_memory = tracemalloc.get_traced_memory()
        tracemalloc.stop()
        
        memory_mb = peak_memory / 1024 / 1024
        
        logger.info(f"✓ Pandas loaded: {len(orders_df):,} orders, {len(products_df):,} products")
        logger.info(f"  Time: {load_time:.4f}s | Memory: {memory_mb:.2f}MB")
        
        return orders_df, products_df, load_time
    
    def run(self, orders_df: pd.DataFrame, products_df: pd.DataFrame) -> Tuple[pd.DataFrame, Dict[str, float]]:
        """
        Run complete Pandas benchmark.
        
        Args:
            orders_df (pd.DataFrame): Orders data
            products_df (pd.DataFrame): Products data
            
        Returns:
            Tuple of (result_df, timings_dict)
        """
        logger.info("\nProcessing with Pandas...")
        timings = {}
        
        # Join
        logger.info("  Joining orders with products...")
        start = time.perf_counter()
        merged = pd.merge(orders_df, products_df, on='product_id', how='inner')
        timings['join'] = time.perf_counter() - start
        logger.info(f"    ✓ Join: {timings['join']:.4f}s ({len(merged):,} rows)")
        
        # Calculate revenue
        logger.info("  Calculating revenue...")
        start = time.perf_counter()
        merged['revenue'] = merged['quantity'] * merged['price']
        timings['calculate_revenue'] = time.perf_counter() - start
        logger.info(f"    ✓ Revenue calculated: {timings['calculate_revenue']:.4f}s")
        
        # Group and aggregate
        logger.info("  Grouping by customer and summing...")
        start = time.perf_counter()
        customer_revenue = merged.groupby('customer_id').agg({
            'revenue': 'sum',
            'order_id': 'count'
        }).rename(columns={'order_id': 'order_count'})
        customer_revenue['avg_order_value'] = customer_revenue['revenue'] / customer_revenue['order_count']
        customer_revenue = customer_revenue.reset_index()
        timings['group_agg'] = time.perf_counter() - start
        logger.info(f"    ✓ Grouped: {timings['group_agg']:.4f}s")
        
        # Get top 10
        logger.info("  Getting top 10 customers...")
        start = time.perf_counter()
        top_10 = customer_revenue.nlargest(10, 'revenue')
        timings['top_10'] = time.perf_counter() - start
        logger.info(f"    ✓ Top 10: {timings['top_10']:.4f}s")
        
        # Calculate total
        timings['total'] = sum(v for k, v in timings.items() if k != 'total')
        
        return top_10, timings
    
    def display_results(self, results: pd.DataFrame) -> None:
        """Display results."""
        logger.info("\nPandas - Top 10 Customers by Revenue:")
        logger.info(results.to_string(index=False))


class PySparkBenchmark:
    """Benchmark PySpark operations."""
    
    def __init__(self, data_dir: str = "data/raw"):
        """Initialize PySpark benchmark."""
        self.data_dir = Path(data_dir)
        self.spark: Optional[SparkSession] = None
        self.timings: Dict[str, float] = {}
    
    def create_spark_session(self) -> SparkSession:
        """Create optimized Spark session."""
        logger.info("Creating Spark session...")
        start = time.perf_counter()
        
        self.spark = (SparkSession.builder
            .appName("PandasVsPySpark")
            .master("local[*]")
            .config("spark.driver.memory", "4g")
            .config("spark.sql.adaptive.enabled", "true")
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
            .getOrCreate())
        
        init_time = time.perf_counter() - start
        logger.info(f"✓ Spark session created: {init_time:.4f}s")
        
        return self.spark
    
    def load_data(self) -> Tuple:
        """
        Load Parquet files with PySpark.
        
        Returns:
            Tuple of (orders_df, products_df, load_time_seconds)
        """
        if not self.spark:
            self.create_spark_session()
        
        logger.info("Loading Parquet files with PySpark...")
        
        start = time.perf_counter()
        
        orders_path = self.data_dir / "orders.parquet"
        products_path = self.data_dir / "products.parquet"
        
        orders_df = self.spark.read.parquet(str(orders_path))
        products_df = self.spark.read.parquet(str(products_path))
        
        # Force evaluation
        orders_count = orders_df.count()
        products_count = products_df.count()
        
        load_time = time.perf_counter() - start
        
        logger.info(f"✓ PySpark loaded: {orders_count:,} orders, {products_count:,} products")
        logger.info(f"  Time: {load_time:.4f}s")
        
        return orders_df, products_df, load_time
    
    def run(self, orders_df, products_df) -> Tuple:
        """
        Run complete PySpark benchmark.
        
        Args:
            orders_df: PySpark orders dataframe
            products_df: PySpark products dataframe
            
        Returns:
            Tuple of (result_df, timings_dict)
        """
        logger.info("\nProcessing with PySpark...")
        timings = {}
        
        # Join
        logger.info("  Joining orders with products...")
        start = time.perf_counter()
        joined = orders_df.join(products_df, 'product_id', 'inner')
        joined_count = joined.count()
        timings['join'] = time.perf_counter() - start
        logger.info(f"    ✓ Join: {timings['join']:.4f}s ({joined_count:,} rows)")
        
        # Calculate revenue
        logger.info("  Calculating revenue...")
        start = time.perf_counter()
        with_revenue = joined.withColumn('revenue', col('quantity') * col('price'))
        with_revenue.count()  # Force evaluation
        timings['calculate_revenue'] = time.perf_counter() - start
        logger.info(f"    ✓ Revenue calculated: {timings['calculate_revenue']:.4f}s")
        
        # Group and aggregate
        logger.info("  Grouping by customer and summing...")
        start = time.perf_counter()
        customer_revenue = with_revenue.groupBy('customer_id').agg(
            spark_sum('revenue').alias('total_revenue'),
            count('order_id').alias('order_count')
        )
        customer_revenue = customer_revenue.withColumn(
            'avg_order_value',
            col('total_revenue') / col('order_count')
        )
        customer_revenue.count()  # Force evaluation
        timings['group_agg'] = time.perf_counter() - start
        logger.info(f"    ✓ Grouped: {timings['group_agg']:.4f}s")
        
        # Get top 10
        logger.info("  Getting top 10 customers...")
        start = time.perf_counter()
        top_10 = customer_revenue.orderBy(desc('total_revenue')).limit(10)
        top_10.cache()
        top_10_count = top_10.count()
        timings['top_10'] = time.perf_counter() - start
        logger.info(f"    ✓ Top 10: {timings['top_10']:.4f}s")
        
        # Calculate total
        timings['total'] = sum(v for k, v in timings.items() if k != 'total')
        
        return top_10, timings
    
    def display_results(self, results) -> None:
        """Display results."""
        logger.info("\nPySpark - Top 10 Customers by Revenue:")
        results.show(truncate=False, n=20)
    
    def cleanup(self) -> None:
        """Stop Spark session."""
        if self.spark:
            self.spark.stop()
            logger.info("✓ Spark session stopped")


class ComparisonReport:
    """Generate and display comparison report."""
    
    def __init__(self, pandas_timings: Dict[str, float], pyspark_timings: Dict[str, float]):
        """Initialize comparison report."""
        self.pandas_timings = pandas_timings
        self.pyspark_timings = pyspark_timings
    
    def print_comparison(self) -> None:
        """Print detailed comparison."""
        logger.info("\n" + "=" * 130)
        logger.info("PERFORMANCE COMPARISON: PANDAS VS PYSPARK")
        logger.info("=" * 130)
        
        operations = ['join', 'calculate_revenue', 'group_agg', 'top_10', 'total']
        
        logger.info("\n" + "-" * 130)
        logger.info(
            f"{'Operation':<25} "
            f"{'Pandas (s)':<18} "
            f"{'PySpark (s)':<18} "
            f"{'Speedup':<18} "
            f"{'Winner':<20}"
        )
        logger.info("-" * 130)
        
        for op in operations:
            pandas_time = self.pandas_timings.get(op, 0)
            pyspark_time = self.pyspark_timings.get(op, 0)
            
            if pandas_time > 0 and pyspark_time > 0:
                speedup = pandas_time / pyspark_time
                winner = f"Pandas ({speedup:.1f}x)" if speedup > 1 else f"PySpark ({1/speedup:.1f}x)"
            else:
                speedup = 0
                winner = "N/A"
            
            op_display = op.replace('_', ' ').title()
            
            logger.info(
                f"{op_display:<25} "
                f"{pandas_time:>16.4f}s  "
                f"{pyspark_time:>16.4f}s  "
                f"{speedup:>16.2f}x  "
                f"{winner:>18}"
            )
        
        logger.info("-" * 130)
        
        # Summary
        pandas_total = self.pandas_timings.get('total', 0)
        pyspark_total = self.pyspark_timings.get('total', 0)
        
        if pyspark_total > 0:
            total_speedup = pandas_total / pyspark_total
        else:
            total_speedup = 0
        
        logger.info(
            f"{'TOTAL':<25} "
            f"{pandas_total:>16.4f}s  "
            f"{pyspark_total:>16.4f}s  "
            f"{total_speedup:>16.2f}x"
        )
        
        logger.info("\n" + "=" * 130)
        logger.info("INSIGHTS & RECOMMENDATIONS")
        logger.info("=" * 130)
        
        if pandas_total < pyspark_total:
            improvement = ((pyspark_total - pandas_total) / pandas_total) * 100
            logger.info(f"\n✓ PANDAS IS {total_speedup:.2f}x FASTER than PySpark")
            logger.info(f"  Pandas takes {pandas_total:.4f}s vs PySpark {pyspark_total:.4f}s")
            logger.info(f"  Performance difference: {improvement:.1f}%")
            logger.info("\nWhy Pandas wins on small/medium datasets:")
            logger.info("  • Single-machine, in-memory processing is efficient")
            logger.info("  • No JVM startup overhead (~30-40 seconds)")
            logger.info("  • No serialization/deserialization delays")
            logger.info("  • NumPy/Pandas C extensions are highly optimized")
            logger.info("  • Lower memory fragmentation")
        else:
            improvement = ((pandas_total - pyspark_total) / pandas_total) * 100
            logger.info(f"\n✓ PYSPARK IS {1/total_speedup:.2f}x FASTER than Pandas")
            logger.info(f"  PySpark takes {pyspark_total:.4f}s vs Pandas {pandas_total:.4f}s")
            logger.info(f"  Performance improvement: {improvement:.1f}%")
            logger.info("\nWhy PySpark excels:")
            logger.info("  • Distributed processing across multiple nodes")
            logger.info("  • Parallel execution strategies")
            logger.info("  • Better optimization for massive datasets")
        
        logger.info("\nRECOMMENDATIONS:")
        logger.info("  For < 100M rows on single machine:")
        logger.info("    → Use PANDAS (simpler, faster for exploration)")
        logger.info("  For > 100M rows or distributed processing:")
        logger.info("    → Use PYSPARK (scales horizontally)")
        logger.info("  For production ETL pipelines:")
        logger.info("    → Use PYSPARK (integrates with Hadoop ecosystem)")
        logger.info("  For data science/ML:")
        logger.info("    → Use PANDAS (rich ecosystem, faster iteration)")
        
        logger.info("\n" + "=" * 130 + "\n")


def main():
    """Main entry point."""
    import argparse
    
    parser = argparse.ArgumentParser(
        description="Compare Pandas vs PySpark performance on 500K rows",
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    
    parser.add_argument(
        '--pandas-only',
        action='store_true',
        help='Run only Pandas benchmark'
    )
    
    parser.add_argument(
        '--pyspark-only',
        action='store_true',
        help='Run only PySpark benchmark'
    )
    
    parser.add_argument(
        '--verbose',
        action='store_true',
        help='Enable verbose logging'
    )
    
    parser.add_argument(
        '--data-dir',
        default='data/raw',
        help='Directory containing Parquet files (default: data/raw)'
    )
    
    args = parser.parse_args()
    
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    pandas_timings = {}
    pyspark_timings = {}
    
    try:
        # Run Pandas benchmark
        if not args.pyspark_only:
            logger.info("=" * 100)
            logger.info("PANDAS BENCHMARK")
            logger.info("=" * 100)
            
            pandas_bench = PandasBenchmark(data_dir=args.data_dir)
            orders_pd, products_pd, load_time_pd = pandas_bench.load_data()
            results_pd, pandas_timings = pandas_bench.run(orders_pd, products_pd)
            pandas_timings['load'] = load_time_pd
            pandas_bench.display_results(results_pd)
        
        # Run PySpark benchmark
        if not args.pandas_only:
            logger.info("\n" + "=" * 100)
            logger.info("PYSPARK BENCHMARK")
            logger.info("=" * 100)
            
            pyspark_bench = PySparkBenchmark(data_dir=args.data_dir)
            pyspark_bench.create_spark_session()
            orders_spark, products_spark, load_time_spark = pyspark_bench.load_data()
            results_spark, pyspark_timings = pyspark_bench.run(orders_spark, products_spark)
            pyspark_timings['load'] = load_time_spark
            pyspark_bench.display_results(results_spark)
            pyspark_bench.cleanup()
        
        # Print comparison if both ran
        if pandas_timings and pyspark_timings:
            report = ComparisonReport(pandas_timings, pyspark_timings)
            report.print_comparison()
        
    except Exception as e:
        logger.error(f"Benchmark failed: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
