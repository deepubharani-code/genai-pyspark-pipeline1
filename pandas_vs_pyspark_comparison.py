"""
Performance Comparison: Pandas vs PySpark on 1M Rows

This script compares the performance of Pandas and PySpark when:
1. Loading Parquet files
2. Joining dataframes on product_id
3. Calculating revenue (quantity * price)
4. Grouping by customer_id and summing revenue
5. Getting top 10 customers by revenue

Operations timed with nanosecond precision and presented in comparison table.
"""

import time
import logging
from pathlib import Path
from typing import Tuple, Dict, Any
import tracemalloc
import sys

import pandas as pd
import numpy as np
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum, desc, lit
import pyspark.sql.functions as F

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class PerformanceComparison:
    """Compare Pandas vs PySpark performance on 1M row datasets."""
    
    def __init__(self, data_dir: str = 'data/raw'):
        """
        Initialize performance comparison.
        
        Args:
            data_dir: Directory containing Parquet files
        """
        self.data_dir = Path(data_dir)
        self.results = {}
        self.spark = None
        
    def create_spark_session(self) -> SparkSession:
        """
        Create optimized Spark session for comparison.
        
        Returns:
            SparkSession configured with performance optimizations
        """
        self.spark = SparkSession.builder \
            .appName("PandasVsPySpark") \
            .config("spark.driver.memory", "4g") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
            .config("spark.sql.autoBroadcastJoinThreshold", "128mb") \
            .getOrCreate()
        
        logger.info("Spark session created")
        return self.spark
    
    # ==================== PANDAS OPERATIONS ====================
    
    def pandas_load_data(self) -> Tuple[pd.DataFrame, pd.DataFrame, float]:
        """
        Load Parquet files using Pandas.
        
        Returns:
            Tuple of (orders_df, products_df, load_time_ms)
        """
        logger.info("Loading data with Pandas...")
        tracemalloc.start()
        start_time = time.perf_counter()
        
        try:
            orders_path = self.data_dir / 'orders.parquet'
            products_path = self.data_dir / 'products.parquet'
            
            if not orders_path.exists() or not products_path.exists():
                logger.warning(f"Parquet files not found in {self.data_dir}, generating sample data")
                orders_df, products_df = self._generate_pandas_sample_data()
            else:
                orders_df = pd.read_parquet(orders_path)
                products_df = pd.read_parquet(products_path)
            
            end_time = time.perf_counter()
            current, peak = tracemalloc.get_traced_memory()
            tracemalloc.stop()
            
            load_time_ms = (end_time - start_time) * 1000
            memory_mb = peak / 1024 / 1024
            
            logger.info(f"Pandas loaded {len(orders_df):,} orders, {len(products_df):,} products")
            logger.info(f"Time: {load_time_ms:.2f}ms, Memory: {memory_mb:.2f}MB")
            
            return orders_df, products_df, load_time_ms
            
        except Exception as e:
            logger.error(f"Pandas load error: {e}")
            raise
    
    def pandas_process_data(self, orders_df: pd.DataFrame, products_df: pd.DataFrame) -> Tuple[pd.DataFrame, Dict[str, float]]:
        """
        Process data using Pandas (join, calculate revenue, group, top 10).
        
        Args:
            orders_df: Orders dataframe
            products_df: Products dataframe
        
        Returns:
            Tuple of (result_df, timing_dict)
        """
        timings = {}
        
        logger.info("Processing data with Pandas...")
        tracemalloc.start()
        
        # Join operation
        start = time.perf_counter()
        merged_df = orders_df.merge(products_df, on='product_id', how='inner')
        timings['join'] = (time.perf_counter() - start) * 1000
        logger.info(f"Join time: {timings['join']:.2f}ms, rows: {len(merged_df):,}")
        
        # Calculate revenue
        start = time.perf_counter()
        if 'quantity' in merged_df.columns and 'price' in merged_df.columns:
            merged_df['revenue'] = merged_df['quantity'] * merged_df['price']
        timings['revenue'] = (time.perf_counter() - start) * 1000
        logger.info(f"Revenue calculation time: {timings['revenue']:.2f}ms")
        
        # Group and aggregate
        start = time.perf_counter()
        result_df = merged_df.groupby('customer_id')['revenue'].sum().reset_index()
        result_df.columns = ['customer_id', 'total_revenue']
        timings['groupby'] = (time.perf_counter() - start) * 1000
        logger.info(f"Groupby time: {timings['groupby']:.2f}ms")
        
        # Get top 10
        start = time.perf_counter()
        result_df = result_df.nlargest(10, 'total_revenue').reset_index(drop=True)
        timings['top10'] = (time.perf_counter() - start) * 1000
        logger.info(f"Top 10 selection time: {timings['top10']:.2f}ms")
        
        # Total processing time
        timings['total'] = timings['join'] + timings['revenue'] + timings['groupby'] + timings['top10']
        
        current, peak = tracemalloc.get_traced_memory()
        tracemalloc.stop()
        timings['memory_mb'] = peak / 1024 / 1024
        
        return result_df, timings
    
    # ==================== PYSPARK OPERATIONS ====================
    
    def pyspark_load_data(self) -> Tuple[Any, Any, float]:
        """
        Load Parquet files using PySpark.
        
        Returns:
            Tuple of (orders_df, products_df, load_time_ms)
        """
        logger.info("Loading data with PySpark...")
        tracemalloc.start()
        start_time = time.perf_counter()
        
        try:
            orders_path = str(self.data_dir / 'orders.parquet')
            products_path = str(self.data_dir / 'products.parquet')
            
            if not Path(orders_path).exists() or not Path(products_path).exists():
                logger.warning(f"Parquet files not found in {self.data_dir}, generating sample data")
                orders_df, products_df = self._generate_pyspark_sample_data()
            else:
                try:
                    orders_df = self.spark.read.parquet(orders_path)
                    products_df = self.spark.read.parquet(products_path)
                except Exception as e:
                    logger.warning(f"Could not read Parquet files (likely timestamp format issue): {e}")
                    logger.warning("Falling back to generated sample data")
                    orders_df, products_df = self._generate_pyspark_sample_data()
            
            # Trigger lazy evaluation for accurate timing
            orders_count = orders_df.count()
            products_count = products_df.count()
            
            end_time = time.perf_counter()
            current, peak = tracemalloc.get_traced_memory()
            tracemalloc.stop()
            
            load_time_ms = (end_time - start_time) * 1000
            memory_mb = peak / 1024 / 1024
            
            logger.info(f"PySpark loaded {orders_count:,} orders, {products_count:,} products")
            logger.info(f"Time: {load_time_ms:.2f}ms, Memory: {memory_mb:.2f}MB")
            
            return orders_df, products_df, load_time_ms
            
        except Exception as e:
            logger.error(f"PySpark load error: {e}")
            raise
    
    def pyspark_process_data(self, orders_df: Any, products_df: Any) -> Tuple[Any, Dict[str, float]]:
        """
        Process data using PySpark (join, calculate revenue, group, top 10).
        
        Args:
            orders_df: PySpark orders dataframe
            products_df: PySpark products dataframe
        
        Returns:
            Tuple of (result_df, timing_dict)
        """
        timings = {}
        
        logger.info("Processing data with PySpark...")
        tracemalloc.start()
        
        # Join operation
        start = time.perf_counter()
        merged_df = orders_df.join(products_df, 'product_id', 'inner')
        merged_df.count()  # Trigger evaluation
        timings['join'] = (time.perf_counter() - start) * 1000
        logger.info(f"Join time: {timings['join']:.2f}ms")
        
        # Calculate revenue
        start = time.perf_counter()
        if 'quantity' in [f.name for f in merged_df.schema.fields] and \
           'price' in [f.name for f in merged_df.schema.fields]:
            merged_df = merged_df.withColumn('revenue', col('quantity') * col('price'))
        merged_df.count()  # Trigger evaluation
        timings['revenue'] = (time.perf_counter() - start) * 1000
        logger.info(f"Revenue calculation time: {timings['revenue']:.2f}ms")
        
        # Group and aggregate
        start = time.perf_counter()
        result_df = merged_df.groupBy('customer_id').agg(spark_sum('revenue').alias('total_revenue'))
        result_df.count()  # Trigger evaluation
        timings['groupby'] = (time.perf_counter() - start) * 1000
        logger.info(f"Groupby time: {timings['groupby']:.2f}ms")
        
        # Get top 10
        start = time.perf_counter()
        result_df = result_df.orderBy(desc('total_revenue')).limit(10)
        result_df.count()  # Trigger evaluation
        timings['top10'] = (time.perf_counter() - start) * 1000
        logger.info(f"Top 10 selection time: {timings['top10']:.2f}ms")
        
        # Total processing time
        timings['total'] = timings['join'] + timings['revenue'] + timings['groupby'] + timings['top10']
        
        current, peak = tracemalloc.get_traced_memory()
        tracemalloc.stop()
        timings['memory_mb'] = peak / 1024 / 1024
        
        return result_df, timings
    
    # ==================== SAMPLE DATA GENERATION ====================
    
    def _generate_pandas_sample_data(self, rows: int = 1_000_000) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """
        Generate 1M row sample data for Pandas.
        
        Args:
            rows: Number of rows to generate
        
        Returns:
            Tuple of (orders_df, products_df)
        """
        logger.info(f"Generating {rows:,} sample rows for Pandas...")
        
        # Generate orders
        np.random.seed(42)
        orders_data = {
            'order_id': range(1, rows + 1),
            'customer_id': np.random.randint(1, 10001, rows),
            'product_id': np.random.randint(1, 501, rows),
            'quantity': np.random.randint(1, 20, rows),
            'order_date': pd.date_range('2023-01-01', periods=rows, freq='min')
        }
        orders_df = pd.DataFrame(orders_data)
        
        # Generate products
        products_data = {
            'product_id': range(1, 501),
            'product_name': [f'Product_{i}' for i in range(1, 501)],
            'price': np.random.uniform(10, 500, 500)
        }
        products_df = pd.DataFrame(products_data)
        
        return orders_df, products_df
    
    def _generate_pyspark_sample_data(self, rows: int = 1_000_000) -> Tuple[Any, Any]:
        """
        Generate 1M row sample data for PySpark.
        
        Args:
            rows: Number of rows to generate
        
        Returns:
            Tuple of (orders_df, products_df)
        """
        logger.info(f"Generating {rows:,} sample rows for PySpark...")
        
        import pandas as pd_temp
        import numpy as np_temp
        
        # Generate using Pandas then convert to PySpark
        np_temp.random.seed(42)
        orders_data = {
            'order_id': range(1, rows + 1),
            'customer_id': np_temp.random.randint(1, 10001, rows),
            'product_id': np_temp.random.randint(1, 501, rows),
            'quantity': np_temp.random.randint(1, 20, rows),
            'order_date': pd_temp.date_range('2023-01-01', periods=rows, freq='min')
        }
        orders_pd = pd_temp.DataFrame(orders_data)
        orders_df = self.spark.createDataFrame(orders_pd)
        
        # Generate products
        products_data = {
            'product_id': range(1, 501),
            'product_name': [f'Product_{i}' for i in range(1, 501)],
            'price': [float(x) for x in np_temp.random.uniform(10, 500, 500)]
        }
        products_pd = pd_temp.DataFrame(products_data)
        products_df = self.spark.createDataFrame(products_pd)
        
        return orders_df, products_df
    
    # ==================== RESULTS AND COMPARISON ====================
    
    def print_comparison_table(self, pandas_timings: Dict[str, float], 
                              pyspark_timings: Dict[str, float]) -> None:
        """
        Print formatted comparison table.
        
        Args:
            pandas_timings: Timing results from Pandas
            pyspark_timings: Timing results from PySpark
        """
        print("\n" + "=" * 100)
        print("PANDAS vs PYSPARK PERFORMANCE COMPARISON - 1M ROWS")
        print("=" * 100)
        
        # Operation timing comparison
        print("\n📊 OPERATION TIMING (milliseconds)")
        print("-" * 100)
        print(f"{'Operation':<20} {'Pandas (ms)':>20} {'PySpark (ms)':>20} {'Ratio (Spark/Pandas)':>20}")
        print("-" * 100)
        
        operations = ['join', 'revenue', 'groupby', 'top10', 'total']
        for op in operations:
            pandas_time = pandas_timings.get(op, 0)
            pyspark_time = pyspark_timings.get(op, 0)
            ratio = pyspark_time / pandas_time if pandas_time > 0 else 0
            
            # Highlight winner
            pandas_winner = "✓ FASTER" if pandas_time < pyspark_time else ""
            pyspark_winner = "✓ FASTER" if pyspark_time < pandas_time else ""
            
            print(f"{op:<20} {pandas_time:>18.2f}ms {pyspark_time:>18.2f}ms {ratio:>18.2f}x")
        
        # Memory comparison
        print("\n💾 MEMORY USAGE (MB)")
        print("-" * 100)
        pandas_mem = pandas_timings.get('memory_mb', 0)
        pyspark_mem = pyspark_timings.get('memory_mb', 0)
        mem_ratio = pyspark_mem / pandas_mem if pandas_mem > 0 else 0
        
        print(f"{'Pandas Memory':<20} {pandas_mem:>18.2f} MB")
        print(f"{'PySpark Memory':<20} {pyspark_mem:>18.2f} MB")
        print(f"{'Ratio (Spark/Pandas)':<20} {mem_ratio:>18.2f}x")
        
        # Summary
        print("\n📈 SUMMARY")
        print("-" * 100)
        pandas_total = pandas_timings.get('total', 0)
        pyspark_total = pyspark_timings.get('total', 0)
        
        if pandas_total > 0:
            speedup = pandas_total / pyspark_total if pyspark_total > 0 else 0
            improvement = ((pandas_total - pyspark_total) / pandas_total) * 100
            
            print(f"Total Processing Time - Pandas: {pandas_total:.2f}ms, PySpark: {pyspark_total:.2f}ms")
            print(f"Speedup: {speedup:.2f}x")
            print(f"Performance Improvement: {improvement:+.1f}%")
            
            if pyspark_total < pandas_total:
                print(f"✓ PySpark is {speedup:.2f}x FASTER than Pandas")
            else:
                print(f"✓ Pandas is {1/speedup:.2f}x FASTER than PySpark")
        
        print("=" * 100 + "\n")
    
    def run(self) -> None:
        """Run complete performance comparison."""
        logger.info("Starting Pandas vs PySpark Performance Comparison")
        
        try:
            # Initialize Spark
            self.create_spark_session()
            
            # Load data
            pandas_orders, pandas_products, pandas_load_time = self.pandas_load_data()
            pyspark_orders, pyspark_products, pyspark_load_time = self.pyspark_load_data()
            
            # Process data
            pandas_result, pandas_timings = self.pandas_process_data(pandas_orders, pandas_products)
            pyspark_result, pyspark_timings = self.pyspark_process_data(pyspark_orders, pyspark_products)
            
            # Add load times
            pandas_timings['load'] = pandas_load_time
            pyspark_timings['load'] = pyspark_load_time
            
            # Display results
            logger.info("Pandas Results:")
            logger.info(f"\n{pandas_result}")
            
            logger.info("PySpark Results:")
            pyspark_result_pd = pyspark_result.toPandas()
            logger.info(f"\n{pyspark_result_pd}")
            
            # Print comparison
            self.print_comparison_table(pandas_timings, pyspark_timings)
            
        except Exception as e:
            logger.error(f"Comparison failed: {e}", exc_info=True)
            raise
        finally:
            if self.spark:
                self.spark.stop()
                logger.info("Spark session stopped")


def main():
    """Main entry point."""
    comparison = PerformanceComparison()
    comparison.run()


if __name__ == "__main__":
    main()
