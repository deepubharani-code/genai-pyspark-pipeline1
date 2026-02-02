"""
Analytics Execution Script

Orchestrates the complete SalesAnalytics pipeline with:
- Spark session initialization
- Data loading from Parquet files
- Three analytics operations (top customers, category sales, monthly trends)
- Execution timing for each operation
- Formatted output display
- Proper resource cleanup

Usage:
    python run_analytics.py
    python run_analytics.py --top-customers 20
    python run_analytics.py --data-dir /path/to/data --verbose
"""

import logging
import sys
import time
from pathlib import Path
from typing import Dict, Tuple

from src.spark_analytics import SalesAnalytics
from pyspark.sql import DataFrame

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class AnalyticsRunner:
    """Orchestrates the complete analytics pipeline with timing and reporting."""
    
    def __init__(self, data_dir: str = "data/raw"):
        """
        Initialize the analytics runner.
        
        Args:
            data_dir (str): Directory containing Parquet files. Defaults to "data/raw".
        """
        self.data_dir = Path(data_dir)
        self.analytics = None
        self.execution_times: Dict[str, float] = {}
        self.results: Dict[str, DataFrame] = {}
        
    def initialize(self) -> None:
        """Initialize Spark session and analytics engine."""
        logger.info("=" * 80)
        logger.info("INITIALIZING ANALYTICS PIPELINE")
        logger.info("=" * 80)
        
        start_time = time.perf_counter()
        self.analytics = SalesAnalytics()
        init_time = time.perf_counter() - start_time
        
        logger.info(f"✓ Spark session initialized in {init_time:.4f}s")
        self.execution_times['initialization'] = init_time
        
    def load_data(self) -> Tuple[DataFrame, DataFrame, DataFrame]:
        """
        Load all required data files from Parquet format.
        
        Returns:
            Tuple[DataFrame, DataFrame, DataFrame]: (customers, products, orders)
            
        Raises:
            FileNotFoundError: If any Parquet file is missing
        """
        logger.info("\n" + "=" * 80)
        logger.info("LOADING DATA")
        logger.info("=" * 80)
        
        files = {
            'customers': self.data_dir / "customers.parquet",
            'products': self.data_dir / "products.parquet",
            'orders': self.data_dir / "orders.parquet"
        }
        
        # Check if files exist
        missing_files = [name for name, path in files.items() if not path.exists()]
        if missing_files:
            logger.warning(f"Missing files: {', '.join(missing_files)}")
            logger.info(f"Looking in: {self.data_dir.absolute()}")
            logger.info("Attempting to load available files...")
        
        # Load files with timing
        data = {}
        for name, path in files.items():
            if not path.exists():
                logger.warning(f"Skipping {name}: file not found at {path}")
                continue
                
            logger.info(f"\nLoading {name}...")
            start_time = time.perf_counter()
            
            try:
                df = self.analytics.load_parquet(str(path))
                load_time = time.perf_counter() - start_time
                
                row_count = df.count()
                col_count = len(df.columns)
                
                logger.info(f"✓ {name} loaded in {load_time:.4f}s")
                logger.info(f"  Rows: {row_count:,} | Columns: {col_count}")
                logger.info(f"  Columns: {', '.join(df.columns)}")
                
                self.execution_times[f'load_{name}'] = load_time
                data[name] = df
                
            except Exception as e:
                logger.error(f"Failed to load {name}: {e}")
                raise
        
        return data['customers'], data['products'], data['orders']
    
    def run_top_customers_analysis(self, customers: DataFrame, orders: DataFrame, 
                                   products: DataFrame, n: int = 10) -> DataFrame:
        """
        Run top customers by revenue analysis.
        
        Args:
            customers (DataFrame): Customer data
            orders (DataFrame): Orders data
            products (DataFrame): Product data
            n (int): Number of top customers to return. Defaults to 10.
            
        Returns:
            DataFrame: Top customers with spending metrics
        """
        logger.info("\n" + "=" * 80)
        logger.info(f"ANALYSIS 1: TOP {n} CUSTOMERS BY REVENUE")
        logger.info("=" * 80)
        
        start_time = time.perf_counter()
        
        try:
            result = self.analytics.top_customers_by_revenue(orders, products, n=n)
            exec_time = time.perf_counter() - start_time
            
            # Force evaluation
            result.cache()
            count = result.count()
            
            logger.info(f"\n✓ Analysis completed in {exec_time:.4f}s")
            logger.info(f"  Returned {count} customers\n")
            
            # Display results
            result.show(truncate=False)
            
            # Display summary statistics
            logger.info("\n" + "-" * 80)
            logger.info("SUMMARY STATISTICS")
            logger.info("-" * 80)
            
            for row in result.collect():
                logger.info(f"Customer {row.customer_id}: "
                           f"${row.total_spend:,.2f} spend | "
                           f"{int(row.order_count)} orders | "
                           f"${row.avg_order_value:,.2f} avg/order")
            
            self.execution_times['top_customers'] = exec_time
            self.results['top_customers'] = result
            
            return result
            
        except Exception as e:
            logger.error(f"Analysis failed: {e}", exc_info=True)
            raise
    
    def run_category_analysis(self, orders: DataFrame, products: DataFrame) -> DataFrame:
        """
        Run sales by category analysis.
        
        Args:
            orders (DataFrame): Orders data
            products (DataFrame): Product data
            
        Returns:
            DataFrame: Category sales breakdown
        """
        logger.info("\n" + "=" * 80)
        logger.info("ANALYSIS 2: SALES BY CATEGORY")
        logger.info("=" * 80)
        
        start_time = time.perf_counter()
        
        try:
            result = self.analytics.sales_by_category(orders, products)
            exec_time = time.perf_counter() - start_time
            
            # Force evaluation
            result.cache()
            count = result.count()
            
            logger.info(f"\n✓ Analysis completed in {exec_time:.4f}s")
            logger.info(f"  Analyzed {count} categories\n")
            
            # Display results
            result.show(truncate=False)
            
            # Display summary statistics
            logger.info("\n" + "-" * 80)
            logger.info("CATEGORY BREAKDOWN")
            logger.info("-" * 80)
            
            total_revenue = 0
            for row in result.collect():
                logger.info(f"{row.category:15} | "
                           f"Revenue: ${row.total_revenue:>12,.2f} | "
                           f"Units: {int(row.total_units_sold):>6} | "
                           f"Orders: {int(row.order_count):>5} | "
                           f"Avg Value: ${row.avg_order_value:>10,.2f}")
                total_revenue += row.total_revenue
            
            logger.info("-" * 80)
            logger.info(f"{'TOTAL':15} | Revenue: ${total_revenue:>12,.2f}")
            
            self.execution_times['sales_by_category'] = exec_time
            self.results['sales_by_category'] = result
            
            return result
            
        except Exception as e:
            logger.error(f"Analysis failed: {e}", exc_info=True)
            raise
    
    def run_trends_analysis(self, orders: DataFrame, products: DataFrame) -> DataFrame:
        """
        Run monthly trends analysis.
        
        Args:
            orders (DataFrame): Orders data
            products (DataFrame): Product data
            
        Returns:
            DataFrame: Monthly revenue trends with MoM growth
        """
        logger.info("\n" + "=" * 80)
        logger.info("ANALYSIS 3: MONTH-OVER-MONTH REVENUE TRENDS")
        logger.info("=" * 80)
        
        start_time = time.perf_counter()
        
        try:
            result = self.analytics.monthly_trends(orders, products)
            exec_time = time.perf_counter() - start_time
            
            # Force evaluation
            result.cache()
            count = result.count()
            
            logger.info(f"\n✓ Analysis completed in {exec_time:.4f}s")
            logger.info(f"  Analyzed {count} months\n")
            
            # Display results
            result.show(truncate=False)
            
            # Display summary statistics
            logger.info("\n" + "-" * 80)
            logger.info("TREND ANALYSIS")
            logger.info("-" * 80)
            
            for row in result.collect():
                month_str = str(row.month)[:10]
                growth_str = f"{row.mom_growth_pct:+.2f}%" if row.mom_growth_pct else "N/A (baseline)"
                logger.info(f"{month_str} | "
                           f"Revenue: ${row.current_revenue:>12,.2f} | "
                           f"Transactions: {int(row.transaction_count):>6} | "
                           f"Avg Value: ${row.avg_transaction_value:>10,.2f} | "
                           f"MoM Growth: {growth_str:>10}")
            
            self.execution_times['monthly_trends'] = exec_time
            self.results['monthly_trends'] = result
            
            return result
            
        except Exception as e:
            logger.error(f"Analysis failed: {e}", exc_info=True)
            raise
    
    def print_execution_summary(self) -> None:
        """Print summary of execution times."""
        logger.info("\n" + "=" * 80)
        logger.info("EXECUTION TIME SUMMARY")
        logger.info("=" * 80)
        
        logger.info(f"\n{'Operation':<30} {'Time (s)':<15} {'Status'}")
        logger.info("-" * 60)
        
        total_time = 0
        for operation, elapsed_time in self.execution_times.items():
            formatted_op = operation.replace('_', ' ').title()
            logger.info(f"{formatted_op:<30} {elapsed_time:>12.4f}s  ✓")
            total_time += elapsed_time
        
        logger.info("-" * 60)
        logger.info(f"{'TOTAL':<30} {total_time:>12.4f}s")
        logger.info("")
        
        # Performance breakdown
        logger.info("PERFORMANCE BREAKDOWN:")
        logger.info(f"  Initialization: {self.execution_times.get('initialization', 0):.4f}s")
        logger.info(f"  Data Loading:   {sum(v for k, v in self.execution_times.items() if k.startswith('load_')):.4f}s")
        logger.info(f"  Analytics:      {sum(v for k, v in self.execution_times.items() if any(x in k for x in ['top_', 'sales_', 'monthly_', 'trends'])):.4f}s")
    
    def cleanup(self) -> None:
        """Stop Spark session and clean up resources."""
        logger.info("\n" + "=" * 80)
        logger.info("CLEANUP")
        logger.info("=" * 80)
        
        if self.analytics:
            try:
                self.analytics.spark.stop()
                logger.info("✓ Spark session stopped successfully")
            except Exception as e:
                logger.error(f"Error stopping Spark session: {e}")
        
        logger.info("\n" + "=" * 80)
        logger.info("ANALYTICS PIPELINE COMPLETE")
        logger.info("=" * 80 + "\n")
    
    def run(self, top_n: int = 10) -> None:
        """
        Execute the complete analytics pipeline.
        
        Args:
            top_n (int): Number of top customers to return. Defaults to 10.
        """
        try:
            # Initialize
            self.initialize()
            
            # Load data
            customers, products, orders = self.load_data()
            
            # Run analyses
            self.run_top_customers_analysis(customers, orders, products, n=top_n)
            self.run_category_analysis(orders, products)
            self.run_trends_analysis(orders, products)
            
            # Print summary
            self.print_execution_summary()
            
        except Exception as e:
            logger.error(f"Pipeline failed: {e}", exc_info=True)
            sys.exit(1)
        finally:
            self.cleanup()


def main():
    """Main entry point for the analytics pipeline."""
    import argparse
    
    parser = argparse.ArgumentParser(
        description="Run PySpark sales analytics pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python run_analytics.py                    # Run with defaults (top 10 customers)
  python run_analytics.py --top-customers 20 # Get top 20 customers
  python run_analytics.py --data-dir /path/to/data  # Specify custom data directory
        """
    )
    
    parser.add_argument(
        '--top-customers', 
        type=int, 
        default=10,
        help='Number of top customers to display (default: 10)'
    )
    
    parser.add_argument(
        '--data-dir',
        type=str,
        default='data/raw',
        help='Directory containing Parquet files (default: data/raw)'
    )
    
    parser.add_argument(
        '--verbose',
        action='store_true',
        help='Enable verbose logging'
    )
    
    args = parser.parse_args()
    
    # Set logging level
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    # Run pipeline
    runner = AnalyticsRunner(data_dir=args.data_dir)
    runner.run(top_n=args.top_customers)


if __name__ == "__main__":
    main()