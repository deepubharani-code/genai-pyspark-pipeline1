"""
PySpark Analytics Example and Testing Module

This script demonstrates how to use the SalesAnalytics class with various
data patterns and shows the output of each analytical method.

Usage:
    python spark_analytics_example.py
    
    Or in interactive mode:
    >>> from spark_analytics_example import create_sample_data, run_analytics_demo
    >>> run_analytics_demo()
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, TimestampType
from datetime import datetime, timedelta
import logging
import sys
import os

# Add project root to path
sys.path.append(os.path.abspath(os.path.dirname(__file__)))

from src.spark_analytics import SalesAnalytics

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def create_sample_data(spark: SparkSession) -> tuple[DataFrame, DataFrame]:
    """
    Create sample orders and products DataFrames for demonstration.
    
    Args:
        spark (SparkSession): Active Spark session.
        
    Returns:
        tuple[DataFrame, DataFrame]: (orders_df, products_df)
    """
    logger.info("Creating sample data...")
    
    # Create Products DataFrame
    products_data = [
        (1, "Laptop", "Electronics", 999.99),
        (2, "Mouse", "Electronics", 29.99),
        (3, "T-Shirt", "Clothing", 19.99),
        (4, "Jeans", "Clothing", 59.99),
        (5, "Book", "Books", 14.99),
        (6, "Monitor", "Electronics", 299.99),
        (7, "Shoes", "Clothing", 79.99),
        (8, "Pen", "Books", 2.99),
    ]
    
    products_schema = StructType([
        StructField("product_id", IntegerType(), True),
        StructField("product_name", StringType(), True),
        StructField("category", StringType(), True),
        StructField("price", DoubleType(), True),
    ])
    
    products_df = spark.createDataFrame(products_data, schema=products_schema)
    logger.info(f"✓ Created products DataFrame with {products_df.count()} products")
    
    # Create Orders DataFrame
    orders_data = []
    base_date = datetime(2023, 1, 1)
    
    # Generate sample orders
    customer_ids = list(range(1, 21))  # 20 customers
    
    for order_id in range(1, 501):  # 500 orders
        customer_id = customer_ids[(order_id - 1) % len(customer_ids)]
        product_id = (order_id % 8) + 1  # Cycle through products
        quantity = (order_id % 5) + 1
        order_date = base_date + timedelta(days=(order_id - 1) // 5)  # Spread over months
        
        orders_data.append((order_id, customer_id, product_id, quantity, order_date))
    
    orders_schema = StructType([
        StructField("order_id", IntegerType(), True),
        StructField("customer_id", IntegerType(), True),
        StructField("product_id", IntegerType(), True),
        StructField("quantity", IntegerType(), True),
        StructField("order_date", TimestampType(), True),
    ])
    
    orders_df = spark.createDataFrame(orders_data, schema=orders_schema)
    logger.info(f"✓ Created orders DataFrame with {orders_df.count()} orders")
    
    return orders_df, products_df


def demonstrate_top_customers(analytics: SalesAnalytics, orders_df: DataFrame, products_df: DataFrame) -> None:
    """Demonstrate the top_customers_by_revenue method."""
    logger.info("\n" + "="*80)
    logger.info("DEMONSTRATION: TOP CUSTOMERS BY REVENUE")
    logger.info("="*80)
    
    top_5 = analytics.top_customers_by_revenue(orders_df, products_df, n=5)
    
    logger.info("\nTop 5 Customers by Revenue:")
    logger.info("-" * 80)
    top_5.show(truncate=False)
    
    logger.info("\nAnalytical Details:")
    top_5.collect()[0]  # Get first row
    for row in top_5.collect():
        logger.info(f"Customer {row.customer_id}: "
                   f"${row.total_spend:,.2f} spend | "
                   f"{int(row.order_count)} orders | "
                   f"${row.avg_order_value:,.2f} avg order value")


def demonstrate_sales_by_category(analytics: SalesAnalytics, orders_df: DataFrame, products_df: DataFrame) -> None:
    """Demonstrate the sales_by_category method."""
    logger.info("\n" + "="*80)
    logger.info("DEMONSTRATION: SALES BY CATEGORY")
    logger.info("="*80)
    
    category_sales = analytics.sales_by_category(orders_df, products_df)
    
    logger.info("\nSales by Category:")
    logger.info("-" * 80)
    category_sales.show(truncate=False)
    
    logger.info("\nCategory Performance Summary:")
    for row in category_sales.collect():
        logger.info(f"{row.category:15} | "
                   f"Revenue: ${row.total_revenue:>12,.2f} | "
                   f"Units Sold: {int(row.total_units_sold):>6} | "
                   f"Orders: {int(row.order_count):>6} | "
                   f"Avg Value: ${row.avg_order_value:>10,.2f}")


def demonstrate_monthly_trends(analytics: SalesAnalytics, orders_df: DataFrame, products_df: DataFrame) -> None:
    """Demonstrate the monthly_trends method."""
    logger.info("\n" + "="*80)
    logger.info("DEMONSTRATION: MONTH-OVER-MONTH REVENUE TRENDS")
    logger.info("="*80)
    
    trends = analytics.monthly_trends(orders_df, products_df)
    
    logger.info("\nMonthly Revenue Trends:")
    logger.info("-" * 80)
    trends.show(truncate=False)
    
    logger.info("\nMonth-over-Month Growth Analysis:")
    for row in trends.collect():
        month_str = str(row.month)[:10]
        growth_str = f"{row.mom_growth_pct:+.2f}%" if row.mom_growth_pct else "N/A (First month)"
        logger.info(f"{month_str} | Revenue: ${row.current_revenue:>12,.2f} | "
                   f"Transactions: {int(row.transaction_count):>6} | "
                   f"MoM Growth: {growth_str:>10}")


def run_analytics_demo() -> None:
    """Run the complete analytics demonstration."""
    logger.info("="*80)
    logger.info("PYSPARK ANALYTICS DEMONSTRATION")
    logger.info("="*80)
    
    try:
        # Initialize analytics engine
        analytics = SalesAnalytics(app_name="SalesAnalyticsDemo")
        spark = analytics.spark
        
        # Create sample data
        orders_df, products_df = create_sample_data(spark)
        
        # Show data schemas
        logger.info("\n📋 ORDERS SCHEMA:")
        orders_df.printSchema()
        
        logger.info("\n📋 PRODUCTS SCHEMA:")
        products_df.printSchema()
        
        # Run demonstrations
        demonstrate_top_customers(analytics, orders_df, products_df)
        demonstrate_sales_by_category(analytics, orders_df, products_df)
        demonstrate_monthly_trends(analytics, orders_df, products_df)
        
        logger.info("\n" + "="*80)
        logger.info("✅ DEMONSTRATION COMPLETED SUCCESSFULLY")
        logger.info("="*80)
        
    except Exception as e:
        logger.error(f"❌ Demonstration failed: {e}", exc_info=True)
        raise
    finally:
        logger.info("\nCleaning up resources...")
        analytics.spark.stop()
        logger.info("✓ Spark session stopped")


def run_with_real_data() -> None:
    """Run analytics with real data from parquet files."""
    logger.info("="*80)
    logger.info("PYSPARK ANALYTICS - REAL DATA")
    logger.info("="*80)
    
    try:
        analytics = SalesAnalytics()
        analytics.run()
    except Exception as e:
        logger.error(f"Failed to run analytics with real data: {e}")
        logger.info("Falling back to demonstration with sample data...")
        run_analytics_demo()


if __name__ == "__main__":
    # Check if real data exists
    from pathlib import Path
    from src.config import RAW_DATA_DIR
    
    orders_path = RAW_DATA_DIR / "orders.parquet"
    products_path = RAW_DATA_DIR / "products.parquet"
    
    if orders_path.exists() and products_path.exists():
        logger.info("✓ Real data files found. Running with real data...")
        run_with_real_data()
    else:
        logger.info("⚠️  Real data files not found.")
        logger.info(f"  Expected: {orders_path}")
        logger.info(f"  Expected: {products_path}")
        logger.info("\nRunning demonstration with sample data instead...\n")
        run_analytics_demo()
