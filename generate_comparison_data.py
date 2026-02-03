"""
Generate sample data for Pandas vs PySpark comparison

Creates compatible Parquet files with proper Spark-compatible schema:
- orders.parquet: order_id, customer_id, product_id, quantity, order_date
- products.parquet: product_id, product_name, category, price

Usage:
    python generate_comparison_data.py
    python generate_comparison_data.py --rows 1000000
"""

import logging
from pathlib import Path
from typing import Dict
import numpy as np
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, DateType

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def generate_sample_data(n_orders: int = 1_000_000, n_products: int = 10_000, n_customers: int = 50_000):
    """
    Generate sample orders and products data compatible with both Pandas and PySpark.
    
    Args:
        n_orders (int): Number of orders
        n_products (int): Number of products
        n_customers (int): Number of customers
    """
    logger.info("=" * 80)
    logger.info("GENERATING SAMPLE DATA FOR COMPARISON")
    logger.info("=" * 80)
    
    # Create output directory
    data_dir = Path("data/raw")
    data_dir.mkdir(parents=True, exist_ok=True)
    
    np.random.seed(42)
    
    # Generate products
    logger.info(f"\nGenerating {n_products:,} products...")
    categories = ['Electronics', 'Clothing', 'Home', 'Sports', 'Books', 'Toys']
    
    products_df = pd.DataFrame({
        'product_id': np.arange(1, n_products + 1),
        'product_name': [f'Product_{i}' for i in range(1, n_products + 1)],
        'category': np.random.choice(categories, n_products),
        'price': np.random.uniform(10, 1000, n_products).round(2)
    })
    
    logger.info(f"✓ Generated products DataFrame: {products_df.shape[0]:,} rows, {products_df.shape[1]} cols")
    logger.info(f"  Sample:\n{products_df.head(3)}")
    
    # Generate orders
    logger.info(f"\nGenerating {n_orders:,} orders...")
    
    # Generate dates as strings first to avoid timestamp precision issues
    date_range = pd.date_range('2024-01-01', '2025-12-31', freq='D')
    
    orders_df = pd.DataFrame({
        'order_id': np.arange(1, n_orders + 1),
        'customer_id': np.random.randint(1, n_customers + 1, n_orders),
        'product_id': np.random.randint(1, n_products + 1, n_orders),
        'quantity': np.random.randint(1, 20, n_orders),
        'order_date': np.random.choice(date_range, n_orders)
    })
    
    logger.info(f"✓ Generated orders DataFrame: {orders_df.shape[0]:,} rows, {orders_df.shape[1]} cols")
    logger.info(f"  Sample:\n{orders_df.head(3)}")
    
    # Save as Parquet using PySpark to ensure compatibility
    logger.info("\nSaving to Parquet files (using PySpark for compatibility)...")
    
    spark = SparkSession.builder \
        .appName("GenerateComparisonData") \
        .master("local[*]") \
        .config("spark.driver.memory", "4g") \
        .getOrCreate()
    
    # Convert to Spark DataFrame with proper schema
    orders_schema = StructType([
        StructField('order_id', IntegerType(), False),
        StructField('customer_id', IntegerType(), False),
        StructField('product_id', IntegerType(), False),
        StructField('quantity', IntegerType(), False),
        StructField('order_date', DateType(), False)
    ])
    
    products_schema = StructType([
        StructField('product_id', IntegerType(), False),
        StructField('product_name', StringType(), False),
        StructField('category', StringType(), False),
        StructField('price', DoubleType(), False)
    ])
    
    # Convert dates to proper date type
    orders_df['order_date'] = pd.to_datetime(orders_df['order_date']).dt.date
    
    # Create Spark DataFrames
    orders_spark = spark.createDataFrame(orders_df, schema=orders_schema)
    products_spark = spark.createDataFrame(products_df, schema=products_schema)
    
    # Save to Parquet
    orders_path = data_dir / "orders.parquet"
    products_path = data_dir / "products.parquet"
    
    logger.info(f"\nWriting orders to {orders_path}...")
    orders_spark.write.mode("overwrite").parquet(str(orders_path))
    logger.info(f"✓ Saved orders.parquet ({orders_spark.count():,} rows)")
    
    logger.info(f"\nWriting products to {products_path}...")
    products_spark.write.mode("overwrite").parquet(str(products_path))
    logger.info(f"✓ Saved products.parquet ({products_spark.count():,} rows)")
    
    # Verify by reading back
    logger.info("\nVerifying saved files...")
    
    orders_verify = spark.read.parquet(str(orders_path))
    products_verify = spark.read.parquet(str(products_path))
    
    logger.info(f"✓ Orders schema: {orders_verify.printSchema()}")
    logger.info(f"✓ Orders count: {orders_verify.count():,}")
    logger.info(f"✓ Products schema: {products_verify.printSchema()}")
    logger.info(f"✓ Products count: {products_verify.count():,}")
    
    spark.stop()
    
    logger.info("\n" + "=" * 80)
    logger.info("✓ DATA GENERATION COMPLETE")
    logger.info("=" * 80)
    logger.info(f"\nFiles ready at: {data_dir}")
    logger.info(f"  - {orders_path}")
    logger.info(f"  - {products_path}")


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Generate sample data for Pandas vs PySpark comparison")
    parser.add_argument('--rows', type=int, default=1_000_000, help='Number of orders (default: 1M)')
    args = parser.parse_args()
    
    generate_sample_data(n_orders=args.rows)
