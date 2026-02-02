"""
PySpark Analytics Module

Provides optimized PySpark analytics for sales data processing with:
- Adaptive Query Execution (AQE)
- Kryo serialization for better performance
- Window functions for advanced analytics
- Type hints and comprehensive documentation
"""

import logging
import sys
import os
from typing import Optional

# Add project root to sys.path to allow running script directly
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import DoubleType

from src.config import (
    RAW_DATA_DIR,
    PROCESSED_DATA_DIR
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

class SalesAnalytics:
    """
    Comprehensive sales analytics engine using Apache PySpark.
    
    Features:
    - Optimized Spark session with Adaptive Query Execution (AQE)
    - Kryo serialization for improved performance
    - Advanced aggregations and window functions
    - Support for multi-format data loading
    
    Spark Configuration:
    - Driver Memory: 4GB
    - Master: local[*] (uses all available cores)
    - Adaptive Query Execution: Enabled (optimizes join strategies)
    - Serializer: Kryo (more efficient than default Java serialization)
    
    Example:
        >>> analytics = SalesAnalytics()
        >>> orders = analytics.load_parquet("data/orders.parquet")
        >>> products = analytics.load_parquet("data/products.parquet")
        >>> top_10 = analytics.top_customers_by_revenue(orders, products, n=10)
        >>> top_10.show()
    """

    def __init__(self, app_name: str = "SalesAnalytics"):
        """
        Initialize the SalesAnalytics engine.
        
        Args:
            app_name (str): Spark application name. Defaults to "SalesAnalytics".
        """
        self.spark = self.create_spark_session(app_name)
        logger.info(f"✓ SalesAnalytics initialized with app: {app_name}")

    def create_spark_session(self, app_name: str) -> SparkSession:
        """
        Configure and create an optimized Spark session.
        
        Configurations applied:
        - Driver memory: 4GB for handling large datasets
        - Adaptive Query Execution: Optimizes join order and partitioning
        - Kryo Serialization: More efficient than Java serialization
        - Broadcast threshold: 128MB for automatic broadcast joins
        - Partition default: 200 partitions for optimal parallelism
        
        Args:
            app_name (str): Name of the Spark application.
            
        Returns:
            SparkSession: Configured Spark session instance.
        """
        logger.info(f"Creating Spark session: {app_name}")
        
        spark = SparkSession.builder \
            .appName(app_name) \
            .master("local[*]") \
            .config("spark.driver.memory", "4g") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .config("spark.sql.adaptive.skewJoin.enabled", "true") \
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
            .config("spark.kryoserializer.buffer.max", "512m") \
            .config("spark.sql.autoBroadcastJoinThreshold", "128mb") \
            .config("spark.sql.shuffle.partitions", "200") \
            .config("spark.executor.cores", "4") \
            .getOrCreate()
        
        logger.info("✓ Spark session created with optimized configuration")
        logger.info(f"  - Driver Memory: 4GB")
        logger.info(f"  - Serializer: Kryo")
        logger.info(f"  - AQE Enabled: true")
        logger.info(f"  - Master: local[*]")
        
        return spark

    def load_parquet(self, path: str) -> DataFrame:
        """
        Load a Parquet file into a Spark DataFrame.
        
        Parquet is a columnar storage format optimized for analytical queries with:
        - Compression support (snappy, gzip, etc.)
        - Predicate pushdown for faster filtering
        - Schema inference
        
        Args:
            path (str): File path to the parquet file.
            
        Returns:
            DataFrame: Loaded Spark DataFrame.
            
        Raises:
            FileNotFoundError: If the path does not exist.
            
        Example:
            >>> orders_df = analytics.load_parquet("data/orders.parquet")
            >>> orders_df.show(5)
        """
        logger.info(f"Loading Parquet file: {path}")
        
        try:
            df = self.spark.read.parquet(path)
            row_count = df.count()
            col_count = len(df.columns)
            logger.info(f"✓ Loaded {row_count:,} rows and {col_count} columns")
            return df
        except Exception as e:
            logger.error(f"Failed to load parquet file {path}: {e}")
            raise

    def top_customers_by_revenue(self, orders_df: DataFrame, products_df: DataFrame, n: int = 10) -> DataFrame:
        """
        Calculate total spend per customer and return top N customers by revenue.
        
        This method performs a join between orders and products tables, calculates
        total revenue per customer, and returns the top N customers sorted by revenue.
        
        Process:
        1. Join orders with products on product_id
        2. Calculate revenue per order (price × quantity)
        3. Group by customer_id and sum revenue
        4. Sort by total_spend descending
        5. Limit to top N results
        
        Args:
            orders_df (DataFrame): Orders DataFrame with columns: 
                                   [customer_id, product_id, quantity, order_date, ...]
            products_df (DataFrame): Products DataFrame with columns:
                                    [product_id, price, category, ...]
            n (int): Number of top customers to return. Defaults to 10.
            
        Returns:
            DataFrame: Columns [customer_id, total_spend] sorted by revenue descending.
            
        Example:
            >>> top_10_customers = analytics.top_customers_by_revenue(orders, products, n=10)
            >>> top_10_customers.show()
            +-------------+------------------+
            |customer_id  |total_spend       |
            +-------------+------------------+
            |1024         |45678.90          |
            |2567         |42345.67          |
            ...
        """
        logger.info(f"Calculating top {n} customers by revenue...")
        
        try:
            # Join orders with products to get price information
            joined_df = orders_df.join(
                products_df.select("product_id", "price"),
                on="product_id",
                how="inner"
            )
            
            # Calculate revenue per order (price × quantity)
            revenue_df = joined_df.withColumn(
                "revenue", 
                F.col("price") * F.col("quantity")
            )
            
            # Aggregate by customer and calculate total spend
            customer_spend = revenue_df.groupBy("customer_id") \
                .agg(
                    F.sum("revenue").alias("total_spend"),
                    F.count("*").alias("order_count"),
                    F.avg("revenue").alias("avg_order_value")
                ) \
                .orderBy(F.desc("total_spend")) \
                .limit(n)
            
            logger.info(f"✓ Calculated top {n} customers")
            return customer_spend
            
        except Exception as e:
            logger.error(f"Error calculating top customers: {e}")
            raise

    def sales_by_category(self, orders_df: DataFrame, products_df: DataFrame) -> DataFrame:
        """
        Group sales by product category and calculate revenue and units sold.
        
        Aggregates orders grouped by product category and calculates:
        - Total revenue (price × quantity summed per category)
        - Total units sold (quantity summed per category)
        - Average order value
        - Order count
        
        Process:
        1. Join orders with products on product_id
        2. Calculate revenue per order (price × quantity)
        3. Group by category
        4. Aggregate with sum(revenue), sum(quantity), etc.
        5. Sort by total_revenue descending
        
        Args:
            orders_df (DataFrame): Orders DataFrame with columns:
                                   [product_id, quantity, order_date, ...]
            products_df (DataFrame): Products DataFrame with columns:
                                    [product_id, category, price, ...]
            
        Returns:
            DataFrame: Columns [category, total_revenue, total_units_sold, 
                               avg_order_value, order_count] sorted by revenue.
            
        Example:
            >>> category_sales = analytics.sales_by_category(orders, products)
            >>> category_sales.show()
            +----------+---------------+------------------+-----------------+
            |category  |total_revenue  |total_units_sold  |avg_order_value  |
            +----------+---------------+------------------+-----------------+
            |Electronics|1234567.89    |5600               |220.44           |
            |Clothing  |987654.32      |8900               |110.96           |
            ...
        """
        logger.info("Calculating sales by category...")
        
        try:
            # Join orders with products on product_id
            joined_df = orders_df.join(
                products_df.select("product_id", "category", "price"),
                on="product_id",
                how="inner"
            )
            
            # Calculate revenue per order and aggregate by category
            category_stats = joined_df.withColumn(
                "revenue", 
                F.col("price") * F.col("quantity")
            ) \
            .groupBy("category") \
            .agg(
                F.sum("revenue").alias("total_revenue"),
                F.sum("quantity").alias("total_units_sold"),
                F.count("*").alias("order_count"),
                F.avg("revenue").alias("avg_order_value"),
                F.round(F.avg("quantity"), 2).alias("avg_units_per_order")
            ) \
            .orderBy(F.desc("total_revenue"))
            
            logger.info("✓ Calculated category sales statistics")
            return category_stats
            
        except Exception as e:
            logger.error(f"Error calculating sales by category: {e}")
            raise

    def monthly_trends(self, orders_df: DataFrame, products_df: DataFrame) -> DataFrame:
        """
        Calculate month-over-month (MoM) revenue growth percentage.
        
        Analyzes revenue trends across months and calculates the percentage change
        from the previous month. Uses Spark Window functions for efficient computation.
        
        Process:
        1. Join orders with products on product_id
        2. Calculate revenue per order (price × quantity)
        3. Extract month from order_date
        4. Group by month and sum revenue
        5. Use Window function to get previous month's revenue
        6. Calculate MoM growth percentage: ((current - previous) / previous) × 100
        7. Sort by month ascending
        
        Window Function:
        - LAG(revenue) OVER (ORDER BY month): Gets previous month's revenue
        - Returns NULL for the first month (no previous data)
        
        Args:
            orders_df (DataFrame): Orders DataFrame with columns:
                                   [product_id, quantity, order_date, ...]
            products_df (DataFrame): Products DataFrame with columns:
                                    [product_id, price, ...]
            
        Returns:
            DataFrame: Columns [month, current_revenue, prev_revenue, mom_growth_pct]
                      where mom_growth_pct is the percentage change from previous month.
                      First row will have NULL for prev_revenue and mom_growth_pct.
            
        Example:
            >>> trends = analytics.monthly_trends(orders, products)
            >>> trends.show()
            +----------+----------------+----------------+---------------+
            |month     |current_revenue |prev_revenue    |mom_growth_pct |
            +----------+----------------+----------------+---------------+
            |2023-01-01|45678.90        |NULL            |NULL           |
            |2023-02-01|52345.67        |45678.90        |14.56          |
            |2023-03-01|48900.45        |52345.67        |-6.57          |
            ...
        """
        logger.info("Calculating month-over-month revenue trends...")
        
        try:
            # Join orders with products
            joined_df = orders_df.join(
                products_df.select("product_id", "price"),
                on="product_id",
                how="inner"
            )
            
            # Calculate monthly revenue
            monthly_revenue = joined_df.withColumn(
                "revenue", 
                F.col("price") * F.col("quantity")
            ) \
            .withColumn(
                "month", 
                F.trunc("order_date", "month")
            ) \
            .groupBy("month") \
            .agg(
                F.sum("revenue").alias("current_revenue"),
                F.count("*").alias("transaction_count"),
                F.avg("revenue").alias("avg_transaction_value")
            )
            
            # Define window specification for month-over-month calculation
            window_spec = Window.orderBy("month")
            
            # Calculate MoM growth using LAG window function
            trends_df = monthly_revenue.withColumn(
                "prev_revenue", 
                F.lag("current_revenue").over(window_spec)
            ).withColumn(
                "mom_growth_pct",
                F.when(
                    F.col("prev_revenue").isNotNull(),
                    F.round(
                        ((F.col("current_revenue") - F.col("prev_revenue")) / F.col("prev_revenue")) * 100,
                        2
                    )
                ).otherwise(F.lit(None))
            ).orderBy("month")
            
            logger.info("✓ Calculated monthly revenue trends with MoM growth")
            return trends_df
            
        except Exception as e:
            logger.error(f"Error calculating monthly trends: {e}")
            raise

    def run(self) -> None:
        """
        Execute the complete analytics pipeline.
        
        This method orchestrates the full analytics workflow:
        1. Loads orders and products data from parquet files
        2. Computes top 10 customers by revenue
        3. Analyzes sales by product category
        4. Calculates month-over-month revenue trends
        5. Displays results and saves monthly trends to CSV
        
        Error Handling:
        - Catches and logs any exceptions during pipeline execution
        - Ensures Spark session is properly stopped
        
        Output:
        - Console output: Results displayed using DataFrame.show()
        - File output: Monthly trends saved to PROCESSED_DATA_DIR/monthly_trends/
        
        Example:
            >>> analytics = SalesAnalytics()
            >>> analytics.run()
            ✓ SalesAnalytics initialized...
            Loading Parquet file: data/orders.parquet
            ✓ Loaded 500,000 rows and 4 columns
            ...
            ✅ Analytics pipeline completed successfully
        """
        try:
            logger.info("="*80)
            logger.info("ANALYTICS PIPELINE: Starting")
            logger.info("="*80)
            
            # Define paths (assuming standard naming from data_generator)
            orders_path = str(RAW_DATA_DIR / "orders.parquet")
            products_path = str(RAW_DATA_DIR / "products.parquet")
            
            # Load Data
            logger.info("\n📊 Loading data...")
            orders_df = self.load_parquet(orders_path)
            products_df = self.load_parquet(products_path)
            
            # Show schema information
            logger.info("\n📋 Orders Schema:")
            orders_df.printSchema()
            logger.info("\n📋 Products Schema:")
            products_df.printSchema()
            
            # 1. Top Customers by Revenue
            logger.info("\n" + "="*80)
            logger.info("ANALYSIS 1: TOP 10 CUSTOMERS BY REVENUE")
            logger.info("="*80)
            top_customers = self.top_customers_by_revenue(orders_df, products_df, n=10)
            top_customers.show(truncate=False)
            
            # 2. Sales by Category
            logger.info("\n" + "="*80)
            logger.info("ANALYSIS 2: SALES BY CATEGORY")
            logger.info("="*80)
            category_sales = self.sales_by_category(orders_df, products_df)
            category_sales.show(truncate=False)
            
            # 3. Monthly Trends
            logger.info("\n" + "="*80)
            logger.info("ANALYSIS 3: MONTH-OVER-MONTH REVENUE TRENDS")
            logger.info("="*80)
            trends = self.monthly_trends(orders_df, products_df)
            trends.show(truncate=False)
            
            # Save results to processed data directory
            logger.info("\n💾 Saving results...")
            output_path = str(PROCESSED_DATA_DIR / "monthly_trends")
            trends.write.mode("overwrite").option("header", "true").csv(output_path)
            logger.info(f"✓ Monthly trends saved to: {output_path}")
            
            logger.info("\n" + "="*80)
            logger.info("✅ ANALYTICS PIPELINE: Completed Successfully")
            logger.info("="*80 + "\n")
            
        except Exception as e:
            logger.error(f"❌ Analytics pipeline failed: {e}", exc_info=True)
            raise
        finally:
            logger.info("Stopping Spark session...")
            self.spark.stop()
            logger.info("✓ Spark session stopped")

if __name__ == "__main__":
    analytics = SalesAnalytics()
    analytics.run()