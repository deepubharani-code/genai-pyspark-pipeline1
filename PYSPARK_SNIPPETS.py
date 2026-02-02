"""
Quick Reference: PySpark SalesAnalytics Class Snippets

Copy and paste ready examples for common use cases.
"""

# ============================================================================
# BASIC SETUP
# ============================================================================

from src.spark_analytics import SalesAnalytics
from pyspark.sql import functions as F

# Initialize
analytics = SalesAnalytics(app_name="MyAnalytics")
spark = analytics.spark


# ============================================================================
# LOADING DATA
# ============================================================================

# Load from parquet
orders = analytics.load_parquet("data/orders.parquet")
products = analytics.load_parquet("data/products.parquet")

# Or directly with Spark
orders = spark.read.parquet("data/orders.parquet")
products = spark.read.parquet("data/products.parquet")


# ============================================================================
# TOP CUSTOMERS BY REVENUE
# ============================================================================

# Get top 10 customers
top_10 = analytics.top_customers_by_revenue(orders, products, n=10)
top_10.show()

# Get top 50 customers
top_50 = analytics.top_customers_by_revenue(orders, products, n=50)

# Display with custom formatting
top_10.select(
    F.col("customer_id"),
    F.format_number(F.col("total_spend"), 2).alias("Spend"),
    F.col("order_count"),
    F.round(F.col("avg_order_value"), 2).alias("Avg Order")
).show()

# Filter top customers by spend threshold
high_value = top_10.filter(F.col("total_spend") > 50000)
high_value.show()

# Export to CSV
top_10.write.mode("overwrite").option("header", "true").csv("output/top_customers.csv")

# Get count
customer_count = top_10.count()


# ============================================================================
# SALES BY CATEGORY
# ============================================================================

# Get category breakdown
categories = analytics.sales_by_category(orders, products)
categories.show()

# Filter by revenue threshold
major_categories = categories.filter(F.col("total_revenue") > 100000)
major_categories.show()

# Sort by order count instead of revenue
by_orders = categories.orderBy(F.desc("order_count"))
by_orders.show()

# Calculate percentage of total revenue
categories_with_pct = categories.withColumn(
    "revenue_pct",
    F.round((F.col("total_revenue") / F.sum("total_revenue").over()) * 100, 2)
).select("category", "total_revenue", "revenue_pct", "total_units_sold", "order_count")
categories_with_pct.show()

# Group by high-level categories
categories.select("category").distinct().show()


# ============================================================================
# MONTHLY TRENDS (Month-over-Month Growth)
# ============================================================================

# Get monthly trends
trends = analytics.monthly_trends(orders, products)
trends.show()

# Show only months with positive growth
positive_growth = trends.filter(F.col("mom_growth_pct") > 0)
positive_growth.show()

# Get average monthly growth rate
avg_growth = trends.filter(F.col("mom_growth_pct").isNotNull()) \
    .agg(F.avg("mom_growth_pct").alias("avg_monthly_growth"))
avg_growth.show()

# Find peak revenue month
peak_month = trends.orderBy(F.desc("current_revenue")).limit(1)
peak_month.show()

# Calculate quarter-over-quarter trends
quarterly = trends.withColumn(
    "quarter", F.quarter(F.col("month"))
).groupBy("quarter") \
 .agg(F.sum("current_revenue").alias("quarterly_revenue"))
quarterly.show()


# ============================================================================
# ADVANCED: CUSTOM JOINS AND ANALYSIS
# ============================================================================

# Join orders with products for custom analysis
joined = orders.join(
    products.select("product_id", "category", "price"),
    on="product_id",
    how="inner"
)

# Calculate revenue per order
with_revenue = joined.withColumn(
    "revenue", F.col("price") * F.col("quantity")
)

# Analyze by customer and category
customer_category_sales = with_revenue.groupBy("customer_id", "category") \
    .agg(
        F.sum("revenue").alias("revenue"),
        F.sum("quantity").alias("units"),
        F.count("*").alias("orders")
    ).orderBy(F.desc("revenue"))
customer_category_sales.show()

# Find customers who buy from specific category
electronics_customers = with_revenue.filter(
    F.col("category") == "Electronics"
).select("customer_id").distinct()
electronics_customers.show()

# Cross-category analysis
pivot_analysis = with_revenue.groupBy("customer_id") \
    .pivot("category") \
    .agg(F.sum("revenue")) \
    .fillna(0)
pivot_analysis.show()


# ============================================================================
# WINDOW FUNCTIONS (Advanced)
# ============================================================================

from pyspark.sql.window import Window

# Calculate cumulative revenue by month
monthly_revenue = analytics.monthly_trends(orders, products)
window_spec = Window.orderBy("month")
cumulative = monthly_revenue.withColumn(
    "cumulative_revenue",
    F.sum("current_revenue").over(window_spec)
).select("month", "current_revenue", "cumulative_revenue")
cumulative.show()

# Rank customers by spending
customer_rank = analytics.top_customers_by_revenue(orders, products, n=100)
window_customers = Window.orderBy(F.desc("total_spend"))
ranked = customer_rank.withColumn(
    "rank", F.row_number().over(window_customers)
)
ranked.show()

# Year-over-year comparison (if multi-year data)
trends_with_year = analytics.monthly_trends(orders, products).withColumn(
    "year", F.year(F.col("month"))
).withColumn(
    "month_num", F.month(F.col("month"))
)
# Group by month across years for comparison


# ============================================================================
# DATA VALIDATION & EXPLORATION
# ============================================================================

# Check data quality
print(f"Total orders: {orders.count()}")
print(f"Total products: {products.count()}")
print(f"Unique customers: {orders.select('customer_id').distinct().count()}")

# Show schema
orders.printSchema()
products.printSchema()

# Check for null values
null_counts = orders.select(
    *[F.count(F.when(F.col(c).isNull(), c)).alias(f"{c}_nulls") for c in orders.columns]
)
null_counts.show()

# Get summary statistics
orders.describe("quantity").show()
products.describe("price").show()

# Check data ranges
date_range = orders.agg(
    F.min("order_date").alias("earliest_order"),
    F.max("order_date").alias("latest_order")
)
date_range.show()


# ============================================================================
# EXPORT & SAVE RESULTS
# ============================================================================

# Save as Parquet (most efficient)
results = analytics.top_customers_by_revenue(orders, products)
results.write.mode("overwrite").parquet("output/results.parquet")

# Save as CSV
results.write.mode("overwrite").option("header", "true").csv("output/results.csv")

# Save as JSON
results.write.mode("overwrite").json("output/results.json")

# Save with specific format
results.coalesce(1).write.mode("overwrite") \
    .option("header", "true") \
    .csv("output/single_file")  # Creates single CSV file (not distributed)

# Append mode (for incremental updates)
results.write.mode("append").parquet("output/incremental_results")


# ============================================================================
# PERFORMANCE TIPS
# ============================================================================

# Cache frequently used DataFrames
orders.cache()
products.cache()

# Explain query plan
orders.join(products, "product_id").explain()

# Check execution time
import time
start = time.time()
result = analytics.top_customers_by_revenue(orders, products)
result.collect()  # Force execution
elapsed = time.time() - start
print(f"Execution time: {elapsed:.2f} seconds")

# Repartition for better performance on large datasets
orders_repartitioned = orders.repartition(200)

# Show Spark UI info (runs on localhost:4040 by default)
# Open browser to http://localhost:4040 during execution


# ============================================================================
# FILTERING AND AGGREGATIONS
# ============================================================================

# Filter by date range
from datetime import datetime, timedelta
recent_orders = orders.filter(
    F.col("order_date") >= F.lit("2023-06-01")
)
recent_orders.count()

# Filter by quantity range
large_orders = orders.filter(F.col("quantity") >= 5)
large_orders.count()

# Multiple filters
filtered = orders.filter(
    (F.col("quantity") > 2) & 
    (F.col("order_date") >= F.lit("2023-01-01"))
)
filtered.count()

# Filter with IN clause
product_ids = [1, 2, 3, 4, 5]
filtered_products = products.filter(F.col("product_id").isin(product_ids))
filtered_products.show()

# String filtering
electronics = products.filter(F.col("category") == "Electronics")
electronics.show()

# Regex filtering
matching_names = products.filter(F.col("product_name").rlike("^[A-M]"))
matching_names.show()


# ============================================================================
# AGGREGATIONS
# ============================================================================

# Count distinct values
unique_customers = orders.select(F.countDistinct("customer_id"))
unique_customers.show()

# Multiple aggregations
summary = orders.agg(
    F.count("*").alias("total_orders"),
    F.sum("quantity").alias("total_units"),
    F.avg("quantity").alias("avg_quantity"),
    F.min("quantity").alias("min_quantity"),
    F.max("quantity").alias("max_quantity")
)
summary.show()

# Groupby with multiple aggregations
group_summary = orders.groupBy("customer_id") \
    .agg(
        F.count("*").alias("order_count"),
        F.sum("quantity").alias("total_quantity"),
        F.avg("quantity").alias("avg_order_size")
    ).orderBy(F.desc("order_count"))
group_summary.show()

# Percentile/Quantile calculations
quantiles = orders.approxQuantile("quantity", [0.25, 0.5, 0.75], 0.01)
print(f"Q1: {quantiles[0]}, Median: {quantiles[1]}, Q3: {quantiles[2]}")


# ============================================================================
# CLEANUP
# ============================================================================

# Stop Spark session when done
analytics.spark.stop()

# Or use try-finally
try:
    # Your code here
    pass
finally:
    analytics.spark.stop()
