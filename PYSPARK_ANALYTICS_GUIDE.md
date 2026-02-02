# PySpark Sales Analytics Class Documentation

## Overview

The `SalesAnalytics` class provides a comprehensive analytics engine for processing and analyzing sales data using Apache PySpark. It's designed for scalable, distributed data processing with optimized performance configurations.

## Features

✅ **Optimized Spark Configuration**
- 4GB driver memory for handling large datasets
- Adaptive Query Execution (AQE) for automatic optimization
- Kryo serialization for 2-10x better performance than default Java serialization
- Automatic broadcast join optimization (128MB threshold)

✅ **Advanced Analytics Methods**
- Top customers by revenue with order count and average order value
- Sales analysis by product category
- Month-over-month revenue growth trends using Window functions

✅ **Production-Ready**
- Comprehensive type hints for IDE support
- Detailed docstrings with examples
- Error handling and logging
- Local mode support for development/testing

---

## Class: `SalesAnalytics`

### Initialization

```python
from src.spark_analytics import SalesAnalytics

# Create an instance
analytics = SalesAnalytics(app_name="SalesAnalytics")
```

**Parameters:**
- `app_name` (str, optional): Spark application name. Defaults to "SalesAnalytics"

### Configuration Details

The `create_spark_session()` method configures Spark with these settings:

| Configuration | Value | Purpose |
|--------------|-------|---------|
| `master` | `local[*]` | Uses all available CPU cores |
| `driver.memory` | `4g` | Allocates 4GB to driver process |
| `sql.adaptive.enabled` | `true` | Enables AQE for automatic optimization |
| `sql.adaptive.coalescePartitions.enabled` | `true` | Coalesces small partitions |
| `sql.adaptive.skewJoin.enabled` | `true` | Handles skewed joins automatically |
| `serializer` | `KryoSerializer` | More efficient than Java serialization |
| `kryoserializer.buffer.max` | `512m` | Kryo buffer size for large objects |
| `sql.autoBroadcastJoinThreshold` | `128mb` | Automatic broadcast join threshold |
| `sql.shuffle.partitions` | `200` | Default number of partitions |
| `executor.cores` | `4` | CPU cores per executor |

---

## Methods

### 1. `load_parquet(path: str) -> DataFrame`

Loads a Parquet file into a Spark DataFrame.

**Why Parquet?**
- Columnar storage format (compress better than row-based)
- Schema inference
- Predicate pushdown for faster filtering
- Supports compression (snappy, gzip, etc.)

**Parameters:**
- `path` (str): File path to the parquet file

**Returns:**
- `DataFrame`: Loaded Spark DataFrame

**Example:**
```python
orders_df = analytics.load_parquet("data/orders.parquet")
products_df = analytics.load_parquet("data/products.parquet")
```

**Output:**
```
Loading Parquet file: data/orders.parquet
✓ Loaded 500,000 rows and 4 columns
```

---

### 2. `top_customers_by_revenue(orders_df, products_df, n=10) -> DataFrame`

Calculate total spend per customer and return top N customers.

**Process:**
1. Join orders with products on `product_id` (inner join)
2. Calculate revenue per order: `price × quantity`
3. Group by `customer_id` and aggregate:
   - Sum of revenue (`total_spend`)
   - Count of orders (`order_count`)
   - Average order value (`avg_order_value`)
4. Sort by `total_spend` descending
5. Limit to top N results

**Parameters:**
- `orders_df` (DataFrame): Orders data with columns [customer_id, product_id, quantity, order_date, ...]
- `products_df` (DataFrame): Products data with columns [product_id, price, category, ...]
- `n` (int, optional): Number of top customers to return. Defaults to 10

**Returns:**
- `DataFrame` with columns: [customer_id, total_spend, order_count, avg_order_value]

**Example:**
```python
top_10 = analytics.top_customers_by_revenue(orders_df, products_df, n=10)
top_10.show()
```

**Output:**
```
+-----------+------------------+-----------+------------------+
|customer_id|total_spend       |order_count|avg_order_value   |
+-----------+------------------+-----------+------------------+
|1024       |45678.90          |25         |1827.16           |
|2567       |42345.67          |22         |1924.80           |
|3891       |38920.34          |20         |1946.02           |
...
```

---

### 3. `sales_by_category(orders_df, products_df) -> DataFrame`

Group sales by product category and calculate aggregated metrics.

**Process:**
1. Join orders with products on `product_id` (inner join)
2. Calculate revenue: `price × quantity`
3. Group by `category` and aggregate:
   - Sum of revenue (`total_revenue`)
   - Sum of quantity (`total_units_sold`)
   - Count of orders (`order_count`)
   - Average order value (`avg_order_value`)
   - Average units per order (`avg_units_per_order`)
4. Sort by `total_revenue` descending

**Parameters:**
- `orders_df` (DataFrame): Orders data with columns [product_id, quantity, order_date, ...]
- `products_df` (DataFrame): Products data with columns [product_id, category, price, ...]

**Returns:**
- `DataFrame` with columns: [category, total_revenue, total_units_sold, order_count, avg_order_value, avg_units_per_order]

**Example:**
```python
category_sales = analytics.sales_by_category(orders_df, products_df)
category_sales.show()
```

**Output:**
```
+----------+---------------+------------------+-----------+------------------+------------------+
|category  |total_revenue  |total_units_sold  |order_count|avg_order_value   |avg_units_per_orde|
+----------+---------------+------------------+-----------+------------------+------------------+
|Electronics|1234567.89    |5600               |1200       |1028.81           |4.67              |
|Clothing  |987654.32      |8900               |950        |1039.63           |9.37              |
|Books     |456789.12      |3400               |800        |571.24            |4.25              |
+----------+---------------+------------------+-----------+------------------+------------------+
```

---

### 4. `monthly_trends(orders_df, products_df) -> DataFrame`

Calculate month-over-month (MoM) revenue growth percentage using Window functions.

**Window Function Details:**
- Uses `LAG()` function to get previous month's revenue
- Partitioned by time (ordered by month)
- Calculates: `(current_revenue - prev_revenue) / prev_revenue × 100`

**Process:**
1. Join orders with products
2. Calculate revenue: `price × quantity`
3. Extract month from `order_date` using `TRUNC(order_date, 'month')`
4. Group by month and aggregate:
   - Sum of revenue (`current_revenue`)
   - Count of transactions (`transaction_count`)
   - Average transaction value (`avg_transaction_value`)
5. Use LAG window function to get `prev_revenue`
6. Calculate MoM growth percentage
7. Sort by month ascending

**Parameters:**
- `orders_df` (DataFrame): Orders data with [product_id, quantity, order_date, ...]
- `products_df` (DataFrame): Products data with [product_id, price, ...]

**Returns:**
- `DataFrame` with columns: [month, current_revenue, transaction_count, avg_transaction_value, prev_revenue, mom_growth_pct]

**Example:**
```python
trends = analytics.monthly_trends(orders_df, products_df)
trends.show()
```

**Output:**
```
+----------+----------------+------------------+---------------------+-----------------+----------------+
|month     |current_revenue |transaction_count |avg_transaction_value|prev_revenue     |mom_growth_pct  |
+----------+----------------+------------------+---------------------+-----------------+----------------+
|2023-01-01|88210.35        |155               |569.10               |NULL             |NULL            |
|2023-02-01|76962.80        |140               |549.73               |88210.35         |-12.75          |
|2023-03-01|88707.35        |155               |572.31               |76962.80         |+15.26          |
|2023-04-01|95234.50        |160               |595.22               |88707.35         |+7.41           |
+----------+----------------+------------------+---------------------+-----------------+----------------+
```

**Interpreting Results:**
- `NULL` growth for the first month (no previous data)
- Positive percentage = revenue increased from previous month
- Negative percentage = revenue decreased from previous month
- Example: `-12.75%` means February revenue was 12.75% lower than January

---

### 5. `run() -> None`

Execute the complete analytics pipeline end-to-end.

**Workflow:**
1. Loads orders and products from parquet files
2. Displays data schemas
3. Runs all three analyses (top customers, category sales, monthly trends)
4. Displays results using `DataFrame.show()`
5. Saves monthly trends to CSV in `PROCESSED_DATA_DIR/monthly_trends/`
6. Properly stops Spark session

**Example:**
```python
analytics = SalesAnalytics()
analytics.run()
```

**Output:**
```
================================================================================
ANALYTICS PIPELINE: Starting
================================================================================

📊 Loading data...
Loading Parquet file: /path/to/orders.parquet
✓ Loaded 500,000 rows and 4 columns
...
================================================================================
TOP 10 CUSTOMERS BY REVENUE
================================================================================
[Results displayed]

================================================================================
SALES BY CATEGORY
================================================================================
[Results displayed]

================================================================================
MONTH-OVER-MONTH REVENUE TRENDS
================================================================================
[Results displayed]

💾 Saving results...
✓ Monthly trends saved to: /path/to/processed/monthly_trends

================================================================================
✅ ANALYTICS PIPELINE: Completed Successfully
================================================================================
```

---

## Data Requirements

### Expected DataFrame Schemas

**Orders DataFrame:**
```
root
 |-- order_id: integer (nullable = true)
 |-- customer_id: integer (nullable = true)
 |-- product_id: integer (nullable = true)
 |-- quantity: integer (nullable = true)
 |-- order_date: timestamp (nullable = true)
```

**Products DataFrame:**
```
root
 |-- product_id: integer (nullable = true)
 |-- product_name: string (nullable = true)
 |-- category: string (nullable = true)
 |-- price: double (nullable = true)
```

---

## Performance Optimizations

### Kryo Serialization
```python
.config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
```
- **Benefit**: 2-10x faster serialization than Java serialization
- **Use case**: Large data transfers between nodes, shuffles
- **Trade-off**: Requires class registration for some types (handled automatically for DataFrames)

### Adaptive Query Execution (AQE)
```python
.config("spark.sql.adaptive.enabled", "true")
```
- **Benefits**:
  - Dynamic partition coalescing (reduces small partitions)
  - Skew join optimization (handles data skew)
  - Automatic join strategy selection (broadcast vs. sort-merge)
- **Impact**: Can provide 5-20% performance improvement on complex queries

### Broadcast Join Threshold
```python
.config("spark.sql.autoBroadcastJoinThreshold", "128mb")
```
- **Purpose**: Automatically broadcasts small DataFrames (< 128MB) to workers
- **Benefit**: Eliminates expensive shuffle operation
- **Example**: Joining a 500M row table with a 10M row table broadcasts the smaller one

### Driver Memory
```python
.config("spark.driver.memory", "4g")
```
- **Purpose**: Allocates 4GB to driver process for result collection and aggregations
- **Consider increasing if**: Results are very large or complex aggregations

---

## Usage Examples

### Example 1: Basic Usage

```python
from src.spark_analytics import SalesAnalytics

# Initialize
analytics = SalesAnalytics()

# Load data
orders = analytics.load_parquet("data/orders.parquet")
products = analytics.load_parquet("data/products.parquet")

# Analyze top customers
top_customers = analytics.top_customers_by_revenue(orders, products, n=20)
top_customers.show()

# Get category insights
category_sales = analytics.sales_by_category(orders, products)
category_sales.show()

# Monitor trends
trends = analytics.monthly_trends(orders, products)
trends.show()
```

### Example 2: Custom Filtering

```python
# Filter for specific customer
customer_df = analytics.top_customers_by_revenue(orders, products, n=100)
customer_df.filter(F.col("total_spend") > 10000).show()

# Filter for specific category
category_df = analytics.sales_by_category(orders, products)
category_df.filter(F.col("category") == "Electronics").show()
```

### Example 3: Exporting Results

```python
# Save to different formats
top_customers = analytics.top_customers_by_revenue(orders, products, n=10)

# CSV
top_customers.write.mode("overwrite").option("header", "true").csv("output/top_customers_csv")

# Parquet (more efficient)
top_customers.write.mode("overwrite").parquet("output/top_customers_parquet")

# JSON
top_customers.write.mode("overwrite").json("output/top_customers_json")
```

### Example 4: Creating Custom Analyses

```python
# Build on the existing data joins
joined_df = orders.join(products, "product_id")

# Add custom calculations
custom_analysis = joined_df.withColumn(
    "revenue", F.col("price") * F.col("quantity")
).withColumn(
    "profit", F.col("revenue") * 0.3  # Assuming 30% profit margin
).groupBy("customer_id") \
 .agg(F.sum("profit").alias("total_profit"))

custom_analysis.show()
```

---

## Common Errors & Solutions

### Error: `Illegal Parquet type: INT64 (TIMESTAMP(NANOS,false))`
**Cause:** Parquet file uses nanosecond precision; Spark expects microseconds
**Solution:** Regenerate parquet files with microsecond timestamps or use `spark.sql.parquet.int96RebaseModeInRead`

### Error: `No Partition Defined for Window operation`
**Cause:** Window function spans entire dataset (expected in single-partition scenarios)
**Solution:** This is a warning, not an error. For large datasets, add partitioning:
```python
window_spec = Window.partitionBy("region").orderBy("month")
```

### Error: `OutOfMemoryError: Java heap space`
**Cause:** Data larger than 4GB driver memory
**Solution:** Increase driver memory (if possible) or process data in smaller batches:
```python
.config("spark.driver.memory", "8g")  # Increase from 4g to 8g
```

---

## Testing & Example Scripts

Run the demonstration script with sample data:

```bash
python spark_analytics_example.py
```

This will:
1. Create sample orders and products DataFrames in-memory
2. Run all three analytics methods
3. Display formatted results
4. Clean up resources

---

## Performance Benchmarks

Expected performance on 500,000 rows of sample data:

| Operation | Time | Notes |
|-----------|------|-------|
| Load parquet | 0.5s | Depends on I/O |
| Top 10 customers | 0.2s | Fast aggregation |
| Sales by category | 0.15s | Small output size |
| Monthly trends | 0.3s | Window function adds overhead |
| **Total pipeline** | **~1.5s** | End-to-end run |

---

## Best Practices

1. **Always use Parquet for storage** - Better compression and performance than CSV
2. **Filter early** - Apply filters before joins when possible
3. **Cache intermediate results** - For multi-step analyses:
   ```python
   joined_df = orders.join(products, "product_id").cache()
   ```
4. **Monitor executor logs** - Check Spark UI at `localhost:4040` during execution
5. **Use partitioned output** - For large results, partition on meaningful columns:
   ```python
   results.write.mode("overwrite").partitionBy("month").parquet("output/")
   ```

---

## References

- [Apache Spark Documentation](https://spark.apache.org/docs/latest/api/python/)
- [Spark SQL Functions](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/functions.html)
- [Window Functions Guide](https://spark.apache.org/docs/latest/sql-ref-window-frame.html)
- [Kryo Serialization](https://spark.apache.org/docs/latest/tuning.html#serialization)
