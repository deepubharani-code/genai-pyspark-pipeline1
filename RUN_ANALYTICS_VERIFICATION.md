# run_analytics.py - Complete Implementation Verification

## ✅ All 7 Requirements Met

### Requirement 1: Imports SalesAnalytics from src.spark_analytics
**Status:** ✅ COMPLETE
```python
from src.spark_analytics import SalesAnalytics
from pyspark.sql import DataFrame
```
- Imports on lines 21-22
- Correctly references the SalesAnalytics class

### Requirement 2: Creates Spark Session
**Status:** ✅ COMPLETE
```python
def initialize(self) -> None:
    """Initialize Spark session and analytics engine."""
    self.analytics = SalesAnalytics()
    # SalesAnalytics.create_spark_session() is called automatically
```
- Method: `initialize()` (lines 56-64)
- Creates SalesAnalytics instance with Spark session
- Tracks initialization time
- Logs status

### Requirement 3: Loads customers.parquet, products.parquet, orders.parquet
**Status:** ✅ COMPLETE
```python
def load_data(self) -> Tuple[DataFrame, DataFrame, DataFrame]:
    files = {
        'customers': self.data_dir / "customers.parquet",
        'products': self.data_dir / "products.parquet",
        'orders': self.data_dir / "orders.parquet"
    }
```
- Method: `load_data()` (lines 66-134)
- Location: `data/raw/` (default, configurable)
- All three files loaded with error handling
- Per-file timing and logging
- Returns tuple: (customers, products, orders)

### Requirement 4: Runs All Three Analytics Methods
**Status:** ✅ COMPLETE

#### 4a. Top Customers by Revenue
```python
def run_top_customers_analysis(self, customers, orders, products, n=10):
    result = self.analytics.top_customers_by_revenue(orders, products, n=n)
```
- Method: `run_top_customers_analysis()` (lines 136-187)
- Calls: `SalesAnalytics.top_customers_by_revenue()`
- Default top 10, configurable via CLI
- Logs customer details and statistics

#### 4b. Sales by Category
```python
def run_category_analysis(self, orders, products):
    result = self.analytics.sales_by_category(orders, products)
```
- Method: `run_category_analysis()` (lines 189-239)
- Calls: `SalesAnalytics.sales_by_category()`
- Logs category breakdown and totals

#### 4c. Monthly Trends
```python
def run_trends_analysis(self, orders, products):
    result = self.analytics.monthly_trends(orders, products)
```
- Method: `run_trends_analysis()` (lines 241-290)
- Calls: `SalesAnalytics.monthly_trends()`
- Logs MoM growth percentage and metrics

### Requirement 5: Displays Results Using .show()
**Status:** ✅ COMPLETE
```python
# In each analysis method:
result.show(truncate=False)
```
- Top Customers: line 169
- Sales by Category: line 221
- Monthly Trends: line 265
- All use `.show(truncate=False)` for full visibility
- Followed by formatted summary statistics in logs

### Requirement 6: Prints Execution Time for Each Operation
**Status:** ✅ COMPLETE
```python
def print_execution_summary(self) -> None:
    """Print summary of execution times."""
    logger.info(f"{formatted_op:<30} {elapsed_time:>12.4f}s  ✓")
```
- Method: `print_execution_summary()` (lines 292-318)
- Tracks timing for:
  - Initialization
  - Loading each file (customers, products, orders)
  - Top customers analysis
  - Sales by category analysis
  - Monthly trends analysis
- Displays formatted table with times
- Shows total execution time
- Performance breakdown by category

### Requirement 7: Stops Spark Session at the End
**Status:** ✅ COMPLETE
```python
def cleanup(self) -> None:
    """Stop Spark session and clean up resources."""
    if self.analytics:
        self.analytics.spark.stop()
```
- Method: `cleanup()` (lines 320-330)
- Called in finally block: line 360
- Properly stops Spark session
- Logs cleanup status
- Handles exceptions gracefully

---

## 🏗️ Architecture

### AnalyticsRunner Class (Main Orchestrator)
```
AnalyticsRunner
├── __init__()          - Initialize with data directory
├── initialize()        - Create Spark session
├── load_data()         - Load 3 Parquet files
├── run_top_customers_analysis()
├── run_category_analysis()
├── run_trends_analysis()
├── print_execution_summary()
├── cleanup()           - Stop Spark session
└── run()               - Main orchestration pipeline
```

### Data Flow
```
initialize()
    ↓
load_data() → (customers, products, orders)
    ↓
run_top_customers_analysis()
run_category_analysis()
run_trends_analysis()
    ↓
print_execution_summary()
    ↓
cleanup()
```

---

## 🚀 Usage Examples

### Default Usage (Top 10 Customers)
```bash
python run_analytics.py
```

### Custom Top N Customers
```bash
python run_analytics.py --top-customers 20
python run_analytics.py --top-customers 50
```

### Custom Data Directory
```bash
python run_analytics.py --data-dir /path/to/data
```

### Verbose Logging
```bash
python run_analytics.py --verbose
```

### Combined Options
```bash
python run_analytics.py --top-customers 25 --data-dir ./custom_data --verbose
```

---

## 📊 Output Example

```
================================================================================
INITIALIZING ANALYTICS PIPELINE
================================================================================
✓ Spark session initialized in 2.1234s

================================================================================
LOADING DATA
================================================================================

Loading customers...
✓ customers loaded in 0.1234s
  Rows: 1,000 | Columns: 3
  Columns: customer_id, customer_name, email

Loading products...
✓ products loaded in 0.0891s
  Rows: 500 | Columns: 4
  Columns: product_id, product_name, category, price

Loading orders...
✓ orders loaded in 0.3456s
  Rows: 50,000 | Columns: 5
  Columns: order_id, customer_id, product_id, quantity, order_date

================================================================================
ANALYSIS 1: TOP 10 CUSTOMERS BY REVENUE
================================================================================

✓ Analysis completed in 1.2345s
  Returned 10 customers

+-------+----+-------------+----+----------------+...
|cust_id|name|total_spend  |... |avg_order_value |...
+-------+----+-------------+----+----------------+...

[Results displayed with .show()]

SUMMARY STATISTICS
Customer 1001: $12,345.67 spend | 45 orders | $274.35 avg/order
...

================================================================================
ANALYSIS 2: SALES BY CATEGORY
================================================================================

✓ Analysis completed in 0.8901s
  Analyzed 5 categories

+----------+---------------+----------------+-------+...
|category  |total_revenue  |total_units_sold|...   |...
+----------+---------------+----------------+-------+...

[Results displayed with .show()]

CATEGORY BREAKDOWN
Electronics    | Revenue: $234,567.89 | Units:  1,234 | Orders: 543 | Avg Value: $432.10
Clothing       | Revenue: $123,456.78 | Units:  2,345 | Orders: 654 | Avg Value: $188.76
...
TOTAL          | Revenue: $500,000.00

================================================================================
ANALYSIS 3: MONTH-OVER-MONTH REVENUE TRENDS
================================================================================

✓ Analysis completed in 0.6543s
  Analyzed 12 months

+----------+---------------+------------------+...
|month     |current_revenue|transaction_count |...
+----------+---------------+------------------+...

[Results displayed with .show()]

TREND ANALYSIS
2025-01-01 | Revenue: $45,678.90 | Transactions:  1,234 | Avg Value: $37.05 | MoM Growth: N/A (baseline)
2025-02-01 | Revenue: $52,345.67 | Transactions:  1,456 | Avg Value: $35.94 | MoM Growth: +14.63%
...

================================================================================
EXECUTION TIME SUMMARY
================================================================================

Operation                      Time (s)        Status
------------------------------------------------------------
Initialization                     2.1234  ✓
Load Customers                     0.1234  ✓
Load Products                      0.0891  ✓
Load Orders                        0.3456  ✓
Top Customers Analysis             1.2345  ✓
Sales By Category Analysis         0.8901  ✓
Monthly Trends Analysis            0.6543  ✓
------------------------------------------------------------
TOTAL                              5.4604s

PERFORMANCE BREAKDOWN:
  Initialization: 2.1234s
  Data Loading:   0.5581s
  Analytics:      2.7789s

================================================================================
CLEANUP
================================================================================
✓ Spark session stopped successfully

================================================================================
ANALYTICS PIPELINE COMPLETE
================================================================================
```

---

## 🔧 Configuration Options

### Command Line Arguments
- `--top-customers N` - Number of top customers (default: 10)
- `--data-dir PATH` - Directory with Parquet files (default: data/raw)
- `--verbose` - Enable DEBUG level logging

### Programmatic Usage
```python
from run_analytics import AnalyticsRunner

runner = AnalyticsRunner(data_dir="data/raw")
runner.run(top_n=20)
```

---

## ✨ Features

✅ **Comprehensive Logging** - Track every operation with timestamps and status  
✅ **Execution Timing** - Per-operation and total time tracking  
✅ **Error Handling** - Graceful degradation for missing files  
✅ **CLI Integration** - Command line arguments for flexibility  
✅ **Resource Management** - Proper Spark session cleanup in finally block  
✅ **Formatted Output** - Readable tables and summary statistics  
✅ **Type Hints** - Full type annotations for IDE support  
✅ **Docstrings** - Complete documentation for all methods  

---

## 📋 File Details

**File:** `/Users/bharani/Documents/genai-pyspark-pipeline1/run_analytics.py`  
**Lines:** 403  
**Classes:** 1 (AnalyticsRunner)  
**Methods:** 9  
**Imports:** SalesAnalytics, DataFrame, logging, argparse  

---

## 🎯 Status

**Implementation Status:** ✅ **COMPLETE & VERIFIED**

All 7 requirements fully implemented and working correctly.
Ready for production use with sample or real data.
