"""
Spark Configuration Comparison: 8GB vs 16GB vs 32GB Laptops

Visual reference for optimal settings across different hardware configurations.
"""

# ============================================================================
# CONFIGURATION COMPARISON TABLE
# ============================================================================

COMPARISON_TABLE = """

╔════════════════════════════════════════════════════════════════════════════════╗
║                  SPARK CONFIGURATION BY LAPTOP SPECS                           ║
╚════════════════════════════════════════════════════════════════════════════════╝


┌─────────────────────────────────────┬──────────────┬──────────────┬──────────────┐
│ SETTING                             │   8GB RAM    │   16GB RAM   │   32GB RAM   │
│                                     │   4 Cores    │   8 Cores    │  16 Cores    │
├─────────────────────────────────────┼──────────────┼──────────────┼──────────────┤
│ spark.driver.memory                 │     2g       │     6g       │    12g       │
├─────────────────────────────────────┼──────────────┼──────────────┼──────────────┤
│ spark.executor.memory               │     2g       │     6g       │    12g       │
├─────────────────────────────────────┼──────────────┼──────────────┼──────────────┤
│ spark.sql.shuffle.partitions        │     32       │     64       │    128       │
│ (cores × 8)                         │   (4×8)      │   (8×8)      │  (16×8)      │
├─────────────────────────────────────┼──────────────┼──────────────┼──────────────┤
│ spark.sql.adaptive.enabled          │    true      │    true      │    true      │
├─────────────────────────────────────┼──────────────┼──────────────┼──────────────┤
│ spark.serializer                    │   Kryo       │   Kryo       │   Kryo       │
├─────────────────────────────────────┼──────────────┼──────────────┼──────────────┤
│ spark.kryoserializer.buffer.max     │    256m      │    512m      │     1g       │
├─────────────────────────────────────┼──────────────┼──────────────┼──────────────┤
│ spark.sql.adaptive.coalescePartitions│   true      │    true      │    true      │
├─────────────────────────────────────┼──────────────┼──────────────┼──────────────┤
│ spark.sql.autoBroadcastJoinThreshold│     64mb     │    128mb     │    256mb     │
└─────────────────────────────────────┴──────────────┴──────────────┴──────────────┘


MEMORY BREAKDOWN:

8GB LAPTOP (4 Cores):
├─ System/OS:            2.0GB ████████████████████
├─ Spark Driver:         2.0GB ████████████████████
├─ Spark Executor:       2.0GB ████████████████████
├─ Buffer/Overhead:      2.0GB ████████████████████
└─ TOTAL:              8.0GB

16GB LAPTOP (8 Cores):  ← RECOMMENDED FOR 1M ROWS
├─ System/OS:            4.0GB ████████████████████
├─ Spark Driver:         6.0GB ██████████████████████████████
├─ Spark Executor:       6.0GB ██████████████████████████████
└─ TOTAL:              16.0GB

32GB LAPTOP (16 Cores):
├─ System/OS:            8.0GB ████████████████████
├─ Spark Driver:        12.0GB ██████████████████████████████████████████
├─ Spark Executor:      12.0GB ██████████████████████████████████████████
└─ TOTAL:              32.0GB


DATASET SIZE RECOMMENDATIONS:

Dataset Size    │ RAM      │ Cores │ Partitions │ Configuration
────────────────┼──────────┼───────┼────────────┼─────────────────────────
Small (100K)    │ ≥ 4GB    │ ≥ 2   │    8-16    │ Lightweight (3g+3g)
Medium (1M)     │ ≥ 16GB   │ ≥ 8   │    64      │ Optimized (6g+6g)
Large (10M+)    │ ≥ 32GB   │ ≥ 16  │    128+    │ High-Memory (12g+12g)
Massive (100M+) │ CLUSTER  │ CLUSTER│  256-512   │ Distributed setup


PERFORMANCE EXPECTATIONS:

Configuration           │ 1M Rows   │ Join Time │ GroupBy Time │ Memory Usage
────────────────────────┼───────────┼───────────┼──────────────┼─────────────
8GB (2g+2g, 32 part)    │ 3-5s      │ 500-800ms │ 300-500ms    │ ~3-4GB
16GB (6g+6g, 64 part)   │ 1-2s      │ 200-400ms │ 100-200ms    │ ~6-8GB ✓
32GB (12g+12g, 128 part)│ 0.5-1s    │ 100-200ms │ 50-100ms     │ ~12-16GB


SHUFFLE PARTITIONS EXPLAINED:

Formula: num_partitions = num_cores × 8

WHY × 8?
- 1 task per partition
- 8 tasks per core = good parallelism without overhead
- Balances task granularity with scheduling overhead

8 Cores:
├─ × 1 = 8 partitions   → Too coarse (low parallelism)
├─ × 4 = 32 partitions  → Baseline (light workloads)
├─ × 8 = 64 partitions  │ OPTIMAL (balanced)
├─ × 10 = 80 partitions │ Good for complex queries
├─ × 16 = 128 partitions → For data with skew
└─ × 25 = 200 partitions → Too fine (overhead > gains)

For 1M row groupBy:

8 partitions:    125,000 rows/partition (underutilized cores)
64 partitions:   15,625 rows/partition (OPTIMAL)
200 partitions:  5,000 rows/partition (too many, overhead dominates)


MEMORY PER PARTITION:

1M rows with 64 partitions:
├─ Rows per partition: 15,625 rows
├─ Bytes per row: ~512 bytes (typ)
├─ Memory per partition: ~8MB
├─ Safe margin: Partitions can grow 2-5x during shuffle
├─ Final per partition: ~40-80MB
├─ Total with 64: ~2.5GB ✓ (well within 6GB executor memory)

Calculation:
1,000,000 rows ÷ 64 partitions = 15,625 rows/partition
15,625 rows × 512 bytes = ~8MB baseline
× 4-8x (shuffle intermediate data) = 32-64MB final

With 6GB executor memory:
6,000MB ÷ 64 partitions = ~93MB per partition maximum ✓ SAFE


KRYO SERIALIZATION BUFFER:

Default (64MB):
└─ Works for: small objects, primitive types
└─ Fails for: serializing 1M row DataFrame (~500-800MB)
└─ Solution: Increase to 512MB or 1GB

512MB (recommended for 16GB):
├─ Fits 1M rows without resizing buffer
├─ Covers intermediate shuffle data
└─ No performance penalty

1GB (recommended for 32GB):
├─ Handles 10M+ rows
├─ Extra buffer for complex objects
└─ Not necessary unless you have large UDFs


BROADCAST JOIN THRESHOLD:

Default (10MB):
└─ Only tables < 10MB get broadcast
└─ Orders ⨝ Products: products (~5MB) not broadcast
└─ Results in slow shuffle join

Optimized (128MB for 16GB):
├─ Tables < 128MB get broadcast
├─ Products table (~5MB) gets broadcast
├─ Each executor gets copy (~5MB × 8 executors = 40MB)
├─ 40MB << 16GB system RAM ✓ SAFE
└─ Join performance: 25x FASTER

High-memory (256MB for 32GB):
├─ Can broadcast even larger lookup tables
└─ Ideal for customer/product master data


ADAPTIVE QUERY EXECUTION (AQE) BENEFITS:

Query: SELECT category, SUM(revenue) FROM orders GROUP BY category

WITHOUT AQE (using 64 partitions initially):
Stage 1: Read 1M orders → shuffle to 64 partitions
Stage 2: GroupBy on 64 partitions
├─ Reality: Only 8 categories exist
├─ Result: 56 partitions nearly empty, 8 partitions have data
└─ Tasks executed: 64 (many tiny, inefficient)

WITH AQE:
Stage 1: Read 1M orders → shuffle to 64 partitions
[Runtime statistics collected]
Stage 2 (REPLANNED):
├─ Detects: Only 8 unique values
├─ Action: Coalesce 64 → 8 partitions
├─ Replan: GroupBy on 8 partitions
└─ Tasks executed: 8 (balanced, efficient)

IMPACT:
✓ 64 → 8 partitions (87.5% reduction in tasks)
✓ Reduced scheduling overhead
✓ Better CPU cache utilization
✓ Faster execution (typically 5-20%)


CONFIGURATION GENERATION FORMULA:

For any laptop:

1. RAM: system_ram - 4GB (OS) = available_for_spark
2. Split: available_for_spark ÷ 2 = driver_memory = executor_memory
3. Partitions: num_cores × 8
4. Kryo Buffer: driver_memory × 10% (round to 256m/512m/1g)
5. Broadcast: min(available_for_spark × 50%, executor_memory × 20%)

EXAMPLE (16GB, 8 cores):
1. 16GB - 4GB = 12GB available
2. 12GB ÷ 2 = 6GB driver, 6GB executor ✓
3. 8 × 8 = 64 partitions ✓
4. 6GB × 10% = 600MB → round to 512m ✓
5. min(12GB × 50%, 6GB × 20%) = min(6GB, 1.2GB) = 1.2GB → 128mb reasonable ✓


WHEN TO TUNE FURTHER:

Monitor these metrics and adjust:

Metric                          │ Problem Signal           │ Adjustment
────────────────────────────────┼──────────────────────────┼──────────────────
Executor memory usage > 80%     │ Memory pressure          │ Increase memory
GC time > 10% of task time      │ Garbage collection       │ Increase memory
Partitions with <1000 rows      │ Many tiny partitions     │ Lower partitions
Task time variance > 2x         │ Data skew                │ AQE handles auto
Shuffle bytes spilled to disk   │ Spill overhead           │ Increase partitions
Broadcast timeout errors        │ Buffer too small         │ Increase buffer

"""

# ============================================================================
# COPY-PASTE CONFIGURATIONS
# ============================================================================

FULL_CONFIG_SNIPPETS = {
    "8GB_4CORE": '''
# For 8GB laptop with 4 cores
spark = SparkSession.builder \\
    .appName("EcommerceLightweight") \\
    .config("spark.driver.memory", "2g") \\
    .config("spark.executor.memory", "2g") \\
    .config("spark.sql.shuffle.partitions", "32") \\
    .config("spark.sql.adaptive.enabled", "true") \\
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \\
    .config("spark.kryoserializer.buffer.max", "256m") \\
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \\
    .config("spark.sql.autoBroadcastJoinThreshold", "64mb") \\
    .getOrCreate()
    ''',
    
    "16GB_8CORE": '''
# For 16GB laptop with 8 cores (RECOMMENDED)
spark = SparkSession.builder \\
    .appName("EcommerceOptimized") \\
    .config("spark.driver.memory", "6g") \\
    .config("spark.executor.memory", "6g") \\
    .config("spark.sql.shuffle.partitions", "64") \\
    .config("spark.sql.adaptive.enabled", "true") \\
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \\
    .config("spark.kryoserializer.buffer.max", "512m") \\
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \\
    .config("spark.sql.autoBroadcastJoinThreshold", "128mb") \\
    .getOrCreate()
    ''',
    
    "32GB_16CORE": '''
# For 32GB laptop with 16 cores
spark = SparkSession.builder \\
    .appName("EcommerceHighMemory") \\
    .config("spark.driver.memory", "12g") \\
    .config("spark.executor.memory", "12g") \\
    .config("spark.sql.shuffle.partitions", "128") \\
    .config("spark.sql.adaptive.enabled", "true") \\
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \\
    .config("spark.kryoserializer.buffer.max", "1g") \\
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \\
    .config("spark.sql.autoBroadcastJoinThreshold", "256mb") \\
    .getOrCreate()
    '''
}


# ============================================================================
# PRINT FUNCTION
# ============================================================================

def print_comparison():
    """Print the comparison table and guidance."""
    print(COMPARISON_TABLE)
    print("\n" + "="*80)
    print("COPY-PASTE CONFIGURATIONS")
    print("="*80 + "\n")
    
    for name, code in FULL_CONFIG_SNIPPETS.items():
        print(f"\n{name}:")
        print("─" * 80)
        print(code)


if __name__ == "__main__":
    print_comparison()
