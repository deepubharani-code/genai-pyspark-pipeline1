import time
import os
import sys
import logging
import pandas as pd
from tqdm import tqdm
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

try:
    from codecarbon import EmissionsTracker
except ImportError:
    sys.exit("Error: 'codecarbon' module not found. Please run: pip install -r requirements.txt")

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger("benchmark")

# Suppress verbose logs from libraries
logging.getLogger("codecarbon").setLevel(logging.ERROR)
logging.getLogger("py4j").setLevel(logging.ERROR)

DATA_DIR = "data/raw"
CSV_FILE = os.path.join(DATA_DIR, "orders.csv")
PARQUET_FILE = os.path.join(DATA_DIR, "orders.parquet")

def get_spark_session():
    return SparkSession.builder \
        .appName("Benchmark") \
        .master("local[*]") \
        .config("spark.driver.memory", "4g") \
        .config("spark.ui.showConsoleProgress", "false") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark

def run_benchmark(name, func, *args):
    # Initialize tracker
    # measure_power_secs: Interval to measure power (lower is more accurate for short tasks)
    tracker = EmissionsTracker(
        project_name=name, 
        measure_power_secs=0.05, 
        save_to_file=False,
        logging_logger=logging.getLogger("codecarbon")
    )
    
    tracker.start()
    start_time = time.time()
    
    try:
        func(*args)
    except Exception as e:
        tqdm.write(f"Failed to run {name}: {e}")
        tracker.stop()
        return None
        
    duration = time.time() - start_time
    tracker.stop()
    
    # Energy in Joules (kWh * 3,600,000). Handle case where data might be missing for very fast tasks.
    if tracker.final_emissions_data:
        energy_joules = tracker.final_emissions_data.energy_consumed * 3_600_000
    else:
        tqdm.write(f"Warning: No energy data captured for {name} (task too fast).")
        energy_joules = 0.0
    
    return {
        "Method": name,
        "Time (s)": duration,
        "Energy (Joules)": energy_joules,
        "Avg Power (Watts)": energy_joules / duration if duration > 0 else 0
    }

def pandas_process(file_path, file_type):
    if file_type == 'csv':
        df = pd.read_csv(file_path)
    else:
        df = pd.read_parquet(file_path)
    
    # Aggregation: Sum quantity by product_id
    _ = df.groupby("product_id")["quantity"].sum()

def pyspark_process(spark, file_path, file_type):
    if file_type == 'csv':
        df = spark.read.csv(file_path, header=True, inferSchema=True)
    else:
        df = spark.read.parquet(file_path)
        
    # Action required to trigger computation (collect brings result to driver)
    df.groupBy("product_id").agg(F.sum("quantity")).collect()

def main():
    if not os.path.exists(CSV_FILE) or not os.path.exists(PARQUET_FILE):
        print("Data files not found. Please run generate_sample_data.py first.")
        return

    results = []
    
    with tqdm(total=5, desc="Starting Benchmark", unit="step") as pbar:
        # 1. Pandas CSV
        pbar.set_description("Benchmarking: Pandas CSV")
        res = run_benchmark("Pandas CSV", pandas_process, CSV_FILE, 'csv')
        if res: results.append(res)
        pbar.update(1)
        
        # 2. Pandas Parquet
        pbar.set_description("Benchmarking: Pandas Parquet")
        res = run_benchmark("Pandas Parquet", pandas_process, PARQUET_FILE, 'parquet')
        if res: results.append(res)
        pbar.update(1)
        
        # Spark Setup
        pbar.set_description("Initializing Spark Session")
        spark = get_spark_session()
        pbar.update(1)
        
        # 3. PySpark CSV
        pbar.set_description("Benchmarking: PySpark CSV")
        res = run_benchmark("PySpark CSV", pyspark_process, spark, CSV_FILE, 'csv')
        if res: results.append(res)
        pbar.update(1)
        
        # 4. PySpark Parquet
        pbar.set_description("Benchmarking: PySpark Parquet")
        res = run_benchmark("PySpark Parquet", pyspark_process, spark, PARQUET_FILE, 'parquet')
        if res: results.append(res)
        pbar.update(1)
        
        pbar.set_description("Benchmark Complete")
    
    spark.stop()
    
    # Display Comparison Table
    print("\n" + "="*75)
    print(f"{'Method':<20} | {'Time (s)':<10} | {'Energy (J)':<12} | {'Avg Power (W)':<12}")
    print("-" * 75)
    for r in results:
        print(f"{r['Method']:<20} | {r['Time (s)']:<10.4f} | {r['Energy (Joules)']:<12.4f} | {r['Avg Power (Watts)']:<12.4f}")
    print("="*75)

if __name__ == "__main__":
    main()