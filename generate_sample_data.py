import pandas as pd
import numpy as np
import os
import time

# Ensure data directory exists
os.makedirs('data/raw', exist_ok=True)

# Configuration
np.random.seed(42)
n_rows = 100_000
n_products = 10_000
n_customers = 5_000

print(f"Generating {n_customers} customers...")
customers_pd = pd.DataFrame({
    'customer_id': range(1, n_customers + 1),
    'name': [f'Customer_{i}' for i in range(1, n_customers + 1)],
    'email': [f'customer{i}@example.com' for i in range(1, n_customers + 1)]
})

print(f"Generating {n_products} products...")
products_pd = pd.DataFrame({
    'product_id': range(1, n_products + 1),
    'product_name': [f'Product_{i}' for i in range(1, n_products + 1)],
    'category': np.random.choice(['Electronics', 'Clothing', 'Books'], n_products),
    'price': np.round(np.random.uniform(10, 500, n_products), 2)
})

print(f"Generating {n_rows} orders...")
# Generate random dates within the last year
start_date = pd.Timestamp('2023-01-01').value
end_date = pd.Timestamp('2024-01-01').value
random_dates = pd.to_datetime(np.random.randint(start_date, end_date, n_rows))

orders_pd = pd.DataFrame({
    'order_id': range(1, n_rows + 1),
    'customer_id': np.random.randint(1, n_customers + 1, n_rows),
    'product_id': np.random.randint(1, n_products + 1, n_rows),
    'quantity': np.random.randint(1, 10, n_rows),
    'order_date': random_dates
})

print(f"\n--- Benchmarking Parquet vs CSV (Orders Data: {n_rows} rows) ---")

# Parquet Benchmark
start = time.time()
orders_pd.to_parquet('data/raw/orders.parquet', index=False)
pq_time = time.time() - start
pq_size = os.path.getsize('data/raw/orders.parquet') / (1024 * 1024)
print(f"Parquet: {pq_time:.4f}s, {pq_size:.2f} MB")

# CSV Benchmark
start = time.time()
orders_pd.to_csv('data/raw/orders.csv', index=False)
csv_time = time.time() - start
csv_size = os.path.getsize('data/raw/orders.csv') / (1024 * 1024)
print(f"CSV:     {csv_time:.4f}s, {csv_size:.2f} MB")
print(f"Comparison: Parquet is {csv_size / pq_size:.2f}x smaller than CSV")

# Save others as Parquet for pipeline compatibility
customers_pd.to_parquet('data/raw/customers.parquet', index=False)
products_pd.to_parquet('data/raw/products.parquet', index=False)

print("Sample data generated successfully.")