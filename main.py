import logging
import os
import time
from src import config
from src.data_generator import SyntheticDataGenerator

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

def get_file_size_mb(filepath):
    """Returns file size in megabytes."""
    return os.path.getsize(filepath) / (1024 * 1024)

def main():
    try:
        start_total = time.time()
        
        # Initialize generator
        generator = SyntheticDataGenerator()
        
        # 1. Generate Customers
        logger.info("Generating customers...")
        start_time = time.time()
        customers_df = generator.generate_customers(count=100000)
        
        customers_path = config.RAW_DATA_DIR / "customers.parquet"
        customers_df.to_parquet(customers_path, index=False)
        
        duration = time.time() - start_time
        size = get_file_size_mb(customers_path)
        logger.info(f"Customers: {len(customers_df)} records generated in {duration:.2f}s. "
                    f"Saved to {customers_path} ({size:.2f} MB)")

        # 2. Generate Products
        logger.info("Generating products...")
        start_time = time.time()
        products_df = generator.generate_products(count=10000)
        
        products_path = config.RAW_DATA_DIR / "products.parquet"
        products_df.to_parquet(products_path, index=False)
        
        duration = time.time() - start_time
        size = get_file_size_mb(products_path)
        logger.info(f"Products: {len(products_df)} records generated in {duration:.2f}s. "
                    f"Saved to {products_path} ({size:.2f} MB)")

        # 3. Generate Orders
        logger.info("Generating orders...")
        start_time = time.time()
        orders_df = generator.generate_orders(
            count=1000000,
            customers_df=customers_df,
            products_df=products_df
        )
        
        orders_path = config.RAW_DATA_DIR / "orders.parquet"
        orders_df.to_parquet(orders_path, index=False)
        
        duration = time.time() - start_time
        size = get_file_size_mb(orders_path)
        logger.info(f"Orders: {len(orders_df)} records generated in {duration:.2f}s. "
                    f"Saved to {orders_path} ({size:.2f} MB)")

        total_duration = time.time() - start_total
        logger.info(f"Total process completed successfully in {total_duration:.2f} seconds.")

    except Exception as e:
        logger.error(f"An error occurred during data generation: {e}", exc_info=True)
        raise

if __name__ == "__main__":
    main()