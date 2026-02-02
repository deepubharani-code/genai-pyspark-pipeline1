import logging
import random
import uuid
from typing import Optional

import numpy as np
import pandas as pd
from faker import Faker
from tqdm import tqdm
from src.config import (
    RAW_DATA_DIR,
    RANDOM_SEED,
    CUSTOMERS_FILE,
    PRODUCTS_FILE,
    ORDERS_FILE
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

class SyntheticDataGenerator:
    """
    Generates synthetic e-commerce data using Faker and NumPy.
    """

    def __init__(self, seed: int = RANDOM_SEED):
        self.faker = Faker()
        Faker.seed(seed)
        np.random.seed(seed)
        random.seed(seed)
        logger.info(f"Initialized SyntheticDataGenerator with seed {seed}")

    def generate_customers(self, count: int = 100000) -> pd.DataFrame:
        """
        Generates synthetic customer data.
        
        Args:
            count: Number of customers to generate.
            
        Returns:
            pd.DataFrame: DataFrame containing customer data.
        """
        logger.info(f"Generating {count} customers...")
        
        data = []
        for _ in tqdm(range(count), desc="Customers"):
            data.append({
                "customer_id": str(uuid.uuid4()),
                "name": self.faker.name(),
                "email": self.faker.email(),
                "city": self.faker.city(),
                "country": self.faker.country(),
                "registration_date": self.faker.date_between(start_date="-2y", end_date="today")
            })
            
        df = pd.DataFrame(data)
        
        # Generate age using NumPy normal distribution (mean=35, std=10)
        ages = np.random.normal(loc=35, scale=10, size=count)
        df["age"] = np.clip(ages, 18, 90).astype(int)
        
        return df

    def generate_products(self, count: int = 10000) -> pd.DataFrame:
        """
        Generates synthetic product data.
        
        Args:
            count: Number of products to generate.
            
        Returns:
            pd.DataFrame: DataFrame containing product data.
        """
        logger.info(f"Generating {count} products...")
        
        categories = ["Electronics", "Clothing", "Home", "Sports", "Books"]
        
        data = {
            "product_id": [str(uuid.uuid4()) for _ in range(count)],
            "name": [f"{self.faker.word().capitalize()} {self.faker.word().capitalize()}" for _ in range(count)],
            "category": np.random.choice(categories, size=count),
            "price": np.round(np.random.uniform(10, 500, size=count), 2),
            "stock": np.random.randint(0, 1000, size=count),
            "rating": np.round(np.random.uniform(1, 5, size=count), 1)
        }
        
        return pd.DataFrame(data)

    def generate_orders(
        self, 
        count: int = 1000000, 
        customers_df: Optional[pd.DataFrame] = None, 
        products_df: Optional[pd.DataFrame] = None
    ) -> pd.DataFrame:
        """
        Generates synthetic order data with Pareto distribution for customer frequency.
        
        Args:
            count: Number of orders to generate.
            customers_df: DataFrame of existing customers.
            products_df: DataFrame of existing products.
            
        Returns:
            pd.DataFrame: DataFrame containing order data.
        """
        if customers_df is None or products_df is None:
            raise ValueError("Customers and Products DataFrames must be provided.")

        logger.info(f"Generating {count} orders...")
        
        customer_ids = customers_df["customer_id"].values
        product_ids = products_df["product_id"].values
        
        # Pareto distribution for customer selection (20% make 80% of orders)
        # Shape parameter alpha=1.16 approximates 80/20 rule
        alpha = 1.16
        weights = np.random.pareto(alpha, len(customer_ids))
        probs = weights / weights.sum()
        
        logger.info("Sampling customers (Pareto distribution)...")
        selected_customers = np.random.choice(
            customer_ids, 
            size=count, 
            p=probs
        )
        
        logger.info("Sampling products (Uniform distribution)...")
        selected_products = np.random.choice(product_ids, size=count)
        
        # Generate other fields
        quantities = np.random.randint(1, 11, size=count)
        
        # Vectorized date generation (approximate)
        days_back = np.random.randint(0, 365, size=count)
        base_date = np.datetime64('today')
        order_dates = base_date - days_back.astype('timedelta64[D]')
        
        df = pd.DataFrame({
            "order_id": [str(uuid.uuid4()) for _ in range(count)],
            "customer_id": selected_customers,
            "product_id": selected_products,
            "quantity": quantities,
            "order_date": order_dates
        })
        
        return df

    def save_dataframe(self, df: pd.DataFrame, filename: str) -> None:
        """Saves DataFrame to CSV."""
        filepath = RAW_DATA_DIR / filename
        logger.info(f"Saving {len(df)} records to {filepath}...")
        df.to_csv(filepath, index=False)
        logger.info("Save complete.")

    def run(self):
        """Executes the full data generation pipeline."""
        # Generate Customers
        customers = self.generate_customers(count=100000)
        self.save_dataframe(customers, CUSTOMERS_FILE)
        
        # Generate Products
        products = self.generate_products(count=10000)
        self.save_dataframe(products, PRODUCTS_FILE)
        
        # Generate Orders
        orders = self.generate_orders(
            count=1000000, 
            customers_df=customers, 
            products_df=products
        )
        self.save_dataframe(orders, ORDERS_FILE)
        
        logger.info("Data generation pipeline finished successfully.")

if __name__ == "__main__":
    generator = SyntheticDataGenerator()
    generator.run()