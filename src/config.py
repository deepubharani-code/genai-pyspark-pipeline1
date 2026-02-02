import os
from pathlib import Path

# Project root directory
PROJECT_ROOT = Path(__file__).parent.parent

# Data directories
DATA_DIR = PROJECT_ROOT / "data"
RAW_DATA_DIR = DATA_DIR / "raw"
PROCESSED_DATA_DIR = DATA_DIR / "processed"

# Ensure directories exist
RAW_DATA_DIR.mkdir(parents=True, exist_ok=True)
PROCESSED_DATA_DIR.mkdir(parents=True, exist_ok=True)

# Data Generation Settings
NUM_CUSTOMERS = 1000
NUM_PRODUCTS = 50
NUM_ORDERS = 5000
RANDOM_SEED = 42

# File names
CUSTOMERS_FILE = "customers.csv"
PRODUCTS_FILE = "products.csv"
ORDERS_FILE = "orders.csv"