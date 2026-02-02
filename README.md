# genai-pyspark-pipeline1
Synthetic data generation and PySpark analytics with AI-assisted development

## Project Structure

- `src/`: Source code for data generation and analytics.
- `data/`: Directory for storing raw and processed data (ignored by git).
- `notebooks/`: Jupyter notebooks for exploration.
- `tests/`: Unit tests.

## Setup

1. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

2. Generate synthetic data:
   ```bash
   python src/data_generator.py
   ```
   This will create CSV files in `data/raw/`.

3. Run Spark analytics:
   ```bash
   python src/spark_analytics.py
   ```
   This will process the data and save results to `data/processed/`.

## Requirements

- Python 3.8+
- Java 8+ (for PySpark)
