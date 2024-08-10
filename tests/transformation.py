import pandas as pd
from pathlib import Path
from dags.Yayasan_ETL import clean_data

# URL for retrieving data from the government data site
DATA_URL = r'data/Salesdata.json'

# Paths for saving raw and cleaned data
OUTPUT_DIR = Path('data')
RAW_DATA_PATH = OUTPUT_DIR / 'order_data.csv'
CLEAN_DATA_PATH = OUTPUT_DIR / 'clean_order_data.csv'

# Testing: Define basic test functions for the transformation logic
def test_clean_data():
    try:
        # Load a sample raw data file
        raw_data = pd.read_csv(RAW_DATA_PATH)

        # Call the clean_data function
        clean_data(RAW_DATA_PATH, CLEAN_DATA_PATH)

        # Load the cleaned data
        cleaned_data = pd.read_csv(CLEAN_DATA_PATH)

        # Test: Ensure the cleaned data is not empty
        assert not cleaned_data.empty, "Test Failed: Cleaned data is empty"

        # Test: Ensure the cleaned data has only 'Cars' or 'Motorcycles' in PRODUCTLINE
        assert all(cleaned_data['PRODUCTLINE'].isin(['Cars', 'Motorcycles'])), \
            "Test Failed: Cleaned data contains invalid PRODUCTLINE values"

        print("All tests passed!")
    except AssertionError as e:
        print(f"Test assertion failed: {e}")
        raise
    except Exception as e:
        print(f"Error during testing: {e}")
        raise

test_clean_data()