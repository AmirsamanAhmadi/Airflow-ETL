# Import necessary modules
import pandas as pd
from airflow import DAG
import airflow
from airflow.operators.python import PythonOperator
import json
import requests
from datetime import datetime
import psycopg2
from sqlalchemy import create_engine
from pathlib import Path

# Database credentials for PostgreSQL connection
DB_CONFIG = {
    'host': 'customdb',
    'database': 'postgres',
    'user': 'postgres',
    'password': 'securepassword',
    'port': 5432
}

# URL for retrieving data from the government data site
DATA_URL = r'data/Salesdata.json'

# Paths for saving raw and cleaned data
OUTPUT_DIR = Path('data')
RAW_DATA_PATH = OUTPUT_DIR / 'order_data.csv'
CLEAN_DATA_PATH = OUTPUT_DIR / 'clean_order_data.csv'

# Define the DAG object
dag = DAG(
    dag_id='Yayasan_ETL',
    start_date=airflow.utils.dates.days_ago(0),  # Change this to a specific start date if needed
    schedule_interval='@daily',  # Run the DAG once every day,
    catchup=False  # Set to False to avoid backfilling
)

# Function to extract raw data from the given URL and save it as a CSV file
def get_raw_data(url, output_path):
    try:
        with open(url, 'r') as file:
            data = json.load(file)
        
        df = pd.DataFrame(data)
        df['data_received_at'] = datetime.now()
        df.to_csv(output_path, index=False)
        print(f"Raw data successfully saved to {output_path}")
    except Exception as e:
        print(f"Error during data extraction: {e}")
        raise

# Function to clean the raw data and save it to a new CSV file
def clean_data(raw_input_path, output_path):
    try:
        df = pd.read_csv(raw_input_path)

        # Data quality check: Ensure the raw data is not empty
        if df.empty:
            raise ValueError("No data found in the raw data file.")

        # Drop rows with missing values in the 'STATE' column
        df_cleaned = df.dropna(subset=['STATE'])

        # Filter rows where 'PRODUCTLINE' is either 'Cars' or 'Motorcycles'
        filtered_df = df_cleaned[df_cleaned['PRODUCTLINE'].isin(['Cars', 'Motorcycles'])]

        # Data quality check: Validate row count after cleaning
        if filtered_df.shape[0] == 0:
            raise ValueError("No valid rows after cleaning the data.")

        filtered_df.to_csv(output_path, index=False)
        print("Data cleaning successful")
    except Exception as e:
        print(f"Error during data cleaning: {e}")
        raise

# Function to load cleaned data into a PostgreSQL database
def load_to_db(db_host, db_name, db_user, db_pswd, db_port, data_path):
    try:
        df = pd.read_csv(data_path)
        df['data_ingested_at'] = datetime.now()

        engine = create_engine(f"postgresql+psycopg2://{db_user}:{db_pswd}@{db_host}:{db_port}/{db_name}")
        # Make sure salesdata schema has been created in postgresql
        df.to_sql('salesdata', con=engine, schema='salesdata', if_exists='replace', index=False)
        print("Data successfully loaded into the database")
    except Exception as e:
        print(f"Error during data loading: {e}")
        raise

# Define the task for raw data extraction
extract_raw_data = PythonOperator(
    task_id='extract_raw_data',
    python_callable=get_raw_data,
    op_kwargs={'url': DATA_URL, 'output_path': RAW_DATA_PATH},
    dag=dag,
)

# Define the task for data cleaning
clean_raw_data = PythonOperator(
    task_id='clean_raw_data',
    python_callable=clean_data,
    op_kwargs={'raw_input_path': RAW_DATA_PATH, 'output_path': CLEAN_DATA_PATH},
    dag=dag,
)

# Define the task for loading cleaned data into the database
load_data_to_db = PythonOperator(
    task_id='load_data_to_db',
    python_callable=load_to_db,
    op_kwargs={
        'db_host': DB_CONFIG['host'],
        'db_name': DB_CONFIG['database'],
        'db_user': DB_CONFIG['user'],
        'db_pswd': DB_CONFIG['password'],
        'db_port': DB_CONFIG['port'],
        'data_path': CLEAN_DATA_PATH
    },
    dag=dag,
)

# Set task execution order in the DAG
extract_raw_data >> clean_raw_data >> load_data_to_db


