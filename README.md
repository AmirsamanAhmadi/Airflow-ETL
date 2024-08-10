# Airflow Setup with Docker and Python Environment

This repository provides a setup for Apache Airflow using Docker and Python 3.11. Follow the instructions below to get started.

## Prerequisites

- Docker
- Docker Compose
- Python 3.11

## Setup Instructions

### 1. Create Required Directories and Set Environment Variables

Create the necessary directories and set up the environment variable for Airflow:


```
mkdir -p ./dags ./logs ./plugins ./data ./tests
echo -e "AIRFLOW_UID=$(id -u)" > .env
```

### 2. Initialize the Airflow Database

Initialize the database for Airflow:


```
docker compose up airflow-init
```

### 3. Start All Services

Start Airflow and other services:


```
docker compose up
```

### 4. Create and Configure Python Environment

Create a Python virtual environment with Python 3.11 and install the dependencies from the requirements.txt file:

```
# Create a Python 3.11 virtual environment
python3.11 -m venv venv

# Activate the virtual environment
source venv/bin/activate  # On Windows, use `venv\Scripts\activate`

# Install dependencies
pip install -r requirements.txt

```



