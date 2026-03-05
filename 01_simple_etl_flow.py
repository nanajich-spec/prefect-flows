"""
Prefect Example Flow 1: Simple Task Orchestration
This flow demonstrates basic task execution and dependencies
"""

from prefect import flow, task
from datetime import datetime
import time

@task(name="Extract Data", retries=3, retry_delay_seconds=5)
def extract_data():
    """Simulate data extraction"""
    print(f"[{datetime.now()}] Extracting data from source...")
    time.sleep(2)
    data = {"records": 100, "timestamp": datetime.now().isoformat()}
    print(f"Extracted: {data}")
    return data

@task(name="Transform Data", retries=2)
def transform_data(data):
    """Simulate data transformation"""
    print(f"[{datetime.now()}] Transforming data...")
    time.sleep(1)
    data["transformed"] = True
    data["processed_count"] = data["records"] * 2
    print(f"Transformed: {data}")
    return data

@task(name="Load Data", retries=2)
def load_data(data):
    """Simulate data loading"""
    print(f"[{datetime.now()}] Loading data to destination...")
    time.sleep(1)
    print(f"Successfully loaded {data['processed_count']} records")
    return {"status": "success", "loaded_at": datetime.now().isoformat()}

@flow(name="Simple ETL Pipeline", log_prints=True)
def simple_etl_flow():
    """Simple ETL flow demonstrating task dependencies"""
    print(f"Starting ETL pipeline at {datetime.now()}")
    
    # Tasks execute in sequence with dependencies
    raw_data = extract_data()
    processed_data = transform_data(raw_data)
    result = load_data(processed_data)
    
    print(f"Pipeline completed: {result}")
    return result

if __name__ == "__main__":
    # Run the flow
    simple_etl_flow()
