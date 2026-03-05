"""
Prefect Example Flow 4: Data Pipeline with Error Handling
This flow demonstrates robust error handling and retries
"""

from prefect import flow, task
from prefect.tasks import task_input_hash
from datetime import datetime, timedelta
import random
import time

@task(
    name="Fetch API Data",
    retries=3,
    retry_delay_seconds=10,
    cache_key_fn=task_input_hash,
    cache_expiration=timedelta(minutes=5)
)
def fetch_api_data(api_endpoint: str):
    """
    Simulate fetching data from an API with retry logic
    Includes caching to avoid redundant API calls
    """
    print(f"\n[{datetime.now()}] Fetching data from {api_endpoint}...")
    
    # Simulate occasional failures
    if random.random() < 0.3:  # 30% failure rate
        print(f"❌ API request failed - will retry...")
        raise Exception(f"API endpoint {api_endpoint} is temporarily unavailable")
    
    time.sleep(1)
    
    data = {
        "endpoint": api_endpoint,
        "records": random.randint(100, 1000),
        "fetched_at": datetime.now().isoformat()
    }
    
    print(f"✅ Successfully fetched {data['records']} records")
    return data

@task(name="Validate Data", retries=2)
def validate_data(data):
    """Validate the fetched data"""
    print(f"\n[{datetime.now()}] Validating data...")
    
    if data["records"] < 50:
        raise ValueError("Insufficient data records")
    
    if data["records"] > 900:
        print("⚠️  Warning: Unusually high record count")
    
    print(f"✅ Data validation passed: {data['records']} records")
    return {"valid": True, "record_count": data["records"]}

@task(name="Process and Store", retries=1)
def process_and_store(data, validation_result):
    """Process and store the validated data"""
    print(f"\n[{datetime.now()}] Processing and storing data...")
    
    if not validation_result["valid"]:
        raise ValueError("Cannot process invalid data")
    
    time.sleep(1)
    
    result = {
        "processed_records": validation_result["record_count"],
        "stored_at": datetime.now().isoformat(),
        "status": "success"
    }
    
    print(f"✅ Successfully processed and stored {result['processed_records']} records")
    return result

@flow(name="Robust Data Pipeline", log_prints=True)
def robust_data_pipeline_flow(api_endpoints: list):
    """
    A robust data pipeline with error handling and retries
    
    Args:
        api_endpoints: List of API endpoints to fetch data from
    """
    print("="*70)
    print(f"Starting Robust Data Pipeline - {datetime.now()}")
    print("="*70)
    
    results = []
    failed_endpoints = []
    
    for endpoint in api_endpoints:
        try:
            print(f"\n{'─'*70}")
            print(f"Processing endpoint: {endpoint}")
            print(f"{'─'*70}")
            
            # Fetch data with retries
            data = fetch_api_data(endpoint)
            
            # Validate the data
            validation = validate_data(data)
            
            # Process and store
            result = process_and_store(data, validation)
            
            results.append({
                "endpoint": endpoint,
                "status": "success",
                "result": result
            })
            
        except Exception as e:
            print(f"\n❌ Failed to process {endpoint}: {str(e)}")
            failed_endpoints.append({
                "endpoint": endpoint,
                "error": str(e)
            })
    
    # Summary
    print("\n" + "="*70)
    print("PIPELINE EXECUTION SUMMARY")
    print("="*70)
    print(f"Total endpoints: {len(api_endpoints)}")
    print(f"Successful: {len(results)} ✅")
    print(f"Failed: {len(failed_endpoints)} ❌")
    
    if failed_endpoints:
        print("\nFailed Endpoints:")
        for failed in failed_endpoints:
            print(f"  - {failed['endpoint']}: {failed['error']}")
    
    summary = {
        "total": len(api_endpoints),
        "successful": len(results),
        "failed": len(failed_endpoints),
        "success_rate": round(len(results) / len(api_endpoints) * 100, 2),
        "results": results,
        "failures": failed_endpoints
    }
    
    print(f"\nSuccess Rate: {summary['success_rate']}%")
    print("="*70)
    
    return summary

if __name__ == "__main__":
    # Test with multiple API endpoints
    endpoints = [
        "/api/v1/users",
        "/api/v1/orders",
        "/api/v1/products",
        "/api/v1/analytics",
        "/api/v1/metrics"
    ]
    
    result = robust_data_pipeline_flow(endpoints)
