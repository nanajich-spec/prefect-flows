"""
Prefect Example Flow 2: Parallel Task Execution
This flow demonstrates parallel processing for performance
"""

from prefect import flow, task
from datetime import datetime
import time
import random

@task(name="Process Batch")
def process_batch(batch_id: int):
    """Process a single batch of data"""
    processing_time = random.uniform(1, 3)
    print(f"[{datetime.now()}] Processing batch {batch_id}...")
    time.sleep(processing_time)
    
    result = {
        "batch_id": batch_id,
        "records_processed": random.randint(50, 200),
        "processing_time": round(processing_time, 2),
        "status": "completed"
    }
    print(f"Batch {batch_id} completed: {result['records_processed']} records")
    return result

@task(name="Aggregate Results")
def aggregate_results(batch_results):
    """Aggregate results from all batches"""
    print(f"\n[{datetime.now()}] Aggregating results from {len(batch_results)} batches...")
    
    total_records = sum(r["records_processed"] for r in batch_results)
    total_time = sum(r["processing_time"] for r in batch_results)
    
    summary = {
        "total_batches": len(batch_results),
        "total_records": total_records,
        "total_processing_time": round(total_time, 2),
        "average_batch_size": round(total_records / len(batch_results), 2)
    }
    
    print(f"\n✅ Aggregation Summary:")
    for key, value in summary.items():
        print(f"  {key}: {value}")
    
    return summary

@flow(name="Parallel Processing Pipeline", log_prints=True)
def parallel_processing_flow(num_batches: int = 5):
    """
    Process multiple batches in parallel for optimal performance
    
    Args:
        num_batches: Number of batches to process in parallel
    """
    print(f"Starting parallel processing of {num_batches} batches at {datetime.now()}")
    start_time = time.time()
    
    # Submit all batches for parallel processing
    batch_futures = [process_batch.submit(i) for i in range(1, num_batches + 1)]
    
    # Wait for all batches to complete and collect results
    batch_results = [future.result() for future in batch_futures]
    
    # Aggregate the results
    summary = aggregate_results(batch_results)
    
    elapsed_time = round(time.time() - start_time, 2)
    print(f"\n🎉 Pipeline completed in {elapsed_time} seconds")
    print(f"   Parallel efficiency: {summary['total_processing_time']}/{elapsed_time} = "
          f"{round(summary['total_processing_time']/elapsed_time, 2)}x speedup")
    
    return summary

if __name__ == "__main__":
    # Run with 8 parallel batches
    parallel_processing_flow(num_batches=8)
