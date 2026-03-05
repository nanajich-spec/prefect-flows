"""
Prefect Example Flow 5: Fast Scheduling Benchmark
This flow demonstrates Prefect's ability to handle rapid scheduling and execution
"""

from prefect import flow, task
from datetime import datetime
import time
import asyncio

@task(name="Quick Task")
async def quick_task(task_id: int):
    """Fast executing task for benchmarking"""
    start = time.time()
    await asyncio.sleep(0.1)  # Simulate very quick work
    duration = time.time() - start
    return {
        "task_id": task_id,
        "duration": round(duration, 3),
        "completed_at": datetime.now().isoformat()
    }

@task(name="Compute Intensive Task")
def compute_task(task_id: int, iterations: int = 1000):
    """Simulate compute-intensive work"""
    start = time.time()
    result = sum(i ** 2 for i in range(iterations))
    duration = time.time() - start
    return {
        "task_id": task_id,
        "result": result,
        "duration": round(duration, 4)
    }

@flow(name="Fast Scheduling Benchmark", log_prints=True)
async def fast_scheduling_benchmark(
    num_quick_tasks: int = 50,
    num_compute_tasks: int = 20
):
    """
    Benchmark flow to test Prefect's scheduling speed
    
    This demonstrates:
    - Rapid task submission
    - Concurrent task execution
    - Performance metrics collection
    """
    print("="*70)
    print(f"Fast Scheduling Benchmark - {datetime.now()}")
    print("="*70)
    print(f"Quick tasks: {num_quick_tasks}")
    print(f"Compute tasks: {num_compute_tasks}")
    print("="*70)
    
    overall_start = time.time()
    
    # Schedule quick tasks
    print(f"\n[{datetime.now()}] Scheduling {num_quick_tasks} quick tasks...")
    quick_start = time.time()
    quick_futures = [quick_task.submit(i) for i in range(num_quick_tasks)]
    quick_schedule_time = time.time() - quick_start
    
    # Schedule compute tasks
    print(f"[{datetime.now()}] Scheduling {num_compute_tasks} compute tasks...")
    compute_start = time.time()
    compute_futures = [compute_task.submit(i) for i in range(num_compute_tasks)]
    compute_schedule_time = time.time() - compute_start
    
    # Wait for completion
    print(f"\n[{datetime.now()}] Waiting for task completion...")
    
    quick_results = [await future for future in quick_futures]
    compute_results = [future.result() for future in compute_futures]
    
    overall_duration = time.time() - overall_start
    
    # Calculate metrics
    print("\n" + "="*70)
    print("BENCHMARK RESULTS")
    print("="*70)
    
    print(f"\n⚡ Quick Tasks Performance:")
    print(f"   Tasks: {num_quick_tasks}")
    print(f"   Schedule time: {quick_schedule_time:.4f}s")
    print(f"   Avg schedule time per task: {(quick_schedule_time/num_quick_tasks*1000):.2f}ms")
    print(f"   Tasks per second: {(num_quick_tasks/quick_schedule_time):.2f}")
    
    print(f"\n🔢 Compute Tasks Performance:")
    print(f"   Tasks: {num_compute_tasks}")
    print(f"   Schedule time: {compute_schedule_time:.4f}s")
    print(f"   Avg schedule time per task: {(compute_schedule_time/num_compute_tasks*1000):.2f}ms")
    
    print(f"\n📊 Overall Performance:")
    print(f"   Total tasks: {num_quick_tasks + num_compute_tasks}")
    print(f"   Total duration: {overall_duration:.4f}s")
    print(f"   Overall throughput: {((num_quick_tasks + num_compute_tasks)/overall_duration):.2f} tasks/sec")
    
    metrics = {
        "quick_tasks": {
            "count": num_quick_tasks,
            "schedule_time": round(quick_schedule_time, 4),
            "tasks_per_second": round(num_quick_tasks/quick_schedule_time, 2)
        },
        "compute_tasks": {
            "count": num_compute_tasks,
            "schedule_time": round(compute_schedule_time, 4)
        },
        "overall": {
            "total_tasks": num_quick_tasks + num_compute_tasks,
            "total_duration": round(overall_duration, 4),
            "throughput": round((num_quick_tasks + num_compute_tasks)/overall_duration, 2)
        }
    }
    
    print("\n" + "="*70)
    print(f"✅ Benchmark completed successfully!")
    print("="*70)
    
    return metrics

@flow(name="Stress Test - Mass Scheduling", log_prints=True)
def stress_test_flow(num_tasks: int = 100):
    """
    Stress test with mass task scheduling
    Tests how fast Prefect can schedule many tasks
    """
    print("="*70)
    print(f"Stress Test: Scheduling {num_tasks} tasks")
    print("="*70)
    
    start_time = time.time()
    
    # Rapidly schedule tasks
    futures = []
    for i in range(num_tasks):
        future = compute_task.submit(i, iterations=500)
        futures.append(future)
    
    schedule_time = time.time() - start_time
    
    print(f"\n✅ Scheduled {num_tasks} tasks in {schedule_time:.4f}s")
    print(f"⚡ Scheduling rate: {(num_tasks/schedule_time):.2f} tasks/second")
    print(f"📈 Average time per task: {(schedule_time/num_tasks*1000):.2f}ms")
    
    # Wait for all to complete
    print(f"\nWaiting for execution...")
    results = [f.result() for f in futures]
    
    total_time = time.time() - start_time
    
    print(f"\n🎉 All {num_tasks} tasks completed in {total_time:.4f}s")
    print(f"📊 End-to-end throughput: {(num_tasks/total_time):.2f} tasks/second")
    
    return {
        "total_tasks": num_tasks,
        "schedule_time": round(schedule_time, 4),
        "total_time": round(total_time, 4),
        "scheduling_rate": round(num_tasks/schedule_time, 2),
        "throughput": round(num_tasks/total_time, 2)
    }

if __name__ == "__main__":
    print("\n🚀 Running Prefect Fast Scheduling Benchmarks\n")
    
    # Run async benchmark
    import asyncio
    result1 = asyncio.run(fast_scheduling_benchmark(num_quick_tasks=50, num_compute_tasks=30))
    
    print("\n" + "="*70 + "\n")
    
    # Run stress test
    result2 = stress_test_flow(num_tasks=100)
    
    print("\n✅ All benchmarks completed!")
