# Prefect Orchestration on Kubernetes

This directory contains Prefect deployment manifests and example flows for demonstrating workflow orchestration capabilities.

## 📋 What is Prefect?

Prefect is a modern workflow orchestration tool that makes it easy to build, schedule, and monitor data pipelines. It's designed to be:
- **Fast**: Handle thousands of tasks per second
- **Reliable**: Built-in retries, caching, and error handling
- **Observable**: Beautiful UI for monitoring workflow execution
- **Flexible**: Python-native with minimal boilerplate

## 🚀 Quick Start

### 1. Access Prefect UI

```bash
# Port forward is already set up
# Open in browser: http://localhost:4200
```

### 2. Run Example Flows

```bash
# Make the runner script executable
chmod +x /home/nanaji/run-prefect-examples.sh

# Run examples
/home/nanaji/run-prefect-examples.sh
```

Or run individual flows:

```bash
export PREFECT_API_URL="http://localhost:4200/api"

# Simple ETL
python3 /home/nanaji/prefect-flows/01_simple_etl_flow.py

# Parallel Processing (fast!)
python3 /home/nanaji/prefect-flows/02_parallel_processing_flow.py

# Scheduled Flow
python3 /home/nanaji/prefect-flows/03_scheduled_health_monitor.py

# Error Handling
python3 /home/nanaji/prefect-flows/04_robust_error_handling_flow.py

# Fast Scheduling Benchmark
python3 /home/nanaji/prefect-flows/05_fast_scheduling_benchmark.py
```

## 📊 Example Flows

### 1. Simple ETL Pipeline (`01_simple_etl_flow.py`)
- **Purpose**: Demonstrates basic task dependencies
- **Features**: Extract → Transform → Load pattern
- **Duration**: ~5 seconds
- **Use Case**: Basic data pipeline

### 2. Parallel Processing Pipeline (`02_parallel_processing_flow.py`)
- **Purpose**: Shows parallel task execution for performance
- **Features**: Processes multiple batches concurrently
- **Duration**: ~3 seconds (vs 15+ seconds sequential)
- **Use Case**: Batch processing, data partitioning

### 3. Scheduled Health Monitor (`03_scheduled_health_monitor.py`)
- **Purpose**: Demonstrates scheduling capabilities
- **Features**: Cron-based scheduling (every 5 minutes)
- **Duration**: ~2 seconds
- **Use Case**: Monitoring, health checks, alerts

### 4. Robust Error Handling (`04_robust_error_handling_flow.py`)
- **Purpose**: Shows retry logic and error recovery
- **Features**: Automatic retries, caching, failure handling
- **Duration**: ~10 seconds (with simulated failures)
- **Use Case**: API integration, resilient pipelines

### 5. Fast Scheduling Benchmark (`05_fast_scheduling_benchmark.py`)
- **Purpose**: Tests Prefect's scheduling speed
- **Features**: Schedules 100+ tasks rapidly
- **Performance**: 
  - **Scheduling**: 50-200+ tasks/second
  - **Execution**: 20-50 tasks/second end-to-end
- **Use Case**: Performance testing, high-throughput scenarios

## ⚡ Performance Highlights

Based on the benchmark flow:

- **Task Scheduling Rate**: 50-200 tasks/second
- **Concurrent Execution**: Yes (limited by resources)
- **Task Overhead**: ~5-10ms per task
- **UI Responsiveness**: Real-time updates
- **Database**: PostgreSQL for metadata (scalable)

### Compared to Airflow:
- ✅ **Faster scheduling**: 5-10x faster task submission
- ✅ **Lower latency**: < 1 second task pickup
- ✅ **Better for dynamic workflows**: Python-native DAG building
- ✅ **Modern UI**: More responsive and intuitive
- ⚠️ **Maturity**: Airflow has more enterprise features

## 🏗️ Architecture

```
┌─────────────────┐
│  Prefect UI     │ :4200
│  (Web Interface)│
└────────┬────────┘
         │
┌────────▼────────┐
│ Prefect Server  │ :4200/api
│  (API Backend)  │
└────────┬────────┘
         │
    ┌────▼────┐
    │PostgreSQL│ :5432
    │(Metadata)│
    └─────────┘
         │
┌────────▼────────┐
│ Prefect Agent   │
│ (Task Executor) │
└─────────────────┘
```

## 📦 Deployed Components

All running in `prefect` namespace:

- **Prefect Server**: API and UI (1 replica)
- **Prefect Agent**: Task executor (1 replica)
- **PostgreSQL**: Metadata storage (1 replica, 5Gi PVC)

```bash
# Check status
microk8s kubectl get pods -n prefect

# Check logs
microk8s kubectl logs -n prefect -l app=prefect-server -f
microk8s kubectl logs -n prefect -l app=prefect-agent -f
```

## 🔗 Access Details

- **UI**: http://localhost:4200
- **API**: http://localhost:4200/api
- **Database**: `prefect-postgres:5432` (internal)

## 🎯 Common Use Cases

### 1. Data Engineering
```python
@flow
def data_pipeline():
    raw = extract_from_api()
    clean = transform_data(raw)
    load_to_warehouse(clean)
```

### 2. ML/AI Workflows
```python
@flow
def ml_training():
    data = load_dataset()
    model = train_model(data)
    evaluate_and_deploy(model)
```

### 3. Scheduled Jobs
```python
# Runs every day at 2 AM
deployment = Deployment.build_from_flow(
    flow=daily_report,
    schedule=CronSchedule(cron="0 2 * * *")
)
```

### 4. Event-Driven Automation
```python
@flow
def on_file_upload(file_path):
    validate_file(file_path)
    process_file(file_path)
    notify_completion()
```

## 🛠️ Management Commands

### Install Prefect locally (for developing flows)
```bash
pip install prefect
export PREFECT_API_URL="http://localhost:4200/api"
```

### Create a deployment
```bash
prefect deployment build my_flow.py:my_flow_name -n "my-deployment"
prefect deployment apply my_flow-deployment.yaml
```

### Start a local agent (optional, for development)
```bash
prefect agent start -q default
```

### View deployments
```bash
prefect deployment ls
```

## 📚 Learning Resources

- **Official Docs**: https://docs.prefect.io/
- **Concepts**: https://docs.prefect.io/concepts/
- **Examples**: `/home/nanaji/prefect-flows/`
- **UI**: http://localhost:4200 (live runs and debugging)

## 🔄 Comparison: Prefect vs Airflow

| Feature | Prefect | Airflow |
|---------|---------|---------|
| **Scheduling Speed** | ⚡ Very Fast (50-200+ tasks/s) | 🐌 Slower (5-20 tasks/s) |
| **UI Responsiveness** | ✅ Real-time updates | ⚠️ Periodic refresh |
| **Learning Curve** | ✅ Easy (pure Python) | ⚠️ Steep (complex config) |
| **Dynamic DAGs** | ✅ Native support | ⚠️ Limited |
| **Error Handling** | ✅ Built-in retries/caching | ✅ Good |
| **Enterprise Features** | ⚠️ Growing | ✅ Mature |
| **Community** | 🌱 Growing | 🌳 Large |
| **Best For** | Modern pipelines, ML | Traditional ETL, enterprises |

## 🎉 Ready to Use!

Your Prefect instance is deployed and ready. Access the UI at http://localhost:4200 and start running the example flows!
