"""
Prefect Example Flow 3: Scheduled Flow with Cron
This flow demonstrates scheduling capabilities
"""

from prefect import flow, task
from prefect.deployments import Deployment
from prefect.server.schemas.schedules import CronSchedule
from datetime import datetime, timedelta
import random

@task(name="Check System Health")
def check_system_health():
    """Simulate system health check"""
    print(f"\n[{datetime.now()}] Running system health check...")
    
    metrics = {
        "cpu_usage": round(random.uniform(20, 80), 2),
        "memory_usage": round(random.uniform(40, 90), 2),
        "disk_usage": round(random.uniform(30, 70), 2),
        "active_connections": random.randint(10, 100)
    }
    
    print("System Metrics:")
    for metric, value in metrics.items():
        print(f"  {metric}: {value}%")
    
    return metrics

@task(name="Generate Report")
def generate_report(metrics):
    """Generate health report"""
    print(f"\n[{datetime.now()}] Generating health report...")
    
    # Determine overall health
    avg_usage = (metrics["cpu_usage"] + metrics["memory_usage"] + metrics["disk_usage"]) / 3
    
    if avg_usage < 60:
        health_status = "HEALTHY 🟢"
    elif avg_usage < 80:
        health_status = "WARNING 🟡"
    else:
        health_status = "CRITICAL 🔴"
    
    report = {
        "timestamp": datetime.now().isoformat(),
        "health_status": health_status,
        "average_resource_usage": round(avg_usage, 2),
        "metrics": metrics
    }
    
    print(f"\nHealth Status: {health_status}")
    print(f"Average Resource Usage: {avg_usage:.2f}%")
    
    return report

@task(name="Send Notification")
def send_notification(report):
    """Simulate sending notification if needed"""
    if "CRITICAL" in report["health_status"] or "WARNING" in report["health_status"]:
        print(f"\n[{datetime.now()}] 📧 Sending alert notification...")
        print(f"   Alert: System health is {report['health_status']}")
        print(f"   Resource usage: {report['average_resource_usage']}%")
    else:
        print(f"\n[{datetime.now()}] ✅ System healthy - no notification needed")

@flow(name="System Health Monitor", log_prints=True)
def system_health_monitor_flow():
    """
    Scheduled flow to monitor system health
    Runs every 5 minutes to check system metrics
    """
    print("="*60)
    print(f"System Health Monitoring Run - {datetime.now()}")
    print("="*60)
    
    # Check system health
    metrics = check_system_health()
    
    # Generate report
    report = generate_report(metrics)
    
    # Send notification if needed
    send_notification(report)
    
    print("\n" + "="*60)
    print("Health check completed successfully")
    print("="*60)
    
    return report

# Deployment configuration for scheduling
def create_deployment():
    """Create a deployment with cron schedule"""
    deployment = Deployment.build_from_flow(
        flow=system_health_monitor_flow,
        name="health-monitor-every-5min",
        schedule=CronSchedule(cron="*/5 * * * *"),  # Every 5 minutes
        work_queue_name="default",
        tags=["monitoring", "health-check", "scheduled"]
    )
    return deployment

if __name__ == "__main__":
    # Run once immediately
    system_health_monitor_flow()
    
    print("\n" + "="*60)
    print("To deploy this as a scheduled flow:")
    print("="*60)
    print("1. Set PREFECT_API_URL: export PREFECT_API_URL=http://localhost:4200/api")
    print("2. Create deployment: python 03_scheduled_health_monitor.py create")
    print("3. View in UI: http://localhost:4200")
    print("="*60)
