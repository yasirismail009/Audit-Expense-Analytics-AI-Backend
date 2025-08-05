#!/usr/bin/env python3
"""
Real-Time Celery Processing Monitor
===================================

This script provides real-time monitoring of Celery task processing
with detailed logging and status updates.
"""

import os
import sys
import django
import time
import logging
from datetime import datetime
import json

# Setup Django environment
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'analytics.settings')
django.setup()

from core.models import FileProcessingJob
from analytics.celery import app

class RealTimeCeleryMonitor:
    """Real-time monitoring of Celery processing"""
    
    def __init__(self):
        self.setup_logging()
        self.monitoring = True
        
    def setup_logging(self):
        """Setup detailed logging"""
        # Configure logging
        logging.basicConfig(
            level=logging.DEBUG,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.StreamHandler(sys.stdout),
                logging.FileHandler('celery_monitor.log')
            ]
        )
        
        # Enable debug logging for Celery
        logging.getLogger('celery').setLevel(logging.DEBUG)
        logging.getLogger('celery.task').setLevel(logging.DEBUG)
        logging.getLogger('celery.worker').setLevel(logging.DEBUG)
        logging.getLogger('core.tasks').setLevel(logging.DEBUG)
        
    def monitor_continuously(self, interval=5):
        """Monitor Celery processing continuously"""
        print("🔍 REAL-TIME CELERY MONITORING STARTED")
        print("=" * 50)
        print(f"Monitoring interval: {interval} seconds")
        print("Press Ctrl+C to stop monitoring")
        print()
        
        try:
            while self.monitoring:
                self.show_current_status()
                time.sleep(interval)
                
        except KeyboardInterrupt:
            print("\n⏹️  Monitoring stopped by user")
            
    def show_current_status(self):
        """Show current processing status"""
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(f"\n📊 STATUS UPDATE - {timestamp}")
        print("-" * 40)
        
        # Check Celery connection
        self.check_celery_health()
        
        # Show active tasks
        self.show_active_tasks()
        
        # Show processing jobs
        self.show_processing_jobs()
        
        # Show queue status
        self.show_queue_status()
        
    def check_celery_health(self):
        """Check Celery health status"""
        try:
            inspect = app.control.inspect()
            
            # Check active workers
            active = inspect.active()
            if active:
                print(f"✅ Celery Workers: {len(active)} active")
                for worker, tasks in active.items():
                    print(f"   Worker: {worker} - {len(tasks)} active tasks")
            else:
                print("⚠️  No active Celery workers")
                
            # Check registered tasks
            registered = inspect.registered()
            if registered:
                total_tasks = sum(len(tasks) for tasks in registered.values())
                print(f"📋 Registered Tasks: {total_tasks}")
                
        except Exception as e:
            print(f"❌ Celery health check failed: {str(e)}")
            
    def show_active_tasks(self):
        """Show currently active tasks"""
        try:
            inspect = app.control.inspect()
            active = inspect.active()
            
            if active:
                print("\n🔄 ACTIVE TASKS:")
                for worker, tasks in active.items():
                    print(f"  Worker: {worker}")
                    for task in tasks:
                        task_name = task.get('name', 'Unknown')
                        task_id = task.get('id', 'Unknown')
                        start_time = task.get('time_start', 'Unknown')
                        
                        print(f"    Task: {task_name}")
                        print(f"    ID: {task_id}")
                        print(f"    Started: {start_time}")
                        print(f"    Args: {task.get('args', [])}")
                        print()
            else:
                print("\n🔄 No active tasks")
                
        except Exception as e:
            print(f"❌ Failed to get active tasks: {str(e)}")
            
    def show_processing_jobs(self):
        """Show processing job status"""
        try:
            # Get recent jobs
            recent_jobs = FileProcessingJob.objects.all().order_by('-created_at')[:10]
            
            if recent_jobs:
                print("\n📁 PROCESSING JOBS:")
                
                # Count by status
                status_counts = {}
                for job in recent_jobs:
                    status = job.status
                    if status not in status_counts:
                        status_counts[status] = 0
                    status_counts[status] += 1
                
                print("  Status Distribution:")
                for status, count in status_counts.items():
                    print(f"    {status}: {count}")
                
                # Show recent jobs
                print("\n  Recent Jobs:")
                for job in recent_jobs[:3]:
                    print(f"    ID: {job.id}")
                    print(f"    File: {job.data_file.file_name if job.data_file else 'N/A'}")
                    print(f"    Status: {job.status}")
                    print(f"    Created: {job.created_at.strftime('%H:%M:%S')}")
                    if job.started_at:
                        print(f"    Started: {job.started_at.strftime('%H:%M:%S')}")
                    if job.completed_at:
                        print(f"    Completed: {job.completed_at.strftime('%H:%M:%S')}")
                    if job.error_message:
                        print(f"    Error: {job.error_message[:100]}...")
                    print()
            else:
                print("\n📁 No processing jobs found")
                
        except Exception as e:
            print(f"❌ Failed to get processing jobs: {str(e)}")
            
    def show_queue_status(self):
        """Show queue status"""
        try:
            inspect = app.control.inspect()
            
            # Reserved tasks (waiting in queue)
            reserved = inspect.reserved()
            if reserved:
                print("\n📋 QUEUED TASKS:")
                total_queued = 0
                for worker, tasks in reserved.items():
                    print(f"  Worker: {worker} - {len(tasks)} queued")
                    total_queued += len(tasks)
                print(f"  Total queued: {total_queued}")
            else:
                print("\n📋 No queued tasks")
                
        except Exception as e:
            print(f"❌ Failed to get queue status: {str(e)}")
            
    def run_test_task(self):
        """Run a test task to verify processing"""
        print("\n🧪 RUNNING TEST TASK")
        print("-" * 20)
        
        try:
            from core.tasks import debug_task
            
            print("Sending test task...")
            result = debug_task.delay()
            
            print(f"Task ID: {result.id}")
            print("Waiting for completion...")
            
            # Wait for result
            task_result = result.get(timeout=30)
            print(f"✅ Test task completed: {task_result}")
            
        except Exception as e:
            print(f"❌ Test task failed: {str(e)}")

def main():
    """Main function"""
    print("🚀 REAL-TIME CELERY MONITOR")
    print("=" * 40)
    
    monitor = RealTimeCeleryMonitor()
    
    # Run a test task first
    monitor.run_test_task()
    
    # Start continuous monitoring
    print("\nStarting continuous monitoring...")
    monitor.monitor_continuously(interval=10)  # Update every 10 seconds

if __name__ == "__main__":
    main() 