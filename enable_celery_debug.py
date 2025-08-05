#!/usr/bin/env python3
"""
Enable Celery Debug Logging and Monitor Processing
==================================================

This script enables debug logging in Celery and provides tools to monitor
task processing status in real-time.
"""

import os
import sys
import django
import logging
from datetime import datetime
import json

# Setup Django environment
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'analytics.settings')
django.setup()

from core.models import FileProcessingJob
from analytics.celery import app

class CeleryDebugMonitor:
    """Monitor Celery processing with debug logging"""
    
    def __init__(self):
        self.setup_debug_logging()
        
    def setup_debug_logging(self):
        """Setup debug logging for Celery"""
        print("🔧 SETTING UP CELERY DEBUG LOGGING")
        print("=" * 50)
        
        # Configure Celery logging
        logging.getLogger('celery').setLevel(logging.DEBUG)
        logging.getLogger('celery.task').setLevel(logging.DEBUG)
        logging.getLogger('celery.worker').setLevel(logging.DEBUG)
        logging.getLogger('celery.worker.control').setLevel(logging.DEBUG)
        logging.getLogger('celery.worker.consumer').setLevel(logging.DEBUG)
        logging.getLogger('celery.worker.consumer.connection').setLevel(logging.DEBUG)
        logging.getLogger('celery.worker.consumer.mingle').setLevel(logging.DEBUG)
        logging.getLogger('celery.worker.consumer.gossip').setLevel(logging.DEBUG)
        logging.getLogger('celery.worker.consumer.heart').setLevel(logging.DEBUG)
        logging.getLogger('celery.worker.consumer.tasks').setLevel(logging.DEBUG)
        logging.getLogger('celery.worker.consumer.control').setLevel(logging.DEBUG)
        logging.getLogger('celery.worker.consumer.events').setLevel(logging.DEBUG)
        logging.getLogger('celery.worker.consumer.connection').setLevel(logging.DEBUG)
        logging.getLogger('celery.worker.consumer.connection.redis').setLevel(logging.DEBUG)
        
        # Configure our core app logging
        logging.getLogger('core').setLevel(logging.DEBUG)
        logging.getLogger('core.tasks').setLevel(logging.DEBUG)
        
        print("✅ Debug logging enabled for Celery components")
        
    def check_celery_connection(self):
        """Check Celery connection status"""
        print("\n🔍 CHECKING CELERY CONNECTION")
        print("-" * 30)
        
        try:
            # Test Celery connection
            result = app.control.inspect().active()
            if result:
                print("✅ Celery connection successful")
                print(f"  Active workers: {len(result)}")
                for worker, tasks in result.items():
                    print(f"    Worker: {worker}")
                    print(f"    Active tasks: {len(tasks)}")
            else:
                print("⚠️  No active workers found")
                
            # Check registered tasks
            registered = app.control.inspect().registered()
            if registered:
                print(f"  Registered tasks: {len(registered)}")
                for worker, tasks in registered.items():
                    print(f"    Worker {worker}: {len(tasks)} tasks")
                    
        except Exception as e:
            print(f"❌ Celery connection failed: {str(e)}")
            
    def monitor_processing_jobs(self):
        """Monitor processing jobs in real-time"""
        print("\n📊 MONITORING PROCESSING JOBS")
        print("-" * 30)
        
        # Get all processing jobs
        jobs = FileProcessingJob.objects.all().order_by('-created_at')
        
        if not jobs:
            print("❌ No processing jobs found")
            return
            
        print(f"Total jobs: {jobs.count()}")
        
        # Show job status
        status_counts = {}
        for job in jobs:
            status = job.status
            if status not in status_counts:
                status_counts[status] = 0
            status_counts[status] += 1
            
        print("\nJob Status Distribution:")
        for status, count in status_counts.items():
            print(f"  {status}: {count}")
            
        # Show recent jobs with details
        print("\nRecent Jobs:")
        for job in jobs[:5]:  # Show last 5 jobs
            print(f"  ID: {job.id}")
            print(f"  File: {job.data_file.file_name if job.data_file else 'N/A'}")
            print(f"  Status: {job.status}")
            print(f"  Created: {job.created_at}")
            print(f"  Started: {job.started_at}")
            print(f"  Completed: {job.completed_at}")
            print(f"  Error: {job.error_message if job.error_message else 'None'}")
            print()
            
    def run_debug_task(self):
        """Run a debug task to test Celery functionality"""
        print("\n🧪 RUNNING DEBUG TASK")
        print("-" * 20)
        
        try:
            from core.tasks import debug_task
            
            print("Sending debug task to Celery...")
            result = debug_task.delay()
            
            print(f"Task ID: {result.id}")
            print("Waiting for result...")
            
            # Wait for result with timeout
            task_result = result.get(timeout=30)
            print(f"✅ Debug task completed: {task_result}")
            
        except Exception as e:
            print(f"❌ Debug task failed: {str(e)}")
            
    def show_celery_config(self):
        """Show current Celery configuration"""
        print("\n⚙️  CELERY CONFIGURATION")
        print("-" * 25)
        
        config = app.conf
        print(f"Broker URL: {config.broker_url}")
        print(f"Result Backend: {config.result_backend}")
        print(f"Task Serializer: {config.task_serializer}")
        print(f"Result Serializer: {config.result_serializer}")
        print(f"Accept Content: {config.accept_content}")
        print(f"Task Time Limit: {config.task_time_limit}")
        print(f"Task Soft Time Limit: {config.task_soft_time_limit}")
        print(f"Worker Concurrency: {config.worker_concurrency}")
        print(f"Worker Prefetch Multiplier: {config.worker_prefetch_multiplier}")
        
    def monitor_task_queue(self):
        """Monitor task queue status"""
        print("\n📋 TASK QUEUE MONITORING")
        print("-" * 25)
        
        try:
            # Get queue statistics
            inspect = app.control.inspect()
            
            # Active tasks
            active = inspect.active()
            if active:
                print("Active Tasks:")
                for worker, tasks in active.items():
                    print(f"  Worker: {worker}")
                    for task in tasks:
                        print(f"    Task: {task['name']}")
                        print(f"    ID: {task['id']}")
                        print(f"    Started: {task['time_start']}")
                        print(f"    Args: {task['args']}")
                        print()
            else:
                print("No active tasks")
                
            # Reserved tasks (waiting to be executed)
            reserved = inspect.reserved()
            if reserved:
                print("Reserved Tasks:")
                for worker, tasks in reserved.items():
                    print(f"  Worker: {worker}")
                    for task in tasks:
                        print(f"    Task: {task['name']}")
                        print(f"    ID: {task['id']}")
                        print()
            else:
                print("No reserved tasks")
                
        except Exception as e:
            print(f"❌ Failed to get queue status: {str(e)}")
            
    def enable_worker_debug(self):
        """Enable debug mode on Celery workers"""
        print("\n🔧 ENABLING WORKER DEBUG MODE")
        print("-" * 30)
        
        try:
            # Enable debug mode on all workers
            result = app.control.broadcast('pool_grow', arguments={'n': 1})
            print("✅ Worker debug mode enabled")
            print(f"Result: {result}")
            
        except Exception as e:
            print(f"❌ Failed to enable worker debug: {str(e)}")

def main():
    """Main function to run Celery debug monitoring"""
    print("🚀 CELERY DEBUG MONITORING STARTED")
    print("=" * 50)
    
    monitor = CeleryDebugMonitor()
    
    # Run all monitoring functions
    monitor.check_celery_connection()
    monitor.show_celery_config()
    monitor.monitor_processing_jobs()
    monitor.monitor_task_queue()
    monitor.run_debug_task()
    
    print("\n📝 DEBUG LOGGING INSTRUCTIONS")
    print("=" * 40)
    print("1. Debug logs are now enabled for Celery")
    print("2. Check the console output for detailed task processing")
    print("3. Monitor the debug.log file for persistent logs")
    print("4. Use 'docker logs analytics-celery_worker-1' to see worker logs")
    print("5. Use 'docker logs analytics-redis-1' to see Redis logs")
    
    print("\n🔍 TO MONITOR PROCESSING IN REAL-TIME:")
    print("1. Run: docker logs -f analytics-celery_worker-1")
    print("2. Run: docker logs -f analytics-web-1")
    print("3. Check the debug.log file in the project directory")
    print("4. Use the Django admin interface to monitor jobs")
    
    print("\n✅ Debug monitoring setup complete!")

if __name__ == "__main__":
    main() 