#!/usr/bin/env python
"""
Script to start Celery worker for the analytics project with Redis configuration.
This script handles Windows-specific configurations and provides better error handling.
Includes queue monitoring every 15 seconds.
"""

import os
import sys
import django
import time
import threading
from pathlib import Path
from datetime import datetime

# Add the project root to Python path
project_root = Path(__file__).resolve().parent
sys.path.insert(0, str(project_root))

# Set Django settings
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'analytics.settings')

# Initialize Django
django.setup()

from celery import Celery
from analytics.celery import app

def check_queue_status():
    """Check the status of Celery queues every 15 seconds."""
    while True:
        try:
            # Get queue information
            inspect = app.control.inspect()
            
            # Get active tasks
            active = inspect.active()
            # Get reserved tasks
            reserved = inspect.reserved()
            # Get registered tasks
            registered = inspect.registered()
            # Get stats
            stats = inspect.stats()
            
            print(f"\n📊 Queue Status Check - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            print("-" * 60)
            
            # Check active tasks
            if active:
                total_active = sum(len(tasks) for tasks in active.values())
                print(f"🔄 Active Tasks: {total_active}")
                for worker, tasks in active.items():
                    if tasks:
                        print(f"   Worker {worker}: {len(tasks)} active tasks")
            else:
                print("🔄 Active Tasks: 0")
            
            # Check reserved tasks
            if reserved:
                total_reserved = sum(len(tasks) for tasks in reserved.values())
                print(f"⏳ Reserved Tasks: {total_reserved}")
                for worker, tasks in reserved.items():
                    if tasks:
                        print(f"   Worker {worker}: {len(tasks)} reserved tasks")
            else:
                print("⏳ Reserved Tasks: 0")
            
            # Check registered tasks
            if registered:
                total_registered = sum(len(tasks) for tasks in registered.values())
                print(f"📝 Registered Tasks: {total_registered}")
                for worker, tasks in registered.items():
                    if tasks:
                        print(f"   Worker {worker}: {len(tasks)} registered tasks")
            else:
                print("📝 Registered Tasks: 0")
            
            # Check worker stats
            if stats:
                print(f"👥 Active Workers: {len(stats)}")
                for worker, stat in stats.items():
                    print(f"   Worker {worker}: {stat.get('pool', {}).get('processes', 0)} processes")
            else:
                print("👥 Active Workers: 0")
            
            print("-" * 60)
            
        except Exception as e:
            print(f"❌ Error checking queue status: {e}")
        
        # Wait 15 seconds before next check
        time.sleep(15)

def start_worker():
    """Start the Celery worker with proper configuration and queue monitoring."""
    print("🚀 Starting Celery worker with Redis configuration...")
    print(f"📁 Project root: {project_root}")
    print(f"⚙️  Django settings: {os.environ.get('DJANGO_SETTINGS_MODULE')}")
    print(f"🔗 Broker URL: {app.conf.broker_url}")
    print(f"📊 Result backend: {app.conf.result_backend}")
    print(f"⏰ Queue monitoring: Every 15 seconds")
    
    # Start queue monitoring in a separate thread
    monitor_thread = threading.Thread(target=check_queue_status, daemon=True)
    monitor_thread.start()
    print("📊 Queue monitoring started in background thread")
    
    try:
        # Start the worker with multiple queues
        app.worker_main([
            'worker',
            '--loglevel=info',
            '--concurrency=4',
            '--pool=solo',  # Use solo pool for Windows
            '--hostname=analytics-worker@%h',
            '--queues=analytics,ml_training,maintenance'
        ])
    except KeyboardInterrupt:
        print("\n🛑 Worker stopped by user")
    except Exception as e:
        print(f"❌ Error starting worker: {e}")
        sys.exit(1)

if __name__ == '__main__':
    start_worker() 