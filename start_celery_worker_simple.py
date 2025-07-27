#!/usr/bin/env python
"""
Simplified Celery worker with automatic queue listening and monitoring.
"""

import os
import sys
import django
import time
import threading
import signal
from pathlib import Path
from datetime import datetime

# Add the project root to Python path
project_root = Path(__file__).resolve().parent
sys.path.insert(0, str(project_root))

# Set Django settings
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'analytics.settings')

# Initialize Django
django.setup()

from analytics.celery import app

# Global flag to control monitoring
monitoring_active = True

def signal_handler(signum, frame):
    """Handle shutdown signals gracefully."""
    global monitoring_active
    print(f"\n🛑 Received signal {signum}, shutting down gracefully...")
    monitoring_active = False

def monitor_queue():
    """Monitor queue status every 15 seconds."""
    global monitoring_active
    
    print("📊 Starting queue monitoring...")
    
    while monitoring_active:
        try:
            inspect = app.control.inspect()
            
            # Get queue stats
            stats = inspect.stats()
            active = inspect.active()
            reserved = inspect.reserved()
            
            print(f"\n Queue Status - {datetime.now().strftime('%H:%M:%S')}")
            print("-" * 40)
            
            if stats:
                print(f"👥 Workers: {len(stats)}")
                total_active = sum(len(tasks) for tasks in active.values()) if active else 0
                total_reserved = sum(len(tasks) for tasks in reserved.values()) if reserved else 0
                print(f"🔄 Active: {total_active} | ⏳ Reserved: {total_reserved}")
            else:
                print("👥 No workers active")
            
            print("-" * 40)
            
        except Exception as e:
            print(f"❌ Monitoring error: {e}")
        
        # Wait 15 seconds
        for _ in range(15):
            if not monitoring_active:
                break
            time.sleep(1)

def start_worker():
    """Start worker with monitoring."""
    global monitoring_active
    
    print("🚀 Starting Celery worker with monitoring...")
    print(f"👥 Workers: 12 | 🔄 Auto-listen: Enabled")
    
    # Set up signal handlers
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    # Start monitoring in background
    monitor_thread = threading.Thread(target=monitor_queue, daemon=True)
    monitor_thread.start()
    
    try:
        # Start Celery worker (this will block but monitoring runs in background)
        app.worker_main([
            'worker',
            '--loglevel=info',
            '--concurrency=12',
            '--pool=solo',
            '--hostname=analytics-worker@%h',
            '--queues=analytics,ml_training,maintenance',
            '--prefetch-multiplier=1',
        ])
    except KeyboardInterrupt:
        print("\n🛑 Worker stopped by user")
    finally:
        monitoring_active = False

if __name__ == '__main__':
    start_worker() 