#!/usr/bin/env python
"""
Script to start Celery worker and broker for backdated analysis processing
"""

import os
import sys
import subprocess
import time
import signal
import threading

def start_redis_server():
    """Start Redis server if not running"""
    print("🔧 Starting Redis server...")
    try:
        # Try to start Redis server
        redis_process = subprocess.Popen(
            ["redis-server", "--port", "6379"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )
        print("✅ Redis server started")
        return redis_process
    except FileNotFoundError:
        print("⚠️  Redis server not found. Please install Redis or use an existing Redis instance.")
        return None

def start_celery_worker():
    """Start Celery worker"""
    print("🔧 Starting Celery worker...")
    try:
        # Set Django settings
        os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'analytics.settings')
        
        # Start Celery worker
        worker_process = subprocess.Popen([
            sys.executable, "-m", "celery", "-A", "analytics", "worker",
            "--loglevel=info",
            "--concurrency=2",
            "--pool=solo"
        ])
        print("✅ Celery worker started")
        return worker_process
    except Exception as e:
        print(f"❌ Error starting Celery worker: {e}")
        return None

def start_celery_beat():
    """Start Celery beat scheduler"""
    print("🔧 Starting Celery beat scheduler...")
    try:
        # Set Django settings
        os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'analytics.settings')
        
        # Start Celery beat
        beat_process = subprocess.Popen([
            sys.executable, "-m", "celery", "-A", "analytics", "beat",
            "--loglevel=info"
        ])
        print("✅ Celery beat scheduler started")
        return beat_process
    except Exception as e:
        print(f"❌ Error starting Celery beat: {e}")
        return None

def monitor_processes(processes):
    """Monitor running processes"""
    try:
        while True:
            for name, process in processes.items():
                if process and process.poll() is not None:
                    print(f"⚠️  {name} has stopped")
            time.sleep(5)
    except KeyboardInterrupt:
        print("\n🛑 Stopping all processes...")
        for name, process in processes.items():
            if process:
                process.terminate()
                print(f"✅ {name} stopped")

def main():
    """Main function to start all services"""
    print("🚀 Starting Celery services for backdated analysis...")
    print("=" * 50)
    
    processes = {}
    
    # Start Redis server
    redis_process = start_redis_server()
    if redis_process:
        processes['Redis Server'] = redis_process
        time.sleep(2)  # Give Redis time to start
    
    # Start Celery worker
    worker_process = start_celery_worker()
    if worker_process:
        processes['Celery Worker'] = worker_process
    
    # Start Celery beat scheduler
    beat_process = start_celery_beat()
    if beat_process:
        processes['Celery Beat'] = beat_process
    
    print("\n" + "=" * 50)
    print("✅ All services started successfully!")
    print("📋 Running processes:")
    for name, process in processes.items():
        if process:
            print(f"   - {name}: PID {process.pid}")
    
    print("\n🔍 Monitoring processes... (Press Ctrl+C to stop)")
    print("=" * 50)
    
    # Monitor processes
    monitor_processes(processes)

if __name__ == "__main__":
    main() 