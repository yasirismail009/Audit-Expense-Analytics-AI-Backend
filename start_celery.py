#!/usr/bin/env python
"""
Celery Startup Script

This script starts Celery worker and ensures it's always listening for tasks.
"""

import os
import sys
import subprocess
import time
import signal
import psutil
from pathlib import Path

def check_redis():
    """Check if Redis is running"""
    try:
        import redis
        r = redis.Redis(host='localhost', port=6379, db=0)
        r.ping()
        print("✓ Redis is running")
        return True
    except Exception as e:
        print(f"✗ Redis is not running: {e}")
        return False

def check_celery_worker():
    """Check if Celery worker is already running"""
    for proc in psutil.process_iter(['pid', 'name', 'cmdline']):
        try:
            if proc.info['name'] and 'celery' in proc.info['name'].lower():
                cmdline = ' '.join(proc.info['cmdline'] or [])
                if 'worker' in cmdline and 'analytics' in cmdline:
                    print(f"✓ Celery worker already running (PID: {proc.info['pid']})")
                    return proc.info['pid']
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            continue
    return None

def start_redis():
    """Start Redis if not running"""
    if not check_redis():
        print("Starting Redis...")
        try:
            # Try to start Redis (Windows)
            subprocess.run(['redis-server'], check=False, capture_output=True)
            time.sleep(2)
            if check_redis():
                print("✓ Redis started successfully")
                return True
        except FileNotFoundError:
            print("Redis not found. Please install Redis or start it manually.")
            return False
    return True

def start_celery_worker():
    """Start Celery worker"""
    print("Starting Celery worker...")
    
    # Set Django settings
    os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'analytics.settings')
    
    # Start Celery worker
    cmd = [
        sys.executable, '-m', 'celery', '-A', 'analytics', 'worker',
        '--loglevel=info',
        '--concurrency=4',
        '--pool=prefork',
        '--hostname=worker1@%h',
        '--queues=default,analysis,training'
    ]
    
    try:
        process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            bufsize=1,
            universal_newlines=True
        )
        
        print(f"✓ Celery worker started (PID: {process.pid})")
        return process
        
    except Exception as e:
        print(f"✗ Failed to start Celery worker: {e}")
        return None

def monitor_celery(process):
    """Monitor Celery worker and restart if needed"""
    print("Monitoring Celery worker...")
    
    while True:
        if process.poll() is not None:
            print("Celery worker stopped. Restarting...")
            process = start_celery_worker()
            if not process:
                print("Failed to restart Celery worker")
                break
        
        # Check if process is still responsive
        try:
            # Send a simple signal to check if process is alive
            process.send_signal(signal.SIGCONT)
        except:
            print("Celery worker not responding. Restarting...")
            process = start_celery_worker()
            if not process:
                print("Failed to restart Celery worker")
                break
        
        time.sleep(10)  # Check every 10 seconds

def main():
    """Main function"""
    print("="*60)
    print("CELERY WORKER STARTUP SCRIPT")
    print("="*60)
    
    # Check if already running
    existing_pid = check_celery_worker()
    if existing_pid:
        print(f"Celery worker already running with PID: {existing_pid}")
        return
    
    # Start Redis
    if not start_redis():
        print("Cannot start Celery without Redis")
        return
    
    # Start Celery worker
    process = start_celery_worker()
    if not process:
        print("Failed to start Celery worker")
        return
    
    # Monitor and restart if needed
    try:
        monitor_celery(process)
    except KeyboardInterrupt:
        print("\nStopping Celery worker...")
        process.terminate()
        process.wait()
        print("Celery worker stopped")

if __name__ == "__main__":
    main() 