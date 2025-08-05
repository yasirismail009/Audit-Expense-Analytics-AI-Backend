#!/usr/bin/env python
"""
Simple Celery Startup Script

This script starts Celery worker using in-memory broker (no Redis required).
"""

import os
import sys
import subprocess
import time
import signal
import psutil

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

def start_celery_worker():
    """Start Celery worker"""
    print("Starting Celery worker...")
    
    # Set Django settings
    os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'analytics.settings')
    
    # Start Celery worker with in-memory broker
    cmd = [
        sys.executable, '-m', 'celery', '-A', 'analytics', 'worker',
        '--loglevel=info',
        '--concurrency=4',
        '--pool=prefork',
        '--hostname=worker1@%h'
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

def test_celery_connection():
    """Test if Celery is working"""
    try:
        import django
        django.setup()
        
        from analytics.celery import app
        from core.tasks import debug_task
        
        # Test a simple task
        result = debug_task.delay()
        print(f"✓ Celery connection test successful - Task ID: {result.id}")
        return True
        
    except Exception as e:
        print(f"✗ Celery connection test failed: {e}")
        return False

def main():
    """Main function"""
    print("="*60)
    print("SIMPLE CELERY WORKER STARTUP")
    print("="*60)
    
    # Check if already running
    existing_pid = check_celery_worker()
    if existing_pid:
        print(f"Celery worker already running with PID: {existing_pid}")
        return
    
    # Start Celery worker
    process = start_celery_worker()
    if not process:
        print("Failed to start Celery worker")
        return
    
    # Wait a moment for startup
    print("Waiting for Celery worker to start...")
    time.sleep(5)
    
    # Test connection
    if test_celery_connection():
        print("✓ Celery is working and listening for tasks!")
    else:
        print("✗ Celery connection test failed")
    
    print("\nCelery worker is running. Press Ctrl+C to stop.")
    
    # Keep running
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\nStopping Celery worker...")
        process.terminate()
        process.wait()
        print("Celery worker stopped")

if __name__ == "__main__":
    main() 