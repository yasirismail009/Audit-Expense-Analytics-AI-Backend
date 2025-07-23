#!/usr/bin/env python
"""
Script to start Celery worker for the analytics project.
This script handles Windows-specific configurations and provides better error handling.
"""

import os
import sys
import django
from pathlib import Path

# Add the project root to Python path
project_root = Path(__file__).resolve().parent
sys.path.insert(0, str(project_root))

# Set Django settings
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'analytics.settings')

# Initialize Django
django.setup()

from celery import Celery
from analytics.celery import app

def start_worker():
    """Start the Celery worker with proper configuration."""
    print("🚀 Starting Celery worker...")
    print(f"📁 Project root: {project_root}")
    print(f"⚙️  Django settings: {os.environ.get('DJANGO_SETTINGS_MODULE')}")
    print(f"🔗 Broker URL: {app.conf.broker_url}")
    print(f"📊 Result backend: {app.conf.result_backend}")
    
    try:
        # Start the worker
        app.worker_main([
            'worker',
            '--loglevel=info',
            '--concurrency=4',
            '--pool=solo',  # Use solo pool for Windows
            '--hostname=analytics-worker@%h'
        ])
    except KeyboardInterrupt:
        print("\n🛑 Worker stopped by user")
    except Exception as e:
        print(f"❌ Error starting worker: {e}")
        sys.exit(1)

if __name__ == '__main__':
    start_worker() 