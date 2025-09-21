#!/usr/bin/env python
"""
Test script to verify task registration
"""
import os
import sys
import django

# Set up Django
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'analytics.settings')
django.setup()

# Import Celery app
from analytics.celery import app

def test_task_registration():
    """Test if tasks are properly registered"""
    print("Testing Celery task registration...")
    
    # Get all registered tasks
    registered_tasks = list(app.tasks.keys())
    print(f"Total registered tasks: {len(registered_tasks)}")
    
    # Check for specific tasks
    target_tasks = [
        'core.tasks.process_queued_jobs',
        'core.tasks.monitor_processing_jobs',
        'core.tasks.worker_health_check',
        'core.tasks.monitor_worker_performance'
    ]
    
    print("\nChecking for specific tasks:")
    for task_name in target_tasks:
        if task_name in registered_tasks:
            print(f"✓ {task_name} - REGISTERED")
        else:
            print(f"✗ {task_name} - NOT REGISTERED")
    
    print("\nAll registered tasks:")
    for task in sorted(registered_tasks):
        if not task.startswith('celery.'):
            print(f"  - {task}")
    
    # Test direct import
    try:
        from core.tasks.monitoring_tasks import process_queued_jobs
        print(f"\n✓ Successfully imported process_queued_jobs directly: {process_queued_jobs}")
        print(f"  Task name: {process_queued_jobs.name}")
        print(f"  Is registered: {process_queued_jobs.name in registered_tasks}")
    except ImportError as e:
        print(f"\n✗ Failed to import process_queued_jobs: {e}")

if __name__ == '__main__':
    test_task_registration()
