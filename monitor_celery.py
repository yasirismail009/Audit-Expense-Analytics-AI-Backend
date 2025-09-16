#!/usr/bin/env python

import os
import django
import time

# Setup Django
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'analytics.settings')
django.setup()

def monitor_celery():
    """Monitor Celery workers and tasks"""
    
    print("🔍 Celery Worker Monitoring")
    print("=" * 50)
    
    try:
        from celery import current_app
        
        # Get worker stats
        inspect = current_app.control.inspect()
        
        print("📊 Worker Status:")
        stats = inspect.stats()
        if stats:
            for worker, info in stats.items():
                print(f"  Worker: {worker}")
                print(f"    Pool: {info.get('pool', {}).get('max-concurrency', 'N/A')} processes")
                print(f"    Total Tasks: {info.get('total', 'N/A')}")
                print(f"    Clock: {info.get('clock', 'N/A')}")
        else:
            print("  ❌ No workers found or workers not responding")
        
        print("\n🔄 Active Tasks:")
        active = inspect.active()
        if active:
            for worker, tasks in active.items():
                print(f"  Worker: {worker}")
                if tasks:
                    for task in tasks:
                        print(f"    Task: {task.get('name', 'Unknown')}")
                        print(f"    ID: {task.get('id', 'Unknown')}")
                        print(f"    Args: {task.get('args', [])}")
                else:
                    print("    No active tasks")
        else:
            print("  No active tasks")
        
        print("\n📋 Reserved Tasks:")
        reserved = inspect.reserved()
        if reserved:
            for worker, tasks in reserved.items():
                print(f"  Worker: {worker}")
                if tasks:
                    for task in tasks:
                        print(f"    Task: {task.get('name', 'Unknown')}")
                        print(f"    ID: {task.get('id', 'Unknown')}")
                else:
                    print("    No reserved tasks")
        else:
            print("  No reserved tasks")
        
        print("\n🎯 Registered Tasks:")
        registered = inspect.registered()
        if registered:
            for worker, tasks in registered.items():
                print(f"  Worker: {worker}")
                task_names = [task for task in tasks if 'completeness' in task.lower()]
                if task_names:
                    print("    Completeness Tasks:")
                    for task in task_names:
                        print(f"      - {task}")
                else:
                    print("    No completeness-related tasks registered")
        
    except Exception as e:
        print(f"❌ Error monitoring Celery: {e}")
        import traceback
        traceback.print_exc()

def check_task_by_id(task_id):
    """Check specific task status"""
    try:
        from celery.result import AsyncResult
        
        result = AsyncResult(task_id)
        print(f"\n🔍 Task {task_id}:")
        print(f"  Status: {result.status}")
        print(f"  Result: {result.result}")
        print(f"  Ready: {result.ready()}")
        print(f"  Successful: {result.successful()}")
        
    except Exception as e:
        print(f"❌ Error checking task {task_id}: {e}")

if __name__ == '__main__':
    monitor_celery()
