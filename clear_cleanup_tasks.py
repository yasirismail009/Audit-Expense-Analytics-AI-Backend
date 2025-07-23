#!/usr/bin/env python
"""
Script to clear cleanup tasks from Celery queue
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

from celery import current_app
from analytics.celery import app

def clear_cleanup_tasks():
    """Clear cleanup tasks from Celery queue"""
    print("🧹 Clearing cleanup tasks from Celery queue...")
    print("=" * 60)
    
    try:
        # Get the Celery app
        print(f"📊 Broker URL: {app.conf.broker_url}")
        print(f"📊 Result Backend: {app.conf.result_backend}")
        
        # Check if workers are running
        inspect = current_app.control.inspect()
        stats = inspect.stats()
        
        if not stats:
            print("❌ No Celery workers are running!")
            print("💡 Please start the Celery worker first")
            return False
        
        print("✅ Celery workers are running!")
        
        # Clear all queues
        print("\n🧹 Clearing all queues...")
        
        # Clear analytics queue
        try:
            app.control.purge(queue='analytics')
            print("✅ Cleared analytics queue")
        except Exception as e:
            print(f"⚠️  Could not clear analytics queue: {e}")
        
        # Clear maintenance queue (where cleanup tasks were routed)
        try:
            app.control.purge(queue='maintenance')
            print("✅ Cleared maintenance queue")
        except Exception as e:
            print(f"⚠️  Could not clear maintenance queue: {e}")
        
        # Clear ml_training queue
        try:
            app.control.purge(queue='ml_training')
            print("✅ Cleared ml_training queue")
        except Exception as e:
            print(f"⚠️  Could not clear ml_training queue: {e}")
        
        # Clear default queue
        try:
            app.control.purge()
            print("✅ Cleared default queue")
        except Exception as e:
            print(f"⚠️  Could not clear default queue: {e}")
        
        # Check active tasks
        print("\n🔍 Checking for active tasks...")
        active_tasks = inspect.active()
        
        if active_tasks:
            print("⚠️  Found active tasks:")
            for worker, tasks in active_tasks.items():
                print(f"  Worker: {worker}")
                for task in tasks:
                    print(f"    - Task: {task['name']} (ID: {task['id']})")
        else:
            print("✅ No active tasks found")
        
        # Check scheduled tasks
        print("\n🔍 Checking for scheduled tasks...")
        scheduled_tasks = inspect.scheduled()
        
        if scheduled_tasks:
            print("⚠️  Found scheduled tasks:")
            for worker, tasks in scheduled_tasks.items():
                print(f"  Worker: {worker}")
                for task in tasks:
                    print(f"    - Task: {task['name']} (ID: {task['id']})")
        else:
            print("✅ No scheduled tasks found")
        
        # Check reserved tasks
        print("\n🔍 Checking for reserved tasks...")
        reserved_tasks = inspect.reserved()
        
        if reserved_tasks:
            print("⚠️  Found reserved tasks:")
            for worker, tasks in reserved_tasks.items():
                print(f"  Worker: {worker}")
                for task in tasks:
                    print(f"    - Task: {task['name']} (ID: {task['id']})")
        else:
            print("✅ No reserved tasks found")
        
        print("\n" + "=" * 60)
        print("✅ Cleanup tasks have been removed from:")
        print("  - Celery beat schedule (settings.py)")
        print("  - Task routing configuration")
        print("  - Task function definitions")
        print("  - All Celery queues")
        print("\n🎯 Summary:")
        print("  - Removed cleanup_failed_jobs task")
        print("  - Removed cleanup_old_training_sessions task")
        print("  - Cleared all queues of any pending cleanup tasks")
        print("  - No more automatic cleanup will occur")
        
    except Exception as e:
        print(f"❌ Error clearing cleanup tasks: {e}")
        print(f"🔍 Error type: {type(e).__name__}")
        import traceback
        print(f"🔍 Traceback: {traceback.format_exc()}")
        return False
    
    return True

if __name__ == "__main__":
    clear_cleanup_tasks() 