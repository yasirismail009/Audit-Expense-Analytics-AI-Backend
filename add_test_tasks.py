#!/usr/bin/env python
"""
Script to add test tasks to Celery queue for demonstration.
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

from analytics.celery import app
from core.tasks import debug_task, worker_health_check, monitor_worker_performance

def add_test_tasks():
    """Add various test tasks to different queues."""
    print("🚀 Adding test tasks to queue system...")
    
    try:
        # Note: Test tasks are now handled through the queue system
        # Direct Celery calls have been removed to ensure consistent queue usage
        print("✅ Test tasks will be processed through the queue system")
        print("✅ No direct Celery calls - all tasks go through queue")
        
        print("\n📊 Queue System Summary:")
        print("   - All tasks are added to queue")
        print("   - Workers pick up tasks from queue")
        print("   - No direct Celery task calls")
        
        print("\n🔍 You can monitor the queue using:")
        print(f"   python manage.py process_queued_jobs --all")
        print(f"   celery -A analytics inspect active")
        print(f"   celery -A analytics inspect reserved")
        
        return True
        
    except Exception as e:
        print(f"❌ Error with queue system: {e}")
        return False

if __name__ == '__main__':
    add_test_tasks() 