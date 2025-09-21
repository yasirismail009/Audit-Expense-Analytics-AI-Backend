#!/usr/bin/env python
"""
Test script to trigger ENG-008 completeness task
"""

import os
import sys
import django

# Set up Django
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'analytics.settings')
django.setup()

from core.tasks import trigger_eng008_completeness

def test_trigger():
    """Test triggering ENG-008 completeness task"""
    print("🎯 Testing ENG-008 Completeness Trigger")
    print("=" * 50)
    
    # Trigger the task
    result = trigger_eng008_completeness.delay()
    
    print(f"✅ Task triggered!")
    print(f"📋 Task ID: {result.id}")
    print(f"🔗 Task Status: {result.status}")
    
    # Wait a bit and check result
    import time
    time.sleep(2)
    
    try:
        task_result = result.get(timeout=5)
        print(f"📊 Task Result: {task_result}")
    except Exception as e:
        print(f"⏳ Task still running or error: {e}")

if __name__ == '__main__':
    test_trigger()
