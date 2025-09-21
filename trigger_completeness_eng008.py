#!/usr/bin/env python
"""
Simple script to trigger completeness test for ENG-008
Usage: python trigger_completeness_eng008.py
"""

import os
import sys
import django

# Add the project directory to the Python path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# Set up Django
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'analytics.settings')
django.setup()

from core.tasks.completeness_tasks import run_gl_completeness_analysis
from core.models import Engagement, DataFile

def trigger_completeness_for_eng008():
    """Trigger completeness test for ENG-008"""
    
    try:
        # Get ENG-008 engagement
        engagement = Engagement.objects.get(engagement_id='ENG-008')
        print(f"✅ Found engagement: {engagement.engagement_id} - {engagement.engagement_name}")
        
        # Get the latest data file for this engagement
        latest_data_file = DataFile.objects.filter(
            engagement=engagement
        ).order_by('-created_at').first()
        
        if not latest_data_file:
            print("❌ No data file found for ENG-008")
            return
        
        print(f"📁 Using data file: {latest_data_file.file_name} (ID: {latest_data_file.id})")
        
        # Trigger the completeness analysis task
        print("🚀 Triggering completeness analysis task...")
        task_result = run_gl_completeness_analysis.delay(latest_data_file.id)
        
        print(f"✅ Task triggered successfully!")
        print(f"📋 Task ID: {task_result.id}")
        print(f"🔗 Task Status: {task_result.status}")
        
        return task_result.id
        
    except Engagement.DoesNotExist:
        print("❌ ENG-008 engagement not found")
        return None
    except Exception as e:
        print(f"❌ Error: {e}")
        return None

if __name__ == '__main__':
    print("🎯 Triggering Completeness Test for ENG-008")
    print("=" * 50)
    
    task_id = trigger_completeness_for_eng008()
    
    if task_id:
        print("=" * 50)
        print("✅ Completeness test triggered successfully!")
        print(f"📋 Monitor task with ID: {task_id}")
    else:
        print("=" * 50)
        print("❌ Failed to trigger completeness test")
