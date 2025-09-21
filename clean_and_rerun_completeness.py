#!/usr/bin/env python
"""
Clean completeness test results from database and run test again for ENG-008
"""

import os
import sys
import django

# Set up Django
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'analytics.settings')
django.setup()

from core.models import CompletenessTestResult, DataFile, Engagement
from core.tasks import run_eng008_complete_workflow

def clean_completeness_results():
    """Clean all completeness test results from database"""
    try:
        print("🧹 Cleaning completeness test results from database...")
        
        # Get ENG-008 engagement
        engagement = Engagement.objects.get(engagement_id='ENG-008')
        print(f"✅ Found engagement: {engagement.engagement_id} - {engagement.engagement_name}")
        
        # Count existing results
        existing_results = CompletenessTestResult.objects.filter(engagement=engagement)
        count = existing_results.count()
        print(f"📊 Found {count} existing completeness test results")
        
        if count > 0:
            # Delete all completeness test results for ENG-008
            deleted_count = existing_results.delete()[0]
            print(f"🗑️  Deleted {deleted_count} completeness test results")
        else:
            print("ℹ️  No existing completeness test results found")
        
        return True
        
    except Engagement.DoesNotExist:
        print("❌ ENG-008 engagement not found")
        return False
    except Exception as e:
        print(f"❌ Error cleaning completeness results: {e}")
        return False

def run_fresh_completeness_test():
    """Run a fresh completeness test for ENG-008"""
    try:
        print("\n🚀 Running fresh completeness test for ENG-008...")
        
        # Trigger the complete workflow
        result = run_eng008_complete_workflow.delay()
        
        print(f"✅ Fresh completeness test triggered!")
        print(f"📋 Task ID: {result.id}")
        print(f"🔗 Task Status: {result.status}")
        
        return result.id
        
    except Exception as e:
        print(f"❌ Error running fresh completeness test: {e}")
        return None

def main():
    """Main function to clean and rerun completeness test"""
    print("🎯 Clean and Rerun Completeness Test for ENG-008")
    print("=" * 60)
    
    # Step 1: Clean existing results
    if not clean_completeness_results():
        print("❌ Failed to clean existing results. Exiting.")
        return
    
    # Step 2: Run fresh test
    task_id = run_fresh_completeness_test()
    
    if task_id:
        print("\n" + "=" * 60)
        print("✅ Clean and rerun completed successfully!")
        print(f"📋 Monitor task with ID: {task_id}")
        print("🔍 Check worker logs for detailed progress")
    else:
        print("\n" + "=" * 60)
        print("❌ Failed to run fresh completeness test")

if __name__ == '__main__':
    main()
