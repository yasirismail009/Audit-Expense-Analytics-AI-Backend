#!/usr/bin/env python
"""
Clear old 7-step test results and run new 2-step completeness test
"""
import os
import django
import time
from datetime import datetime

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'analytics.settings')
django.setup()

from core.models import DataFile, Engagement, CompletenessTestResult
from core.tasks import run_gl_completeness_analysis
from uuid import UUID

def clear_and_run_new_test():
    engagement_id_str = 'ENG-008'
    gl_file_id_str = '51671285-0c2e-4c26-ae59-0292a21fd113'

    print("🧹 Clearing Old Tests & Running New 2-Step Celery Task")
    print("=" * 70)

    try:
        engagement = Engagement.objects.get(engagement_id=engagement_id_str)
        gl_file = DataFile.objects.get(id=UUID(gl_file_id_str))

        print(f"✅ Engagement: {engagement.engagement_id}")
        print(f"📊 GL File: {gl_file.file_name}")

        # STEP 1: Clear previous results
        print(f"\n🧹 STEP 1: Clearing previous completeness results...")
        previous_results = CompletenessTestResult.objects.filter(engagement=engagement)
        if previous_results.exists():
            print(f"   🗑️  Found {previous_results.count()} previous test results")
            for i, res in enumerate(previous_results):
                print(f"   📋 #{i+1}: {res.test_timestamp.strftime('%Y-%m-%d %H:%M:%S')} - Status: {res.overall_status}, Score: {res.completeness_score:.1f}%")
            deleted_count, _ = previous_results.delete()
            print(f"   ✅ Successfully deleted {deleted_count} records")
        else:
            print("   ℹ️  No previous results found for ENG-008")

        # STEP 2: Submit new Celery task
        print(f"\n🚀 STEP 2: Submitting new 2-step completeness analysis to Celery...")
        print("=" * 70)
        
        # Submit the task to Celery (asynchronous)
        task_result = run_gl_completeness_analysis.delay(gl_file_id_str)
        
        print(f"📋 ✅ Task submitted successfully!")
        print(f"   Task ID: {task_result.id}")
        print(f"   Submitted at: {datetime.now().strftime('%H:%M:%S')}")
        print(f"   Task State: {task_result.state}")
        print(f"   Celery Flower: http://localhost:5555/task/{task_result.id}")
        
        # STEP 3: Monitor task briefly
        print(f"\n⏳ STEP 3: Monitoring task for 30 seconds...")
        print("   (Task will continue running in background)")
        
        start_time = time.time()
        last_state = None
        
        for i in range(6):  # Monitor for 30 seconds (6 x 5 seconds)
            current_state = task_result.state
            elapsed = time.time() - start_time
            
            if current_state != last_state:
                print(f"   [{datetime.now().strftime('%H:%M:%S')}] State: {current_state}")
                last_state = current_state
                
            if task_result.ready():
                print(f"\n🎉 Task completed in {elapsed:.1f} seconds!")
                try:
                    result = task_result.get()
                    if result.get('success'):
                        completeness_results = result.get('completeness_results', {})
                        print(f"   🎯 Status: {completeness_results.get('status', 'Unknown')}")
                        print(f"   📈 Score: {completeness_results.get('completeness_score', 0):.1f}%")
                        print(f"   📋 Steps: {completeness_results.get('summary', {}).get('tests_passed', 'N/A')}/{completeness_results.get('summary', {}).get('total_tests', 'N/A')} passed")
                        print(f"   💾 Result ID: {result.get('test_result_id', 'N/A')}")
                    else:
                        print(f"   ❌ Task failed: {result.get('error', 'Unknown error')}")
                except Exception as e:
                    print(f"   ⚠️  Error getting result: {e}")
                break
            
            time.sleep(5)
        
        if not task_result.ready():
            print(f"\n⏳ Task still running after 30 seconds...")
            print(f"   📋 Task ID: {task_result.id}")
            print(f"   🔄 Current State: {task_result.state}")
            print(f"   💡 Check status later with:")
            print(f"      - Flower: http://localhost:5555/task/{task_result.id}")
            print(f"      - API: GET /api/completeness-test/engagement/ENG-008/")

    except Engagement.DoesNotExist:
        print(f"❌ Engagement {engagement_id_str} not found in database.")
    except DataFile.DoesNotExist:
        print(f"❌ GL DataFile {gl_file_id_str} not found in database.")
    except Exception as e:
        print(f"💥 An unexpected error occurred: {e}")
        import traceback
        traceback.print_exc()

    print(f"\n🎯 Clear and new test script completed.")

if __name__ == '__main__':
    clear_and_run_new_test()
