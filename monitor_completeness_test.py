#!/usr/bin/env python
"""
Monitor completeness test progress in real-time
"""
import os
import django
import time
from datetime import datetime

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'analytics.settings')
django.setup()

from core.models import DataFile, Engagement, CompletenessTestResult
from uuid import UUID

def monitor_test_progress():
    engagement_id_str = 'ENG-008'
    gl_file_id_str = '51671285-0c2e-4c26-ae59-0292a21fd113'
    
    print("📊 Real-time Completeness Test Monitor for ENG-008")
    print("=" * 60)
    print("Press Ctrl+C to stop monitoring...")
    print("=" * 60)
    
    try:
        engagement = Engagement.objects.get(engagement_id=engagement_id_str)
        gl_file = DataFile.objects.get(id=UUID(gl_file_id_str))
        
        print(f"🎯 Monitoring: {engagement.engagement_id}")
        print(f"📊 GL File: {gl_file.file_name}")
        print(f"⏰ Start Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print()
        
        last_count = 0
        start_time = time.time()
        
        while True:
            current_time = datetime.now().strftime('%H:%M:%S')
            elapsed = time.time() - start_time
            
            # Check for new completeness test results
            current_results = CompletenessTestResult.objects.filter(
                engagement=engagement
            ).order_by('-test_timestamp')
            
            current_count = current_results.count()
            
            if current_count > last_count:
                print(f"🆕 [{current_time}] New test result detected!")
                
                latest_result = current_results.first()
                if latest_result:
                    print(f"   📋 Test ID: {latest_result.id}")
                    print(f"   ⏰ Timestamp: {latest_result.test_timestamp.strftime('%Y-%m-%d %H:%M:%S')}")
                    print(f"   🎯 Status: {latest_result.overall_status}")
                    print(f"   📈 Score: {latest_result.completeness_score:.1f}%")
                    print(f"   🧪 Total Tests: {latest_result.total_tests}")
                    print(f"   📊 GL Records: {latest_result.total_gl_records:,}")
                    print(f"   📋 TB Records: {latest_result.total_tb_records:,}")
                    print(f"   ⏱️  Processing Duration: {latest_result.processing_duration:.2f}s")
                    
                    # Show step details if available
                    if hasattr(latest_result, 'step1_file_completeness') and latest_result.step1_file_completeness:
                        step1 = latest_result.step1_file_completeness
                        print(f"   🔍 Step 1: {'✅ PASS' if step1.get('passed', False) else '❌ FAIL'}")
                        if 'gl_net_balance' in step1:
                            print(f"      GL Balance: {step1['gl_net_balance']:.2f}")
                    
                    if hasattr(latest_result, 'step2_gl_tb_reconciliation') and latest_result.step2_gl_tb_reconciliation:
                        step2 = latest_result.step2_gl_tb_reconciliation
                        print(f"   🔍 Step 2: {'✅ PASS' if step2.get('passed', False) else '❌ FAIL'}")
                        if 'pass_rate' in step2:
                            print(f"      Pass Rate: {step2['pass_rate']:.1%}")
                    
                    print(f"   📝 Explanation: {getattr(latest_result, 'explanation', 'N/A')}")
                    print("   " + "="*50)
                
                last_count = current_count
                
                # If we got a result, the test is complete
                if current_count > 0:
                    print(f"✅ Test completed! Total monitoring time: {elapsed:.2f} seconds")
                    break
            else:
                # Show monitoring status
                dots = "." * (int(elapsed) % 4)
                print(f"⏳ [{current_time}] Monitoring{dots:<3} (Elapsed: {elapsed:.0f}s, Results: {current_count})", end="\r")
            
            time.sleep(2)  # Check every 2 seconds
            
    except KeyboardInterrupt:
        print(f"\n🛑 Monitoring stopped by user")
    except Exception as e:
        print(f"❌ Monitoring error: {e}")
        import traceback
        traceback.print_exc()

def quick_status_check():
    """Quick status check without continuous monitoring"""
    engagement_id_str = 'ENG-008'
    
    try:
        engagement = Engagement.objects.get(engagement_id=engagement_id_str)
        results = CompletenessTestResult.objects.filter(
            engagement=engagement
        ).order_by('-test_timestamp')
        
        print(f"📊 Quick Status Check for {engagement_id_str}")
        print("=" * 50)
        
        if results.exists():
            latest = results.first()
            print(f"📋 Latest Test Result:")
            print(f"   ⏰ Timestamp: {latest.test_timestamp.strftime('%Y-%m-%d %H:%M:%S')}")
            print(f"   🎯 Status: {latest.overall_status}")
            print(f"   📈 Score: {latest.completeness_score:.1f}%")
            print(f"   🧪 Total Tests: {latest.total_tests}")
            print(f"   ⏱️  Duration: {latest.processing_duration:.2f}s")
            
            # Check if it's recent (within last 5 minutes)
            time_diff = (datetime.now().replace(tzinfo=latest.test_timestamp.tzinfo) - latest.test_timestamp).total_seconds()
            if time_diff < 300:  # 5 minutes
                print(f"   🆕 Recent test (completed {time_diff:.0f} seconds ago)")
            else:
                print(f"   ⏰ Older test (completed {time_diff/60:.1f} minutes ago)")
                
        else:
            print("   ℹ️  No test results found")
            
        print(f"\n📊 Total Results in DB: {results.count()}")
        
    except Exception as e:
        print(f"❌ Status check error: {e}")

if __name__ == '__main__':
    import sys
    
    if len(sys.argv) > 1 and sys.argv[1] == 'quick':
        quick_status_check()
    else:
        monitor_test_progress()
