#!/usr/bin/env python
"""
Clear previous completeness results for ENG-008 and run fresh test
"""
import os
import django
import sys
from uuid import UUID

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'analytics.settings')
django.setup()

from core.models import DataFile, Engagement, CompletenessTestResult, SAPGLPosting, TrialBalance, GLAccount
from core.tasks import run_gl_completeness_analysis
from django.utils import timezone

def clear_and_test_eng008():
    engagement_id_str = 'ENG-008'
    gl_file_id_str = '51671285-0c2e-4c26-ae59-0292a21fd113'

    print("🧹 Clearing Previous Results & Running Fresh Test for ENG-008")
    print("=" * 70)

    try:
        engagement = Engagement.objects.get(engagement_id=engagement_id_str)
        print(f"✅ Engagement: {engagement.engagement_id}")

        gl_file = DataFile.objects.get(id=UUID(gl_file_id_str))
        print(f"📊 GL File: {gl_file.file_name}")

        # Step 1: Clear previous completeness test results
        print(f"\n🧹 STEP 1: Clearing previous completeness results...")
        previous_results = CompletenessTestResult.objects.filter(engagement=engagement)
        results_count = previous_results.count()
        
        if results_count > 0:
            print(f"   🗑️  Found {results_count} previous test results")
            
            # Show details of what we're deleting
            for i, result in enumerate(previous_results[:5], 1):  # Show first 5
                print(f"   📋 #{i}: {result.test_timestamp.strftime('%Y-%m-%d %H:%M:%S')} - Status: {result.overall_status}, Score: {result.completeness_score:.1f}%")
            
            if results_count > 5:
                print(f"   📋 ... and {results_count - 5} more results")
            
            # Delete all previous results
            deleted_count, deleted_details = previous_results.delete()
            print(f"   ✅ Successfully deleted {deleted_count} records")
            print(f"   📊 Deleted details: {deleted_details}")
        else:
            print(f"   ℹ️  No previous results found for {engagement_id_str}")

        # Step 2: Run fresh completeness analysis
        print(f"\n🚀 STEP 2: Running fresh 2-step completeness analysis...")
        print("=" * 70)
        
        start_time = timezone.now()
        
        # Call the updated task directly
        result = run_gl_completeness_analysis(gl_file_id_str)
        
        end_time = timezone.now()
        duration = (end_time - start_time).total_seconds()

        print("\n" + "=" * 70)
        print("📊 FRESH COMPLETENESS TEST RESULTS")
        print("=" * 70)

        if result.get('success'):
            completeness_results = result.get('completeness_results', {})
            
            print(f"🎯 Overall Status: {completeness_results.get('status', 'Unknown')}")
            print(f"📈 Completeness Score: {completeness_results.get('completeness_score', 0):.1f}%")
            print(f"⏱️  Processing Duration: {completeness_results.get('processing_duration', 0):.2f} seconds")
            print(f"📝 Explanation: {completeness_results.get('explanation', 'N/A')}")
            
            # Step results
            step1_result = completeness_results.get('step1_completeness', {})
            step2_result = completeness_results.get('step2_account_verification', {})
            
            print(f"\n📋 Step-by-Step Results:")
            print(f"   🔍 Step 1 (GL Completeness): {'✅ PASS' if step1_result.get('passed', False) else '❌ FAIL'}")
            print(f"      GL Balance: {step1_result.get('gl_net_balance', 0):.2f}")
            print(f"      Transactions: {step1_result.get('transaction_count', 0):,}")
            print(f"      Accounts: {step1_result.get('account_count', 0):,}")
            
            print(f"   🔍 Step 2 (Account Verification): {'✅ PASS' if step2_result.get('passed', False) else '❌ FAIL'}")
            print(f"      Accounts Verified: {step2_result.get('total_accounts_verified', 0)}")
            print(f"      Accounts Passed: {step2_result.get('accounts_passed', 0)}")
            print(f"      Pass Rate: {step2_result.get('pass_rate', 0):.1%}")
            print(f"      Total Variance: {step2_result.get('total_variance', 0):.2f}")
            
            # Scoring breakdown
            scoring = completeness_results.get('scoring_breakdown', {})
            if scoring:
                print(f"\n💡 Scoring Breakdown:")
                step_scores = scoring.get('step_scores', {})
                step_weights = scoring.get('step_weights', {})
                print(f"   Step 1: {step_scores.get('step1', 0):.1f}% × {step_weights.get('step1', 0)}% = {step_scores.get('step1', 0) * step_weights.get('step1', 0) / 100:.1f}")
                print(f"   Step 2: {step_scores.get('step2', 0):.1f}% × {step_weights.get('step2', 0)}% = {step_scores.get('step2', 0) * step_weights.get('step2', 0) / 100:.1f}")
                print(f"   Final Score: {scoring.get('final_score', 0):.1f}%")

            # Chart data summary
            charts = completeness_results.get('comprehensive_statistics', {}).get('chart_data', {})
            if charts:
                monthly_trends = charts.get('monthly_trends', {})
                top_accounts = charts.get('top_accounts', {})
                
                print(f"\n📊 Chart Data Generated:")
                print(f"   📈 Monthly Trends: {len(monthly_trends.get('labels', []))} months")
                print(f"   📋 Top Accounts: {len(top_accounts.get('labels', []))} accounts")
                
                if monthly_trends.get('labels'):
                    print(f"   📅 Data Period: {monthly_trends['labels'][0]} to {monthly_trends['labels'][-1]}")

            # Verify database save
            latest_test = CompletenessTestResult.objects.filter(engagement=engagement).order_by('-test_timestamp').first()
            if latest_test:
                print(f"\n💾 Fresh Result Saved to Database:")
                print(f"   📋 Test ID: {latest_test.id}")
                print(f"   ⏰ Timestamp: {latest_test.test_timestamp.strftime('%Y-%m-%d %H:%M:%S')}")
                print(f"   🎯 Status: {latest_test.overall_status}")
                print(f"   📈 Score: {latest_test.completeness_score:.1f}%")
                print(f"   🧪 Total Tests: {latest_test.total_tests}")
            else:
                print(f"\n⚠️ Warning: No new test result found in database")

        else:
            print(f"❌ Fresh test failed: {result.get('error', 'Unknown error')}")

        print(f"\n⏱️  Total Operation Duration: {duration:.2f} seconds")

    except Engagement.DoesNotExist:
        print(f"❌ Engagement {engagement_id_str} not found in database.")
    except DataFile.DoesNotExist:
        print(f"❌ GL DataFile {gl_file_id_str} not found in database.")
    except Exception as e:
        print(f"💥 An unexpected error occurred: {e}")
        import traceback
        traceback.print_exc()

    print("\n🎯 Clear and fresh test operation completed.")

if __name__ == '__main__':
    clear_and_test_eng008()
