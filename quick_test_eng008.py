#!/usr/bin/env python
"""
Quick test of 2-step completeness without debug output
"""
import os
import django
import logging

# Reduce logging verbosity for quicker results
logging.getLogger('core.tasks').setLevel(logging.WARNING)

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'analytics.settings')
django.setup()

from core.models import DataFile, Engagement, CompletenessTestResult
from core.tasks import run_gl_completeness_analysis
from uuid import UUID
from django.utils import timezone

def quick_test():
    engagement_id_str = 'ENG-008'
    gl_file_id_str = '51671285-0c2e-4c26-ae59-0292a21fd113'

    print("⚡ Quick 2-Step Completeness Test for ENG-008")
    print("=" * 55)

    try:
        # Clear previous results
        engagement = Engagement.objects.get(engagement_id=engagement_id_str)
        previous_count = CompletenessTestResult.objects.filter(engagement=engagement).count()
        if previous_count > 0:
            CompletenessTestResult.objects.filter(engagement=engagement).delete()
            print(f"🗑️  Cleared {previous_count} previous results")

        # Run test
        print(f"🚀 Running completeness analysis...")
        start_time = timezone.now()
        
        result = run_gl_completeness_analysis(gl_file_id_str)
        
        end_time = timezone.now()
        duration = (end_time - start_time).total_seconds()

        print(f"⏱️  Completed in {duration:.2f} seconds")
        print("=" * 55)

        if result.get('success'):
            completeness_results = result.get('completeness_results', {})
            
            print(f"🎯 Status: {completeness_results.get('status', 'Unknown')}")
            print(f"📈 Score: {completeness_results.get('completeness_score', 0):.1f}%")
            print(f"📝 {completeness_results.get('explanation', 'N/A')}")
            
            # Step details
            step1 = completeness_results.get('step1_completeness', {})
            step2 = completeness_results.get('step2_account_verification', {})
            
            print(f"\n📋 Step Results:")
            print(f"   Step 1: {'✅ PASS' if step1.get('passed', False) else '❌ FAIL'} - GL Balance: {step1.get('gl_net_balance', 0):.2f}")
            print(f"   Step 2: {'✅ PASS' if step2.get('passed', False) else '❌ FAIL'} - Pass Rate: {step2.get('pass_rate', 0):.1%}")
            
            print(f"\n💾 Database: {'✅ Saved' if result.get('test_result_id') else '❌ Not saved'}")
            
        else:
            print(f"❌ Test failed: {result.get('error', 'Unknown error')}")

    except Exception as e:
        print(f"💥 Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == '__main__':
    quick_test()
