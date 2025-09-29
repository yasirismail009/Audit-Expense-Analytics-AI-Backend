#!/usr/bin/env python
"""
Check what data is stored in CompletenessTestResult for ENG-008
"""

import os
import sys
import django
import json

# Set up Django
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'analytics.settings')
django.setup()

from core.models import CompletenessTestResult, Engagement

def check_completeness_data():
    """Check what data is stored in CompletenessTestResult"""
    print("🔍 Checking CompletenessTestResult data for ENG-008")
    print("=" * 60)
    
    try:
        # Get ENG-008 engagement
        engagement = Engagement.objects.get(engagement_id='ENG-008')
        print(f"✅ Engagement: {engagement.engagement_id}")
        
        # Get latest completeness test result
        test_result = CompletenessTestResult.objects.filter(
            engagement=engagement
        ).order_by('-test_timestamp').first()
        
        if not test_result:
            print("❌ No completeness test result found")
            return
        
        print(f"📊 Test Result ID: {test_result.id}")
        print(f"📅 Test Timestamp: {test_result.test_timestamp}")
        print(f"📈 Overall Status: {test_result.overall_status}")
        print(f"📊 Completeness Score: {test_result.completeness_score}")
        
        # Check step2_gl_tb_reconciliation data
        print(f"\n🔍 Step 2 GL-TB Reconciliation Data:")
        step2_data = test_result.step2_gl_tb_reconciliation
        if step2_data:
            print(f"   Type: {type(step2_data)}")
            print(f"   Keys: {list(step2_data.keys()) if isinstance(step2_data, dict) else 'Not a dict'}")
            
            # Check for account_verifications
            if 'account_verifications' in step2_data:
                account_verifications = step2_data['account_verifications']
                print(f"   Account Verifications: {len(account_verifications)} items")
                if account_verifications:
                    print(f"   Sample account verification keys: {list(account_verifications[0].keys()) if account_verifications else 'Empty'}")
            
            # Check for document_verifications
            if 'document_verifications' in step2_data:
                document_verifications = step2_data['document_verifications']
                print(f"   Document Verifications: {len(document_verifications)} items")
                if document_verifications:
                    print(f"   Sample document verification keys: {list(document_verifications[0].keys()) if document_verifications else 'Empty'}")
            else:
                print("   ❌ No document_verifications found in step2_gl_tb_reconciliation")
            
            # Show all available keys
            print(f"\n📋 All available keys in step2_gl_tb_reconciliation:")
            for key in step2_data.keys():
                value = step2_data[key]
                if isinstance(value, list):
                    print(f"   - {key}: {len(value)} items")
                elif isinstance(value, dict):
                    print(f"   - {key}: {len(value)} keys")
                else:
                    print(f"   - {key}: {type(value)} = {value}")
        else:
            print("   ❌ No step2_gl_tb_reconciliation data")
        
        # Check other steps for document data
        print(f"\n🔍 Other Steps:")
        steps = [
            ('step3_debit_credit_balance', test_result.step3_debit_credit_balance),
            ('step4_account_coverage', test_result.step4_account_coverage),
            ('step5_coa_hierarchy_validation', test_result.step5_coa_hierarchy_validation),
            ('step6_account_linking', test_result.step6_account_linking),
            ('step7_transaction_gaps', test_result.step7_transaction_gaps),
        ]
        
        for step_name, step_data in steps:
            if step_data and isinstance(step_data, dict):
                print(f"   {step_name}: {len(step_data)} keys - {list(step_data.keys())}")
            else:
                print(f"   {step_name}: {type(step_data)}")
        
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    check_completeness_data()
