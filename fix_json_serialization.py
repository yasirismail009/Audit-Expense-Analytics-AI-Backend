#!/usr/bin/env python
"""
Fix JSON serialization issues in analysis tasks
"""

import os
import sys
import django
import json
from datetime import datetime, timedelta

# Setup Django
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'analytics.settings')
django.setup()

from core.models import (
    FileProcessingJob, SAPGLPosting, DuplicateAnalysisResult, 
    BackdatedAnalysisResult, RiskScoringDocument, GeneralAnalysisResult
)

def fix_risk_analysis_serialization():
    """Fix the JSON serialization issue in risk analysis"""
    print("=== FIXING RISK ANALYSIS JSON SERIALIZATION ===")
    
    # Find the problematic line in tasks.py
    print("The issue is in core/tasks.py line ~2500 where it tries to serialize SAPGLPosting objects")
    print("Line: 'high_value_risk': len([t for t in transactions if float(t.amount_local_currency) > 1000000])")
    print("The problem is that 'transactions' is a QuerySet of SAPGLPosting objects")
    
    # Fix the issue by converting to list of IDs first
    fix_code = """
    # Replace this line:
    'high_value_risk': len([t for t in transactions if float(t.amount_local_currency) > 1000000]),
    
    # With this:
    'high_value_risk': len([t.id for t in transactions if float(t.amount_local_currency) > 1000000]),
    """
    
    print(fix_code)
    return fix_code

def fix_sync_analysis_serialization():
    """Fix the JSON serialization issue in sync analysis"""
    print("\n=== FIXING SYNC ANALYSIS JSON SERIALIZATION ===")
    
    # The issue is in sync_analysis.py where it's trying to save SAPGLPosting objects to JSON fields
    print("The issue is in core/sync_analysis.py where it's updating transaction.anomaly_analysis_summary")
    print("The problem is that it's trying to store SAPGLPosting objects in JSON fields")
    
    fix_code = """
    # In sync_analysis.py, replace the anomaly_analysis_summary assignment:
    transaction.anomaly_analysis_summary = {
        'risk_score': risk_score,
        'anomaly_types': anomaly_types,
        'is_duplicate': transaction.is_duplicate,
        'is_backdated': transaction.is_backdated,
        'is_holiday_posting': transaction.is_holiday_posting,
        'holiday_name': transaction.holiday_name,
        'backdated_days': transaction.backdated_days
    }
    
    # This should be fine as long as all values are JSON serializable
    # The issue might be elsewhere - let's check if any SAPGLPosting objects are being stored
    """
    
    print(fix_code)
    return fix_code

def check_for_json_serialization_issues():
    """Check for any existing JSON serialization issues in the database"""
    print("\n=== CHECKING FOR EXISTING JSON SERIALIZATION ISSUES ===")
    
    try:
        # Check if there are any transactions with problematic JSON fields
        transactions_with_issues = SAPGLPosting.objects.filter(
            anomaly_analysis_summary__isnull=False
        ).exclude(anomaly_analysis_summary={})
        
        print(f"Transactions with anomaly_analysis_summary: {transactions_with_issues.count()}")
        
        # Try to access the JSON field to see if it causes issues
        for transaction in transactions_with_issues[:5]:
            try:
                summary = transaction.anomaly_analysis_summary
                print(f"Transaction {transaction.id}: JSON field accessible")
            except Exception as e:
                print(f"Transaction {transaction.id}: JSON field error - {e}")
                
    except Exception as e:
        print(f"Error checking JSON fields: {e}")

def create_fixed_analysis_tasks():
    """Create fixed versions of the analysis tasks"""
    print("\n=== CREATING FIXED ANALYSIS TASKS ===")
    
    # Create a fixed version of the risk analysis task
    fixed_risk_analysis = """
    # Fixed version of risk analysis task
    def run_risk_analysis_fixed(self, job_id):
        # ... existing code ...
        
        # Fix the high_value_risk calculation
        high_value_transactions = [
            str(t.id) for t in transactions 
            if float(t.amount_local_currency) > 1000000
        ]
        
        risk_factors = {
            'duplicate_risk': len(duplicate_analysis.duplicate_list) if duplicate_analysis else 0,
            'backdated_risk': len(backdated_analysis.backdated_entries) if backdated_analysis else 0,
            'user_risk': len(user_analysis.user_anomalies) if user_analysis else 0,
            'unusual_days_risk': len(unusual_days_analysis.weekend_postings) if unusual_days_analysis else 0,
            'closing_entries_risk': len(closing_entries_analysis.closing_entries) if closing_entries_analysis else 0,
            'high_value_risk': len(high_value_transactions),  # Fixed: use list of IDs
            'unusual_pattern_risk': len(flagged_transactions)
        }
        
        # ... rest of the code ...
    """
    
    print(fixed_risk_analysis)
    return fixed_risk_analysis

def test_json_serialization():
    """Test JSON serialization with sample data"""
    print("\n=== TESTING JSON SERIALIZATION ===")
    
    # Test with sample data
    sample_data = {
        'risk_score': 75.5,
        'anomaly_types': ['duplicate', 'backdated'],
        'is_duplicate': True,
        'is_backdated': False,
        'is_holiday_posting': False,
        'holiday_name': None,
        'backdated_days': 0
    }
    
    try:
        json_str = json.dumps(sample_data)
        print("✅ JSON serialization test passed")
        print(f"Sample data: {json_str}")
    except Exception as e:
        print(f"❌ JSON serialization test failed: {e}")
    
    # Test with transaction IDs
    try:
        transaction_ids = [str(uuid.uuid4()) for _ in range(3)]
        test_data = {
            'high_value_transactions': transaction_ids,
            'count': len(transaction_ids)
        }
        json_str = json.dumps(test_data)
        print("✅ Transaction IDs JSON serialization test passed")
    except Exception as e:
        print(f"❌ Transaction IDs JSON serialization test failed: {e}")

def main():
    """Main function to fix JSON serialization issues"""
    print("🔧 FIXING JSON SERIALIZATION ISSUES")
    print("=" * 50)
    
    # Run all fixes
    fix_risk_analysis_serialization()
    fix_sync_analysis_serialization()
    check_for_json_serialization_issues()
    create_fixed_analysis_tasks()
    test_json_serialization()
    
    print("\n" + "=" * 50)
    print("✅ JSON serialization fixes identified")
    print("\nTo apply the fixes:")
    print("1. Update core/tasks.py line ~2500 to use transaction IDs instead of objects")
    print("2. Ensure all JSON fields only contain serializable data")
    print("3. Test the analysis tasks again")

if __name__ == "__main__":
    main() 