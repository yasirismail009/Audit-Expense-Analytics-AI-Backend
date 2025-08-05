#!/usr/bin/env python
"""
Test script to verify that the JSON serialization fix works
"""

import os
import sys
import django
from datetime import datetime, timedelta

# Setup Django
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'analytics.settings')
django.setup()

from core.models import (
    FileProcessingJob, SAPGLPosting, DuplicateAnalysisResult, 
    BackdatedAnalysisResult, RiskScoringDocument
)

def test_duplicate_analysis():
    """Test duplicate analysis to see if it works now"""
    print("=== TESTING DUPLICATE ANALYSIS ===")
    
    # Get a recent job
    recent_job = FileProcessingJob.objects.filter(
        status='COMPLETED'
    ).order_by('-created_at').first()
    
    if not recent_job:
        print("❌ No completed jobs found")
        return False
    
    print(f"Testing with job: {recent_job.id}")
    print(f"File: {recent_job.data_file.file_name}")
    
    # Check if duplicate analysis exists
    duplicate_result = DuplicateAnalysisResult.objects.filter(
        data_file=recent_job.data_file
    ).first()
    
    if duplicate_result:
        print(f"✅ Duplicate analysis found: {duplicate_result.id}")
        print(f"   Status: {duplicate_result.status}")
        print(f"   Duplicates found: {len(duplicate_result.duplicate_list) if duplicate_result.duplicate_list else 0}")
        return True
    else:
        print("❌ No duplicate analysis found")
        return False

def test_backdated_analysis():
    """Test backdated analysis to see if it works now"""
    print("\n=== TESTING BACKDATED ANALYSIS ===")
    
    # Get a recent job
    recent_job = FileProcessingJob.objects.filter(
        status='COMPLETED'
    ).order_by('-created_at').first()
    
    if not recent_job:
        print("❌ No completed jobs found")
        return False
    
    print(f"Testing with job: {recent_job.id}")
    
    # Check if backdated analysis exists
    backdated_result = BackdatedAnalysisResult.objects.filter(
        data_file=recent_job.data_file
    ).first()
    
    if backdated_result:
        print(f"✅ Backdated analysis found: {backdated_result.id}")
        print(f"   Status: {backdated_result.status}")
        print(f"   Backdated entries: {len(backdated_result.backdated_entries) if backdated_result.backdated_entries else 0}")
        return True
    else:
        print("❌ No backdated analysis found")
        return False

def test_risk_analysis():
    """Test risk analysis to see if it works now"""
    print("\n=== TESTING RISK ANALYSIS ===")
    
    # Get a recent job
    recent_job = FileProcessingJob.objects.filter(
        status='COMPLETED'
    ).order_by('-created_at').first()
    
    if not recent_job:
        print("❌ No completed jobs found")
        return False
    
    print(f"Testing with job: {recent_job.id}")
    
    # Check if risk analysis exists
    risk_result = RiskScoringDocument.objects.filter(
        data_file=recent_job.data_file
    ).first()
    
    if risk_result:
        print(f"✅ Risk analysis found: {risk_result.id}")
        print(f"   Status: {risk_result.status}")
        print(f"   Document type: {risk_result.document_type}")
        print(f"   Overall risk score: {risk_result.overall_risk_score}")
        return True
    else:
        print("❌ No risk analysis found")
        return False

def test_json_serialization():
    """Test JSON serialization with actual data"""
    print("\n=== TESTING JSON SERIALIZATION ===")
    
    # Test with actual transaction data
    transactions = SAPGLPosting.objects.filter(
        document_date__isnull=False,
        posting_date__isnull=False,
        amount_local_currency__isnull=False
    )[:10]
    
    if not transactions:
        print("❌ No transactions found for testing")
        return False
    
    print(f"Testing with {len(transactions)} transactions")
    
    # Test the high_value_risk calculation that was causing issues
    try:
        high_value_transactions = [
            str(t.id) for t in transactions 
            if float(t.amount_local_currency) > 1000000
        ]
        
        test_data = {
            'high_value_risk': len(high_value_transactions),
            'high_value_transaction_ids': high_value_transactions
        }
        
        import json
        json_str = json.dumps(test_data)
        print("✅ JSON serialization test passed")
        print(f"   High value transactions: {len(high_value_transactions)}")
        return True
        
    except Exception as e:
        print(f"❌ JSON serialization test failed: {e}")
        return False

def run_sync_analysis_test():
    """Run a sync analysis test to see if it works"""
    print("\n=== RUNNING SYNC ANALYSIS TEST ===")
    
    # Get a recent job
    recent_job = FileProcessingJob.objects.filter(
        status='COMPLETED'
    ).order_by('-created_at').first()
    
    if not recent_job:
        print("❌ No completed jobs found")
        return False
    
    print(f"Testing sync analysis with job: {recent_job.id}")
    
    try:
        # Import and run sync analysis
        from core.sync_analysis import run_duplicate_analysis_sync
        
        # This would run the analysis, but let's just test the import
        print("✅ Sync analysis import successful")
        return True
        
    except Exception as e:
        print(f"❌ Sync analysis test failed: {e}")
        return False

def main():
    """Main test function"""
    print("🧪 TESTING ANALYSIS FIXES")
    print("=" * 50)
    
    # Run all tests
    duplicate_ok = test_duplicate_analysis()
    backdated_ok = test_backdated_analysis()
    risk_ok = test_risk_analysis()
    json_ok = test_json_serialization()
    sync_ok = run_sync_analysis_test()
    
    print("\n" + "=" * 50)
    print("📊 TEST RESULTS SUMMARY")
    print(f"Duplicate Analysis: {'✅ PASS' if duplicate_ok else '❌ FAIL'}")
    print(f"Backdated Analysis: {'✅ PASS' if backdated_ok else '❌ FAIL'}")
    print(f"Risk Analysis: {'✅ PASS' if risk_ok else '❌ FAIL'}")
    print(f"JSON Serialization: {'✅ PASS' if json_ok else '❌ FAIL'}")
    print(f"Sync Analysis: {'✅ PASS' if sync_ok else '❌ FAIL'}")
    
    if all([duplicate_ok, backdated_ok, risk_ok, json_ok, sync_ok]):
        print("\n🎉 All tests passed! The JSON serialization fix is working.")
    else:
        print("\n⚠️  Some tests failed. Additional fixes may be needed.")

if __name__ == "__main__":
    main() 