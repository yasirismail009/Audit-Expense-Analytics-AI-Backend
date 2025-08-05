#!/usr/bin/env python
"""
Test script to verify the backdated detection fix works
"""

import os
import sys
import django
from datetime import datetime, timedelta

# Setup Django
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'analytics.settings')
django.setup()

from core.models import (
    FileProcessingJob, SAPGLPosting, BackdatedAnalysisResult
)

def test_backdated_detection():
    """Test the fixed backdated detection"""
    print("=== TESTING FIXED BACKDATED DETECTION ===")
    
    # Get a recent job
    recent_job = FileProcessingJob.objects.filter(
        status='COMPLETED'
    ).order_by('-created_at').first()
    
    if not recent_job:
        print("❌ No completed jobs found")
        return False
    
    print(f"Testing with job: {recent_job.id}")
    print(f"File: {recent_job.data_file.file_name}")
    
    # Run the fixed backdated analysis
    try:
        from core.sync_analysis import run_backdated_analysis_sync
        
        result = run_backdated_analysis_sync(str(recent_job.id))
        
        if 'error' in result:
            print(f"❌ Analysis failed: {result['error']}")
            return False
        
        print(f"✅ Analysis completed successfully")
        print(f"   Backdated entries found: {result.get('backdated_entries_found', 0)}")
        print(f"   Processing duration: {result.get('processing_duration', 0):.2f} seconds")
        
        # Check the database result
        backdated_result = BackdatedAnalysisResult.objects.filter(
            data_file=recent_job.data_file
        ).order_by('-analysis_date').first()
        
        if backdated_result:
            print(f"   Database result: {len(backdated_result.backdated_entries) if backdated_result.backdated_entries else 0} entries")
            return len(backdated_result.backdated_entries) > 0 if backdated_result.backdated_entries else False
        else:
            print("❌ No backdated analysis result found in database")
            return False
            
    except Exception as e:
        print(f"❌ Analysis failed with exception: {e}")
        return False

def verify_backdated_entry():
    """Verify that the specific backdated entry is detected"""
    print("\n=== VERIFYING SPECIFIC BACKDATED ENTRY ===")
    
    # Find the transaction with document date 2025-08-09 and posting date 2025-12-12
    transactions = SAPGLPosting.objects.filter(
        document_date__isnull=False,
        posting_date__isnull=False
    )
    
    backdated_found = 0
    for t in transactions:
        if t.posting_date > t.document_date:
            days_diff = (t.posting_date - t.document_date).days
            backdated_found += 1
            print(f"  Found backdated: {t.document_number}")
            print(f"    Document Date: {t.document_date}")
            print(f"    Posting Date: {t.posting_date}")
            print(f"    Days Difference: {days_diff}")
            print(f"    Amount: {t.amount_local_currency}")
            print(f"    User: {t.user_name}")
            print()
    
    print(f"Total backdated entries found: {backdated_found}")
    return backdated_found > 0

def main():
    """Main test function"""
    print("🧪 TESTING BACKDATED DETECTION FIX")
    print("=" * 50)
    
    # Run tests
    detection_works = test_backdated_detection()
    has_backdated = verify_backdated_entry()
    
    print("\n" + "=" * 50)
    print("📊 TEST RESULTS SUMMARY")
    print(f"Backdated Detection: {'✅ PASS' if detection_works else '❌ FAIL'}")
    print(f"Backdated Entry Exists: {'✅ YES' if has_backdated else '❌ NO'}")
    
    if detection_works and has_backdated:
        print("\n🎉 SUCCESS! Backdated detection is now working correctly.")
    elif has_backdated and not detection_works:
        print("\n⚠️  ISSUE: Backdated entry exists but detection still not working.")
    elif not has_backdated:
        print("\nℹ️  INFO: No backdated entries found in the data.")
    else:
        print("\n❌ FAILURE: Backdated detection is not working.")

if __name__ == "__main__":
    main() 