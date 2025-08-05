#!/usr/bin/env python
"""
Diagnostic script to check why duplicate/backdated detection and risk analysis are failing
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
    BackdatedAnalysisResult, RiskScoringDocument, GeneralAnalysisResult
)

def check_data_quality():
    """Check if there are any data quality issues"""
    print("=== DATA QUALITY CHECK ===")
    
    # Check total transactions
    total_transactions = SAPGLPosting.objects.count()
    print(f"Total transactions in database: {total_transactions}")
    
    if total_transactions == 0:
        print("❌ No transactions found in database!")
        return False
    
    # Check for transactions with missing dates
    missing_document_date = SAPGLPosting.objects.filter(document_date__isnull=True).count()
    missing_posting_date = SAPGLPosting.objects.filter(posting_date__isnull=True).count()
    missing_amount = SAPGLPosting.objects.filter(amount_local_currency__isnull=True).count()
    
    print(f"Transactions missing document_date: {missing_document_date}")
    print(f"Transactions missing posting_date: {missing_posting_date}")
    print(f"Transactions missing amount: {missing_amount}")
    
    # Check for transactions with both dates
    valid_transactions = SAPGLPosting.objects.filter(
        document_date__isnull=False,
        posting_date__isnull=False,
        amount_local_currency__isnull=False
    ).count()
    
    print(f"Valid transactions (with all required fields): {valid_transactions}")
    
    return valid_transactions > 0

def check_analysis_results():
    """Check existing analysis results"""
    print("\n=== ANALYSIS RESULTS CHECK ===")
    
    # Check duplicate analysis results
    duplicate_results = DuplicateAnalysisResult.objects.all()
    print(f"Duplicate analysis results: {duplicate_results.count()}")
    
    for result in duplicate_results:
        print(f"  - File: {result.data_file.file_name}")
        print(f"    Status: {result.status}")
        print(f"    Duplicates found: {len(result.duplicate_list) if result.duplicate_list else 0}")
        print(f"    Analysis date: {result.analysis_date}")
    
    # Check backdated analysis results
    backdated_results = BackdatedAnalysisResult.objects.all()
    print(f"\nBackdated analysis results: {backdated_results.count()}")
    
    for result in backdated_results:
        print(f"  - File: {result.data_file.file_name}")
        print(f"    Status: {result.status}")
        print(f"    Backdated entries: {len(result.backdated_entries) if result.backdated_entries else 0}")
        print(f"    Analysis date: {result.analysis_date}")
    
    # Check risk analysis results
    risk_results = RiskScoringDocument.objects.all()
    print(f"\nRisk analysis results: {risk_results.count()}")
    
    for result in risk_results:
        print(f"  - File: {result.data_file.file_name}")
        print(f"    Status: {result.status}")
        print(f"    Document type: {result.document_type}")
        print(f"    Analysis date: {result.analysis_date}")

def check_processing_jobs():
    """Check processing job status"""
    print("\n=== PROCESSING JOBS CHECK ===")
    
    jobs = FileProcessingJob.objects.all().order_by('-created_at')[:10]
    print(f"Recent processing jobs: {jobs.count()}")
    
    for job in jobs:
        print(f"  - Job ID: {job.id}")
        print(f"    File: {job.data_file.file_name}")
        print(f"    Status: {job.status}")
        print(f"    Created: {job.created_at}")
        print(f"    Updated: {job.updated_at}")
        if job.error_message:
            print(f"    Error: {job.error_message}")

def test_duplicate_detection():
    """Test duplicate detection logic"""
    print("\n=== DUPLICATE DETECTION TEST ===")
    
    # Get a sample of transactions
    transactions = SAPGLPosting.objects.filter(
        document_date__isnull=False,
        posting_date__isnull=False,
        amount_local_currency__isnull=False
    )[:100]
    
    if not transactions:
        print("❌ No valid transactions found for testing")
        return
    
    print(f"Testing duplicate detection on {len(transactions)} transactions")
    
    # Simple duplicate detection test
    transaction_dict = {}
    duplicates_found = 0
    
    for t in transactions:
        key = (float(t.amount_local_currency), t.posting_date, t.gl_account)
        if key in transaction_dict:
            duplicates_found += 1
            print(f"  Found duplicate: {t.document_number} (Amount: {t.amount_local_currency}, Date: {t.posting_date}, Account: {t.gl_account})")
        else:
            transaction_dict[key] = t
    
    print(f"Duplicates found in test: {duplicates_found}")

def test_backdated_detection():
    """Test backdated detection logic"""
    print("\n=== BACKDATED DETECTION TEST ===")
    
    # Get transactions with both dates
    transactions = SAPGLPosting.objects.filter(
        document_date__isnull=False,
        posting_date__isnull=False
    )[:100]
    
    if not transactions:
        print("❌ No transactions with both dates found for testing")
        return
    
    print(f"Testing backdated detection on {len(transactions)} transactions")
    
    backdated_found = 0
    for t in transactions:
        if t.posting_date > t.document_date:
            days_diff = (t.posting_date - t.document_date).days
            backdated_found += 1
            print(f"  Found backdated: {t.document_number} (Doc: {t.document_date}, Post: {t.posting_date}, Days: {days_diff})")
    
    print(f"Backdated entries found in test: {backdated_found}")

def check_analysis_execution():
    """Check if analysis tasks are being executed"""
    print("\n=== ANALYSIS EXECUTION CHECK ===")
    
    # Check if there are any recent analysis results
    recent_duplicate = DuplicateAnalysisResult.objects.filter(
        analysis_date__gte=datetime.now() - timedelta(days=1)
    ).count()
    
    recent_backdated = BackdatedAnalysisResult.objects.filter(
        analysis_date__gte=datetime.now() - timedelta(days=1)
    ).count()
    
    recent_risk = RiskScoringDocument.objects.filter(
        analysis_date__gte=datetime.now() - timedelta(days=1)
    ).count()
    
    print(f"Recent duplicate analysis results (last 24h): {recent_duplicate}")
    print(f"Recent backdated analysis results (last 24h): {recent_backdated}")
    print(f"Recent risk analysis results (last 24h): {recent_risk}")
    
    # Check for failed jobs
    failed_jobs = FileProcessingJob.objects.filter(
        status='FAILED',
        created_at__gte=datetime.now() - timedelta(days=1)
    )
    
    print(f"Failed jobs in last 24h: {failed_jobs.count()}")
    for job in failed_jobs:
        print(f"  - Job {job.id}: {job.error_message}")

def main():
    """Main diagnostic function"""
    print("🔍 ANALYTICS SYSTEM DIAGNOSTIC")
    print("=" * 50)
    
    # Run all checks
    data_ok = check_data_quality()
    check_analysis_results()
    check_processing_jobs()
    
    if data_ok:
        test_duplicate_detection()
        test_backdated_detection()
    
    check_analysis_execution()
    
    print("\n" + "=" * 50)
    print("✅ Diagnostic complete")

if __name__ == "__main__":
    main() 