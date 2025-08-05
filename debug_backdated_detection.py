#!/usr/bin/env python
"""
Debug script to investigate why backdated entries are not being detected
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

def check_transaction_dates():
    """Check transaction dates to see if there are any backdated entries"""
    print("=== CHECKING TRANSACTION DATES ===")
    
    # Get all transactions with both dates
    transactions = SAPGLPosting.objects.filter(
        document_date__isnull=False,
        posting_date__isnull=False
    )
    
    print(f"Total transactions with both dates: {transactions.count()}")
    
    if transactions.count() == 0:
        print("❌ No transactions with both document_date and posting_date found")
        return False
    
    # Check for backdated entries manually
    backdated_found = 0
    backdated_examples = []
    
    for t in transactions:
        if t.posting_date > t.document_date:
            days_diff = (t.posting_date - t.document_date).days
            backdated_found += 1
            backdated_examples.append({
                'id': str(t.id),
                'document_number': t.document_number,
                'document_date': t.document_date,
                'posting_date': t.posting_date,
                'days_difference': days_diff,
                'amount': float(t.amount_local_currency),
                'user': t.user_name,
                'account': t.gl_account
            })
    
    print(f"Backdated entries found manually: {backdated_found}")
    
    if backdated_found > 0:
        print("\nExamples of backdated entries:")
        for i, example in enumerate(backdated_examples[:5]):
            print(f"  {i+1}. Document: {example['document_number']}")
            print(f"     Document Date: {example['document_date']}")
            print(f"     Posting Date: {example['posting_date']}")
            print(f"     Days Difference: {example['days_difference']}")
            print(f"     Amount: {example['amount']}")
            print(f"     User: {example['user']}")
            print(f"     Account: {example['account']}")
            print()
    
    return backdated_found > 0

def check_backdated_analysis_results():
    """Check if backdated analysis results exist"""
    print("\n=== CHECKING BACKDATED ANALYSIS RESULTS ===")
    
    backdated_results = BackdatedAnalysisResult.objects.all()
    print(f"Total backdated analysis results: {backdated_results.count()}")
    
    for result in backdated_results:
        print(f"  - Result ID: {result.id}")
        print(f"    File: {result.data_file.file_name}")
        print(f"    Status: {result.status}")
        print(f"    Analysis Date: {result.analysis_date}")
        print(f"    Backdated entries: {len(result.backdated_entries) if result.backdated_entries else 0}")
        if result.error_message:
            print(f"    Error: {result.error_message}")
        print()

def check_backdated_detection_logic():
    """Test the backdated detection logic"""
    print("\n=== TESTING BACKDATED DETECTION LOGIC ===")
    
    # Get transactions with both dates
    transactions = SAPGLPosting.objects.filter(
        document_date__isnull=False,
        posting_date__isnull=False
    )[:100]  # Test with first 100
    
    if not transactions:
        print("❌ No transactions with both dates found")
        return
    
    print(f"Testing backdated detection on {len(transactions)} transactions")
    
    # Manual backdated detection
    backdated_transactions = []
    backdated_by_user = {}
    backdated_by_account = {}
    
    for transaction in transactions:
        if transaction.document_date and transaction.posting_date:
            days_difference = (transaction.posting_date - transaction.document_date).days
            
            if days_difference > 0:  # Backdated transaction
                backdated_record = {
                    'transaction_id': str(transaction.id),
                    'document_number': transaction.document_number,
                    'gl_account': transaction.gl_account,
                    'user_name': transaction.user_name,
                    'document_date': transaction.document_date.isoformat(),
                    'posting_date': transaction.posting_date.isoformat(),
                    'days_difference': days_difference,
                    'amount': float(transaction.amount_local_currency),
                    'transaction_type': 'DEBIT' if transaction.amount_local_currency < 0 else 'CREDIT',
                    'risk_score': 50 + (days_difference * 2),  # Simple risk scoring
                    'risk_level': 'HIGH' if days_difference > 7 else 'MEDIUM',
                    'detection_method': 'manual_test'
                }
                
                backdated_transactions.append(backdated_record)
                
                # Group by user
                user = transaction.user_name
                if user not in backdated_by_user:
                    backdated_by_user[user] = []
                backdated_by_user[user].append(backdated_record)
                
                # Group by account
                account = transaction.gl_account
                if account not in backdated_by_account:
                    backdated_by_account[account] = []
                backdated_by_account[account].append(backdated_record)
    
    print(f"Backdated transactions found: {len(backdated_transactions)}")
    
    if backdated_transactions:
        print("\nBackdated transactions details:")
        for i, bt in enumerate(backdated_transactions[:5]):
            print(f"  {i+1}. {bt['document_number']} - {bt['days_difference']} days difference")
            print(f"     User: {bt['user_name']}, Account: {bt['gl_account']}")
            print(f"     Risk Score: {bt['risk_score']}, Level: {bt['risk_level']}")
            print()
    
    return len(backdated_transactions) > 0

def check_data_quality():
    """Check data quality issues that might prevent backdated detection"""
    print("\n=== CHECKING DATA QUALITY ===")
    
    # Check for transactions with missing dates
    missing_document_date = SAPGLPosting.objects.filter(document_date__isnull=True).count()
    missing_posting_date = SAPGLPosting.objects.filter(posting_date__isnull=True).count()
    
    print(f"Transactions missing document_date: {missing_document_date}")
    print(f"Transactions missing posting_date: {missing_posting_date}")
    
    # Check for transactions with invalid dates
    invalid_dates = 0
    for t in SAPGLPosting.objects.filter(document_date__isnull=False, posting_date__isnull=False)[:100]:
        try:
            if t.document_date and t.posting_date:
                days_diff = (t.posting_date - t.document_date).days
                if days_diff < 0:  # Document date after posting date
                    invalid_dates += 1
        except:
            invalid_dates += 1
    
    print(f"Transactions with invalid date relationships: {invalid_dates}")
    
    # Check for transactions with same dates (not backdated)
    same_dates = 0
    for t in SAPGLPosting.objects.filter(document_date__isnull=False, posting_date__isnull=False)[:100]:
        if t.document_date == t.posting_date:
            same_dates += 1
    
    print(f"Transactions with same document and posting dates: {same_dates}")

def check_analysis_execution():
    """Check if backdated analysis is being executed"""
    print("\n=== CHECKING ANALYSIS EXECUTION ===")
    
    # Check recent jobs
    recent_jobs = FileProcessingJob.objects.filter(
        created_at__gte=datetime.now() - timedelta(days=1)
    ).order_by('-created_at')[:5]
    
    print(f"Recent jobs (last 24h): {recent_jobs.count()}")
    
    for job in recent_jobs:
        print(f"  - Job ID: {job.id}")
        print(f"    File: {job.data_file.file_name}")
        print(f"    Status: {job.status}")
        print(f"    Created: {job.created_at}")
        if job.error_message:
            print(f"    Error: {job.error_message}")
        print()

def run_backdated_analysis_manually():
    """Run backdated analysis manually to see if it works"""
    print("\n=== RUNNING BACKDATED ANALYSIS MANUALLY ===")
    
    # Get a recent job
    recent_job = FileProcessingJob.objects.filter(
        status='COMPLETED'
    ).order_by('-created_at').first()
    
    if not recent_job:
        print("❌ No completed jobs found")
        return False
    
    print(f"Running manual backdated analysis for job: {recent_job.id}")
    
    try:
        # Import and run the sync analysis
        from core.sync_analysis import run_backdated_analysis_sync
        
        # Run the analysis
        result = run_backdated_analysis_sync(str(recent_job.id))
        
        if 'error' in result:
            print(f"❌ Manual analysis failed: {result['error']}")
            return False
        else:
            print(f"✅ Manual analysis completed successfully")
            print(f"   Result: {result}")
            return True
            
    except Exception as e:
        print(f"❌ Manual analysis failed with exception: {e}")
        return False

def main():
    """Main debug function"""
    print("🔍 DEBUGGING BACKDATED DETECTION")
    print("=" * 50)
    
    # Run all checks
    has_backdated = check_transaction_dates()
    check_backdated_analysis_results()
    detection_works = check_backdated_detection_logic()
    check_data_quality()
    check_analysis_execution()
    
    if has_backdated and not detection_works:
        print("\n⚠️  ISSUE IDENTIFIED:")
        print("Backdated entries exist in the data, but the analysis is not detecting them.")
        print("This suggests the analysis task is not running or failing silently.")
        
        # Try manual analysis
        manual_ok = run_backdated_analysis_manually()
        if manual_ok:
            print("✅ Manual analysis works - the issue is with task execution")
        else:
            print("❌ Manual analysis also fails - there's an issue with the analysis logic")
    
    elif not has_backdated:
        print("\nℹ️  NO BACKDATED ENTRIES FOUND:")
        print("The data doesn't contain any transactions where posting_date > document_date")
        print("This is normal if all transactions are posted on or before their document date")
    
    else:
        print("\n✅ BACKDATED DETECTION WORKING:")
        print("Backdated entries are being detected correctly")
    
    print("\n" + "=" * 50)
    print("🔍 Debug complete")

if __name__ == "__main__":
    main() 