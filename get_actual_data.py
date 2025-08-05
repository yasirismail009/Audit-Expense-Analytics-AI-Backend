#!/usr/bin/env python3
"""
Script to get actual data from database for API documentation
"""

import os
import django

# Setup Django
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'analytics.settings')
django.setup()

from core.models import *

def get_actual_data():
    """Get actual data from database"""
    print("📊 ACTUAL DATA FROM DATABASE")
    print("=" * 50)
    
    # Data Files
    print("\n📁 DATA FILES:")
    files = DataFile.objects.all()
    for i, f in enumerate(files):
        transaction_count = SAPGLPosting.objects.filter(data_file=f).count()
        print(f"  File {i+1}: {f.id}")
        print(f"    Name: {f.file_name}")
        print(f"    Uploaded: {f.uploaded_at}")
        print(f"    Transactions: {transaction_count}")
        print()
    
    # Analysis Results Summary
    print("📈 ANALYSIS RESULTS SUMMARY:")
    print(f"  GeneralAnalysisResult: {GeneralAnalysisResult.objects.count()}")
    print(f"  DuplicateAnalysisResult: {DuplicateAnalysisResult.objects.count()}")
    print(f"  BackdatedAnalysisResult: {BackdatedAnalysisResult.objects.count()}")
    print(f"  UserAnalysisResult: {UserAnalysisResult.objects.count()}")
    print(f"  ClosingEntriesAnalysisResult: {ClosingEntriesAnalysisResult.objects.count()}")
    print(f"  UnusualDaysAnalysisResult: {UnusualDaysAnalysisResult.objects.count()}")
    print(f"  HolidayAnalysisResult: {HolidayAnalysisResult.objects.count()}")
    print(f"  OverallAnalysisResult: {OverallAnalysisResult.objects.count()}")
    print(f"  RiskScoringDocument: {RiskScoringDocument.objects.count()}")
    
    # Sample Analysis Data
    print("\n🔍 SAMPLE ANALYSIS DATA:")
    
    # General Analysis Sample
    general = GeneralAnalysisResult.objects.first()
    if general:
        print(f"\n📊 General Analysis Sample:")
        print(f"  Analysis ID: {general.id}")
        print(f"  Data File: {general.data_file.file_name}")
        print(f"  Status: {general.status}")
        print(f"  Processing Duration: {general.processing_duration}")
        if general.statistical_calculations:
            print(f"  Total Transactions: {general.statistical_calculations.get('total_transactions', 'N/A')}")
            print(f"  Unique Users: {general.statistical_calculations.get('unique_users', 'N/A')}")
            print(f"  Unique Accounts: {general.statistical_calculations.get('unique_accounts', 'N/A')}")
    
    # Duplicate Analysis Sample
    duplicate = DuplicateAnalysisResult.objects.first()
    if duplicate:
        print(f"\n🔍 Duplicate Analysis Sample:")
        print(f"  Analysis ID: {duplicate.id}")
        print(f"  Data File: {duplicate.data_file.file_name}")
        print(f"  Status: {duplicate.status}")
        print(f"  Processing Duration: {duplicate.processing_duration}")
        if duplicate.duplicate_pairs:
            print(f"  Duplicate Pairs: {len(duplicate.duplicate_pairs)}")
    
    # User Analysis Sample
    user_analysis = UserAnalysisResult.objects.first()
    if user_analysis:
        print(f"\n👥 User Analysis Sample:")
        print(f"  Analysis ID: {user_analysis.id}")
        print(f"  Data File: {user_analysis.data_file.file_name}")
        print(f"  Status: {user_analysis.status}")
        print(f"  Processing Duration: {user_analysis.processing_duration}")
        if user_analysis.statistical_summary:
            print(f"  Total Users: {user_analysis.statistical_summary.get('total_users', 'N/A')}")
            print(f"  High Risk Users: {user_analysis.statistical_summary.get('high_risk_users', 'N/A')}")
    
    # Risk Analysis Sample
    risk = RiskScoringDocument.objects.first()
    if risk:
        print(f"\n⚠️ Risk Analysis Sample:")
        print(f"  Document ID: {risk.id}")
        print(f"  Data File: {risk.data_file.file_name}")
        print(f"  Status: {risk.status}")
        print(f"  Processing Duration: {risk.processing_duration}")
        if risk.risk_scoring:
            print(f"  Overall Risk Score: {risk.risk_scoring.get('overall_risk_score', 'N/A')}")
            print(f"  Risk Level: {risk.risk_scoring.get('risk_level', 'N/A')}")
    
    # Job Status
    print(f"\n🔄 JOB STATUS:")
    jobs = FileProcessingJob.objects.all()
    completed = jobs.filter(status='COMPLETED').count()
    pending = jobs.filter(status='PENDING').count()
    failed = jobs.filter(status='FAILED').count()
    print(f"  Total Jobs: {jobs.count()}")
    print(f"  Completed: {completed}")
    print(f"  Pending: {pending}")
    print(f"  Failed: {failed}")
    
    # Transaction Data
    print(f"\n💾 TRANSACTION DATA:")
    total_transactions = SAPGLPosting.objects.count()
    print(f"  Total Transactions: {total_transactions}")
    
    # Sample transaction data
    sample_transaction = SAPGLPosting.objects.first()
    if sample_transaction:
        print(f"  Sample Transaction:")
        print(f"    ID: {sample_transaction.id}")
        print(f"    Document Number: {sample_transaction.document_number}")
        print(f"    Amount: {sample_transaction.amount_local_currency}")
        print(f"    User: {sample_transaction.user_name}")
        print(f"    Account: {sample_transaction.gl_account}")
        print(f"    Posting Date: {sample_transaction.posting_date}")

if __name__ == "__main__":
    get_actual_data() 