#!/usr/bin/env python
"""
Simple script to check UserAnalysisResult data using Django ORM
"""

import os
import sys
import django
import json

# Setup Django
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'analytics.settings')
django.setup()

from core.models import UserAnalysisResult, DataFile, FileProcessingJob, SAPGLPosting

def check_user_analysis_data():
    """Check UserAnalysisResult data using Django ORM"""
    
    print("=" * 80)
    print("USER ANALYSIS DATA CHECK (Django ORM)")
    print("=" * 80)
    
    # Check if we can connect to the database
    try:
        # Test connection by counting records
        total_analyses = UserAnalysisResult.objects.count()
        print(f"✅ Database connection successful")
        print(f"Total User Analysis Results: {total_analyses}")
        print()
    except Exception as e:
        print(f"❌ Database connection failed: {e}")
        return
    
    if total_analyses == 0:
        print("No user analysis results found in database.")
        print()
        print("Checking if there are any data files...")
        data_files = DataFile.objects.all()
        print(f"Data Files: {data_files.count()}")
        for df in data_files[:3]:
            print(f"  - {df.file_name} (ID: {df.id}, Status: {df.status})")
        return
    
    # Get the latest analysis
    latest_analysis = UserAnalysisResult.objects.order_by('-analysis_date').first()
    
    print("LATEST USER ANALYSIS RESULT:")
    print("-" * 40)
    print(f"ID: {latest_analysis.id}")
    print(f"File: {latest_analysis.data_file.file_name if latest_analysis.data_file else 'N/A'}")
    print(f"Analysis Date: {latest_analysis.analysis_date}")
    print(f"Analysis Type: {latest_analysis.analysis_type}")
    print(f"Analysis Version: {latest_analysis.analysis_version}")
    print(f"Status: {latest_analysis.status}")
    print(f"Processing Duration: {latest_analysis.processing_duration} seconds")
    print()
    
    # Check analysis_info
    print("ANALYSIS INFO:")
    print("-" * 20)
    if latest_analysis.analysis_info:
        for key, value in latest_analysis.analysis_info.items():
            print(f"  {key}: {value}")
    else:
        print("  No analysis_info data")
    print()
    
    # Check user_transaction_summary
    print("USER TRANSACTION SUMMARY:")
    print("-" * 30)
    if latest_analysis.user_transaction_summary:
        print(f"  Total users: {len(latest_analysis.user_transaction_summary)}")
        if len(latest_analysis.user_transaction_summary) > 0:
            sample_user = latest_analysis.user_transaction_summary[0]
            print(f"  Sample user data structure:")
            for key, value in sample_user.items():
                if isinstance(value, list) and len(value) > 5:
                    print(f"    {key}: {value[:5]}... (showing first 5 items)")
                else:
                    print(f"    {key}: {value}")
    else:
        print("  No user_transaction_summary data")
    print()
    
    # Check user_anomalies
    print("USER ANOMALIES:")
    print("-" * 20)
    if latest_analysis.user_anomalies:
        print(f"  Total anomalies: {len(latest_analysis.user_anomalies)}")
        if len(latest_analysis.user_anomalies) > 0:
            sample_anomaly = latest_analysis.user_anomalies[0]
            print(f"  Sample anomaly structure:")
            for key, value in sample_anomaly.items():
                print(f"    {key}: {value}")
    else:
        print("  No user_anomalies data")
    print()
    
    # Check user_risk_assessment
    print("USER RISK ASSESSMENT:")
    print("-" * 25)
    if latest_analysis.user_risk_assessment:
        if isinstance(latest_analysis.user_risk_assessment, list):
            print(f"  Total risk assessments: {len(latest_analysis.user_risk_assessment)}")
            if len(latest_analysis.user_risk_assessment) > 0:
                sample_risk = latest_analysis.user_risk_assessment[0]
                print(f"  Sample risk assessment structure:")
                for key, value in sample_risk.items():
                    print(f"    {key}: {value}")
        else:
            print(f"  Risk assessment type: {type(latest_analysis.user_risk_assessment)}")
            print(f"  Risk assessment data: {latest_analysis.user_risk_assessment}")
    else:
        print("  No user_risk_assessment data")
    print()
    
    # Check model methods
    print("MODEL METHOD RESULTS:")
    print("-" * 25)
    print(f"  get_total_users(): {latest_analysis.get_total_users()}")
    print(f"  get_total_transactions(): {latest_analysis.get_total_transactions()}")
    print(f"  get_anomalies_count(): {latest_analysis.get_anomalies_count()}")
    print(f"  get_high_risk_users_count(): {latest_analysis.get_high_risk_users_count()}")
    print()
    
    # Check all analyses summary
    print("ALL ANALYSES SUMMARY:")
    print("-" * 25)
    analyses = UserAnalysisResult.objects.order_by('-analysis_date')[:5]
    for i, analysis in enumerate(analyses):
        print(f"  {i+1}. {analysis.data_file.file_name if analysis.data_file else 'N/A'} - {analysis.analysis_date} - Status: {analysis.status}")
        print(f"     Users: {analysis.get_total_users()}, Transactions: {analysis.get_total_transactions()}, Anomalies: {analysis.get_anomalies_count()}")
    
    if total_analyses > 5:
        print(f"  ... and {total_analyses - 5} more analyses")
    
    print()
    print("=" * 80)

def check_related_data():
    """Check related data"""
    
    print("RELATED DATA:")
    print("-" * 15)
    
    # Data Files
    data_files = DataFile.objects.all()
    print(f"Data Files: {data_files.count()}")
    for df in data_files[:3]:
        print(f"  - {df.file_name} (ID: {df.id}, Status: {df.status}, Records: {df.total_records})")
    print()
    
    # Processing Jobs
    jobs = FileProcessingJob.objects.all()
    print(f"Processing Jobs: {jobs.count()}")
    for job in jobs[:3]:
        print(f"  - Job {job.id} (File: {job.data_file.file_name if job.data_file else 'N/A'}, Status: {job.status})")
    print()
    
    # SAP GL Postings
    postings = SAPGLPosting.objects.all()
    print(f"SAP GL Postings: {postings.count()}")
    if postings.count() > 0:
        unique_users = postings.values('user_name').distinct().count()
        unique_accounts = postings.values('gl_account').distinct().count()
        unique_files = postings.values('data_file').distinct().count()
        print(f"  - Unique Users: {unique_users}")
        print(f"  - Unique Accounts: {unique_accounts}")
        print(f"  - Unique Files: {unique_files}")
    print()

if __name__ == "__main__":
    try:
        check_user_analysis_data()
        check_related_data()
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc() 