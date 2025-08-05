#!/usr/bin/env python
"""
Script to examine the actual data stored in UserAnalysisResult table
"""

import os
import sys
import django
import json
from datetime import datetime

# Setup Django
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'analytics.settings')
django.setup()

from core.models import UserAnalysisResult, DataFile, FileProcessingJob

def check_user_analysis_data():
    """Check what data is stored in UserAnalysisResult table"""
    
    print("=" * 80)
    print("USER ANALYSIS DATA EXAMINATION")
    print("=" * 80)
    
    # Get all user analysis results
    user_analyses = UserAnalysisResult.objects.all().order_by('-analysis_date')
    
    print(f"Total User Analysis Results: {user_analyses.count()}")
    print()
    
    if user_analyses.count() == 0:
        print("No user analysis results found in database.")
        return
    
    # Examine the most recent analysis
    latest_analysis = user_analyses.first()
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
    
    # Check chart_data
    print("CHART DATA:")
    print("-" * 15)
    if latest_analysis.chart_data:
        print(f"  Chart data keys: {list(latest_analysis.chart_data.keys())}")
        for key, value in latest_analysis.chart_data.items():
            if isinstance(value, list):
                print(f"    {key}: {len(value)} items")
            else:
                print(f"    {key}: {type(value)}")
    else:
        print("  No chart_data")
    print()
    
    # Check export_data
    print("EXPORT DATA:")
    print("-" * 15)
    if latest_analysis.export_data:
        print(f"  Export data items: {len(latest_analysis.export_data)}")
        if len(latest_analysis.export_data) > 0:
            sample_export = latest_analysis.export_data[0]
            print(f"  Sample export data structure:")
            for key, value in sample_export.items():
                if isinstance(value, list) and len(value) > 3:
                    print(f"    {key}: {value[:3]}... (showing first 3 items)")
                else:
                    print(f"    {key}: {value}")
    else:
        print("  No export_data")
    print()
    
    # Check other fields
    print("OTHER FIELDS:")
    print("-" * 15)
    print(f"  user_debit_analysis: {len(latest_analysis.user_debit_analysis) if latest_analysis.user_debit_analysis else 0} items")
    print(f"  user_account_distribution: {len(latest_analysis.user_account_distribution) if latest_analysis.user_account_distribution else 0} items")
    print(f"  user_fs_line_distribution: {len(latest_analysis.user_fs_line_distribution) if latest_analysis.user_fs_line_distribution else 0} items")
    print(f"  user_patterns: {type(latest_analysis.user_patterns)}")
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
    for i, analysis in enumerate(user_analyses[:5]):  # Show first 5
        print(f"  {i+1}. {analysis.data_file.file_name if analysis.data_file else 'N/A'} - {analysis.analysis_date} - Status: {analysis.status}")
        print(f"     Users: {analysis.get_total_users()}, Transactions: {analysis.get_total_transactions()}, Anomalies: {analysis.get_anomalies_count()}")
    
    if user_analyses.count() > 5:
        print(f"  ... and {user_analyses.count() - 5} more analyses")
    
    print()
    print("=" * 80)

def check_data_file_structure():
    """Check the structure of related data files"""
    
    print("DATA FILE STRUCTURE:")
    print("-" * 25)
    
    data_files = DataFile.objects.all().order_by('-uploaded_at')[:3]
    
    for i, data_file in enumerate(data_files):
        print(f"Data File {i+1}:")
        print(f"  ID: {data_file.id}")
        print(f"  Name: {data_file.file_name}")
        print(f"  Status: {data_file.status}")
        print(f"  Total Records: {data_file.total_records}")
        print(f"  Processed Records: {data_file.processed_records}")
        print(f"  Failed Records: {data_file.failed_records}")
        print(f"  Uploaded: {data_file.uploaded_at}")
        print(f"  Processed: {data_file.processed_at}")
        print()
        
        # Check related user analyses
        user_analyses = data_file.user_analyses.all()
        print(f"  Related User Analyses: {user_analyses.count()}")
        for analysis in user_analyses:
            print(f"    - {analysis.analysis_date} - Status: {analysis.status}")
        print()

def check_processing_jobs():
    """Check processing jobs structure"""
    
    print("PROCESSING JOBS STRUCTURE:")
    print("-" * 30)
    
    jobs = FileProcessingJob.objects.all().order_by('-created_at')[:3]
    
    for i, job in enumerate(jobs):
        print(f"Job {i+1}:")
        print(f"  ID: {job.id}")
        print(f"  File: {job.data_file.file_name if job.data_file else 'N/A'}")
        print(f"  Status: {job.status}")
        print(f"  Run Anomalies: {job.run_anomalies}")
        print(f"  Requested Anomalies: {job.requested_anomalies}")
        print(f"  Created: {job.created_at}")
        print(f"  Started: {job.started_at}")
        print(f"  Completed: {job.completed_at}")
        print(f"  Duration: {job.processing_duration} seconds")
        print()
        
        # Check related user results
        user_results = job.user_results.all()
        print(f"  Related User Results: {user_results.count()}")
        for result in user_results:
            print(f"    - {result.analysis_date} - Status: {result.status}")
        print()

if __name__ == "__main__":
    try:
        check_user_analysis_data()
        check_data_file_structure()
        check_processing_jobs()
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc() 