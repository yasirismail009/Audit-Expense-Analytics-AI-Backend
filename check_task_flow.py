#!/usr/bin/env python3
"""
Test script to check the task flow and identify where it's failing
"""

import os
import sys
import django
from django.utils import timezone
from datetime import datetime

# Add the project directory to the Python path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# Setup Django
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'analytics.settings')
django.setup()

from core.models import (
    DataFile, 
    FileProcessingJob, 
    SAPGLPosting,
    AnalyticsProcessingResult, 
    MLModelProcessingResult, 
    ProcessingJobTracker
)

def check_task_flow():
    """Check the task flow and identify issues"""
    print("🔍 DEBUG: ===== Checking Task Flow =====")
    
    # Check DataFile
    try:
        data_file = DataFile.objects.latest('uploaded_at')
        print(f"🔍 DEBUG: Latest DataFile: {data_file.id} - {data_file.file_name}")
        print(f"🔍 DEBUG: Uploaded at: {data_file.uploaded_at}")
        print(f"🔍 DEBUG: Status: {data_file.status}")
    except Exception as e:
        print(f"🔍 DEBUG: ERROR getting DataFile: {e}")
        return False
    
    # Check FileProcessingJob
    try:
        job = FileProcessingJob.objects.filter(data_file=data_file).first()
        if job:
            print(f"🔍 DEBUG: FileProcessingJob: {job.id}")
            print(f"🔍 DEBUG: Status: {job.status}")
            print(f"🔍 DEBUG: Run anomalies: {job.run_anomalies}")
            print(f"🔍 DEBUG: Requested anomalies: {job.requested_anomalies}")
        else:
            print(f"🔍 DEBUG: No FileProcessingJob found for this DataFile")
            return False
    except Exception as e:
        print(f"🔍 DEBUG: ERROR getting FileProcessingJob: {e}")
        return False
    
    # Check SAPGLPosting transactions
    try:
        # Check transactions created after file upload
        transactions_after_upload = SAPGLPosting.objects.filter(
            created_at__gte=data_file.uploaded_at
        ).count()
        print(f"🔍 DEBUG: Transactions after upload: {transactions_after_upload}")
        
        # Check all transactions
        total_transactions = SAPGLPosting.objects.count()
        print(f"🔍 DEBUG: Total transactions: {total_transactions}")
        
        if total_transactions == 0:
            print(f"🔍 DEBUG: NO TRANSACTIONS IN DATABASE!")
            return False
        
        # Show sample transactions
        sample_transactions = SAPGLPosting.objects.all()[:5]
        print(f"🔍 DEBUG: Sample transactions:")
        for i, t in enumerate(sample_transactions):
            print(f"  {i+1}. Doc: {t.document_number}, Amount: {t.amount_local_currency}, User: {t.user_name}, Created: {t.created_at}")
            
    except Exception as e:
        print(f"🔍 DEBUG: ERROR checking transactions: {e}")
        return False
    
    # Check existing analytics results
    try:
        analytics_results = AnalyticsProcessingResult.objects.filter(data_file=data_file)
        print(f"🔍 DEBUG: Existing AnalyticsProcessingResults: {analytics_results.count()}")
        for result in analytics_results:
            print(f"  - {result.analytics_type}: {result.processing_status}")
            
        ml_results = MLModelProcessingResult.objects.filter(data_file=data_file)
        print(f"🔍 DEBUG: Existing MLModelProcessingResults: {ml_results.count()}")
        for result in ml_results:
            print(f"  - {result.model_type}: {result.processing_status}")
            
        job_trackers = ProcessingJobTracker.objects.filter(data_file=data_file)
        print(f"🔍 DEBUG: Existing ProcessingJobTrackers: {job_trackers.count()}")
        for tracker in job_trackers:
            print(f"  - Progress: {tracker.overall_progress}% - Steps: {tracker.completed_steps}/{tracker.total_steps}")
            
    except Exception as e:
        print(f"🔍 DEBUG: ERROR checking existing results: {e}")
        return False
    
    # Test _process_file_content logic
    print(f"🔍 DEBUG: ===== Testing _process_file_content logic =====")
    try:
        # Simulate the logic from _process_file_content
        transactions = list(SAPGLPosting.objects.filter(
            created_at__gte=data_file.uploaded_at
        ).order_by('created_at'))
        
        if not transactions:
            print(f"🔍 DEBUG: No transactions found with upload date filter, getting all transactions")
            transactions = list(SAPGLPosting.objects.all().order_by('created_at'))
        
        print(f"🔍 DEBUG: Retrieved {len(transactions)} transactions")
        
        if len(transactions) > 0:
            result = {
                'success': True,
                'transactions': transactions
            }
            print(f"🔍 DEBUG: _process_file_content would return: {result['success']}")
            print(f"🔍 DEBUG: Transaction count: {len(result['transactions'])}")
            return True
        else:
            print(f"🔍 DEBUG: No transactions found!")
            return False
            
    except Exception as e:
        print(f"🔍 DEBUG: ERROR testing _process_file_content logic: {e}")
        return False

if __name__ == "__main__":
    success = check_task_flow()
    if success:
        print("✅ Task flow check PASSED")
    else:
        print("❌ Task flow check FAILED") 