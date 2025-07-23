#!/usr/bin/env python3
"""
Check existing data in the database more thoroughly
"""

import os
import sys
import django

# Add the project directory to the Python path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# Setup Django
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'analytics.settings')
django.setup()

from core.models import (
    DataFile, 
    FileProcessingJob, 
    AnalyticsProcessingResult, 
    MLModelProcessingResult, 
    ProcessingJobTracker,
    SAPGLPosting
)

def check_existing_data():
    """Check existing data in detail"""
    print("🔍 DEBUG: ===== Checking Existing Data in Detail =====")
    
    # Check DataFiles
    data_files = DataFile.objects.all()
    print(f"🔍 DEBUG: Total DataFiles: {data_files.count()}")
    for df in data_files:
        print(f"🔍 DEBUG: DataFile: {df.id} - {df.file_name} - Status: {df.status}")
        print(f"🔍 DEBUG:   Uploaded: {df.uploaded_at}")
        print(f"🔍 DEBUG:   Processed: {df.processed_at}")
    
    # Check FileProcessingJobs
    processing_jobs = FileProcessingJob.objects.all()
    print(f"\n🔍 DEBUG: Total FileProcessingJobs: {processing_jobs.count()}")
    for job in processing_jobs:
        print(f"🔍 DEBUG: Job: {job.id} - File: {job.data_file.file_name} - Status: {job.status}")
        print(f"🔍 DEBUG:   Created: {job.created_at}")
        print(f"🔍 DEBUG:   Started: {job.started_at}")
        print(f"🔍 DEBUG:   Completed: {job.completed_at}")
        print(f"🔍 DEBUG:   Run Anomalies: {job.run_anomalies}")
        print(f"🔍 DEBUG:   Requested Anomalies: {job.requested_anomalies}")
    
    # Check AnalyticsProcessingResults
    analytics_results = AnalyticsProcessingResult.objects.all()
    print(f"\n🔍 DEBUG: Total AnalyticsProcessingResults: {analytics_results.count()}")
    for result in analytics_results:
        print(f"🔍 DEBUG: Analytics: {result.id}")
        print(f"🔍 DEBUG:   Type: {result.analytics_type}")
        print(f"🔍 DEBUG:   Status: {result.processing_status}")
        print(f"🔍 DEBUG:   Data File: {result.data_file.file_name}")
        print(f"🔍 DEBUG:   Processing Job: {result.processing_job.id if result.processing_job else 'None'}")
        print(f"🔍 DEBUG:   Created: {result.created_at}")
        print(f"🔍 DEBUG:   Processed: {result.processed_at}")
        print(f"🔍 DEBUG:   Total Transactions: {result.total_transactions}")
        print(f"🔍 DEBUG:   Total Amount: {result.total_amount}")
    
    # Check MLModelProcessingResults
    ml_results = MLModelProcessingResult.objects.all()
    print(f"\n🔍 DEBUG: Total MLModelProcessingResults: {ml_results.count()}")
    for result in ml_results:
        print(f"🔍 DEBUG: ML: {result.id}")
        print(f"🔍 DEBUG:   Type: {result.model_type}")
        print(f"🔍 DEBUG:   Status: {result.processing_status}")
        print(f"🔍 DEBUG:   Data File: {result.data_file.file_name}")
        print(f"🔍 DEBUG:   Processing Job: {result.processing_job.id if result.processing_job else 'None'}")
        print(f"🔍 DEBUG:   Created: {result.created_at}")
        print(f"🔍 DEBUG:   Processed: {result.processed_at}")
        print(f"🔍 DEBUG:   Anomalies Detected: {result.anomalies_detected}")
        print(f"🔍 DEBUG:   Duplicates Found: {result.duplicates_found}")
    
    # Check ProcessingJobTrackers
    job_trackers = ProcessingJobTracker.objects.all()
    print(f"\n🔍 DEBUG: Total ProcessingJobTrackers: {job_trackers.count()}")
    for tracker in job_trackers:
        print(f"🔍 DEBUG: Tracker: {tracker.id}")
        print(f"🔍 DEBUG:   Progress: {tracker.overall_progress}%")
        print(f"🔍 DEBUG:   Steps: {tracker.completed_steps}/{tracker.total_steps}")
        print(f"🔍 DEBUG:   Current Step: {tracker.current_step}")
        print(f"🔍 DEBUG:   Started: {tracker.started_at}")
        print(f"🔍 DEBUG:   Completed: {tracker.completed_at}")
        print(f"🔍 DEBUG:   Processing Job: {tracker.processing_job.id}")
        print(f"🔍 DEBUG:   Data File: {tracker.data_file.file_name}")
    
    # Check SAPGLPostings
    transactions = SAPGLPosting.objects.all()
    print(f"\n🔍 DEBUG: Total SAPGLPostings: {transactions.count()}")
    if transactions.exists():
        sample = transactions.first()
        print(f"🔍 DEBUG: Sample transaction: {sample}")
        print(f"🔍 DEBUG:   Document: {sample.document_number}")
        print(f"🔍 DEBUG:   Amount: {sample.amount_local_currency}")
        print(f"🔍 DEBUG:   User: {sample.user_name}")
        print(f"🔍 DEBUG:   Created: {sample.created_at}")

if __name__ == "__main__":
    check_existing_data() 