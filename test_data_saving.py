#!/usr/bin/env python3
"""
Test script to check if data is being saved to the database
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
    AnalyticsProcessingResult, 
    MLModelProcessingResult, 
    ProcessingJobTracker,
    SAPGLPosting
)

def check_database_data():
    """Check what data exists in the database"""
    print("🔍 DEBUG: ===== Checking Database Data =====")
    
    # Check DataFiles
    data_files = DataFile.objects.all()
    print(f"🔍 DEBUG: Total DataFiles: {data_files.count()}")
    for df in data_files:
        print(f"🔍 DEBUG: DataFile: {df.id} - {df.file_name} - Status: {df.status}")
    
    # Check FileProcessingJobs
    processing_jobs = FileProcessingJob.objects.all()
    print(f"🔍 DEBUG: Total FileProcessingJobs: {processing_jobs.count()}")
    for job in processing_jobs:
        print(f"🔍 DEBUG: Job: {job.id} - File: {job.data_file.file_name} - Status: {job.status}")
    
    # Check AnalyticsProcessingResults
    analytics_results = AnalyticsProcessingResult.objects.all()
    print(f"🔍 DEBUG: Total AnalyticsProcessingResults: {analytics_results.count()}")
    for result in analytics_results:
        print(f"🔍 DEBUG: Analytics: {result.id} - Type: {result.analytics_type} - Status: {result.processing_status}")
    
    # Check MLModelProcessingResults
    ml_results = MLModelProcessingResult.objects.all()
    print(f"🔍 DEBUG: Total MLModelProcessingResults: {ml_results.count()}")
    for result in ml_results:
        print(f"🔍 DEBUG: ML: {result.id} - Type: {result.model_type} - Status: {result.processing_status}")
    
    # Check ProcessingJobTrackers
    job_trackers = ProcessingJobTracker.objects.all()
    print(f"🔍 DEBUG: Total ProcessingJobTrackers: {job_trackers.count()}")
    for tracker in job_trackers:
        print(f"🔍 DEBUG: Tracker: {tracker.id} - Progress: {tracker.overall_progress}% - Steps: {tracker.completed_steps}/{tracker.total_steps}")
    
    # Check SAPGLPostings
    transactions = SAPGLPosting.objects.all()
    print(f"🔍 DEBUG: Total SAPGLPostings: {transactions.count()}")
    if transactions.exists():
        print(f"🔍 DEBUG: Sample transaction: {transactions.first()}")

def create_test_job():
    """Create a test processing job to trigger the data saving"""
    print("🔍 DEBUG: ===== Creating Test Job =====")
    
    # Get the first data file
    try:
        data_file = DataFile.objects.first()
        if not data_file:
            print("🔍 DEBUG: No DataFile found, creating one...")
            data_file = DataFile.objects.create(
                file_name="Test File.csv",
                file_size=1024,
                engagement_id="TEST-001",
                client_name="Test Client",
                company_name="Test Company",
                fiscal_year=2024,
                audit_start_date=datetime(2024, 1, 1).date(),
                audit_end_date=datetime(2024, 12, 31).date(),
                status='PENDING'
            )
            print(f"🔍 DEBUG: Created DataFile: {data_file.id}")
        
        # Create a processing job
        job = FileProcessingJob.objects.create(
            data_file=data_file,
            file_hash="test_hash_123",
            run_anomalies=True,
            requested_anomalies=['duplicate'],
            status='PENDING'
        )
        print(f"🔍 DEBUG: Created FileProcessingJob: {job.id}")
        
        return job
        
    except Exception as e:
        print(f"🔍 DEBUG: ERROR creating test job: {e}")
        return None

def trigger_processing(job_id):
    """Trigger the processing for a job"""
    print(f"🔍 DEBUG: ===== Triggering Processing for Job {job_id} =====")
    
    try:
        from core.tasks import process_file_with_anomalies
        result = process_file_with_anomalies.delay(job_id)
        print(f"🔍 DEBUG: Celery task started: {result.id}")
        return result
    except Exception as e:
        print(f"🔍 DEBUG: ERROR triggering processing: {e}")
        return None

if __name__ == "__main__":
    print("🔍 DEBUG: ===== Starting Database Data Test =====")
    
    # Check current data
    check_database_data()
    
    # Create test job
    job = create_test_job()
    
    if job:
        # Trigger processing
        result = trigger_processing(str(job.id))
        
        if result:
            print(f"🔍 DEBUG: Processing triggered successfully")
            print(f"🔍 DEBUG: Task ID: {result.id}")
            print(f"🔍 DEBUG: Check Celery worker logs for processing details")
        else:
            print(f"🔍 DEBUG: Failed to trigger processing")
    
    print("🔍 DEBUG: ===== Test Complete =====") 