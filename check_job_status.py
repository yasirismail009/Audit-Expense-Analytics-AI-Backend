#!/usr/bin/env python3
"""
Script to check the status of a processing job
"""

import os
import django

# Setup Django
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'analytics.settings')
django.setup()

from core.models import FileProcessingJob
from django.utils import timezone

def check_job_status(job_id):
    """Check the status of a processing job"""
    
    try:
        job = FileProcessingJob.objects.get(id=job_id)
        
        print(f"Job ID: {job.id}")
        print(f"File: {job.data_file.file_name}")
        print(f"Status: {job.status}")
        print(f"Created: {job.created_at}")
        print(f"Started: {job.started_at}")
        print(f"Completed: {job.completed_at}")
        print(f"Processing Duration: {job.processing_duration} seconds")
        print(f"Run Anomalies: {job.run_anomalies}")
        print(f"Requested Anomalies: {job.requested_anomalies}")
        
        if job.error_message:
            print(f"Error: {job.error_message}")
        
        # Check if there are any results
        if job.analytics_results:
            print(f"Analytics Results: Available")
        if job.anomaly_results:
            print(f"Anomaly Results: Available")
        if job.ml_training_results:
            print(f"ML Training Results: Available")
            
        return job
        
    except FileProcessingJob.DoesNotExist:
        print(f"Error: Job with ID {job_id} not found")
        return None
    except Exception as e:
        print(f"Error checking job status: {e}")
        return None

if __name__ == "__main__":
    job_id = "fa6315d5-1588-4cfc-a260-96a0a9c78820"
    
    print(f"Checking job status for {job_id}...")
    job = check_job_status(job_id) 