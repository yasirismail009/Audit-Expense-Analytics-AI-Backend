#!/usr/bin/env python3
"""
Script to queue a specific file for processing
"""

import os
import sys
import django

# Setup Django
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'analytics.settings')
django.setup()

from core.models import DataFile, FileProcessingJob
from django.utils import timezone

def queue_file_for_processing(file_id):
    """Queue a file for processing by creating a FileProcessingJob"""
    
    try:
        # Check if file exists
        data_file = DataFile.objects.get(id=file_id)
        print(f"Found file: {data_file.file_name}")
        print(f"File status: {data_file.status}")
        
        # Check if there's already a pending job for this file
        existing_job = FileProcessingJob.objects.filter(
            data_file=data_file,
            status__in=['PENDING', 'QUEUED', 'PROCESSING']
        ).first()
        
        if existing_job:
            print(f"File already has a pending job: {existing_job.id}")
            print(f"Job status: {existing_job.status}")
            return existing_job
        
        # Create a new processing job
        job = FileProcessingJob.objects.create(
            data_file=data_file,
            file_hash=data_file.file_name,  # Using filename as hash for now
            run_anomalies=True,
            requested_anomalies=['duplicate', 'backdated', 'holiday', 'overall'],
            status='PENDING'
        )
        
        print(f"Created processing job: {job.id}")
        print(f"Job status: {job.status}")
        print(f"Requested anomalies: {job.requested_anomalies}")
        
        return job
        
    except DataFile.DoesNotExist:
        print(f"Error: File with ID {file_id} not found")
        return None
    except Exception as e:
        print(f"Error creating processing job: {e}")
        return None

if __name__ == "__main__":
    file_id = "f9215989-0e3c-4f64-ba4d-7eb3fd3fe0af"
    
    print(f"Queueing file {file_id} for processing...")
    job = queue_file_for_processing(file_id)
    
    if job:
        print(f"\n✅ File queued successfully!")
        print(f"Job ID: {job.id}")
        print(f"File: {job.data_file.file_name}")
        print(f"Status: {job.status}")
        print(f"Created: {job.created_at}")
    else:
        print("\n❌ Failed to queue file for processing")
        sys.exit(1) 