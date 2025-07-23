#!/usr/bin/env python
"""
Script to check queue status
"""

import os
import sys
import django
from pathlib import Path

# Add the project root to Python path
project_root = Path(__file__).resolve().parent
sys.path.insert(0, str(project_root))

# Set Django settings
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'analytics.settings')

# Initialize Django
django.setup()

from core.models import FileProcessingJob

def check_queue_status():
    """Check the current queue status"""
    print("📊 Queue Status Check...")
    
    total_jobs = FileProcessingJob.objects.count()
    queued_jobs = FileProcessingJob.objects.filter(status='QUEUED').count()
    completed_jobs = FileProcessingJob.objects.filter(status='COMPLETED').count()
    failed_jobs = FileProcessingJob.objects.filter(status='FAILED').count()
    processing_jobs = FileProcessingJob.objects.filter(status='PROCESSING').count()
    
    print(f"Total Jobs: {total_jobs}")
    print(f"Queued: {queued_jobs}")
    print(f"Processing: {processing_jobs}")
    print(f"Completed: {completed_jobs}")
    print(f"Failed: {failed_jobs}")
    
    # Show recent jobs
    recent_jobs = FileProcessingJob.objects.order_by('-created_at')[:3]
    if recent_jobs:
        print(f"\n🕒 Recent Jobs:")
        for job in recent_jobs:
            print(f"  {str(job.id)[:8]}... - {job.status} - {job.data_file.file_name if job.data_file else 'No file'}")

if __name__ == '__main__':
    check_queue_status() 