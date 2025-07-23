#!/usr/bin/env python
"""
Script to monitor Celery queue every 10-15 seconds
"""

import os
import sys
import django
import time
from pathlib import Path

# Add the project root to Python path
project_root = Path(__file__).resolve().parent
sys.path.insert(0, str(project_root))

# Set Django settings
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'analytics.settings')

# Initialize Django
django.setup()

from core.models import FileProcessingJob
from django.utils import timezone

def monitor_queue():
    """Monitor the queue every 10-15 seconds"""
    print("🔍 Starting Celery Queue Monitor...")
    print("=" * 60)
    
    while True:
        try:
            # Get current time
            current_time = timezone.now().strftime("%Y-%m-%d %H:%M:%S")
            
            # Get queue statistics
            total_jobs = FileProcessingJob.objects.count()
            queued_jobs = FileProcessingJob.objects.filter(status='QUEUED').count()
            processing_jobs = FileProcessingJob.objects.filter(status='PROCESSING').count()
            completed_jobs = FileProcessingJob.objects.filter(status='COMPLETED').count()
            failed_jobs = FileProcessingJob.objects.filter(status='FAILED').count()
            celery_error_jobs = FileProcessingJob.objects.filter(status='CELERY_ERROR').count()
            skipped_jobs = FileProcessingJob.objects.filter(status='SKIPPED').count()
            
            # Get recent jobs
            recent_jobs = FileProcessingJob.objects.order_by('-created_at')[:5]
            
            # Print status
            print(f"\n📊 Queue Status - {current_time}")
            print("-" * 40)
            print(f"Total Jobs: {total_jobs}")
            print(f"Queued: {queued_jobs}")
            print(f"Processing: {processing_jobs}")
            print(f"Completed: {completed_jobs}")
            print(f"Failed: {failed_jobs}")
            print(f"Celery Errors: {celery_error_jobs}")
            print(f"Skipped: {skipped_jobs}")
            
            # Show recent jobs
            if recent_jobs:
                print(f"\n🕒 Recent Jobs (Last 5):")
                print("-" * 40)
                for job in recent_jobs:
                    duration = ""
                    if job.started_at and job.completed_at:
                        duration = f" ({(job.completed_at - job.started_at).total_seconds():.1f}s)"
                    elif job.started_at:
                        duration = f" (running for {(timezone.now() - job.started_at).total_seconds():.1f}s)"
                    
                    print(f"  {str(job.id)[:8]}... - {job.status} - {job.data_file.file_name if job.data_file else 'No file'}{duration}")
            
            # Show processing jobs details
            if processing_jobs > 0:
                print(f"\n⚙️ Currently Processing:")
                print("-" * 40)
                processing_job_list = FileProcessingJob.objects.filter(status='PROCESSING')
                for job in processing_job_list:
                    if job.started_at:
                        running_time = (timezone.now() - job.started_at).total_seconds()
                        print(f"  {str(job.id)[:8]}... - Running for {running_time:.1f}s - {job.data_file.file_name if job.data_file else 'No file'}")
            
            # Show queued jobs
            if queued_jobs > 0:
                print(f"\n⏳ Queued Jobs:")
                print("-" * 40)
                queued_job_list = FileProcessingJob.objects.filter(status='QUEUED').order_by('created_at')
                for job in queued_job_list:
                    wait_time = (timezone.now() - job.created_at).total_seconds()
                    print(f"  {str(job.id)[:8]}... - Waiting for {wait_time:.1f}s - {job.data_file.file_name if job.data_file else 'No file'}")
            
            # Show failed jobs
            if failed_jobs > 0:
                print(f"\n❌ Failed Jobs:")
                print("-" * 40)
                failed_job_list = FileProcessingJob.objects.filter(status='FAILED').order_by('-created_at')[:3]
                for job in failed_job_list:
                    print(f"  {str(job.id)[:8]}... - {job.error_message[:50] if job.error_message else 'No error message'}...")
            
            print("\n" + "=" * 60)
            print("⏰ Waiting 12 seconds before next check...")
            
            # Wait 12 seconds
            time.sleep(12)
            
        except KeyboardInterrupt:
            print("\n🛑 Queue monitoring stopped by user")
            break
        except Exception as e:
            print(f"\n❌ Error monitoring queue: {e}")
            print("⏰ Retrying in 12 seconds...")
            time.sleep(12)

if __name__ == '__main__':
    monitor_queue() 