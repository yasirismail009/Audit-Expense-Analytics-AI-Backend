#!/usr/bin/env python3
"""
Script to check the queue status for uploaded files
"""

import os
import sys
import django
from datetime import datetime

# Add the project directory to Python path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# Setup Django
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'analytics.settings')
django.setup()

from core.models import FileProcessingJob, DataFile
from django.db.models import Count
from django.utils import timezone

def check_queue_status():
    """Check the status of files in the queue"""
    
    print("🔍 FILE QUEUE STATUS CHECK")
    print("=" * 50)
    
    # Get all processing jobs ordered by creation date
    jobs = FileProcessingJob.objects.all().order_by('-created_at')
    
    if not jobs.exists():
        print("❌ No processing jobs found in the system")
        return
    
    # Count jobs by status
    status_counts = jobs.values('status').annotate(count=Count('status')).order_by('status')
    
    print("\n📊 JOB STATUS SUMMARY:")
    print("-" * 30)
    for status_info in status_counts:
        status = status_info['status']
        count = status_info['count']
        print(f"  {status}: {count} jobs")
    
    # Show recent jobs (last 10)
    print(f"\n📁 RECENT PROCESSING JOBS (Last 10):")
    print("-" * 50)
    
    for job in jobs[:10]:
        print(f"\n🔹 Job ID: {job.id}")
        print(f"   File: {job.data_file.file_name if job.data_file else 'N/A'}")
        print(f"   Status: {job.status}")
        print(f"   Created: {job.created_at.strftime('%Y-%m-%d %H:%M:%S')}")
        
        if job.started_at:
            print(f"   Started: {job.started_at.strftime('%Y-%m-%d %H:%M:%S')}")
        
        if job.completed_at:
            print(f"   Completed: {job.completed_at.strftime('%Y-%m-%d %H:%M:%S')}")
        
        if job.processing_duration:
            print(f"   Duration: {job.processing_duration:.2f} seconds")
        
        if job.error_message:
            print(f"   Error: {job.error_message[:100]}...")
        
        if job.run_anomalies:
            print(f"   Anomaly Detection: Enabled")
            if job.requested_anomalies:
                print(f"   Requested Anomalies: {', '.join(job.requested_anomalies)}")
    
    # Show pending/queued jobs specifically
    pending_jobs = jobs.filter(status__in=['PENDING', 'QUEUED'])
    
    if pending_jobs.exists():
        print(f"\n⏳ PENDING/QUEUED JOBS ({pending_jobs.count()}):")
        print("-" * 40)
        
        for job in pending_jobs:
            print(f"\n  🔸 Job ID: {job.id}")
            print(f"     File: {job.data_file.file_name if job.data_file else 'N/A'}")
            print(f"     Status: {job.status}")
            print(f"     Created: {job.created_at.strftime('%Y-%m-%d %H:%M:%S')}")
            
            # Calculate wait time
            wait_time = timezone.now() - job.created_at
            print(f"     Wait Time: {wait_time}")
    else:
        print("\n✅ No pending or queued jobs found")
    
    # Show failed jobs
    failed_jobs = jobs.filter(status='FAILED')
    
    if failed_jobs.exists():
        print(f"\n❌ FAILED JOBS ({failed_jobs.count()}):")
        print("-" * 30)
        
        for job in failed_jobs[:5]:  # Show last 5 failed jobs
            print(f"\n  🔸 Job ID: {job.id}")
            print(f"     File: {job.data_file.file_name if job.data_file else 'N/A'}")
            print(f"     Created: {job.created_at.strftime('%Y-%m-%d %H:%M:%S')}")
            if job.error_message:
                print(f"     Error: {job.error_message[:150]}...")

def check_celery_status():
    """Check Celery queue status"""
    print("\n\n🔧 CELERY QUEUE STATUS:")
    print("=" * 30)
    
    try:
        from celery import current_app
        from celery.task.control import inspect
        
        i = inspect()
        
        # Check active tasks
        active = i.active()
        if active:
            print("\n🔄 ACTIVE TASKS:")
            total_active = 0
            for worker, tasks in active.items():
                print(f"  Worker: {worker} - {len(tasks)} active")
                total_active += len(tasks)
            print(f"  Total active: {total_active}")
        else:
            print("\n✅ No active tasks")
        
        # Check reserved tasks (queued)
        reserved = i.reserved()
        if reserved:
            print("\n📋 QUEUED TASKS:")
            total_queued = 0
            for worker, tasks in reserved.items():
                print(f"  Worker: {worker} - {len(tasks)} queued")
                total_queued += len(tasks)
            print(f"  Total queued: {total_queued}")
        else:
            print("\n✅ No queued tasks")
        
        # Check registered tasks
        registered = i.registered()
        if registered:
            print(f"\n📝 REGISTERED TASKS: {len(registered)}")
        
    except Exception as e:
        print(f"❌ Failed to get Celery status: {str(e)}")
        print("   Make sure Celery is running")

def main():
    """Main function"""
    check_queue_status()
    check_celery_status()
    
    print("\n\n💡 TIPS:")
    print("- If your file shows 'PENDING' status, it's waiting to be processed")
    print("- If it shows 'QUEUED' status, it's in the Celery queue")
    print("- If it shows 'PROCESSING' status, it's currently being analyzed")
    print("- If it shows 'COMPLETED' status, the analysis is finished")
    print("- If it shows 'FAILED' status, there was an error during processing")

if __name__ == "__main__":
    main()
