#!/usr/bin/env python
"""
Check if file upload triggers task submission
"""

import os
import django
import time
from django.utils import timezone

# Setup Django environment
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'analytics.settings')
django.setup()

from core.models import DataFile, FileProcessingJob
from analytics.celery import app

def check_recent_jobs():
    """Check for recent processing jobs"""
    print("=== Checking Recent Processing Jobs ===")
    
    try:
        # Get recent jobs (last 10 minutes)
        recent_time = timezone.now() - timezone.timedelta(minutes=10)
        recent_jobs = FileProcessingJob.objects.filter(
            created_at__gte=recent_time
        ).order_by('-created_at')
        
        print(f"Found {recent_jobs.count()} recent jobs")
        
        for job in recent_jobs:
            print(f"\nJob ID: {job.id}")
            print(f"  File: {job.data_file.file_name}")
            print(f"  Status: {job.status}")
            print(f"  Created: {job.created_at}")
            print(f"  File Hash: {job.file_hash}")
            
            if job.analytics_results:
                print(f"  Analytics Results: {job.analytics_results}")
        
        return recent_jobs
        
    except Exception as e:
        print(f"❌ Error checking recent jobs: {e}")
        return None

def check_celery_queue():
    """Check current Celery queue status"""
    print("\n=== Checking Celery Queue ===")
    
    try:
        i = app.control.inspect()
        
        # Check active tasks
        active_tasks = i.active()
        if active_tasks:
            print(f"Active tasks: {len(active_tasks)}")
            for worker, tasks in active_tasks.items():
                print(f"  {worker}: {len(tasks)} tasks")
                for task in tasks:
                    print(f"    - {task['name']} (ID: {task['id']})")
        else:
            print("No active tasks")
        
        # Check reserved tasks
        reserved_tasks = i.reserved()
        if reserved_tasks:
            print(f"Reserved tasks: {len(reserved_tasks)}")
            for worker, tasks in reserved_tasks.items():
                print(f"  {worker}: {len(tasks)} tasks")
                for task in tasks:
                    print(f"    - {task['name']} (ID: {task['id']})")
        else:
            print("No reserved tasks")
        
        # Check registered workers
        registered_workers = i.registered()
        if registered_workers:
            print(f"Registered workers: {len(registered_workers)}")
            for worker_name in registered_workers.keys():
                print(f"  - {worker_name}")
        else:
            print("No registered workers")
        
        return True
        
    except Exception as e:
        print(f"❌ Error checking queue: {e}")
        return False

def monitor_for_new_jobs():
    """Monitor for new jobs being created"""
    print("\n=== Monitoring for New Jobs ===")
    print("Upload a file now and watch for new jobs...")
    
    try:
        # Get current job count
        initial_count = FileProcessingJob.objects.count()
        print(f"Initial job count: {initial_count}")
        
        # Monitor for 60 seconds
        for i in range(20):  # 20 checks, 3 seconds apart
            time.sleep(3)
            current_count = FileProcessingJob.objects.count()
            
            if current_count > initial_count:
                print(f"✅ New job detected! Count: {current_count}")
                
                # Get the new job
                new_jobs = FileProcessingJob.objects.filter(
                    created_at__gte=timezone.now() - timezone.timedelta(seconds=10)
                )
                
                for job in new_jobs:
                    print(f"  New Job ID: {job.id}")
                    print(f"  File: {job.data_file.file_name}")
                    print(f"  Status: {job.status}")
                
                return True
            else:
                print(f"⏳ No new jobs yet... (Count: {current_count})")
        
        print("⏰ No new jobs detected in 60 seconds")
        return False
        
    except Exception as e:
        print(f"❌ Error monitoring jobs: {e}")
        return False

def test_manual_job_creation():
    """Test creating a job manually to see if tasks are triggered"""
    print("\n=== Testing Manual Job Creation ===")
    
    try:
        # Get a data file
        data_file = DataFile.objects.first()
        if not data_file:
            print("❌ No data file found")
            return False
        
        # Create a job manually
        job = FileProcessingJob.objects.create(
            data_file=data_file,
            file_hash=f"manual_test_{int(time.time())}",
            run_anomalies=True,
            status='PENDING'
        )
        
        print(f"✅ Created manual job: {job.id}")
        print(f"  File: {job.data_file.file_name}")
        print(f"  Status: {job.status}")
        
        # Check if any tasks were triggered
        time.sleep(5)
        check_celery_queue()
        
        return True
        
    except Exception as e:
        print(f"❌ Error creating manual job: {e}")
        return False

def main():
    """Main function"""
    print("🔍 UPLOAD TRIGGER CHECK")
    print(f"Check started at: {timezone.now()}")
    
    # Check recent jobs
    recent_jobs = check_recent_jobs()
    
    # Check current queue
    check_celery_queue()
    
    # Test manual job creation
    test_manual_job_creation()
    
    # Monitor for new jobs
    print("\n" + "="*50)
    print("Now upload a file and watch for new jobs...")
    monitor_for_new_jobs()
    
    print("\n✅ Upload trigger check completed!")

if __name__ == "__main__":
    main() 