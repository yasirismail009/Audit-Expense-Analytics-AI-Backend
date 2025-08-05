#!/usr/bin/env python3
"""
Script to create a test job for testing auto-processing
"""

import os
import django

# Setup Django
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'analytics.settings')
django.setup()

from django.utils import timezone
from core.models import FileProcessingJob, DataFile

def create_test_job():
    """Create a test job for testing auto-processing"""
    print("🔧 Creating test job...")
    
    try:
        # Get the first data file
        data_file = DataFile.objects.first()
        if not data_file:
            print("❌ No data files found")
            return None
        
        print(f"📋 Using data file: {data_file.file_name}")
        
        # Create a test job with PENDING status
        test_job = FileProcessingJob.objects.create(
            data_file=data_file,
            status='PENDING',
            run_anomalies=True,
            requested_anomalies=['general', 'duplicate', 'backdated'],
            created_at=timezone.now()
        )
        
        print(f"✅ Test job created: {test_job.id}")
        print(f"   Status: {test_job.status}")
        print(f"   File: {test_job.data_file.file_name}")
        
        return test_job
        
    except Exception as e:
        print(f"❌ Error creating test job: {e}")
        import traceback
        traceback.print_exc()
        return None

def main():
    """Main function"""
    print("🧪 TEST JOB CREATION")
    print("=" * 40)
    
    job = create_test_job()
    
    if job:
        print(f"\n🎉 Test job created successfully!")
        print(f"   Job ID: {job.id}")
        print(f"   Status: {job.status}")
        print(f"   File: {job.data_file.file_name}")
        print(f"\n⏰ Auto-processing should pick up this job within 1 minute...")
    else:
        print(f"\n❌ Failed to create test job")

if __name__ == "__main__":
    main() 