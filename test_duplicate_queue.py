#!/usr/bin/env python
"""
Script to test duplicate analysis in the queue
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

from core.models import FileProcessingJob, DataFile
from django.utils import timezone
import uuid

def test_duplicate_queue():
    """Test duplicate analysis in the queue"""
    print("🧪 Testing Duplicate Analysis in Queue...")
    
    try:
        # Create a test data file
        test_data_file = DataFile.objects.create(
            file_name="test_duplicate_analysis.csv",
            file_size=2048,
            engagement_id="TEST-DUP-001",
            client_name="Test Duplicate Client",
            company_name="Test Duplicate Company",
            fiscal_year=2025,
            audit_start_date=timezone.now().date(),
            audit_end_date=timezone.now().date(),
            status='COMPLETED'
        )
        
        # Create a test processing job with duplicate analysis enabled
        test_job = FileProcessingJob.objects.create(
            data_file=test_data_file,
            file_hash="test_duplicate_hash_1234567890abcdef",
            run_anomalies=True,  # Enable anomalies to trigger duplicate analysis
            requested_anomalies=['duplicates'],  # Request duplicate analysis
            status='QUEUED'
        )
        
        print(f"✅ Created test job: {str(test_job.id)[:8]}...")
        print(f"   File: {test_data_file.file_name}")
        print(f"   Status: {test_job.status}")
        print(f"   Run Anomalies: {test_job.run_anomalies}")
        print(f"   Requested Anomalies: {test_job.requested_anomalies}")
        print(f"   Created at: {test_job.created_at}")
        
        # Check queue status
        total_jobs = FileProcessingJob.objects.count()
        queued_jobs = FileProcessingJob.objects.filter(status='QUEUED').count()
        
        print(f"\n📊 Queue Status After Test:")
        print(f"   Total Jobs: {total_jobs}")
        print(f"   Queued Jobs: {queued_jobs}")
        
        return test_job.id
        
    except Exception as e:
        print(f"❌ Error testing duplicate queue: {e}")
        return None

if __name__ == '__main__':
    test_duplicate_queue() 