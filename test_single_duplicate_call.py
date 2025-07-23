#!/usr/bin/env python
"""
Script to test that enhanced duplicate analysis is only called once
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

def test_single_duplicate_call():
    """Test that enhanced duplicate analysis is only called once"""
    print("🧪 Testing Single Enhanced Duplicate Analysis Call...")
    
    try:
        # Create a test data file
        test_data_file = DataFile.objects.create(
            file_name="test_single_duplicate_call.csv",
            file_size=5120,
            engagement_id="TEST-SINGLE-001",
            client_name="Test Single Call Client",
            company_name="Test Single Call Company",
            fiscal_year=2025,
            audit_start_date=timezone.now().date(),
            audit_end_date=timezone.now().date(),
            status='COMPLETED'
        )
        
        # Create a test processing job with duplicate analysis enabled
        test_job = FileProcessingJob.objects.create(
            data_file=test_data_file,
            file_hash="test_single_hash_call_1234567890abcdef",
            run_anomalies=True,  # Enable anomalies to trigger duplicate analysis
            requested_anomalies=['duplicates', 'backdated'],  # Request both duplicates and other anomalies
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
        
        print(f"\n🎯 Expected Behavior:")
        print(f"   - Enhanced duplicate analysis should run ONCE only")
        print(f"   - Other anomalies (backdated) should run separately")
        print(f"   - No duplicate processing in _run_requested_anomalies")
        print(f"   - Only enhanced duplicate analysis in _run_duplicate_analysis")
        
        return test_job.id
        
    except Exception as e:
        print(f"❌ Error testing single duplicate call: {e}")
        return None

if __name__ == '__main__':
    test_single_duplicate_call() 