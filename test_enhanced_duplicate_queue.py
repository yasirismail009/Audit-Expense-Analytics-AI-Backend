#!/usr/bin/env python
"""
Script to test enhanced duplicate analysis as primary method in queue
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

def test_enhanced_duplicate_queue():
    """Test enhanced duplicate analysis as primary method in queue"""
    print("🧪 Testing Enhanced Duplicate Analysis as Primary Method...")
    
    try:
        # Create a test data file
        test_data_file = DataFile.objects.create(
            file_name="test_enhanced_duplicate_primary.csv",
            file_size=4096,
            engagement_id="TEST-ENH-001",
            client_name="Test Enhanced Client",
            company_name="Test Enhanced Company",
            fiscal_year=2025,
            audit_start_date=timezone.now().date(),
            audit_end_date=timezone.now().date(),
            status='COMPLETED'
        )
        
        # Create a test processing job with duplicate analysis enabled
        test_job = FileProcessingJob.objects.create(
            data_file=test_data_file,
            file_hash="test_enhanced_hash_primary_1234567890abcdef",
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
        
        print(f"\n🎯 Expected Behavior:")
        print(f"   - Enhanced duplicate analysis should run FIRST (primary method)")
        print(f"   - ML model should enhance results (optional)")
        print(f"   - Full enhanced analysis with all 6 duplicate types")
        print(f"   - Comprehensive breakdowns and chart data")
        
        return test_job.id
        
    except Exception as e:
        print(f"❌ Error testing enhanced duplicate queue: {e}")
        return None

if __name__ == '__main__':
    test_enhanced_duplicate_queue() 