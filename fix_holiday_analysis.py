#!/usr/bin/env python3
"""
Script to fix holiday analysis issue by updating the processing job
"""

import os
import django

# Setup Django
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'analytics.settings')
django.setup()

from core.models import FileProcessingJob, DataFile
from django.utils import timezone
import hashlib

def fix_holiday_analysis_issue():
    """Fix the holiday analysis issue by creating a new job with correct anomaly types"""
    
    file_id = "f9215989-0e3c-4f64-ba4d-7eb3fd3fe0af"
    
    try:
        # Check if file exists
        data_file = DataFile.objects.get(id=file_id)
        print(f"Found file: {data_file.file_name}")
        print(f"File status: {data_file.status}")
        
        # Check current jobs
        existing_jobs = FileProcessingJob.objects.filter(data_file=data_file).order_by('-created_at')
        print(f"\n📋 Current jobs for this file ({len(existing_jobs)} total):")
        
        for i, job in enumerate(existing_jobs[:3]):
            status_icon = "🟢" if job.status == 'COMPLETED' else "🟡" if job.status == 'PROCESSING' else "🔴"
            print(f"  {i+1}. {status_icon} {job.id} - {job.status}")
            print(f"      Requested anomalies: {job.requested_anomalies}")
            print(f"      Created: {job.created_at}")
        
        # Check if there's a job with holiday analysis
        holiday_job = FileProcessingJob.objects.filter(
            data_file=data_file,
            requested_anomalies__contains=['holiday']
        ).first()
        
        if holiday_job:
            print(f"\n✅ Found existing job with holiday analysis: {holiday_job.id}")
            print(f"Status: {holiday_job.status}")
            print(f"Anomalies: {holiday_job.requested_anomalies}")
            return holiday_job
        
        # Create a new job with all required anomalies including holiday
        print(f"\n🔄 Creating new job with complete anomaly analysis...")
        
        timestamp = timezone.now().isoformat()
        unique_hash = hashlib.sha256(f"{data_file.file_name}_holiday_fix_{timestamp}".encode()).hexdigest()
        
        new_job = FileProcessingJob.objects.create(
            data_file=data_file,
            file_hash=unique_hash,
            run_anomalies=True,
            requested_anomalies=['duplicate', 'backdated', 'holiday', 'overall', 'unusual_days', 'user_analysis'],
            status='PENDING'
        )
        
        print(f"✅ Created new job: {new_job.id}")
        print(f"Status: {new_job.status}")
        print(f"Requested anomalies: {new_job.requested_anomalies}")
        print(f"Hash: {new_job.file_hash}")
        
        return new_job
        
    except DataFile.DoesNotExist:
        print(f"❌ Error: File with ID {file_id} not found")
        return None
    except Exception as e:
        print(f"❌ Error: {e}")
        return None

def check_ml_analysis_orchestrator():
    """Check if the ML Analysis Orchestrator properly handles holiday analysis"""
    
    try:
        from core.ml_analysis_orchestrator import MLAnalysisOrchestrator
        
        # Check if holiday analysis method exists
        orchestrator = MLAnalysisOrchestrator()
        
        if hasattr(orchestrator, 'run_holiday_analysis'):
            print("✅ MLAnalysisOrchestrator has run_holiday_analysis method")
        else:
            print("❌ MLAnalysisOrchestrator missing run_holiday_analysis method")
        
        # Check available methods
        methods = [method for method in dir(orchestrator) if method.startswith('run_') and method.endswith('_analysis')]
        print(f"Available analysis methods: {methods}")
        
        return True
        
    except Exception as e:
        print(f"❌ Error checking MLAnalysisOrchestrator: {e}")
        return False

if __name__ == "__main__":
    print("🔧 Fixing Holiday Analysis Issue")
    print("=" * 50)
    
    # Check orchestrator
    print("\n1. Checking ML Analysis Orchestrator...")
    check_ml_analysis_orchestrator()
    
    # Fix the job
    print("\n2. Creating new job with holiday analysis...")
    job = fix_holiday_analysis_issue()
    
    if job:
        print(f"\n✅ SUCCESS! New job created: {job.id}")
        print(f"🚀 The parallel processor will pick up this job automatically")
        print(f"📊 This job includes: {job.requested_anomalies}")
    else:
        print("\n❌ Failed to create new job") 