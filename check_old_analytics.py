#!/usr/bin/env python3
"""
Check for old analytics results that might be interfering
"""

import os
import sys
import django

# Add the project directory to the Python path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# Setup Django
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'analytics.settings')
django.setup()

from core.models import (
    DataFile, 
    FileProcessingJob, 
    AnalyticsProcessingResult, 
    MLModelProcessingResult, 
    ProcessingJobTracker,
    AnalyticsResult,
    DuplicateAnalysisResult
)

def check_old_analytics():
    """Check for old analytics results"""
    print("🔍 DEBUG: ===== Checking Old Analytics Results =====")
    
    # Check old AnalyticsResult model
    old_analytics = AnalyticsResult.objects.all()
    print(f"🔍 DEBUG: Total old AnalyticsResult: {old_analytics.count()}")
    for result in old_analytics:
        print(f"🔍 DEBUG: Old Analytics: {result.id} - Type: {result.analysis_type} - File: {result.data_file.file_name}")
    
    # Check old DuplicateAnalysisResult model
    old_duplicates = DuplicateAnalysisResult.objects.all()
    print(f"🔍 DEBUG: Total old DuplicateAnalysisResult: {old_duplicates.count()}")
    for result in old_duplicates:
        print(f"🔍 DEBUG: Old Duplicate: {result.id} - Type: {result.analysis_type} - File: {result.data_file.file_name}")
    
    # Check new AnalyticsProcessingResult with no processing_job
    new_analytics_no_job = AnalyticsProcessingResult.objects.filter(processing_job__isnull=True)
    print(f"🔍 DEBUG: Total new AnalyticsProcessingResult with no job: {new_analytics_no_job.count()}")
    for result in new_analytics_no_job:
        print(f"🔍 DEBUG: New Analytics (no job): {result.id} - Type: {result.analytics_type} - File: {result.data_file.file_name}")
    
    # Check new MLModelProcessingResult with no processing_job
    new_ml_no_job = MLModelProcessingResult.objects.filter(processing_job__isnull=True)
    print(f"🔍 DEBUG: Total new MLModelProcessingResult with no job: {new_ml_no_job.count()}")
    for result in new_ml_no_job:
        print(f"🔍 DEBUG: New ML (no job): {result.id} - Type: {result.model_type} - File: {result.data_file.file_name}")
    
    # Check ProcessingJobTracker with no processing_job
    trackers_no_job = ProcessingJobTracker.objects.filter(processing_job__isnull=True)
    print(f"🔍 DEBUG: Total ProcessingJobTracker with no job: {trackers_no_job.count()}")
    for tracker in trackers_no_job:
        print(f"🔍 DEBUG: Tracker (no job): {tracker.id} - Progress: {tracker.overall_progress}%")

if __name__ == "__main__":
    check_old_analytics() 