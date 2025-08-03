#!/usr/bin/env python3
"""
Script to check the completed job with holiday analysis
"""

import os
import django

# Setup Django
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'analytics.settings')
django.setup()

from core.models import FileProcessingJob, HolidayAnalysisResult
from django.utils import timezone

def check_completed_job_with_holiday():
    """Check the completed job that includes holiday analysis"""
    
    job_id = "dcef44c8-781c-4f2e-a73c-9efee4ee2653"
    
    try:
        job = FileProcessingJob.objects.get(id=job_id)
        
        print(f"Job ID: {job.id}")
        print(f"File: {job.data_file.file_name}")
        print(f"Status: {job.status}")
        print(f"Created: {job.created_at}")
        print(f"Started: {job.started_at}")
        print(f"Completed: {job.completed_at}")
        print(f"Processing Duration: {job.processing_duration} seconds")
        print(f"Requested Anomalies: {job.requested_anomalies}")
        
        if job.error_message:
            print(f"Error: {job.error_message}")
        
        # Check if there are holiday analysis results
        holiday_results = HolidayAnalysisResult.objects.filter(processing_job=job)
        print(f"\n📊 Holiday Analysis Results: {holiday_results.count()} found")
        
        for i, holiday_result in enumerate(holiday_results):
            print(f"\n  Holiday Result {i+1}:")
            print(f"    ID: {holiday_result.id}")
            print(f"    Status: {holiday_result.status}")
            print(f"    Analysis Date: {holiday_result.analysis_date}")
            print(f"    Processing Duration: {holiday_result.processing_duration} seconds")
            
            # Check analysis info
            analysis_info = holiday_result.analysis_info
            if analysis_info:
                print(f"    Total Transactions: {analysis_info.get('total_transactions', 'N/A')}")
                print(f"    Holiday Postings: {analysis_info.get('holiday_postings_count', 'N/A')}")
                print(f"    Holiday Percentage: {analysis_info.get('holiday_percentage', 'N/A')}%")
                print(f"    Unique Holidays: {analysis_info.get('unique_holidays', 'N/A')}")
            
            # Check if there are actual holiday postings
            holiday_postings = holiday_result.holiday_postings
            if holiday_postings:
                print(f"    Holiday Postings Found: {len(holiday_postings)}")
                if len(holiday_postings) > 0:
                    print(f"    Sample Holiday Posting: {holiday_postings[0]}")
            else:
                print(f"    No holiday postings found")
            
            if holiday_result.error_message:
                print(f"    Error: {holiday_result.error_message}")
        
        # Check anomaly results
        if job.anomaly_results:
            print(f"\n📈 Anomaly Results Available: {len(job.anomaly_results)} keys")
            for key in job.anomaly_results.keys():
                print(f"    - {key}")
        else:
            print(f"\n❌ No anomaly results found")
            
        return job
        
    except FileProcessingJob.DoesNotExist:
        print(f"❌ Error: Job with ID {job_id} not found")
        return None
    except Exception as e:
        print(f"❌ Error checking job: {e}")
        return None

if __name__ == "__main__":
    print("🔍 Checking Completed Job with Holiday Analysis")
    print("=" * 50)
    
    job = check_completed_job_with_holiday() 