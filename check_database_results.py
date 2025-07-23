#!/usr/bin/env python3
"""
Check what's actually saved in the database for the latest file
"""

import os
import sys
import django
import json
from django.utils import timezone
from datetime import datetime

# Add the project directory to the Python path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# Setup Django
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'analytics.settings')
django.setup()

from core.models import (
    DataFile, 
    FileProcessingJob, 
    SAPGLPosting,
    AnalyticsProcessingResult, 
    MLModelProcessingResult, 
    ProcessingJobTracker
)

def check_database_results():
    """Check what's actually saved in the database"""
    print("🔍 DEBUG: ===== Checking Database Results =====")
    
    # Get the latest data file
    data_file = DataFile.objects.latest('uploaded_at')
    print(f"🔍 DEBUG: Latest DataFile: {data_file.id} - {data_file.file_name}")
    
    # Check AnalyticsProcessingResult
    print(f"\n🔍 DEBUG: ===== AnalyticsProcessingResult =====")
    analytics_results = AnalyticsProcessingResult.objects.filter(data_file=data_file)
    print(f"🔍 DEBUG: Total AnalyticsProcessingResult: {analytics_results.count()}")
    
    for result in analytics_results:
        print(f"\n🔍 DEBUG: Analytics ID: {result.id}")
        print(f"🔍 DEBUG: Type: {result.analytics_type}")
        print(f"🔍 DEBUG: Status: {result.processing_status}")
        print(f"🔍 DEBUG: Created: {result.created_at}")
        print(f"🔍 DEBUG: Processing Job: {result.processing_job}")
        print(f"🔍 DEBUG: Total Transactions: {result.total_transactions}")
        print(f"🔍 DEBUG: Total Amount: {result.total_amount}")
        print(f"🔍 DEBUG: Unique Users: {result.unique_users}")
        print(f"🔍 DEBUG: Unique Accounts: {result.unique_accounts}")
        print(f"🔍 DEBUG: Flagged Transactions: {result.flagged_transactions}")
        print(f"🔍 DEBUG: High Risk Transactions: {result.high_risk_transactions}")
        print(f"🔍 DEBUG: Anomalies Found: {result.anomalies_found}")
        print(f"🔍 DEBUG: Duplicates Found: {result.duplicates_found}")
        
        # Check specific data fields based on analytics type
        if result.analytics_type == 'default_analytics':
            if result.trial_balance_data:
                print(f"🔍 DEBUG: Trial Balance Data keys: {list(result.trial_balance_data.keys())}")
            if result.chart_data:
                print(f"🔍 DEBUG: Chart Data keys: {list(result.chart_data.keys())}")
                
        elif result.analytics_type == 'comprehensive_expense':
            if result.expense_breakdown:
                print(f"🔍 DEBUG: Expense Breakdown keys: {list(result.expense_breakdown.keys())}")
            if result.user_patterns:
                print(f"🔍 DEBUG: User Patterns keys: {list(result.user_patterns.keys())}")
            if result.account_patterns:
                print(f"🔍 DEBUG: Account Patterns keys: {list(result.account_patterns.keys())}")
            if result.temporal_patterns:
                print(f"🔍 DEBUG: Temporal Patterns keys: {list(result.temporal_patterns.keys())}")
            if result.risk_assessment:
                print(f"🔍 DEBUG: Risk Assessment keys: {list(result.risk_assessment.keys())}")
                
        elif result.analytics_type == 'duplicate_analysis':
            # For duplicate analysis, check if there's data in any of the fields
            data_fields = ['trial_balance_data', 'expense_breakdown', 'user_patterns', 
                          'account_patterns', 'temporal_patterns', 'risk_assessment', 
                          'chart_data', 'export_data']
            for field in data_fields:
                field_data = getattr(result, field)
                if field_data and isinstance(field_data, dict) and len(field_data) > 0:
                    print(f"🔍 DEBUG: {field} has data with keys: {list(field_data.keys())}")
    
    # Check MLModelProcessingResult
    print(f"\n🔍 DEBUG: ===== MLModelProcessingResult =====")
    ml_results = MLModelProcessingResult.objects.filter(data_file=data_file)
    print(f"🔍 DEBUG: Total MLModelProcessingResult: {ml_results.count()}")
    
    for result in ml_results:
        print(f"\n🔍 DEBUG: ML ID: {result.id}")
        print(f"🔍 DEBUG: Type: {result.model_type}")
        print(f"🔍 DEBUG: Status: {result.processing_status}")
        print(f"🔍 DEBUG: Created: {result.created_at}")
        print(f"🔍 DEBUG: Processing Job: {result.processing_job}")
        print(f"🔍 DEBUG: Anomalies Detected: {result.anomalies_detected}")
        print(f"🔍 DEBUG: Duplicates Found: {result.duplicates_found}")
        print(f"🔍 DEBUG: Risk Score: {result.risk_score}")
        print(f"🔍 DEBUG: Confidence Score: {result.confidence_score}")
        print(f"🔍 DEBUG: Data Size: {result.data_size}")
        
        # Check detailed results
        if result.detailed_results:
            print(f"🔍 DEBUG: Detailed Results keys: {list(result.detailed_results.keys())}")
        if result.model_metrics:
            print(f"🔍 DEBUG: Model Metrics keys: {list(result.model_metrics.keys())}")
        if result.feature_importance:
            print(f"🔍 DEBUG: Feature Importance keys: {list(result.feature_importance.keys())}")
    
    # Check ProcessingJobTracker
    print(f"\n🔍 DEBUG: ===== ProcessingJobTracker =====")
    job_trackers = ProcessingJobTracker.objects.filter(data_file=data_file)
    print(f"🔍 DEBUG: Total ProcessingJobTracker: {job_trackers.count()}")
    
    for tracker in job_trackers:
        print(f"\n🔍 DEBUG: Tracker ID: {tracker.id}")
        print(f"🔍 DEBUG: Progress: {tracker.overall_progress}%")
        print(f"🔍 DEBUG: Steps: {tracker.completed_steps}/{tracker.total_steps}")
        print(f"🔍 DEBUG: Created: {tracker.created_at}")
        print(f"🔍 DEBUG: Processing Job: {tracker.processing_job}")
        print(f"🔍 DEBUG: File Processing Status: {tracker.file_processing_status}")
        print(f"🔍 DEBUG: Analytics Status: {tracker.analytics_status}")
        print(f"🔍 DEBUG: ML Processing Status: {tracker.ml_processing_status}")
        print(f"🔍 DEBUG: Anomaly Detection Status: {tracker.anomaly_detection_status}")
        
        # Check step details
        if tracker.step_details:
            print(f"🔍 DEBUG: Step Details: {len(tracker.step_details)} steps")
            for step in tracker.step_details[:3]:  # Show first 3 steps
                print(f"  - {step}")
        if tracker.error_log:
            print(f"🔍 DEBUG: Error Log: {len(tracker.error_log)} errors")
            for error in tracker.error_log[:3]:  # Show first 3 errors
                print(f"  - {error}")
    
    # Check FileProcessingJob
    print(f"\n🔍 DEBUG: ===== FileProcessingJob =====")
    jobs = FileProcessingJob.objects.filter(data_file=data_file)
    print(f"🔍 DEBUG: Total FileProcessingJob: {jobs.count()}")
    
    for job in jobs:
        print(f"\n🔍 DEBUG: Job ID: {job.id}")
        print(f"🔍 DEBUG: Status: {job.status}")
        print(f"🔍 DEBUG: Started: {job.started_at}")
        print(f"🔍 DEBUG: Completed: {job.completed_at}")
        print(f"🔍 DEBUG: Run anomalies: {job.run_anomalies}")
        print(f"🔍 DEBUG: Requested anomalies: {job.requested_anomalies}")
        
        # Check if job has analytics_results
        if job.analytics_results:
            print(f"🔍 DEBUG: Analytics Results keys: {list(job.analytics_results.keys())}")
        if job.anomaly_results:
            print(f"🔍 DEBUG: Anomaly Results keys: {list(job.anomaly_results.keys())}")
        if job.ml_training_results:
            print(f"🔍 DEBUG: ML Training Results keys: {list(job.ml_training_results.keys())}")
    
    # Test the API endpoints
    print(f"\n🔍 DEBUG: ===== Testing API Endpoints =====")
    
    # Test database-stored comprehensive analytics
    from core.views import DatabaseStoredComprehensiveAnalyticsView
    from django.test import RequestFactory
    
    try:
        factory = RequestFactory()
        request = factory.get(f'/api/database-stored/comprehensive-analytics/{data_file.id}/')
        view = DatabaseStoredComprehensiveAnalyticsView.as_view()
        response = view(request, file_id=data_file.id)
        print(f"🔍 DEBUG: Database-stored comprehensive analytics API response: {response.status_code}")
        if response.status_code == 200:
            print(f"🔍 DEBUG: API returned data successfully")
        else:
            print(f"🔍 DEBUG: API error: {response.status_code}")
    except Exception as e:
        print(f"🔍 DEBUG: API test error: {e}")
    
    # Test database-stored duplicate analysis
    from core.views import DatabaseStoredDuplicateAnalysisView
    
    try:
        request = factory.get(f'/api/database-stored/duplicate-analysis/{data_file.id}/')
        view = DatabaseStoredDuplicateAnalysisView.as_view()
        response = view(request, file_id=data_file.id)
        print(f"🔍 DEBUG: Database-stored duplicate analysis API response: {response.status_code}")
        if response.status_code == 200:
            print(f"🔍 DEBUG: API returned data successfully")
        else:
            print(f"🔍 DEBUG: API error: {response.status_code}")
    except Exception as e:
        print(f"🔍 DEBUG: API test error: {e}")

if __name__ == "__main__":
    check_database_results() 