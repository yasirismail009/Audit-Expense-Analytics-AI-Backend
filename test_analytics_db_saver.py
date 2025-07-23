#!/usr/bin/env python3
"""
Test script to verify AnalyticsDBSaver is working correctly
"""

import os
import sys
import django
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
    AnalyticsProcessingResult, 
    MLModelProcessingResult, 
    ProcessingJobTracker,
    SAPGLPosting
)
from core.analytics_db_saver import AnalyticsDBSaver

def test_analytics_db_saver():
    """Test the AnalyticsDBSaver functionality"""
    print("🔍 DEBUG: ===== Testing AnalyticsDBSaver =====")
    
    # Get the latest data file and job
    try:
        data_file = DataFile.objects.latest('uploaded_at')
        print(f"🔍 DEBUG: Using DataFile: {data_file.id} - {data_file.file_name}")
        
        # Get or create a processing job
        job, created = FileProcessingJob.objects.get_or_create(
            data_file=data_file,
            defaults={
                'file_hash': 'test_hash_123',
                'run_anomalies': True,
                'requested_anomalies': ['duplicate'],
                'status': 'PENDING'
            }
        )
        
        if created:
            print(f"🔍 DEBUG: Created new FileProcessingJob: {job.id}")
        else:
            print(f"🔍 DEBUG: Using existing FileProcessingJob: {job.id}")
        
        # Initialize AnalyticsDBSaver
        print(f"🔍 DEBUG: Initializing AnalyticsDBSaver...")
        db_saver = AnalyticsDBSaver(job)
        print(f"🔍 DEBUG: AnalyticsDBSaver initialized successfully")
        
        # Test saving default analytics
        print(f"🔍 DEBUG: Testing save_default_analytics...")
        test_analytics_data = {
            'total_transactions': 100,
            'total_amount': 1000000.0,
            'total_debits': 600000.0,
            'total_credits': 400000.0,
            'trial_balance': 200000.0,
            'gl_account_summaries': [
                {
                    'account_id': '1000',
                    'total_debits': 300000.0,
                    'total_credits': 100000.0,
                    'trial_balance': 200000.0,
                    'transaction_count': 50
                }
            ],
            'unique_accounts': 10,
            'unique_users': 5,
            'chart_data': {'test': 'data'},
            'export_data': []
        }
        
        try:
            analytics_result = db_saver.save_default_analytics(test_analytics_data)
            print(f"🔍 DEBUG: Default analytics saved successfully with ID: {analytics_result.id}")
        except Exception as e:
            print(f"🔍 DEBUG: ERROR saving default analytics: {e}")
            return False
        
        # Test saving comprehensive analytics
        print(f"🔍 DEBUG: Testing save_comprehensive_analytics...")
        test_comprehensive_data = {
            'summary': {
                'total_transactions': 100,
                'total_amount': 1000000.0,
                'unique_users': 5,
                'unique_accounts': 10
            },
            'expense_breakdown': {'test': 'data'},
            'user_patterns': {'test': 'data'},
            'account_patterns': {'test': 'data'},
            'temporal_patterns': {'test': 'data'},
            'risk_assessment': {'test': 'data'},
            'chart_data': {'test': 'data'},
            'export_data': []
        }
        
        try:
            comprehensive_result = db_saver.save_comprehensive_analytics(test_comprehensive_data)
            print(f"🔍 DEBUG: Comprehensive analytics saved successfully with ID: {comprehensive_result.id}")
        except Exception as e:
            print(f"🔍 DEBUG: ERROR saving comprehensive analytics: {e}")
            return False
        
        # Test saving duplicate analysis
        print(f"🔍 DEBUG: Testing save_duplicate_analysis...")
        test_duplicate_data = {
            'analysis_info': {
                'total_transactions': 100,
                'total_duplicate_groups': 5,
                'total_duplicate_transactions': 10,
                'total_amount_involved': 50000.0
            },
            'duplicate_list': [
                {
                    'id': 'test1',
                    'gl_account': '1000',
                    'amount': 10000.0,
                    'duplicate_type': 'Type 1 Duplicate',
                    'risk_score': 75
                }
            ],
            'breakdowns': {'test': 'data'},
            'chart_data': {'test': 'data'},
            'export_data': []
        }
        
        try:
            duplicate_result = db_saver.save_duplicate_analysis(test_duplicate_data)
            print(f"🔍 DEBUG: Duplicate analysis saved successfully with ID: {duplicate_result.id}")
        except Exception as e:
            print(f"🔍 DEBUG: ERROR saving duplicate analysis: {e}")
            return False
        
        # Test saving ML processing results
        print(f"🔍 DEBUG: Testing save_ml_processing_result...")
        test_ml_data = {
            'anomalies_detected': 5,
            'duplicates_found': 10,
            'risk_score': 0.75,
            'confidence_score': 0.85,
            'data_size': 100,
            'detailed_results': {'test': 'data'},
            'model_metrics': {'test': 'data'},
            'feature_importance': {'test': 'data'}
        }
        
        try:
            ml_result = db_saver.save_ml_processing_result(test_ml_data, 'all')
            print(f"🔍 DEBUG: ML processing result saved successfully with ID: {ml_result.id}")
        except Exception as e:
            print(f"🔍 DEBUG: ERROR saving ML processing result: {e}")
            return False
        
        # Test finalizing processing
        print(f"🔍 DEBUG: Testing finalize_processing...")
        try:
            db_saver.finalize_processing(success=True)
            print(f"🔍 DEBUG: Processing finalized successfully")
        except Exception as e:
            print(f"🔍 DEBUG: ERROR finalizing processing: {e}")
            return False
        
        # Check what was saved
        print(f"🔍 DEBUG: ===== Checking saved data =====")
        analytics_results = AnalyticsProcessingResult.objects.filter(data_file=data_file)
        print(f"🔍 DEBUG: Total AnalyticsProcessingResults: {analytics_results.count()}")
        for result in analytics_results:
            print(f"🔍 DEBUG: Analytics: {result.id} - Type: {result.analytics_type} - Status: {result.processing_status}")
        
        ml_results = MLModelProcessingResult.objects.filter(data_file=data_file)
        print(f"🔍 DEBUG: Total MLModelProcessingResults: {ml_results.count()}")
        for result in ml_results:
            print(f"🔍 DEBUG: ML: {result.id} - Type: {result.model_type} - Status: {result.processing_status}")
        
        job_trackers = ProcessingJobTracker.objects.filter(data_file=data_file)
        print(f"🔍 DEBUG: Total ProcessingJobTrackers: {job_trackers.count()}")
        for tracker in job_trackers:
            print(f"🔍 DEBUG: Tracker: {tracker.id} - Progress: {tracker.overall_progress}% - Steps: {tracker.completed_steps}/{tracker.total_steps}")
        
        print(f"🔍 DEBUG: ===== AnalyticsDBSaver test completed successfully =====")
        return True
        
    except Exception as e:
        print(f"🔍 DEBUG: ERROR in test: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = test_analytics_db_saver()
    if success:
        print("✅ AnalyticsDBSaver test PASSED")
    else:
        print("❌ AnalyticsDBSaver test FAILED") 