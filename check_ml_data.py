#!/usr/bin/env python3
"""
Diagnostic script to check ML analysis data storage and retrieval
"""

import os
import sys
import django
from django.conf import settings

# Add the project directory to the Python path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# Setup Django
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'analytics.settings')
django.setup()

from core.models import DataFile, FileProcessingJob, SAPGLPosting
from core.ml_models import MLAnomalyDetector
from core.views import ComprehensiveDuplicateAnalysisView
from django.utils import timezone
from datetime import date
import json

def check_ml_data_storage():
    """Check what ML analysis data is currently stored"""
    
    print("=== ML Data Storage Diagnostic ===\n")
    
    try:
        # 1. Check DataFiles
        print("1. Checking DataFiles...")
        data_files = DataFile.objects.all().order_by('-uploaded_at')[:5]
        print(f"Found {len(data_files)} DataFiles")
        
        for df in data_files:
            print(f"  - {df.file_name} (ID: {df.id}) - Status: {df.status}")
        
        if not data_files:
            print("  ❌ No DataFiles found!")
            return False
        
        # 2. Check FileProcessingJobs
        print("\n2. Checking FileProcessingJobs...")
        processing_jobs = FileProcessingJob.objects.all().order_by('-created_at')[:5]
        print(f"Found {len(processing_jobs)} FileProcessingJobs")
        
        for job in processing_jobs:
            print(f"  - Job {job.id} - File: {job.data_file.file_name} - Status: {job.status}")
            if job.anomaly_results:
                print(f"    Has anomaly_results: {len(str(job.anomaly_results))} chars")
                if 'duplicate_analysis' in job.anomaly_results:
                    print(f"    Has duplicate_analysis: ✅")
                else:
                    print(f"    No duplicate_analysis in anomaly_results")
            else:
                print(f"    No anomaly_results")
        
        # 3. Check ML Model Data
        print("\n3. Checking ML Model Data...")
        ml_detector = MLAnomalyDetector()
        
        if ml_detector.duplicate_model:
            print("  ✅ Duplicate model is available")
            print(f"  Model data keys: {list(ml_detector.duplicate_model.model_data.keys())}")
            
            # Check for saved analysis
            for file_id in data_files:
                if ml_detector.duplicate_model.has_saved_analysis(str(file_id.id)):
                    print(f"  ✅ Has saved analysis for file {file_id.id}")
                    saved_analysis = ml_detector.duplicate_model.get_saved_analysis(str(file_id.id))
                    print(f"    Analysis keys: {list(saved_analysis.keys()) if saved_analysis else 'None'}")
                else:
                    print(f"  ❌ No saved analysis for file {file_id.id}")
        else:
            print("  ❌ Duplicate model is not available")
        
        # 4. Test the API view with a real file
        print("\n4. Testing API View...")
        if data_files:
            test_file = data_files[0]
            print(f"Testing with file: {test_file.file_name} (ID: {test_file.id})")
            
            view = ComprehensiveDuplicateAnalysisView()
            result = view._get_existing_ml_analysis(test_file)
            
            print(f"API result keys: {list(result.keys())}")
            if 'analysis_status' in result:
                status = result['analysis_status']['status']
                message = result['analysis_status']['message']
                print(f"Analysis status: {status}")
                print(f"Message: {message}")
                
                if status == 'FOUND':
                    print("✅ Analysis found!")
                    duplicate_analysis = result.get('duplicate_analysis', {})
                    print(f"Total duplicates: {duplicate_analysis.get('total_duplicates', 0)}")
                    print(f"Duplicate amount: {duplicate_analysis.get('duplicate_amount', 0)}")
                else:
                    print("❌ Analysis not found")
            else:
                print("❌ No analysis_status in result")
        
        # 5. Check if there are any transactions
        print("\n5. Checking Transactions...")
        total_transactions = SAPGLPosting.objects.count()
        print(f"Total transactions in database: {total_transactions}")
        
        if total_transactions > 0:
            sample_transactions = SAPGLPosting.objects.all()[:3]
            print("Sample transactions:")
            for t in sample_transactions:
                print(f"  - {t.document_number} - {t.amount_local_currency} - {t.user_name}")
        
        # 6. Check if any processing jobs have duplicate analysis
        print("\n6. Checking for Duplicate Analysis in Processing Jobs...")
        jobs_with_duplicates = []
        for job in processing_jobs:
            if job.anomaly_results and 'duplicate_analysis' in job.anomaly_results:
                jobs_with_duplicates.append(job)
        
        print(f"Found {len(jobs_with_duplicates)} jobs with duplicate analysis")
        for job in jobs_with_duplicates:
            duplicate_data = job.anomaly_results['duplicate_analysis']
            print(f"  - Job {job.id}: {list(duplicate_data.keys())}")
            if 'duplicate_list' in duplicate_data:
                print(f"    Duplicate list length: {len(duplicate_data['duplicate_list'])}")
        
        return True
        
    except Exception as e:
        print(f"❌ Error during diagnostic: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = check_ml_data_storage()
    sys.exit(0 if success else 1) 