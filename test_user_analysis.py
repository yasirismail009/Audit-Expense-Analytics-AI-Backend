#!/usr/bin/env python3
"""
Test script for User Analysis implementation

This script tests the user analysis functionality to ensure it works correctly
with the existing analytics system.
"""

import os
import sys
import django
from django.utils import timezone
from datetime import datetime, timedelta
from decimal import Decimal

# Setup Django
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'analytics.settings')
django.setup()

from core.models import DataFile, SAPGLPosting, UserAnalysisResult
from core.user_analysis import UserAnalyzer
from core.sync_analysis import run_user_analysis_sync

def create_test_data():
    """Create test data for user analysis"""
    print("Creating test data...")
    
    # Create a test data file
    data_file = DataFile.objects.create(
        file_name='test_user_analysis.csv',
        file_size=1024,
        engagement_id='TEST_001',
        client_name='Test Client',
        company_name='Test Company',
        fiscal_year=2024,
        audit_start_date=datetime(2024, 1, 1).date(),
        audit_end_date=datetime(2024, 12, 31).date(),
        status='COMPLETED'
    )
    
    # Create test transactions with different user patterns
    test_transactions = [
        # User 1: High transaction frequency
        {'user_name': 'USER001', 'amount': 50000, 'count': 50},
        # User 2: High average amount
        {'user_name': 'USER002', 'amount': 500000, 'count': 5},
        # User 3: Normal pattern
        {'user_name': 'USER003', 'amount': 100000, 'count': 20},
        # User 4: Low activity
        {'user_name': 'USER004', 'amount': 50000, 'count': 3},
        # User 5: High account diversity
        {'user_name': 'USER005', 'amount': 75000, 'count': 15},
    ]
    
    # Create transactions
    for user_data in test_transactions:
        for i in range(user_data['count']):
            # Create multiple accounts for USER005 to test diversity
            if user_data['user_name'] == 'USER005':
                gl_account = f"100{i % 5}"  # 5 different accounts
            else:
                gl_account = "1001"  # Same account for others
            
            SAPGLPosting.objects.create(
                data_file=data_file,
                document_number=f"DOC{user_data['user_name']}{i:03d}",
                posting_date=datetime(2024, 6, 15).date(),
                gl_account=gl_account,
                amount_local_currency=Decimal(str(user_data['amount'])),
                transaction_type='DEBIT' if i % 2 == 0 else 'CREDIT',
                local_currency='SAR',
                text=f"Test transaction {i} for {user_data['user_name']}",
                user_name=user_data['user_name'],
                fiscal_year=2024,
                posting_period=6
            )
    
    print(f"Created {data_file.id} with {SAPGLPosting.objects.filter(data_file=data_file).count()} transactions")
    return data_file

def test_user_analyzer():
    """Test the UserAnalyzer class directly"""
    print("\nTesting UserAnalyzer...")
    
    # Create test data
    data_file = create_test_data()
    
    # Test UserAnalyzer
    analyzer = UserAnalyzer()
    results = analyzer.run_user_analysis(data_file)
    
    print(f"User Analysis completed: {results['analysis_id']}")
    print(f"Processing duration: {results['processing_duration']:.2f} seconds")
    
    # Verify results
    user_analysis = UserAnalysisResult.objects.get(id=results['analysis_id'])
    print(f"Total users: {user_analysis.get_total_users()}")
    print(f"Total transactions: {user_analysis.get_total_transactions()}")
    print(f"Anomalies detected: {user_analysis.get_anomalies_count()}")
    print(f"High risk users: {user_analysis.get_high_risk_users_count()}")
    
    return data_file

def test_sync_analysis():
    """Test the sync analysis function"""
    print("\nTesting sync analysis...")
    
    # Create test data
    data_file = create_test_data()
    
    # Create a processing job
    from core.models import FileProcessingJob
    job = FileProcessingJob.objects.create(
        data_file=data_file,
        file_hash='test_hash',
        status='PENDING'
    )
    
    # Test sync analysis
    results = run_user_analysis_sync(job.id)
    
    print(f"Sync analysis completed: {results.get('status')}")
    if 'analysis_id' in results:
        print(f"Analysis ID: {results['analysis_id']}")
        print(f"Processing duration: {results['processing_duration']:.2f} seconds")
    
    return data_file

def test_api_endpoint():
    """Test the API endpoint"""
    print("\nTesting API endpoint...")
    
    # Create test data
    data_file = create_test_data()
    
    # Run analysis first
    analyzer = UserAnalyzer()
    analyzer.run_user_analysis(data_file)
    
    # Test API view
    from core.views import UserAnalysisView
    from django.test import RequestFactory
    
    factory = RequestFactory()
    request = factory.get(f'/api/user-analysis/{data_file.id}/')
    view = UserAnalysisView.as_view()
    
    response = view(request, file_id=data_file.id)
    
    print(f"API Response Status: {response.status_code}")
    if response.status_code == 200:
        data = response.data
        print(f"Total users: {data['summary']['total_users']}")
        print(f"Anomalies detected: {data['summary']['anomalies_detected']}")
        print(f"Chart types available: {data['visualizations']['chart_types']}")
    
    return data_file

def cleanup_test_data(data_file):
    """Clean up test data"""
    print(f"\nCleaning up test data for file {data_file.id}...")
    
    # Delete transactions
    SAPGLPosting.objects.filter(data_file=data_file).delete()
    
    # Delete analysis results
    UserAnalysisResult.objects.filter(data_file=data_file).delete()
    
    # Delete processing jobs
    from core.models import FileProcessingJob
    FileProcessingJob.objects.filter(data_file=data_file).delete()
    
    # Delete data file
    data_file.delete()
    
    print("Test data cleaned up successfully")

def main():
    """Main test function"""
    print("Starting User Analysis Tests...")
    
    test_files = []
    
    try:
        # Test 1: Direct UserAnalyzer
        data_file1 = test_user_analyzer()
        test_files.append(data_file1)
        
        # Test 2: Sync Analysis
        data_file2 = test_sync_analysis()
        test_files.append(data_file2)
        
        # Test 3: API Endpoint
        data_file3 = test_api_endpoint()
        test_files.append(data_file3)
        
        print("\n" + "="*50)
        print("ALL TESTS COMPLETED SUCCESSFULLY!")
        print("="*50)
        
    except Exception as e:
        print(f"\nTest failed with error: {e}")
        import traceback
        traceback.print_exc()
    
    finally:
        # Clean up test data
        for data_file in test_files:
            try:
                cleanup_test_data(data_file)
            except Exception as e:
                print(f"Error cleaning up {data_file.id}: {e}")

if __name__ == "__main__":
    main() 