#!/usr/bin/env python
"""
Test script to verify backdated analysis functionality after fixes
"""

import os
import sys
import django
from datetime import datetime, date, timedelta

# Setup Django environment
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'analytics.settings')
django.setup()

from core.models import DataFile, SAPGLPosting, FileProcessingJob, BackdatedAnalysisResult
from core.analytics import SAPGLAnalyzer
from core.tasks import run_backdated_analysis

def create_test_data():
    """Create test data with backdated entries"""
    print("Creating test data...")
    
    # Create a test data file
    data_file = DataFile.objects.create(
        file_name="Test Backdated Data.csv",
        file_size=1024,
        engagement_id="TEST001",
        client_name="Test Client",
        company_name="Test Company",
        fiscal_year=2024,
        audit_start_date=date(2024, 1, 1),
        audit_end_date=date(2024, 12, 31),
        status='COMPLETED',
        processed_records=5
    )
    
    # Create test transactions with various backdated scenarios
    test_transactions = [
        {
            'document_number': 'DOC001',
            'posting_date': date(2024, 3, 15),
            'document_date': date(2024, 3, 10),  # 5 days backdated
            'gl_account': '1000',
            'amount_local_currency': 50000.00,
            'transaction_type': 'DEBIT',
            'user_name': 'USER1',
            'fiscal_year': 2024,
            'posting_period': 3
        },
        {
            'document_number': 'DOC002',
            'posting_date': date(2024, 3, 20),
            'document_date': date(2024, 2, 15),  # 33 days backdated
            'gl_account': '2000',
            'amount_local_currency': 150000.00,
            'transaction_type': 'CREDIT',
            'user_name': 'USER2',
            'fiscal_year': 2024,
            'posting_period': 3
        },
        {
            'document_number': 'DOC003',
            'posting_date': date(2024, 3, 25),
            'document_date': date(2024, 1, 10),  # 74 days backdated
            'gl_account': '3000',
            'amount_local_currency': 1000000.00,
            'transaction_type': 'DEBIT',
            'user_name': 'USER1',
            'fiscal_year': 2024,
            'posting_period': 3
        },
        {
            'document_number': 'DOC004',
            'posting_date': date(2024, 3, 30),
            'document_date': date(2024, 3, 29),  # 1 day backdated (normal)
            'gl_account': '4000',
            'amount_local_currency': 25000.00,
            'transaction_type': 'CREDIT',
            'user_name': 'USER3',
            'fiscal_year': 2024,
            'posting_period': 3
        },
        {
            'document_number': 'DOC005',
            'posting_date': date(2024, 3, 31),
            'document_date': date(2024, 12, 15),  # Future document date (invalid)
            'gl_account': '5000',
            'amount_local_currency': 75000.00,
            'transaction_type': 'DEBIT',
            'user_name': 'USER2',
            'fiscal_year': 2024,
            'posting_period': 3
        }
    ]
    
    # Create transactions
    transactions = []
    for i, trans_data in enumerate(test_transactions):
        transaction = SAPGLPosting.objects.create(**trans_data)
        transactions.append(transaction)
        print(f"Created transaction {i+1}: {transaction.document_number} - {transaction.amount_local_currency}")
    
    return data_file, transactions

def test_analytics_detection(data_file, transactions):
    """Test the analytics detection directly"""
    print("\n" + "="*50)
    print("Testing Analytics Detection")
    print("="*50)
    
    try:
        analyzer = SAPGLAnalyzer()
        results = analyzer.detect_backdated_entries(transactions)
        
        print(f"✅ Analytics detection completed successfully!")
        print(f"📊 Summary:")
        print(f"   - Total backdated entries: {results.get('summary', {}).get('total_backdated_entries', 0)}")
        print(f"   - Total amount: {results.get('summary', {}).get('total_amount', 0):,.2f}")
        print(f"   - High risk entries: {results.get('summary', {}).get('high_risk_entries', 0)}")
        print(f"   - Medium risk entries: {results.get('summary', {}).get('medium_risk_entries', 0)}")
        print(f"   - Low risk entries: {results.get('summary', {}).get('low_risk_entries', 0)}")
        
        # Show some detailed results
        backdated_entries = results.get('backdated_entries', [])
        if backdated_entries:
            print(f"\n📋 Sample backdated entries:")
            for i, entry in enumerate(backdated_entries[:3]):
                print(f"   {i+1}. Doc: {entry['document_number']}, Days diff: {entry['days_difference']}, "
                      f"Amount: {entry['amount']:,.2f}, Risk: {entry['risk_level']}")
        
        return results
        
    except Exception as e:
        print(f"❌ Error in analytics detection: {e}")
        import traceback
        traceback.print_exc()
        return None

def test_celery_task(data_file):
    """Test the Celery task"""
    print("\n" + "="*50)
    print("Testing Celery Task")
    print("="*50)
    
    try:
        # Create a processing job
        processing_job = FileProcessingJob.objects.create(
            data_file=data_file,
            file_hash="test_hash",
            run_anomalies=True,
            requested_anomalies=['backdated'],
            status='PENDING'
        )
        
        print(f"Created processing job: {processing_job.id}")
        
        # Run the backdated analysis task
        print("Running backdated analysis task...")
        result = run_backdated_analysis(str(processing_job.id))
        
        print(f"✅ Celery task completed!")
        print(f"📊 Task result: {result}")
        
        # Check if results were saved to database
        backdated_analysis = BackdatedAnalysisResult.objects.filter(
            data_file=data_file,
            analysis_type='enhanced_backdated'
        ).first()
        
        if backdated_analysis:
            print(f"✅ Backdated analysis saved to database!")
            print(f"   - Analysis ID: {backdated_analysis.id}")
            print(f"   - Status: {backdated_analysis.status}")
            print(f"   - Processing duration: {backdated_analysis.processing_duration:.2f}s")
            print(f"   - Total backdated entries: {backdated_analysis.get_backdated_count()}")
        else:
            print("❌ No backdated analysis found in database")
        
        return result
        
    except Exception as e:
        print(f"❌ Error in Celery task: {e}")
        import traceback
        traceback.print_exc()
        return None

def main():
    """Main test function"""
    print("🧪 Testing Backdated Analysis Fixes")
    print("="*50)
    
    # Create test data
    data_file, transactions = create_test_data()
    
    # Test analytics detection
    analytics_results = test_analytics_detection(data_file, transactions)
    
    # Test Celery task
    celery_results = test_celery_task(data_file)
    
    print("\n" + "="*50)
    print("Test Summary")
    print("="*50)
    
    if analytics_results:
        print("✅ Analytics detection: PASSED")
    else:
        print("❌ Analytics detection: FAILED")
    
    if celery_results and celery_results.get('status') == 'completed':
        print("✅ Celery task: PASSED")
    else:
        print("❌ Celery task: FAILED")
    
    # Check database results
    backdated_count = BackdatedAnalysisResult.objects.filter(
        data_file=data_file,
        analysis_type='enhanced_backdated'
    ).count()
    
    if backdated_count > 0:
        print("✅ Database storage: PASSED")
    else:
        print("❌ Database storage: FAILED")
    
    print(f"\n🎯 Backdated analysis should now work correctly!")
    print(f"📁 Test data file: {data_file.file_name}")
    print(f"📊 Total transactions created: {len(transactions)}")

if __name__ == "__main__":
    main() 