#!/usr/bin/env python3
"""
Test script for enhanced backdated entry analysis
"""

import os
import sys
import django
import requests
import json
from datetime import datetime, date, timedelta

# Setup Django
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'analytics.settings')
django.setup()

from core.models import DataFile, SAPGLPosting, BackdatedAnalysisResult
from core.analytics import SAPGLAnalyzer
from core.tasks import run_backdated_analysis

def create_test_data():
    """Create test data with backdated entries"""
    print("Creating test data...")
    
    # Create a test data file
    data_file = DataFile.objects.create(
        file_name='test_backdated_data.csv',
        file_size=1024,
        engagement_id='TEST001',
        client_name='Test Client',
        company_name='Test Company',
        fiscal_year=2024,
        audit_start_date=date(2024, 1, 1),
        audit_end_date=date(2024, 12, 31),
        status='COMPLETED'
    )
    
    # Create test transactions with backdated entries
    test_transactions = [
        # Normal transaction
        {
            'document_number': 'DOC001',
            'posting_date': date(2024, 1, 15),
            'document_date': date(2024, 1, 10),
            'gl_account': '1000',
            'amount_local_currency': 10000.00,
            'transaction_type': 'DEBIT',
            'user_name': 'USER1',
            'text': 'Normal transaction',
            'fiscal_year': 2024,
            'posting_period': 1
        },
        # Backdated transaction (7 days)
        {
            'document_number': 'DOC002',
            'posting_date': date(2024, 1, 20),
            'document_date': date(2024, 1, 13),
            'gl_account': '2000',
            'amount_local_currency': 50000.00,
            'transaction_type': 'CREDIT',
            'user_name': 'USER2',
            'text': 'Backdated transaction 7 days',
            'fiscal_year': 2024,
            'posting_period': 1
        },
        # Significantly backdated transaction (30 days)
        {
            'document_number': 'DOC003',
            'posting_date': date(2024, 2, 15),
            'document_date': date(2024, 1, 16),
            'gl_account': '3000',
            'amount_local_currency': 100000.00,
            'transaction_type': 'DEBIT',
            'user_name': 'USER1',
            'text': 'Significantly backdated transaction',
            'fiscal_year': 2024,
            'posting_period': 2
        },
        # Extremely backdated transaction (90 days)
        {
            'document_number': 'DOC004',
            'posting_date': date(2024, 4, 15),
            'document_date': date(2024, 1, 16),
            'gl_account': '4000',
            'amount_local_currency': 500000.00,
            'transaction_type': 'CREDIT',
            'user_name': 'USER3',
            'text': 'Extremely backdated transaction',
            'fiscal_year': 2024,
            'posting_period': 4
        },
        # High-value backdated transaction
        {
            'document_number': 'DOC005',
            'posting_date': date(2024, 1, 25),
            'document_date': date(2024, 1, 10),
            'gl_account': '5000',
            'amount_local_currency': 1500000.00,
            'transaction_type': 'DEBIT',
            'user_name': 'USER2',
            'text': 'High-value backdated transaction',
            'fiscal_year': 2024,
            'posting_period': 1
        },
        # Month-end backdated transaction
        {
            'document_number': 'DOC006',
            'posting_date': date(2024, 1, 31),
            'document_date': date(2024, 1, 20),
            'gl_account': '6000',
            'amount_local_currency': 75000.00,
            'transaction_type': 'CREDIT',
            'user_name': 'USER1',
            'text': 'Month-end backdated transaction',
            'fiscal_year': 2024,
            'posting_period': 1
        }
    ]
    
    # Create transactions
    for i, trans_data in enumerate(test_transactions):
        SAPGLPosting.objects.create(
            **trans_data
        )
    
    print(f"Created {len(test_transactions)} test transactions")
    return data_file

def test_analytics_detection():
    """Test the enhanced backdated detection"""
    print("\nTesting enhanced backdated detection...")
    
    # Get all transactions
    transactions = list(SAPGLPosting.objects.all())
    
    # Run enhanced backdated analysis
    analyzer = SAPGLAnalyzer()
    results = analyzer.detect_backdated_entries(transactions)
    
    print(f"Analysis completed:")
    print(f"- Total backdated entries: {results['summary']['total_backdated_entries']}")
    print(f"- Total amount: {results['summary']['total_amount']}")
    print(f"- High risk entries: {results['summary']['high_risk_entries']}")
    print(f"- Medium risk entries: {results['summary']['medium_risk_entries']}")
    print(f"- Low risk entries: {results['summary']['low_risk_entries']}")
    
    # Print detailed results
    print("\nDetailed backdated entries:")
    for entry in results['backdated_entries']:
        print(f"- {entry['document_number']}: {entry['days_difference']} days, "
              f"Amount: {entry['amount']}, Risk: {entry['risk_level']} ({entry['risk_score']})")
    
    # Print audit recommendations
    print("\nAudit recommendations:")
    for priority, recommendations in results['audit_recommendations'].items():
        if recommendations:
            print(f"\n{priority.upper()} PRIORITY:")
            for rec in recommendations:
                print(f"  - {rec}")
    
    return results

def test_celery_task():
    """Test the Celery task for backdated analysis"""
    print("\nTesting Celery task...")
    
    # Get the data file
    data_file = DataFile.objects.first()
    if not data_file:
        print("No data file found")
        return
    
    # Run the Celery task
    result = run_backdated_analysis(str(data_file.id))
    
    print(f"Celery task result: {result}")
    
    # Check if results were saved to database
    backdated_results = BackdatedAnalysisResult.objects.filter(
        data_file=data_file,
        analysis_type='enhanced_backdated'
    ).first()
    
    if backdated_results:
        print(f"Results saved to database with ID: {backdated_results.id}")
        print(f"Processing duration: {backdated_results.processing_duration} seconds")
    else:
        print("No results found in database")

def test_api_endpoints():
    """Test the API endpoints"""
    print("\nTesting API endpoints...")
    
    # Get the data file
    data_file = DataFile.objects.first()
    if not data_file:
        print("No data file found")
        return
    
    base_url = "http://localhost:8000/api"
    
    # Test starting backdated analysis
    print("Testing POST /api/backdated-analysis/")
    try:
        response = requests.post(f"{base_url}/backdated-analysis/", json={
            'file_id': str(data_file.id)
        })
        print(f"Response status: {response.status_code}")
        if response.status_code == 202:
            result = response.json()
            print(f"Task started: {result}")
        else:
            print(f"Error: {response.text}")
    except Exception as e:
        print(f"Error testing API: {e}")
    
    # Test getting backdated analysis results
    print("\nTesting GET /api/backdated-analysis/")
    try:
        response = requests.get(f"{base_url}/backdated-analysis/?file_id={data_file.id}")
        print(f"Response status: {response.status_code}")
        if response.status_code == 200:
            result = response.json()
            print(f"Analysis results: {result.get('summary', {})}")
        else:
            print(f"Error: {response.text}")
    except Exception as e:
        print(f"Error testing API: {e}")

def main():
    """Main test function"""
    print("=== Enhanced Backdated Entry Analysis Test ===")
    
    # Create test data
    data_file = create_test_data()
    
    # Test analytics detection
    results = test_analytics_detection()
    
    # Test Celery task
    test_celery_task()
    
    # Test API endpoints (if server is running)
    test_api_endpoints()
    
    print("\n=== Test completed ===")
    print(f"Test data file ID: {data_file.id}")
    print("You can now test the API endpoints manually or run the Celery worker")

if __name__ == "__main__":
    main() 