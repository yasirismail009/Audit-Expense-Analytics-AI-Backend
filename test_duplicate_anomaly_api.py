#!/usr/bin/env python3
"""
Test script to demonstrate the consolidated duplicate anomaly API
"""

import os
import sys
import django
import requests
import json
from datetime import datetime, date

# Add the project directory to Python path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# Setup Django
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'analytics.settings')
django.setup()

from core.models import SAPGLPosting, DataFile, AnalysisSession
from core.analytics import SAPGLAnalyzer

BASE_URL = "http://localhost:8000/api"

def test_duplicate_anomaly_api():
    """Test the consolidated duplicate anomaly API"""
    
    print("=== Duplicate Anomaly API Test ===\n")
    
    # 1. Test GET request with sheet_id parameter
    print("1. Testing GET request with sheet_id parameter:")
    
    # First, get a sample file ID
    try:
        sample_file = DataFile.objects.first()  # type: ignore
        if sample_file:
            sheet_id = str(sample_file.id)
            print(f"   Using sheet_id: {sheet_id}")
            
            # Test GET request
            response = requests.get(f"{BASE_URL}/duplicate-anomalies/", params={
                'sheet_id': sheet_id,
                'duplicate_threshold': 2,
                'date_from': '2024-01-01',
                'date_to': '2024-12-31'
            })
            
            if response.status_code == 200:
                data = response.json()
                print(f"   ✓ GET request successful")
                print(f"   Total duplicates: {data.get('total_duplicates', 0)}")
                print(f"   Total transactions involved: {data.get('total_transactions_involved', 0)}")
                print(f"   Total amount involved: {data.get('total_amount_involved', 0)}")
                print(f"   Charts data available: {'charts_data' in data}")
                print(f"   Training data available: {'training_data' in data}")
            else:
                print(f"   ✗ GET request failed: {response.status_code}")
                print(f"   Error: {response.text}")
        else:
            print("   ✗ No data files found")
    except Exception as e:
        print(f"   ✗ Error testing GET request: {e}")
    
    print()
    
    # 2. Test POST request with custom parameters
    print("2. Testing POST request with custom parameters:")
    
    try:
        # Get a sample session ID
        sample_session = AnalysisSession.objects.first()  # type: ignore
        if sample_session:
            sheet_id = str(sample_session.id)
            print(f"   Using sheet_id: {sheet_id}")
            
            # Test POST request
            post_data = {
                'sheet_id': sheet_id,
                'duplicate_threshold': 3,
                'include_all_types': True,
                'duplicate_types': [1, 2, 3],  # Only Type 1, 2, 3
                'date_from': '2024-01-01',
                'date_to': '2024-12-31',
                'min_amount': 1000,
                'max_amount': 1000000
            }
            
            response = requests.post(f"{BASE_URL}/duplicate-anomalies/analyze/", json=post_data)
            
            if response.status_code == 200:
                data = response.json()
                print(f"   ✓ POST request successful")
                print(f"   Message: {data.get('message', 'N/A')}")
                print(f"   Total duplicates: {data.get('total_duplicates', 0)}")
                print(f"   Type breakdown: {list(data.get('type_breakdown', {}).keys())}")
                print(f"   Charts data available: {'charts_data' in data}")
                print(f"   Training data available: {'training_data' in data}")
                
                # Show sample duplicate data
                duplicates = data.get('duplicates', [])
                if duplicates:
                    print(f"   Sample duplicate:")
                    sample_dup = duplicates[0]
                    print(f"     Type: {sample_dup.get('type')}")
                    print(f"     GL Account: {sample_dup.get('gl_account')}")
                    print(f"     Amount: {sample_dup.get('amount')}")
                    print(f"     Count: {sample_dup.get('count')}")
                    print(f"     Risk Score: {sample_dup.get('risk_score')}")
                
                # Show charts data structure
                charts_data = data.get('charts_data', {})
                if charts_data:
                    print(f"   Charts data structure:")
                    print(f"     - Duplicate flags breakdown: {len(charts_data.get('duplicate_flags_breakdown', {}).get('labels', []))} types")
                    print(f"     - Monthly data: {len(charts_data.get('monthly_duplicate_data', []))} months")
                    print(f"     - User breakdown: {len(charts_data.get('user_breakdown', []))} users")
                    print(f"     - FS line breakdown: {len(charts_data.get('fs_line_breakdown', []))} accounts")
                
                # Show training data structure
                training_data = data.get('training_data', {})
                if training_data:
                    print(f"   Training data structure:")
                    print(f"     - Total samples: {training_data.get('total_samples', 0)}")
                    print(f"     - Duplicate samples: {training_data.get('duplicate_samples', 0)}")
                    print(f"     - Non-duplicate samples: {training_data.get('non_duplicate_samples', 0)}")
                    print(f"     - Features: {len(training_data.get('training_features', []))}")
                    print(f"     - Feature importance: {list(training_data.get('feature_importance', {}).keys())}")
            else:
                print(f"   ✗ POST request failed: {response.status_code}")
                print(f"   Error: {response.text}")
        else:
            print("   ✗ No analysis sessions found")
    except Exception as e:
        print(f"   ✗ Error testing POST request: {e}")
    
    print()
    
    # 3. Test error handling
    print("3. Testing error handling:")
    
    # Test without sheet_id
    try:
        response = requests.get(f"{BASE_URL}/duplicate-anomalies/")
        if response.status_code == 400:
            print("   ✓ Correctly rejected request without sheet_id")
        else:
            print(f"   ✗ Expected 400 error, got {response.status_code}")
    except Exception as e:
        print(f"   ✗ Error testing without sheet_id: {e}")
    
    # Test with invalid sheet_id
    try:
        response = requests.get(f"{BASE_URL}/duplicate-anomalies/", params={'sheet_id': 'invalid-id'})
        if response.status_code == 200:
            data = response.json()
            if data.get('total_duplicates') == 0:
                print("   ✓ Correctly handled invalid sheet_id (returned empty results)")
            else:
                print("   ✗ Unexpected results for invalid sheet_id")
        else:
            print(f"   ✗ Unexpected response for invalid sheet_id: {response.status_code}")
    except Exception as e:
        print(f"   ✗ Error testing invalid sheet_id: {e}")
    
    print()
    
    # 4. Show API usage examples
    print("4. API Usage Examples:")
    print()
    print("   GET Request (Simple):")
    print("   curl -X GET 'http://localhost:8000/api/duplicate-anomalies/?sheet_id=YOUR_SHEET_ID'")
    print()
    print("   GET Request (With Filters):")
    print("   curl -X GET 'http://localhost:8000/api/duplicate-anomalies/\\")
    print("     ?sheet_id=YOUR_SHEET_ID\\")
    print("     &duplicate_threshold=2\\")
    print("     &date_from=2024-01-01\\")
    print("     &date_to=2024-12-31\\")
    print("     &min_amount=1000\\")
    print("     &max_amount=1000000'")
    print()
    print("   POST Request (Advanced):")
    print("   curl -X POST 'http://localhost:8000/api/duplicate-anomalies/analyze/'\\")
    print("     -H 'Content-Type: application/json'\\")
    print("     -d '{")
    print("       \"sheet_id\": \"YOUR_SHEET_ID\",")
    print("       \"duplicate_threshold\": 3,")
    print("       \"include_all_types\": false,")
    print("       \"duplicate_types\": [1, 2, 3],")
    print("       \"date_from\": \"2024-01-01\",")
    print("       \"date_to\": \"2024-12-31\",")
    print("       \"min_amount\": 1000,")
    print("       \"max_amount\": 1000000")
    print("     }'")
    print()
    
    print("=== Test Complete ===")

def test_direct_analysis():
    """Test direct analysis without API"""
    
    print("\n=== Direct Analysis Test ===\n")
    
    # Get all transactions
    transactions = SAPGLPosting.objects.all()  # type: ignore
    print(f"Total transactions in database: {transactions.count()}")
    
    if transactions.exists():
        # Test duplicate detection directly
        analyzer = SAPGLAnalyzer()
        analyzer.analysis_config['duplicate_threshold'] = 2
        
        duplicates = analyzer.detect_duplicate_entries(list(transactions))
        
        print(f"Found {len(duplicates)} duplicate groups")
        
        # Show breakdown by type
        type_counts = {}
        for dup in duplicates:
            dup_type = dup['type']
            type_counts[dup_type] = type_counts.get(dup_type, 0) + 1
        
        print("Breakdown by type:")
        for dup_type, count in type_counts.items():
            print(f"  {dup_type}: {count}")
        
        # Show sample duplicates
        if duplicates:
            print("\nSample duplicates:")
            for i, dup in enumerate(duplicates[:3]):
                print(f"  {i+1}. {dup['type']}")
                print(f"     GL Account: {dup['gl_account']}")
                print(f"     Amount: {dup['amount']}")
                print(f"     Count: {dup['count']}")
                print(f"     Risk Score: {dup['risk_score']}")
                print()
    else:
        print("No transactions found in database")

if __name__ == "__main__":
    test_direct_analysis()
    test_duplicate_anomaly_api() 