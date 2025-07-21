#!/usr/bin/env python3
"""
Test script for duplicate anomaly API fixes
"""

import requests
import json
import sys
from datetime import datetime

# Configuration
BASE_URL = "http://localhost:8000"
API_BASE = f"{BASE_URL}/api"

def test_duplicate_anomaly_list():
    """Test the duplicate anomaly list endpoint"""
    print("Testing duplicate anomaly list endpoint...")
    
    # Test with a valid sheet_id
    params = {
        'sheet_id': '1',  # Assuming sheet_id 1 exists
        'duplicate_threshold': 2
    }
    
    try:
        response = requests.get(f"{API_BASE}/duplicate-anomalies/", params=params)
        
        print(f"Status Code: {response.status_code}")
        
        if response.status_code == 200:
            data = response.json()
            print("✅ Success! Response structure:")
            print(f"  - Sheet ID: {data.get('sheet_id')}")
            print(f"  - Total Duplicates: {data.get('total_duplicates')}")
            print(f"  - Total Transactions Involved: {data.get('total_transactions_involved')}")
            print(f"  - Total Amount Involved: {data.get('total_amount_involved')}")
            print(f"  - Type Breakdown: {len(data.get('type_breakdown', {}))} types")
            print(f"  - Charts Data: {len(data.get('charts_data', {}))} chart types")
            print(f"  - Training Data: {data.get('training_data', {}).get('total_samples', 0)} samples")
            return True
        else:
            print(f"❌ Error: {response.status_code}")
            print(f"Response: {response.text}")
            return False
            
    except Exception as e:
        print(f"❌ Exception: {e}")
        return False

def test_duplicate_anomaly_analyze():
    """Test the duplicate anomaly analyze endpoint"""
    print("\nTesting duplicate anomaly analyze endpoint...")
    
    # Test data
    payload = {
        'sheet_id': '1',  # Assuming sheet_id 1 exists
        'duplicate_threshold': 2,
        'include_all_types': True,
        'date_from': '2024-01-01',
        'date_to': '2024-12-31'
    }
    
    try:
        response = requests.post(f"{API_BASE}/duplicate-anomalies/analyze/", json=payload)
        
        print(f"Status Code: {response.status_code}")
        
        if response.status_code == 200:
            data = response.json()
            print("✅ Success! Response structure:")
            print(f"  - Message: {data.get('message')}")
            print(f"  - Sheet ID: {data.get('sheet_id')}")
            print(f"  - Total Duplicates: {data.get('total_duplicates')}")
            print(f"  - Total Transactions Involved: {data.get('total_transactions_involved')}")
            print(f"  - Total Amount Involved: {data.get('total_amount_involved')}")
            return True
        else:
            print(f"❌ Error: {response.status_code}")
            print(f"Response: {response.text}")
            return False
            
    except Exception as e:
        print(f"❌ Exception: {e}")
        return False

def test_duplicate_anomaly_without_sheet_id():
    """Test error handling when sheet_id is missing"""
    print("\nTesting error handling for missing sheet_id...")
    
    try:
        response = requests.get(f"{API_BASE}/duplicate-anomalies/")
        
        print(f"Status Code: {response.status_code}")
        
        if response.status_code == 400:
            data = response.json()
            print("✅ Success! Properly handled missing sheet_id:")
            print(f"  - Error: {data.get('error')}")
            return True
        else:
            print(f"❌ Unexpected status code: {response.status_code}")
            print(f"Response: {response.text}")
            return False
            
    except Exception as e:
        print(f"❌ Exception: {e}")
        return False

def test_duplicate_anomaly_invalid_sheet_id():
    """Test error handling with invalid sheet_id"""
    print("\nTesting error handling for invalid sheet_id...")
    
    params = {
        'sheet_id': '999999',  # Non-existent sheet_id
        'duplicate_threshold': 2
    }
    
    try:
        response = requests.get(f"{API_BASE}/duplicate-anomalies/", params=params)
        
        print(f"Status Code: {response.status_code}")
        
        if response.status_code == 200:
            data = response.json()
            print("✅ Success! Handled invalid sheet_id gracefully:")
            print(f"  - Total Duplicates: {data.get('total_duplicates')}")
            print(f"  - Should be 0 for invalid sheet_id")
            return True
        else:
            print(f"❌ Unexpected status code: {response.status_code}")
            print(f"Response: {response.text}")
            return False
            
    except Exception as e:
        print(f"❌ Exception: {e}")
        return False

def main():
    """Run all tests"""
    print("=" * 60)
    print("DUPLICATE ANOMALY API TEST SUITE")
    print("=" * 60)
    
    tests = [
        ("Duplicate Anomaly List", test_duplicate_anomaly_list),
        ("Duplicate Anomaly Analyze", test_duplicate_anomaly_analyze),
        ("Missing Sheet ID Error", test_duplicate_anomaly_without_sheet_id),
        ("Invalid Sheet ID Handling", test_duplicate_anomaly_invalid_sheet_id)
    ]
    
    results = []
    
    for test_name, test_func in tests:
        print(f"\n{'='*20} {test_name} {'='*20}")
        try:
            result = test_func()
            results.append((test_name, result))
        except Exception as e:
            print(f"❌ Test failed with exception: {e}")
            results.append((test_name, False))
    
    # Summary
    print("\n" + "=" * 60)
    print("TEST SUMMARY")
    print("=" * 60)
    
    passed = 0
    total = len(results)
    
    for test_name, result in results:
        status = "✅ PASS" if result else "❌ FAIL"
        print(f"{test_name}: {status}")
        if result:
            passed += 1
    
    print(f"\nOverall: {passed}/{total} tests passed")
    
    if passed == total:
        print("🎉 All tests passed! The duplicate anomaly API is working correctly.")
        return 0
    else:
        print("⚠️  Some tests failed. Please check the implementation.")
        return 1

if __name__ == "__main__":
    sys.exit(main()) 