#!/usr/bin/env python3
"""
Test script to verify that core URLs are accessible without authentication.
Run this script to test the core endpoints.
"""

import requests
import json

# Base URL for the analytics API
BASE_URL = "http://localhost:8000/api"

def test_core_endpoints():
    """Test core endpoints without authentication"""
    
    endpoints_to_test = [
        "/data-files/",
        "/processing-jobs/",
        "/files-listing/",
        "/ml-model-training/",
    ]
    
    print("Testing core endpoints without authentication...")
    print("=" * 50)
    
    for endpoint in endpoints_to_test:
        url = f"{BASE_URL}{endpoint}"
        try:
            response = requests.get(url)
            status = "✅ PASS" if response.status_code in [200, 201, 204] else "❌ FAIL"
            print(f"{status} {endpoint} - Status: {response.status_code}")
            
            if response.status_code == 401:
                print(f"   ⚠️  Authentication required for {endpoint}")
            elif response.status_code == 403:
                print(f"   ⚠️  Permission denied for {endpoint}")
            elif response.status_code >= 500:
                print(f"   ⚠️  Server error for {endpoint}")
                
        except requests.exceptions.ConnectionError:
            print(f"❌ FAIL {endpoint} - Connection error (server not running?)")
        except Exception as e:
            print(f"❌ FAIL {endpoint} - Error: {str(e)}")
    
    print("\n" + "=" * 50)
    print("Test completed!")

if __name__ == "__main__":
    test_core_endpoints()
