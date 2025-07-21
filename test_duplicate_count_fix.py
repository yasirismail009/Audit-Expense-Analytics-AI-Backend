#!/usr/bin/env python3
"""
Test script to verify duplicate counting consistency between endpoints
"""

import requests
import json
import sys
from datetime import datetime

# Configuration
BASE_URL = "http://localhost:8000"
API_BASE = f"{BASE_URL}/api"

def test_duplicate_count_consistency():
    """Test that duplicate counts are consistent between endpoints"""
    print("=== Testing Duplicate Count Consistency ===\n")
    
    # Get a list of files first
    try:
        response = requests.get(f"{API_BASE}/file-list/")
        if response.status_code == 200:
            data = response.json()
            files = data.get('files', [])
            
            if not files:
                print("❌ No files found. Please upload a file first.")
                return False
            
            # Test with the first file
            test_file = files[0]
            file_id = test_file['id']
            print(f"Testing with file: {test_file['file_name']} (ID: {file_id})")
            
            # Test file-summary endpoint
            print("\n1. Testing file-summary endpoint...")
            summary_response = requests.get(f"{API_BASE}/file-summary/{file_id}/")
            
            if summary_response.status_code == 200:
                summary_data = summary_response.json()
                file_summary_duplicates = summary_data.get('anomaly_summary', {}).get('duplicate_entries', 0)
                print(f"   ✅ File-summary duplicate count: {file_summary_duplicates}")
            else:
                print(f"   ❌ File-summary error: {summary_response.status_code}")
                return False
            
            # Test duplicate-anomalies/analyze/ endpoint
            print("\n2. Testing duplicate-anomalies/analyze/ endpoint...")
            analyze_payload = {
                'sheet_id': file_id,
                'duplicate_threshold': 2,
                'include_all_types': True
            }
            
            analyze_response = requests.post(f"{API_BASE}/duplicate-anomalies/analyze/", json=analyze_payload)
            
            if analyze_response.status_code == 200:
                analyze_data = analyze_response.json()
                api_duplicate_groups = analyze_data.get('total_duplicates', 0)
                api_unique_transactions = analyze_data.get('unique_duplicate_transactions', 0)
                print(f"   ✅ API duplicate groups: {api_duplicate_groups}")
                print(f"   ✅ API unique transactions: {api_unique_transactions}")
            else:
                print(f"   ❌ API error: {analyze_response.status_code}")
                print(f"   Response: {analyze_response.text}")
                return False
            
            # Compare results
            print("\n3. Comparing results...")
            if file_summary_duplicates == api_unique_transactions:
                print(f"   ✅ SUCCESS: Counts match! Both show {file_summary_duplicates} unique duplicate transactions")
                return True
            else:
                print(f"   ❌ MISMATCH: File-summary shows {file_summary_duplicates}, API shows {api_unique_transactions} unique transactions")
                print(f"   📊 Additional info: API found {api_duplicate_groups} duplicate groups")
                return False
                
        else:
            print(f"❌ Error getting file list: {response.status_code}")
            return False
            
    except Exception as e:
        print(f"❌ Exception: {e}")
        return False

def test_duplicate_types_explanation():
    """Explain the different duplicate types and why counts might differ"""
    print("\n=== Duplicate Types Explanation ===\n")
    
    print("The system detects 6 types of duplicates:")
    print("1. Type 1: Account Number + Amount")
    print("2. Type 2: Account Number + Source + Amount")
    print("3. Type 3: Account Number + User + Amount")
    print("4. Type 4: Account Number + Posted Date + Amount")
    print("5. Type 5: Account Number + Effective Date + Amount")
    print("6. Type 6: Account Number + Effective Date + Posted Date + User + Source + Amount")
    print()
    print("⚠️  IMPORTANT: The same transaction can appear in multiple duplicate types!")
    print("   Example: A transaction could be:")
    print("   - Part of Type 1 (same account + amount)")
    print("   - Part of Type 3 (same account + user + amount)")
    print("   - Part of Type 4 (same account + date + amount)")
    print()
    print("📊 Counting Methods:")
    print("   - File-summary: Counts UNIQUE transaction IDs involved in ANY duplicate type")
    print("   - API (old): Counted duplicate GROUPS (each group can have multiple transactions)")
    print("   - API (new): Now provides BOTH counts for clarity")
    print()
    print("🔧 Fix Applied:")
    print("   - API now includes 'unique_duplicate_transactions' field")
    print("   - This matches the file-summary counting method")
    print("   - 'total_duplicates' still shows number of duplicate groups")

def main():
    """Main test function"""
    print("Duplicate Count Consistency Test")
    print("=" * 40)
    
    # Test the consistency
    success = test_duplicate_count_consistency()
    
    # Explain the duplicate types
    test_duplicate_types_explanation()
    
    if success:
        print("\n✅ All tests passed! Duplicate counting is now consistent.")
    else:
        print("\n❌ Tests failed. Please check the implementation.")
    
    print("\n=== Test Complete ===")

if __name__ == "__main__":
    main() 