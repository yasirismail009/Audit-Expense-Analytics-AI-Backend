#!/usr/bin/env python3
"""
Simple test to verify GL analysis endpoint fix
"""

import requests
import json

BASE_URL = "http://localhost:8000/api"

def test_gl_analysis():
    """Test the GL analysis endpoint"""
    print("Testing GL Account Analysis (Fixed Version)...")
    print("=" * 50)
    
    try:
        # Test GL analysis endpoint
        response = requests.get(f"{BASE_URL}/gl-accounts/analysis/")
        
        print(f"Status Code: {response.status_code}")
        
        if response.status_code == 200:
            data = response.json()
            print(f"✓ SUCCESS! Found {len(data)} accounts with analysis")
            
            if data:
                print("\nAnalysis Results:")
                for i, account in enumerate(data[:3], 1):
                    print(f"\n{i}. {account['account_id']} - {account['account_name']}")
                    print(f"   Type: {account['account_type']}")
                    print(f"   Category: {account['account_category']}")
                    print(f"   Current Balance: {account['current_balance']:,.2f} SAR")
                    print(f"   Total Debits: {account['total_debits']:,.2f} SAR")
                    print(f"   Total Credits: {account['total_credits']:,.2f} SAR")
                    print(f"   Transaction Count: {account['transaction_count']}")
                    print(f"   Risk Score: {account['risk_score']}")
                    print(f"   High Value Transactions: {account['high_value_transactions']}")
                    print(f"   Flagged Transactions: {account['flagged_transactions']}")
            else:
                print("⚠️ No analysis results returned")
                print("This might mean:")
                print("- No GL accounts exist")
                print("- No transactions exist")
                print("- No GL accounts have transactions")
        else:
            print(f"✗ FAILED: {response.status_code}")
            print(f"Response: {response.text}")
            
    except requests.exceptions.ConnectionError:
        print("✗ Cannot connect to server")
        print("Make sure the server is running: python manage.py runserver")
    except Exception as e:
        print(f"✗ Error: {e}")

def test_other_endpoints():
    """Test other GL endpoints"""
    print("\n" + "=" * 50)
    print("Testing Other GL Endpoints...")
    
    endpoints = [
        ("GL Accounts List", "/gl-accounts/"),
        ("Trial Balance", "/gl-accounts/trial-balance/"),
        ("GL Charts", "/gl-accounts/charts/"),
    ]
    
    for name, endpoint in endpoints:
        try:
            response = requests.get(f"{BASE_URL}{endpoint}")
            status = "✓" if response.status_code == 200 else "✗"
            print(f"{status} {name}: {response.status_code}")
            
            if response.status_code == 200:
                data = response.json()
                if isinstance(data, list):
                    print(f"   Found {len(data)} items")
                elif isinstance(data, dict):
                    print(f"   Response contains {len(data)} keys")
        except Exception as e:
            print(f"✗ {name}: {e}")

if __name__ == "__main__":
    test_gl_analysis()
    test_other_endpoints()
    
    print("\n" + "=" * 50)
    print("Test Complete!")
    print("\nIf you're still not getting results, run the debug script:")
    print("python debug_gl_analysis.py") 