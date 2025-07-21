#!/usr/bin/env python3
"""
Quick test for GL Account analysis endpoint
"""

import requests
import json

BASE_URL = "http://localhost:8000/api"

def quick_test():
    """Quick test of GL analysis endpoint"""
    print("Testing GL Account Analysis Endpoint...")
    print("=" * 40)
    
    # Test 1: Check if endpoint exists
    print("1. Testing endpoint availability...")
    try:
        response = requests.get(f"{BASE_URL}/gl-accounts/analysis/")
        print(f"   Status Code: {response.status_code}")
        
        if response.status_code == 200:
            data = response.json()
            print(f"   ✓ Endpoint working! Found {len(data)} accounts")
            
            if data:
                print("\n   Sample data:")
                for account in data[:2]:
                    print(f"   - {account['account_id']}: {account['account_name']}")
                    print(f"     Balance: {account['current_balance']}")
                    print(f"     Risk Score: {account['risk_score']}")
            else:
                print("   ⚠️ No data returned - this might be the issue")
        else:
            print(f"   ✗ Endpoint failed: {response.text}")
            
    except requests.exceptions.ConnectionError:
        print("   ✗ Cannot connect to server")
        print("   Make sure the server is running: python manage.py runserver")
        return
    except Exception as e:
        print(f"   ✗ Error: {e}")
        return
    
    # Test 2: Check if GL accounts exist
    print("\n2. Checking GL accounts...")
    try:
        response = requests.get(f"{BASE_URL}/gl-accounts/")
        if response.status_code == 200:
            accounts = response.json()
            print(f"   Found {len(accounts)} GL accounts")
            if accounts:
                print("   Sample accounts:")
                for acc in accounts[:3]:
                    print(f"   - {acc['account_id']}: {acc['account_name']}")
        else:
            print(f"   ✗ Failed to get GL accounts: {response.text}")
    except Exception as e:
        print(f"   ✗ Error: {e}")
    
    # Test 3: Check if transactions exist
    print("\n3. Checking transactions...")
    try:
        response = requests.get(f"{BASE_URL}/postings/")
        if response.status_code == 200:
            transactions = response.json()
            print(f"   Found {len(transactions)} transactions")
            if transactions:
                print("   Sample transactions:")
                for tx in transactions[:3]:
                    print(f"   - {tx['document_number']}: {tx['gl_account']} - {tx['amount_local_currency']}")
        else:
            print(f"   ✗ Failed to get transactions: {response.text}")
    except Exception as e:
        print(f"   ✗ Error: {e}")
    
    # Test 4: Check other GL endpoints
    print("\n4. Testing other GL endpoints...")
    endpoints = [
        ("Trial Balance", "/gl-accounts/trial-balance/"),
        ("Charts", "/gl-accounts/charts/"),
        ("GL Accounts List", "/gl-accounts/"),
    ]
    
    for name, endpoint in endpoints:
        try:
            response = requests.get(f"{BASE_URL}{endpoint}")
            status = "✓" if response.status_code == 200 else "✗"
            print(f"   {status} {name}: {response.status_code}")
        except Exception as e:
            print(f"   ✗ {name}: {e}")

if __name__ == "__main__":
    quick_test() 