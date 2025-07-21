#!/usr/bin/env python3
"""
Debug script for GL Account analysis issues
This script will help identify and fix problems with the GL account analysis endpoint
"""

import requests
import json
import csv
import io
from datetime import datetime

# Configuration
BASE_URL = "http://localhost:8000/api"

def check_server_status():
    """Check if the server is running"""
    print("1. Checking server status...")
    try:
        response = requests.get(f"{BASE_URL}/")
        if response.status_code == 200:
            print("✓ Server is running")
            return True
        else:
            print(f"✗ Server returned status code: {response.status_code}")
            return False
    except requests.exceptions.ConnectionError:
        print("✗ Cannot connect to server. Make sure the server is running on localhost:8000")
        return False

def check_gl_accounts_exist():
    """Check if GL accounts exist in the database"""
    print("\n2. Checking if GL accounts exist...")
    try:
        response = requests.get(f"{BASE_URL}/gl-accounts/")
        if response.status_code == 200:
            accounts = response.json()
            print(f"✓ Found {len(accounts)} GL accounts")
            if accounts:
                print("  Sample accounts:")
                for account in accounts[:3]:
                    print(f"    - {account['account_id']}: {account['account_name']}")
            return len(accounts) > 0
        else:
            print(f"✗ Failed to get GL accounts: {response.status_code}")
            return False
    except Exception as e:
        print(f"✗ Error checking GL accounts: {e}")
        return False

def check_transactions_exist():
    """Check if transactions exist in the database"""
    print("\n3. Checking if transactions exist...")
    try:
        response = requests.get(f"{BASE_URL}/postings/")
        if response.status_code == 200:
            transactions = response.json()
            print(f"✓ Found {len(transactions)} transactions")
            if transactions:
                print("  Sample transactions:")
                for transaction in transactions[:3]:
                    print(f"    - {transaction['document_number']}: {transaction['amount_local_currency']} {transaction.get('transaction_type', 'N/A')}")
            return len(transactions) > 0
        else:
            print(f"✗ Failed to get transactions: {response.status_code}")
            return False
    except Exception as e:
        print(f"✗ Error checking transactions: {e}")
        return False

def create_test_data_if_needed():
    """Create test data if none exists"""
    print("\n4. Creating test data if needed...")
    
    # Check if we need to create GL accounts
    gl_response = requests.get(f"{BASE_URL}/gl-accounts/")
    if gl_response.status_code == 200 and len(gl_response.json()) == 0:
        print("  Creating GL account master data...")
        create_gl_accounts()
    
    # Check if we need to create transactions
    tx_response = requests.get(f"{BASE_URL}/postings/")
    if tx_response.status_code == 200 and len(tx_response.json()) == 0:
        print("  Creating test transactions...")
        create_test_transactions()

def create_gl_accounts():
    """Create sample GL accounts"""
    gl_accounts_data = [
        {
            "Account ID": "1000",
            "Account Name": "Cash and Cash Equivalents",
            "Account Type": "Asset",
            "Account Category": "Current Assets",
            "Account Subcategory": "Cash",
            "Normal Balance": "DEBIT",
            "Is Active": "TRUE"
        },
        {
            "Account ID": "1100",
            "Account Name": "Accounts Receivable",
            "Account Type": "Asset",
            "Account Category": "Current Assets",
            "Account Subcategory": "Receivables",
            "Normal Balance": "DEBIT",
            "Is Active": "TRUE"
        },
        {
            "Account ID": "2000",
            "Account Name": "Accounts Payable",
            "Account Type": "Liability",
            "Account Category": "Current Liabilities",
            "Account Subcategory": "Payables",
            "Normal Balance": "CREDIT",
            "Is Active": "TRUE"
        },
        {
            "Account ID": "4000",
            "Account Name": "Revenue",
            "Account Type": "Revenue",
            "Account Category": "Income",
            "Account Subcategory": "Sales",
            "Normal Balance": "CREDIT",
            "Is Active": "TRUE"
        },
        {
            "Account ID": "5000",
            "Account Name": "Cost of Goods Sold",
            "Account Type": "Expense",
            "Account Category": "Cost of Sales",
            "Account Subcategory": "Direct Costs",
            "Normal Balance": "DEBIT",
            "Is Active": "TRUE"
        }
    ]
    
    # Create CSV content
    csv_content = io.StringIO()
    fieldnames = ["Account ID", "Account Name", "Account Type", "Account Category", "Account Subcategory", "Normal Balance", "Is Active"]
    writer = csv.DictWriter(csv_content, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(gl_accounts_data)
    
    # Upload GL Account master data
    files = {
        'file': ('gl_accounts.csv', csv_content.getvalue(), 'text/csv')
    }
    
    response = requests.post(f"{BASE_URL}/gl-accounts/upload-master-data/", files=files)
    
    if response.status_code == 200:
        result = response.json()
        print(f"  ✓ Created {result.get('created_count', 0)} GL accounts")
    else:
        print(f"  ✗ Failed to create GL accounts: {response.text}")

def create_test_transactions():
    """Create sample transactions"""
    transactions_data = [
        {
            "Document Number": "DOC001",
            "Posting Date": "2025-01-15",
            "G/L Account": "1000",
            "Amount in Local Currency": "1000000",
            "Transaction Type": "DEBIT",
            "Local Currency": "SAR",
            "Text": "Cash deposit",
            "Document Date": "2025-01-15",
            "User Name": "USER001",
            "Document Type": "DZ",
            "Fiscal Year": "2025",
            "Posting Period": "1"
        },
        {
            "Document Number": "DOC002",
            "Posting Date": "2025-01-16",
            "G/L Account": "1100",
            "Amount in Local Currency": "500000",
            "Transaction Type": "DEBIT",
            "Local Currency": "SAR",
            "Text": "Sales on credit",
            "Document Date": "2025-01-16",
            "User Name": "USER002",
            "Document Type": "SA",
            "Fiscal Year": "2025",
            "Posting Period": "1"
        },
        {
            "Document Number": "DOC003",
            "Posting Date": "2025-01-17",
            "G/L Account": "2000",
            "Amount in Local Currency": "300000",
            "Transaction Type": "CREDIT",
            "Local Currency": "SAR",
            "Text": "Purchase on credit",
            "Document Date": "2025-01-17",
            "User Name": "USER003",
            "Document Type": "TR",
            "Fiscal Year": "2025",
            "Posting Period": "1"
        },
        {
            "Document Number": "DOC004",
            "Posting Date": "2025-01-18",
            "G/L Account": "4000",
            "Amount in Local Currency": "500000",
            "Transaction Type": "CREDIT",
            "Local Currency": "SAR",
            "Text": "Revenue recognition",
            "Document Date": "2025-01-18",
            "User Name": "USER001",
            "Document Type": "SA",
            "Fiscal Year": "2025",
            "Posting Period": "1"
        },
        {
            "Document Number": "DOC005",
            "Posting Date": "2025-01-19",
            "G/L Account": "5000",
            "Amount in Local Currency": "200000",
            "Transaction Type": "DEBIT",
            "Local Currency": "SAR",
            "Text": "Cost of goods sold",
            "Document Date": "2025-01-19",
            "User Name": "USER002",
            "Document Type": "TR",
            "Fiscal Year": "2025",
            "Posting Period": "1"
        }
    ]
    
    # Create CSV content
    csv_content = io.StringIO()
    fieldnames = ["Document Number", "Posting Date", "G/L Account", "Amount in Local Currency", "Transaction Type", 
                  "Local Currency", "Text", "Document Date", "User Name", "Document Type", "Fiscal Year", "Posting Period"]
    writer = csv.DictWriter(csv_content, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(transactions_data)
    
    # Upload transactions
    files = {
        'file': ('transactions.csv', csv_content.getvalue(), 'text/csv')
    }
    data = {
        'engagement_id': 'DEBUG001',
        'client_name': 'Debug Client',
        'company_name': 'Debug Company',
        'fiscal_year': '2025',
        'audit_start_date': '2025-01-01',
        'audit_end_date': '2025-12-31'
    }
    
    response = requests.post(f"{BASE_URL}/file-upload-analysis/", files=files, data=data)
    
    if response.status_code == 201:
        result = response.json()
        print(f"  ✓ Created {result.get('file', {}).get('processed_records', 0)} transactions")
    else:
        print(f"  ✗ Failed to create transactions: {response.text}")

def test_gl_analysis_endpoint():
    """Test the GL analysis endpoint with detailed error reporting"""
    print("\n5. Testing GL analysis endpoint...")
    
    try:
        response = requests.get(f"{BASE_URL}/gl-accounts/analysis/")
        print(f"  Response status: {response.status_code}")
        
        if response.status_code == 200:
            analysis_data = response.json()
            print(f"  ✓ GL analysis successful!")
            print(f"  Total accounts analyzed: {len(analysis_data)}")
            
            if analysis_data:
                print("\n  Analysis Results:")
                for i, account in enumerate(analysis_data[:5], 1):
                    print(f"    {i}. {account['account_id']} - {account['account_name']}")
                    print(f"       Type: {account['account_type']}")
                    print(f"       Balance: {account['current_balance']:,.2f} SAR")
                    print(f"       Debits: {account['total_debits']:,.2f} SAR")
                    print(f"       Credits: {account['total_credits']:,.2f} SAR")
                    print(f"       Transactions: {account['transaction_count']}")
                    print(f"       Risk Score: {account['risk_score']}")
                    print()
            else:
                print("  ⚠️ No analysis results returned")
                print("  This might indicate:")
                print("    - No GL accounts with transactions")
                print("    - Issues with account-transaction relationships")
                print("    - Problems with the analysis logic")
        else:
            print(f"  ✗ GL analysis failed with status {response.status_code}")
            print(f"  Response: {response.text}")
            
    except Exception as e:
        print(f"  ✗ Error testing GL analysis: {e}")

def check_account_transaction_relationships():
    """Check if GL accounts are properly linked to transactions"""
    print("\n6. Checking account-transaction relationships...")
    
    try:
        # Get GL accounts
        gl_response = requests.get(f"{BASE_URL}/gl-accounts/")
        if gl_response.status_code != 200:
            print("  ✗ Cannot get GL accounts")
            return
        
        accounts = gl_response.json()
        if not accounts:
            print("  ⚠️ No GL accounts found")
            return
        
        # Get transactions
        tx_response = requests.get(f"{BASE_URL}/postings/")
        if tx_response.status_code != 200:
            print("  ✗ Cannot get transactions")
            return
        
        transactions = tx_response.json()
        if not transactions:
            print("  ⚠️ No transactions found")
            return
        
        # Check relationships
        account_ids = {acc['account_id'] for acc in accounts}
        transaction_accounts = {tx['gl_account'] for tx in transactions}
        
        print(f"  GL Account IDs: {sorted(account_ids)}")
        print(f"  Transaction Account IDs: {sorted(transaction_accounts)}")
        
        # Find mismatches
        missing_accounts = transaction_accounts - account_ids
        unused_accounts = account_ids - transaction_accounts
        
        if missing_accounts:
            print(f"  ⚠️ Transactions reference missing accounts: {missing_accounts}")
        
        if unused_accounts:
            print(f"  ⚠️ GL accounts with no transactions: {unused_accounts}")
        
        common_accounts = account_ids & transaction_accounts
        if common_accounts:
            print(f"  ✓ Found {len(common_accounts)} accounts with transactions: {sorted(common_accounts)}")
        else:
            print("  ✗ No accounts have transactions - this is the problem!")
            
    except Exception as e:
        print(f"  ✗ Error checking relationships: {e}")

def test_individual_endpoints():
    """Test individual endpoints to isolate issues"""
    print("\n7. Testing individual endpoints...")
    
    endpoints = [
        ("GL Accounts List", f"{BASE_URL}/gl-accounts/"),
        ("Transactions List", f"{BASE_URL}/postings/"),
        ("GL Analysis", f"{BASE_URL}/gl-accounts/analysis/"),
        ("Trial Balance", f"{BASE_URL}/gl-accounts/trial-balance/"),
        ("GL Charts", f"{BASE_URL}/gl-accounts/charts/"),
    ]
    
    for name, url in endpoints:
        try:
            response = requests.get(url)
            status = "✓" if response.status_code == 200 else "✗"
            print(f"  {status} {name}: {response.status_code}")
            if response.status_code != 200:
                print(f"    Error: {response.text[:100]}...")
        except Exception as e:
            print(f"  ✗ {name}: Error - {e}")

def main():
    """Main debugging function"""
    print("=== GL Account Analysis Debug Script ===")
    print("This script will help identify and fix issues with GL account analysis")
    print("=" * 50)
    
    # Step 1: Check server status
    if not check_server_status():
        print("\n❌ Server is not running. Please start the Django server first:")
        print("   python manage.py runserver")
        return
    
    # Step 2: Check if data exists
    has_accounts = check_gl_accounts_exist()
    has_transactions = check_transactions_exist()
    
    # Step 3: Create test data if needed
    if not has_accounts or not has_transactions:
        create_test_data_if_needed()
    
    # Step 4: Check relationships
    check_account_transaction_relationships()
    
    # Step 5: Test individual endpoints
    test_individual_endpoints()
    
    # Step 6: Test GL analysis
    test_gl_analysis_endpoint()
    
    print("\n" + "=" * 50)
    print("Debugging complete!")
    print("\nIf you're still not getting GL analysis results, check:")
    print("1. Database migrations are applied")
    print("2. GL accounts are properly created")
    print("3. Transactions are linked to GL accounts")
    print("4. Server logs for any errors")
    print("\nTo apply migrations:")
    print("   python manage.py makemigrations")
    print("   python manage.py migrate")

if __name__ == "__main__":
    main() 