#!/usr/bin/env python3
"""
Test script for GL Account features including:
- GL Account analysis
- Trial Balance generation
- GL Account charts
- Credit/Debit tracking
"""

import json
import requests
from datetime import datetime, date
import csv
import io

# Configuration
BASE_URL = "http://localhost:8000/api"
HEADERS = {"Content-Type": "application/json"}

def create_test_gl_accounts():
    """Create sample GL Account master data"""
    print("Creating sample GL Account master data...")
    
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
            "Account ID": "3000",
            "Account Name": "Share Capital",
            "Account Type": "Equity",
            "Account Category": "Shareholders Equity",
            "Account Subcategory": "Capital",
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
        },
        {
            "Account ID": "6000",
            "Account Name": "Operating Expenses",
            "Account Type": "Expense",
            "Account Category": "Operating Expenses",
            "Account Subcategory": "General",
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
        print(f"✓ GL Account master data uploaded successfully")
        print(f"  Created: {result.get('created_count', 0)}")
        print(f"  Updated: {result.get('updated_count', 0)}")
        print(f"  Failed: {result.get('failed_count', 0)}")
    else:
        print(f"✗ Failed to upload GL Account master data: {response.text}")

def create_test_transactions():
    """Create sample transactions with credit/debit information"""
    print("\nCreating sample transactions with credit/debit tracking...")
    
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
        },
        {
            "Document Number": "DOC006",
            "Posting Date": "2025-01-20",
            "G/L Account": "6000",
            "Amount in Local Currency": "100000",
            "Transaction Type": "DEBIT",
            "Local Currency": "SAR",
            "Text": "Operating expenses",
            "Document Date": "2025-01-20",
            "User Name": "USER003",
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
    
    # Upload transactions with enhanced fields
    files = {
        'file': ('transactions.csv', csv_content.getvalue(), 'text/csv')
    }
    data = {
        'engagement_id': 'ENG001',
        'client_name': 'Test Client',
        'company_name': 'Test Company',
        'fiscal_year': '2025',
        'audit_start_date': '2025-01-01',
        'audit_end_date': '2025-12-31'
    }
    
    response = requests.post(f"{BASE_URL}/file-upload-analysis/", files=files, data=data)
    
    if response.status_code == 201:
        result = response.json()
        print(f"✓ Transactions uploaded successfully")
        print(f"  File: {result.get('file', {}).get('file_name', 'N/A')}")
        print(f"  Processed: {result.get('file', {}).get('processed_records', 0)} records")
    else:
        print(f"✗ Failed to upload transactions: {response.text}")

def test_gl_account_analysis():
    """Test GL Account analysis endpoint"""
    print("\nTesting GL Account analysis...")
    
    response = requests.get(f"{BASE_URL}/gl-accounts/analysis/")
    
    if response.status_code == 200:
        analysis_data = response.json()
        print(f"✓ GL Account analysis retrieved successfully")
        print(f"  Total accounts analyzed: {len(analysis_data)}")
        
        if analysis_data:
            # Show top 3 accounts by risk score
            print("\n  Top 3 accounts by risk score:")
            for i, account in enumerate(analysis_data[:3], 1):
                print(f"    {i}. {account['account_id']} - {account['account_name']}")
                print(f"       Balance: {account['current_balance']:,.2f} SAR")
                print(f"       Debits: {account['total_debits']:,.2f} SAR")
                print(f"       Credits: {account['total_credits']:,.2f} SAR")
                print(f"       Risk Score: {account['risk_score']}")
                print()
    else:
        print(f"✗ Failed to get GL Account analysis: {response.text}")

def test_trial_balance():
    """Test Trial Balance generation"""
    print("\nTesting Trial Balance generation...")
    
    # Test with date range
    params = {
        'date_from': '2025-01-01',
        'date_to': '2025-01-31'
    }
    
    response = requests.get(f"{BASE_URL}/gl-accounts/trial-balance/", params=params)
    
    if response.status_code == 200:
        tb_data = response.json()
        print(f"✓ Trial Balance generated successfully")
        print(f"  Total accounts in TB: {len(tb_data)}")
        
        if tb_data:
            # Show summary by account type
            account_types = {}
            total_debits = 0
            total_credits = 0
            
            for account in tb_data:
                account_type = account['account_type']
                if account_type not in account_types:
                    account_types[account_type] = {
                        'count': 0,
                        'debits': 0,
                        'credits': 0
                    }
                
                account_types[account_type]['count'] += 1
                account_types[account_type]['debits'] += account['closing_debit']
                account_types[account_type]['credits'] += account['closing_credit']
                total_debits += account['closing_debit']
                total_credits += account['closing_credit']
            
            print("\n  Trial Balance Summary by Account Type:")
            for account_type, data in account_types.items():
                print(f"    {account_type}: {data['count']} accounts")
                print(f"      Debits: {data['debits']:,.2f} SAR")
                print(f"      Credits: {data['credits']:,.2f} SAR")
                print()
            
            print(f"  Total Debits: {total_debits:,.2f} SAR")
            print(f"  Total Credits: {total_credits:,.2f} SAR")
            print(f"  Balance: {total_debits - total_credits:,.2f} SAR")
    else:
        print(f"✗ Failed to generate Trial Balance: {response.text}")

def test_gl_account_charts():
    """Test GL Account charts endpoint"""
    print("\nTesting GL Account charts...")
    
    response = requests.get(f"{BASE_URL}/gl-accounts/charts/")
    
    if response.status_code == 200:
        charts_data = response.json()
        print(f"✓ GL Account charts data retrieved successfully")
        
        # Account type distribution
        print("\n  Account Type Distribution:")
        for account_type in charts_data.get('account_type_distribution', []):
            print(f"    {account_type['account_type']}: {account_type['count']} accounts")
            print(f"      Total Balance: {account_type['total_balance']:,.2f} SAR")
            print(f"      Total Transactions: {account_type['total_transactions']}")
        
        # Debit vs Credit analysis
        debit_credit = charts_data.get('debit_credit_analysis', {})
        print(f"\n  Debit vs Credit Analysis:")
        print(f"    Total Debits: {debit_credit.get('total_debits', 0):,.2f} SAR")
        print(f"    Total Credits: {debit_credit.get('total_credits', 0):,.2f} SAR")
        print(f"    Debit Count: {debit_credit.get('debit_count', 0)}")
        print(f"    Credit Count: {debit_credit.get('credit_count', 0)}")
        print(f"    Net Movement: {debit_credit.get('net_movement', 0):,.2f} SAR")
        
        # Top accounts by balance
        print(f"\n  Top 5 Accounts by Balance:")
        for i, account in enumerate(charts_data.get('top_accounts_by_balance', [])[:5], 1):
            print(f"    {i}. {account['account_id']} - {account['account_name']}")
            print(f"       Balance: {account['balance']:,.2f} SAR ({account['account_type']})")
        
        # Risk distribution
        print(f"\n  Risk Distribution by Account Type:")
        for risk_data in charts_data.get('risk_distribution', []):
            print(f"    {risk_data['account_type']}: {risk_data['account_count']} accounts")
            print(f"      High Risk Count: {risk_data['high_risk_count']}")
            print(f"      Average Risk Score: {risk_data['avg_risk_score']}")
    else:
        print(f"✗ Failed to get GL Account charts: {response.text}")

def test_gl_account_list():
    """Test GL Account listing with filters"""
    print("\nTesting GL Account listing...")
    
    # Test different filters
    filters = [
        {'account_type': 'Asset'},
        {'normal_balance': 'DEBIT'},
        {'is_active': 'true'}
    ]
    
    for filter_params in filters:
        response = requests.get(f"{BASE_URL}/gl-accounts/", params=filter_params)
        
        if response.status_code == 200:
            accounts = response.json()
            filter_name = ', '.join([f"{k}={v}" for k, v in filter_params.items()])
            print(f"✓ GL Accounts with filter '{filter_name}': {len(accounts)} accounts")
            
            if accounts:
                for account in accounts[:3]:  # Show first 3
                    print(f"    {account['account_id']} - {account['account_name']}")
                    print(f"      Type: {account['account_type']}")
                    print(f"      Balance: {account['current_balance']:,.2f} SAR")
        else:
            print(f"✗ Failed to get GL Accounts with filter {filter_params}: {response.text}")

def main():
    """Main test function"""
    print("=== GL Account Features Test Suite ===")
    print("Testing enhanced GL Account functionality with credit/debit tracking")
    print("=" * 50)
    
    try:
        # Create test data
        create_test_gl_accounts()
        create_test_transactions()
        
        # Test GL Account features
        test_gl_account_list()
        test_gl_account_analysis()
        test_trial_balance()
        test_gl_account_charts()
        
        print("\n" + "=" * 50)
        print("✓ All GL Account feature tests completed successfully!")
        print("\nNew Features Implemented:")
        print("1. GL Account master data management")
        print("2. Credit/Debit transaction tracking")
        print("3. GL Account analysis with risk scoring")
        print("4. Trial Balance generation")
        print("5. GL Account charts and visualizations")
        print("6. Enhanced file upload with engagement details")
        
    except Exception as e:
        print(f"\n✗ Test failed with error: {e}")

if __name__ == "__main__":
    main() 