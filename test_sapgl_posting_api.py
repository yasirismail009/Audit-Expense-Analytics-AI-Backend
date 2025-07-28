#!/usr/bin/env python3
import os
import django
import json

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'analytics.settings')
django.setup()

from core.models import DataFile, SAPGLPosting
from django.http import HttpRequest
from core.views import SAPGLPostingListView

def test_sapgl_posting_api():
    print("Testing SAPGLPosting Listing API")
    print("=" * 50)
    
    # Get the test file
    data_file = DataFile.objects.first()
    if not data_file:
        print("No data files found")
        return
    
    print(f"File: {data_file.file_name}")
    print(f"File ID: {data_file.id}")
    
    # Test the API endpoint
    print(f"\n🌐 Testing SAPGLPosting Listing API:")
    request = HttpRequest()
    request.method = 'GET'
    view = SAPGLPostingListView()
    
    try:
        # Test basic listing
        view.kwargs = {'file_id': str(data_file.id)}
        view.request = request
        response = view.get(request)
        
        if response.status_code == 200:
            response_data = response.data
            print(f"✅ API Response Status: {response.status_code}")
            
            # Check response structure
            print(f"\n📊 Response Structure:")
            print(f"  File ID: {response_data.get('file_id')}")
            print(f"  Total Count: {response_data.get('total_count')}")
            print(f"  Filters Applied: {response_data.get('filters_applied')}")
            
            # Check summary statistics
            summary = response_data.get('summary', {})
            print(f"\n📈 Summary Statistics:")
            print(f"  Total Transactions: {summary.get('total_transactions')}")
            print(f"  Total Amount: {summary.get('total_amount')}")
            print(f"  Average Amount: {summary.get('avg_amount')}")
            print(f"  Min Amount: {summary.get('min_amount')}")
            print(f"  Max Amount: {summary.get('max_amount')}")
            print(f"  Average Risk Score: {summary.get('avg_risk_score')}")
            print(f"  Duplicate Count: {summary.get('duplicate_count')}")
            print(f"  Backdated Count: {summary.get('backdated_count')}")
            print(f"  High Value Count: {summary.get('high_value_count')}")
            print(f"  Debit Count: {summary.get('debit_count')}")
            print(f"  Credit Count: {summary.get('credit_count')}")
            print(f"  Unique Users: {summary.get('unique_users')}")
            print(f"  Unique Accounts: {summary.get('unique_accounts')}")
            
            # Check transactions data
            transactions = response_data.get('transactions', [])
            print(f"\n📋 Transactions Data:")
            print(f"  Number of transactions returned: {len(transactions)}")
            
            if transactions:
                # Show first transaction as example
                first_transaction = transactions[0]
                print(f"  First transaction example:")
                print(f"    ID: {first_transaction.get('id')}")
                print(f"    Document Number: {first_transaction.get('document_number')}")
                print(f"    Amount: {first_transaction.get('amount_local_currency')}")
                print(f"    GL Account: {first_transaction.get('gl_account')}")
                print(f"    User: {first_transaction.get('user_name')}")
                print(f"    Posting Date: {first_transaction.get('posting_date')}")
                print(f"    Is High Value: {first_transaction.get('is_high_value')}")
            
        else:
            print(f"❌ API error: {response.status_code}")
            print(f"Response: {response.data}")
            
    except Exception as e:
        print(f"❌ API test error: {e}")
    
    # Test with filters
    print(f"\n🔍 Testing with Filters:")
    
    # Test date filter
    print(f"\n📅 Testing date filter:")
    request_with_date = HttpRequest()
    request_with_date.method = 'GET'
    request_with_date.GET = {'date_from': '2025-01-01', 'date_to': '2025-01-31'}
    
    try:
        view.kwargs = {'file_id': str(data_file.id)}
        view.request = request_with_date
        response = view.get(request_with_date)
        if response.status_code == 200:
            filtered_data = response.data
            print(f"  Date filtered transactions: {filtered_data.get('total_count')}")
            print(f"  Filters applied: {filtered_data.get('filters_applied')}")
        else:
            print(f"  ❌ Date filter error: {response.status_code}")
    except Exception as e:
        print(f"  ❌ Date filter test error: {e}")
    
    # Test amount filter
    print(f"\n💰 Testing amount filter:")
    request_with_amount = HttpRequest()
    request_with_amount.method = 'GET'
    request_with_amount.GET = {'min_amount': '1000000'}  # 1M SAR
    
    try:
        view.kwargs = {'file_id': str(data_file.id)}
        view.request = request_with_amount
        response = view.get(request_with_amount)
        if response.status_code == 200:
            filtered_data = response.data
            print(f"  High value transactions (>= 1M): {filtered_data.get('total_count')}")
            print(f"  Filters applied: {filtered_data.get('filters_applied')}")
        else:
            print(f"  ❌ Amount filter error: {response.status_code}")
    except Exception as e:
        print(f"  ❌ Amount filter test error: {e}")
    
    # Test duplicate filter
    print(f"\n🔄 Testing duplicate filter:")
    request_with_duplicate = HttpRequest()
    request_with_duplicate.method = 'GET'
    request_with_duplicate.GET = {'is_duplicate': 'true'}
    
    try:
        view.kwargs = {'file_id': str(data_file.id)}
        view.request = request_with_duplicate
        response = view.get(request_with_duplicate)
        if response.status_code == 200:
            filtered_data = response.data
            print(f"  Duplicate transactions: {filtered_data.get('total_count')}")
            print(f"  Filters applied: {filtered_data.get('filters_applied')}")
        else:
            print(f"  ❌ Duplicate filter error: {response.status_code}")
    except Exception as e:
        print(f"  ❌ Duplicate filter test error: {e}")

if __name__ == "__main__":
    test_sapgl_posting_api() 