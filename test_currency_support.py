#!/usr/bin/env python3
"""
Test script to verify currency support in FileSummaryView
"""

import os
import sys
import django
from decimal import Decimal
from datetime import date

# Add the project directory to the Python path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# Setup Django
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'analytics.settings')
django.setup()

from core.models import DataFile, SAPGLPosting, GLAccount
from core.views import FileSummaryView
from django.test import RequestFactory
from django.contrib.auth.models import User
import json

def test_currency_support():
    """Test that currency information is properly included in FileSummaryView"""
    
    print("Testing currency support in FileSummaryView...")
    
    # Create test data
    print("Creating test data...")
    
    # Create a test user
    user, created = User.objects.get_or_create(
        username='testuser',
        defaults={'email': 'test@example.com'}
    )
    
    # Create test GL account
    gl_account, created = GLAccount.objects.get_or_create(
        account_id='100000',
        defaults={
            'account_name': 'Test Cash Account',
            'account_type': 'Asset',
            'account_category': 'Cash',
            'normal_balance': 'DEBIT'
        }
    )
    
    # Create test data file
    data_file = DataFile.objects.create(
        file_name='test_currency.csv',
        file_size=1024,
        engagement_id='TEST001',
        client_name='Test Client',
        company_name='Test Company',
        fiscal_year=2024,
        audit_start_date=date(2024, 1, 1),
        audit_end_date=date(2024, 12, 31),
        total_records=3,
        processed_records=3,
        failed_records=0,
        status='COMPLETED'
    )
    
    # Create test transactions with different currencies
    transactions_data = [
        {
            'document_number': '1000000001',
            'posting_date': date(2024, 1, 15),
            'gl_account': '100000',
            'amount_local_currency': Decimal('50000.00'),
            'local_currency': 'SAR',
            'transaction_type': 'DEBIT',
            'user_name': 'USER001',
            'fiscal_year': 2024,
            'posting_period': 1
        },
        {
            'document_number': '1000000002',
            'posting_date': date(2024, 1, 16),
            'gl_account': '100000',
            'amount_local_currency': Decimal('75000.00'),
            'local_currency': 'SAR',
            'transaction_type': 'CREDIT',
            'user_name': 'USER002',
            'fiscal_year': 2024,
            'posting_period': 1
        },
        {
            'document_number': '1000000003',
            'posting_date': date(2024, 1, 17),
            'gl_account': '100000',
            'amount_local_currency': Decimal('120000.00'),
            'local_currency': 'SAR',
            'transaction_type': 'DEBIT',
            'user_name': 'USER001',
            'fiscal_year': 2024,
            'posting_period': 1
        }
    ]
    
    # Create transactions
    for trans_data in transactions_data:
        SAPGLPosting.objects.create(**trans_data)
    
    print(f"Created {len(transactions_data)} test transactions")
    
    # Test FileSummaryView
    print("Testing FileSummaryView...")
    
    # Create request factory
    factory = RequestFactory()
    request = factory.get(f'/api/file-summary/{data_file.id}/')
    request.user = user
    
    # Create view instance
    view = FileSummaryView()
    view.request = request
    
    # Get response
    response = view.retrieve(request, pk=data_file.id)
    
    # Check response status
    if response.status_code == 200:
        print("✓ FileSummaryView returned 200 OK")
        
        # Get response data
        response_data = response.data
        
        # Check file info
        file_info = response_data.get('file_info', {})
        if 'currency' in file_info:
            print(f"✓ Currency found in file_info: {file_info['currency']}")
        else:
            print("✗ Currency missing from file_info")
        
        # Check summary statistics
        summary_stats = response_data.get('summary_statistics', {})
        if 'currency' in summary_stats:
            print(f"✓ Currency found in summary_statistics: {summary_stats['currency']}")
        else:
            print("✗ Currency missing from summary_statistics")
        
        # Check charts data
        charts_data = response_data.get('charts_data', {})
        
        # Check top users by amount
        top_users = charts_data.get('top_users_by_amount', [])
        if top_users and 'currency' in top_users[0]:
            print(f"✓ Currency found in top_users_by_amount: {top_users[0]['currency']}")
        else:
            print("✗ Currency missing from top_users_by_amount")
        
        # Check top accounts by transactions
        top_accounts = charts_data.get('top_accounts_by_transactions', [])
        if top_accounts and 'currency' in top_accounts[0]:
            print(f"✓ Currency found in top_accounts_by_transactions: {top_accounts[0]['currency']}")
        else:
            print("✗ Currency missing from top_accounts_by_transactions")
        
        # Check monthly transaction volume
        monthly_volume = charts_data.get('monthly_transaction_volume', [])
        if monthly_volume and 'currency' in monthly_volume[0]:
            print(f"✓ Currency found in monthly_transaction_volume: {monthly_volume[0]['currency']}")
        else:
            print("✗ Currency missing from monthly_transaction_volume")
        
        # Check GL account data
        gl_account_data = response_data.get('gl_account_data', [])
        if gl_account_data and 'currency' in gl_account_data[0]:
            print(f"✓ Currency found in gl_account_data: {gl_account_data[0]['currency']}")
        else:
            print("✗ Currency missing from gl_account_data")
        
        # Check GL charts data
        gl_charts_data = response_data.get('gl_charts_data', {})
        if 'currency' in gl_charts_data:
            print(f"✓ Currency found in gl_charts_data: {gl_charts_data['currency']}")
        else:
            print("✗ Currency missing from gl_charts_data")
        
        # Print sample response structure
        print("\nSample response structure:")
        print(json.dumps({
            'file_info': {
                'file_name': file_info.get('file_name'),
                'status': file_info.get('status'),
                'currency': file_info.get('currency')
            },
            'summary_statistics': {
                'total_transactions': summary_stats.get('total_transactions'),
                'total_amount': summary_stats.get('total_amount'),
                'currency': summary_stats.get('currency')
            },
            'charts_data': {
                'top_users_by_amount': top_users[:1] if top_users else [],
                'top_accounts_by_transactions': top_accounts[:1] if top_accounts else [],
                'monthly_transaction_volume': monthly_volume[:1] if monthly_volume else []
            }
        }, indent=2, default=str))
        
    else:
        print(f"✗ FileSummaryView returned status code: {response.status_code}")
        print(f"Response: {response.data}")
    
    # Cleanup test data
    print("\nCleaning up test data...")
    SAPGLPosting.objects.filter(document_number__in=['1000000001', '1000000002', '1000000003']).delete()
    data_file.delete()
    gl_account.delete()
    user.delete()
    
    print("Test completed!")

if __name__ == '__main__':
    test_currency_support() 