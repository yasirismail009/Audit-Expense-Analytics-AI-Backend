#!/usr/bin/env python3
"""
Debug script to investigate GL account field issues in duplicate detection
"""

import os
import sys
import django
from decimal import Decimal

# Add the project directory to Python path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# Setup Django
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'analytics.settings')
django.setup()

from core.models import SAPGLPosting, GLAccount
from core.analytics import SAPGLAnalyzer

def debug_gl_account_issue():
    """Debug why GL account fields are empty in duplicate detection"""
    
    print("=== GL Account Field Debug ===\n")
    
    # 1. Check if we have any transactions
    transactions = SAPGLPosting.objects.all()
    print(f"1. Total transactions in database: {transactions.count()}")
    
    if not transactions.exists():
        print("   ✗ No transactions found!")
        return
    
    # 2. Check GL account field values
    print("\n2. Checking GL account field values:")
    
    # Sample some transactions
    sample_transactions = transactions[:10]
    for i, tx in enumerate(sample_transactions):
        print(f"   Transaction {i+1}:")
        print(f"     ID: {tx.id}")
        print(f"     GL Account: '{tx.gl_account}' (type: {type(tx.gl_account)})")
        print(f"     Amount: {tx.amount_local_currency}")
        print(f"     Document: {tx.document_number}")
        print(f"     User: {tx.user_name}")
        print()
    
    # 3. Check for empty GL accounts
    empty_gl_accounts = transactions.filter(gl_account__isnull=True) | transactions.filter(gl_account='')
    print(f"3. Transactions with empty GL accounts: {empty_gl_accounts.count()}")
    
    if empty_gl_accounts.exists():
        print("   Sample empty GL account transactions:")
        for i, tx in enumerate(empty_gl_accounts[:5]):
            print(f"     {i+1}. ID: {tx.id}, Amount: {tx.amount_local_currency}, Document: {tx.document_number}")
    
    # 4. Check unique GL accounts
    unique_gl_accounts = transactions.exclude(gl_account__isnull=True).exclude(gl_account='').values_list('gl_account', flat=True).distinct()
    print(f"\n4. Unique GL accounts in transactions: {len(unique_gl_accounts)}")
    print(f"   Sample GL accounts: {list(unique_gl_accounts[:10])}")
    
    # 5. Check GL Account master data
    gl_accounts = GLAccount.objects.all()
    print(f"\n5. GL Account master data: {gl_accounts.count()} accounts")
    
    if gl_accounts.exists():
        print("   Sample GL accounts:")
        for i, acc in enumerate(gl_accounts[:5]):
            print(f"     {i+1}. {acc.account_id} - {acc.account_name}")
    
    # 6. Test duplicate detection with sample data
    print("\n6. Testing duplicate detection:")
    
    # Get transactions with non-empty GL accounts
    valid_transactions = transactions.exclude(gl_account__isnull=True).exclude(gl_account='')
    print(f"   Valid transactions (with GL accounts): {valid_transactions.count()}")
    
    if valid_transactions.exists():
        # Test with a small sample
        sample_size = min(100, valid_transactions.count())
        test_transactions = list(valid_transactions[:sample_size])
        
        analyzer = SAPGLAnalyzer()
        duplicates = analyzer.detect_duplicate_entries(test_transactions)
        
        print(f"   Found {len(duplicates)} duplicate groups")
        
        for i, dup in enumerate(duplicates[:3]):  # Show first 3
            print(f"   Duplicate {i+1}:")
            print(f"     Type: {dup['type']}")
            print(f"     GL Account: '{dup['gl_account']}'")
            print(f"     Amount: {dup['amount']}")
            print(f"     Count: {dup['count']}")
            
            # Check transaction details
            for j, tx in enumerate(dup['transactions'][:2]):  # Show first 2 transactions
                print(f"     Transaction {j+1}: GL Account = '{tx['gl_account']}'")
            print()
    else:
        print("   ✗ No valid transactions with GL accounts found!")
    
    # 7. Check data consistency
    print("\n7. Data consistency check:")
    
    # Check if GL accounts in transactions match master data
    transaction_gl_accounts = set(unique_gl_accounts)
    master_gl_accounts = set(GLAccount.objects.values_list('account_id', flat=True))
    
    missing_in_master = transaction_gl_accounts - master_gl_accounts
    unused_in_master = master_gl_accounts - transaction_gl_accounts
    
    print(f"   GL accounts in transactions but not in master: {len(missing_in_master)}")
    if missing_in_master:
        print(f"     Sample: {list(missing_in_master)[:5]}")
    
    print(f"   GL accounts in master but not in transactions: {len(unused_in_master)}")
    if unused_in_master:
        print(f"     Sample: {list(unused_in_master)[:5]}")
    
    common_accounts = transaction_gl_accounts & master_gl_accounts
    print(f"   Common GL accounts: {len(common_accounts)}")

if __name__ == "__main__":
    debug_gl_account_issue() 