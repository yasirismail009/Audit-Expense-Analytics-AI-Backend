#!/usr/bin/env python3
"""
Test script to verify duplicate detection fix and show actual GL account values
"""

import os
import sys
import django

# Add the project directory to Python path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# Setup Django
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'analytics.settings')
django.setup()

from core.models import SAPGLPosting, AnalysisSession
from core.analytics import SAPGLAnalyzer

# BASE_URL = "http://localhost:8000/api"  # Not used without requests

def test_duplicate_detection():
    """Test duplicate detection and show actual GL account values"""
    
    print("=== Duplicate Detection Test ===\n")
    
    # 1. Get all transactions
    transactions = SAPGLPosting.objects.all()
    print(f"1. Total transactions: {transactions.count()}")
    
    if not transactions.exists():
        print("   ✗ No transactions found!")
        return
    
    # 2. Show sample transactions with GL accounts
    print("\n2. Sample transactions with GL accounts:")
    sample_transactions = transactions[:5]
    for i, tx in enumerate(sample_transactions):
        print(f"   Transaction {i+1}:")
        print(f"     ID: {tx.id}")
        print(f"     GL Account: '{tx.gl_account}'")
        print(f"     Amount: {tx.amount_local_currency}")
        print(f"     Document: {tx.document_number}")
        print(f"     User: {tx.user_name}")
        print()
    
    # 3. Test duplicate detection directly
    print("3. Testing duplicate detection:")
    analyzer = SAPGLAnalyzer()
    duplicates = analyzer.detect_duplicate_entries(list(transactions))
    
    print(f"   Found {len(duplicates)} duplicate groups")
    
    for i, dup in enumerate(duplicates[:3]):  # Show first 3
        print(f"   Duplicate {i+1}:")
        print(f"     Type: {dup['type']}")
        print(f"     Criteria: {dup['criteria']}")
        print(f"     GL Account: '{dup['gl_account']}'")
        print(f"     Amount: {dup['amount']}")
        print(f"     Count: {dup['count']}")
        print(f"     Risk Score: {dup['risk_score']}")
        
        # Show transaction details
        print(f"     Transactions:")
        for j, tx in enumerate(dup['transactions'][:2]):  # Show first 2
            print(f"       {j+1}. ID: {tx['id']}")
            print(f"          GL Account: '{tx['gl_account']}'")
            print(f"          Amount: {tx['amount']}")
            print(f"          User: {tx['user_name']}")
            print(f"          Document: {tx['document_number']}")
        print()
    
    # 4. Test via API if server is running
    print("4. Testing via API:")
    print("   ⚠️ Skipping API test (requests not available)")
    print("   Run the server and test manually with:")
    print("   curl http://localhost:8000/api/sessions/{session_id}/summary/")
    
    # 5. Show GL account distribution
    print("5. GL Account distribution:")
    gl_accounts = transactions.values('gl_account').distinct()
    print(f"   Unique GL accounts: {gl_accounts.count()}")
    
    for gl_acc in gl_accounts[:5]:  # Show first 5
        account_id = gl_acc['gl_account']
        count = transactions.filter(gl_account=account_id).count()
        print(f"     '{account_id}': {count} transactions")
    
    # 6. Check for empty GL accounts
    empty_gl_count = transactions.filter(gl_account='').count()
    if empty_gl_count > 0:
        print(f"   ⚠️ {empty_gl_count} transactions have empty GL accounts")
    else:
        print("   ✓ All transactions have GL accounts")

if __name__ == "__main__":
    test_duplicate_detection() 