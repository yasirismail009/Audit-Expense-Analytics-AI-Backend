#!/usr/bin/env python3
"""
Test script to verify duplicate type priority logic
Ensures each transaction only appears in the highest priority duplicate type
"""

import os
import sys
import django
from datetime import datetime, date
import pandas as pd

# Setup Django
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'analytics.settings')
django.setup()

from core.models import SAPGLPosting, DataFile
from core.enhanced_duplicate_analysis import EnhancedDuplicateAnalyzer

def create_test_data():
    """Create test data with known duplicate patterns"""
    print("🔧 Creating test data...")
    
    # Create test transactions that should trigger different duplicate types
    # Add unique IDs to each transaction
    test_transactions = [
        # Type 6 Duplicate: Account + Effective Date + Posted Date + User + Source + Amount
        {
            'id': 'TXN001',
            'gl_account': '1000',
            'amount': 5000.00,
            'user_name': 'John Doe',
            'document_number': 'DOC001',
            'posting_date': date(2024, 1, 15),
            'document_date': date(2024, 1, 14),
            'document_type': 'Invoice',
            'source': 'SAP',
            'text': 'Test transaction 1',
            'transaction_type': 'DEBIT'
        },
        {
            'gl_account': '1000',
            'amount': 5000.00,
            'user_name': 'John Doe',
            'document_number': 'DOC002',
            'posting_date': date(2024, 1, 15),
            'document_date': date(2024, 1, 14),
            'document_type': 'Invoice',
            'source': 'SAP',
            'text': 'Test transaction 2',
            'transaction_type': 'DEBIT'
        },
        {
            'gl_account': '1000',
            'amount': 5000.00,
            'user_name': 'John Doe',
            'document_number': 'DOC003',
            'posting_date': date(2024, 1, 15),
            'document_date': date(2024, 1, 14),
            'document_type': 'Invoice',
            'source': 'SAP',
            'text': 'Test transaction 3',
            'transaction_type': 'DEBIT'
        },
        
        # Type 5 Duplicate: Account + Amount + User + Date + Document Type + Source
        {
            'gl_account': '2000',
            'amount': 3000.00,
            'user_name': 'Jane Smith',
            'document_number': 'DOC004',
            'posting_date': date(2024, 1, 16),
            'document_date': date(2024, 1, 16),
            'document_type': 'Invoice',
            'source': 'SAP',
            'text': 'Test transaction 4',
            'transaction_type': 'CREDIT'
        },
        {
            'gl_account': '2000',
            'amount': 3000.00,
            'user_name': 'Jane Smith',
            'document_number': 'DOC005',
            'posting_date': date(2024, 1, 16),
            'document_date': date(2024, 1, 16),
            'document_type': 'Invoice',
            'source': 'SAP',
            'text': 'Test transaction 5',
            'transaction_type': 'CREDIT'
        },
        {
            'gl_account': '2000',
            'amount': 3000.00,
            'user_name': 'Jane Smith',
            'document_number': 'DOC006',
            'posting_date': date(2024, 1, 16),
            'document_date': date(2024, 1, 16),
            'document_type': 'Invoice',
            'source': 'SAP',
            'text': 'Test transaction 6',
            'transaction_type': 'CREDIT'
        },
        
        # Type 4 Duplicate: Account + Amount + User + Date + Document Type
        {
            'gl_account': '3000',
            'amount': 2000.00,
            'user_name': 'Bob Wilson',
            'document_number': 'DOC007',
            'posting_date': date(2024, 1, 17),
            'document_date': date(2024, 1, 17),
            'document_type': 'Invoice',
            'source': 'SAP',
            'text': 'Test transaction 7',
            'transaction_type': 'DEBIT'
        },
        {
            'gl_account': '3000',
            'amount': 2000.00,
            'user_name': 'Bob Wilson',
            'document_number': 'DOC008',
            'posting_date': date(2024, 1, 17),
            'document_date': date(2024, 1, 17),
            'document_type': 'Invoice',
            'source': 'SAP',
            'text': 'Test transaction 8',
            'transaction_type': 'DEBIT'
        },
        {
            'gl_account': '3000',
            'amount': 2000.00,
            'user_name': 'Bob Wilson',
            'document_number': 'DOC009',
            'posting_date': date(2024, 1, 17),
            'document_date': date(2024, 1, 17),
            'document_type': 'Invoice',
            'source': 'SAP',
            'text': 'Test transaction 9',
            'transaction_type': 'DEBIT'
        },
        
        # Type 3 Duplicate: Account + Amount + User + Date
        {
            'gl_account': '4000',
            'amount': 1500.00,
            'user_name': 'Alice Brown',
            'document_number': 'DOC010',
            'posting_date': date(2024, 1, 18),
            'document_date': date(2024, 1, 18),
            'document_type': 'Invoice',
            'source': 'SAP',
            'text': 'Test transaction 10',
            'transaction_type': 'CREDIT'
        },
        {
            'gl_account': '4000',
            'amount': 1500.00,
            'user_name': 'Alice Brown',
            'document_number': 'DOC011',
            'posting_date': date(2024, 1, 18),
            'document_date': date(2024, 1, 18),
            'document_type': 'Invoice',
            'source': 'SAP',
            'text': 'Test transaction 11',
            'transaction_type': 'CREDIT'
        },
        {
            'gl_account': '4000',
            'amount': 1500.00,
            'user_name': 'Alice Brown',
            'document_number': 'DOC012',
            'posting_date': date(2024, 1, 18),
            'document_date': date(2024, 1, 18),
            'document_type': 'Invoice',
            'source': 'SAP',
            'text': 'Test transaction 12',
            'transaction_type': 'CREDIT'
        },
        
        # Type 2 Duplicate: Account + Amount + User
        {
            'gl_account': '5000',
            'amount': 1000.00,
            'user_name': 'Charlie Davis',
            'document_number': 'DOC013',
            'posting_date': date(2024, 1, 19),
            'document_date': date(2024, 1, 19),
            'document_type': 'Invoice',
            'source': 'SAP',
            'text': 'Test transaction 13',
            'transaction_type': 'DEBIT'
        },
        {
            'gl_account': '5000',
            'amount': 1000.00,
            'user_name': 'Charlie Davis',
            'document_number': 'DOC014',
            'posting_date': date(2024, 1, 20),
            'document_date': date(2024, 1, 20),
            'document_type': 'Invoice',
            'source': 'SAP',
            'text': 'Test transaction 14',
            'transaction_type': 'DEBIT'
        },
        {
            'gl_account': '5000',
            'amount': 1000.00,
            'user_name': 'Charlie Davis',
            'document_number': 'DOC015',
            'posting_date': date(2024, 1, 21),
            'document_date': date(2024, 1, 21),
            'document_type': 'Invoice',
            'source': 'SAP',
            'text': 'Test transaction 15',
            'transaction_type': 'DEBIT'
        },
        
        # Type 1 Duplicate: Account + Amount
        {
            'gl_account': '6000',
            'amount': 500.00,
            'user_name': 'David Lee',
            'document_number': 'DOC016',
            'posting_date': date(2024, 1, 22),
            'document_date': date(2024, 1, 22),
            'document_type': 'Invoice',
            'source': 'SAP',
            'text': 'Test transaction 16',
            'transaction_type': 'CREDIT'
        },
        {
            'gl_account': '6000',
            'amount': 500.00,
            'user_name': 'Eve Johnson',
            'document_number': 'DOC017',
            'posting_date': date(2024, 1, 23),
            'document_date': date(2024, 1, 23),
            'document_type': 'Invoice',
            'source': 'SAP',
            'text': 'Test transaction 17',
            'transaction_type': 'CREDIT'
        },
        {
            'gl_account': '6000',
            'amount': 500.00,
            'user_name': 'Frank Miller',
            'document_number': 'DOC018',
            'posting_date': date(2024, 1, 24),
            'document_date': date(2024, 1, 24),
            'document_type': 'Invoice',
            'source': 'SAP',
            'text': 'Test transaction 18',
            'transaction_type': 'CREDIT'
        }
    ]
    
    # Add unique IDs to all transactions
    for i, transaction in enumerate(test_transactions, 1):
        transaction['id'] = f'TXN{i:03d}'
    
    return test_transactions

def test_duplicate_type_priority():
    """Test that each transaction only appears in the highest priority duplicate type"""
    print("🔍 Testing Duplicate Type Priority Logic")
    print("=" * 60)
    print(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Create test data
    test_transactions = create_test_data()
    print(f"✅ Created {len(test_transactions)} test transactions")
    
    # Initialize the enhanced duplicate analyzer
    analyzer = EnhancedDuplicateAnalyzer()
    analyzer.duplicate_threshold = 2  # Minimum 2 transactions to form a duplicate
    
    # Run duplicate analysis
    print("\n🔍 Running duplicate analysis...")
    analysis_result = analyzer.analyze_duplicates(test_transactions)
    
    # Get duplicate groups
    duplicate_groups = analysis_result.get('duplicate_list', [])
    print(f"✅ Found {len(duplicate_groups)} duplicate entries")
    
    # Analyze results
    print("\n📊 Duplicate Type Analysis:")
    print("-" * 40)
    
    # Group by duplicate type
    type_analysis = {}
    transaction_ids_by_type = {}
    
    for entry in duplicate_groups:
        dup_type = entry['duplicate_type']
        transaction_id = entry['transaction_id']
        
        if dup_type not in type_analysis:
            type_analysis[dup_type] = {
                'count': 0,
                'transactions': set(),
                'total_amount': 0.0
            }
            transaction_ids_by_type[dup_type] = set()
        
        type_analysis[dup_type]['count'] += 1
        type_analysis[dup_type]['transactions'].add(transaction_id)
        type_analysis[dup_type]['total_amount'] += entry['amount']
        transaction_ids_by_type[dup_type].add(transaction_id)
    
    # Display results by type (Type 6 to Type 1)
    expected_types = [
        'Type 6 Duplicate',
        'Type 5 Duplicate', 
        'Type 4 Duplicate',
        'Type 3 Duplicate',
        'Type 2 Duplicate',
        'Type 1 Duplicate'
    ]
    
    all_processed_transactions = set()
    
    for expected_type in expected_types:
        if expected_type in type_analysis:
            data = type_analysis[expected_type]
            print(f"✅ {expected_type}:")
            print(f"   - Transactions: {len(data['transactions'])}")
            print(f"   - Total Amount: ${data['total_amount']:,.2f}")
            print(f"   - Transaction IDs: {sorted(data['transactions'])}")
            
            # Check for overlap with previously processed transactions
            overlap = data['transactions'] & all_processed_transactions
            if overlap:
                print(f"   ❌ OVERLAP DETECTED: {overlap}")
            else:
                print(f"   ✅ No overlap with previous types")
            
            all_processed_transactions.update(data['transactions'])
        else:
            print(f"❌ {expected_type}: Not found")
    
    # Final validation
    print(f"\n🎯 Final Validation:")
    print("-" * 30)
    
    total_unique_transactions = len(all_processed_transactions)
    total_duplicate_entries = len(duplicate_groups)
    
    print(f"Total unique transactions in duplicates: {total_unique_transactions}")
    print(f"Total duplicate entries: {total_duplicate_entries}")
    
    if total_unique_transactions == total_duplicate_entries:
        print("✅ SUCCESS: Each transaction appears only once (no duplicates across types)")
    else:
        print("❌ FAILED: Some transactions appear in multiple duplicate types")
        print(f"   Expected: {total_unique_transactions} entries")
        print(f"   Actual: {total_duplicate_entries} entries")
        print(f"   Difference: {total_duplicate_entries - total_unique_transactions} duplicate entries")
    
    # Check for any transaction appearing in multiple types
    transaction_count = {}
    for entry in duplicate_groups:
        transaction_id = entry['transaction_id']
        if transaction_id not in transaction_count:
            transaction_count[transaction_id] = []
        transaction_count[transaction_id].append(entry['duplicate_type'])
    
    duplicates_found = False
    for transaction_id, types in transaction_count.items():
        if len(types) > 1:
            print(f"❌ Transaction {transaction_id} appears in multiple types: {types}")
            duplicates_found = True
    
    if not duplicates_found:
        print("✅ SUCCESS: No transaction appears in multiple duplicate types")
    
    return analysis_result

def main():
    """Main test function"""
    try:
        result = test_duplicate_type_priority()
        print("\n✅ Test completed successfully!")
        
        # Save results for inspection
        import json
        with open('duplicate_type_priority_test_results.json', 'w') as f:
            json.dump(result, f, indent=2, default=str)
        print("📄 Results saved to duplicate_type_priority_test_results.json")
        
    except Exception as e:
        print(f"❌ Error during test: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main() 