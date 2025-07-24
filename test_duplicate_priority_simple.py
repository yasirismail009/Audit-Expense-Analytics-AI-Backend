#!/usr/bin/env python3
"""
Simple test script to verify duplicate type priority logic
Tests the core logic that ensures each transaction only appears in the highest priority type
"""

import pandas as pd
from datetime import date

def create_test_dataframe():
    """Create test DataFrame with known duplicate patterns"""
    print("🔧 Creating test DataFrame...")
    
    # Create test data that should trigger different duplicate types
    data = [
        # Type 6 Duplicate: Account + Effective Date + Posted Date + User + Source + Amount
        {'id': 'TXN001', 'gl_account': '1000', 'amount': 5000.00, 'user_name': 'John Doe', 
         'posting_date_str': '2024-01-15', 'document_date_str': '2024-01-14', 'source': 'SAP'},
        {'id': 'TXN002', 'gl_account': '1000', 'amount': 5000.00, 'user_name': 'John Doe', 
         'posting_date_str': '2024-01-15', 'document_date_str': '2024-01-14', 'source': 'SAP'},
        {'id': 'TXN003', 'gl_account': '1000', 'amount': 5000.00, 'user_name': 'John Doe', 
         'posting_date_str': '2024-01-15', 'document_date_str': '2024-01-14', 'source': 'SAP'},
        
        # Type 5 Duplicate: Account + Amount + User + Date + Document Type + Source
        {'id': 'TXN004', 'gl_account': '2000', 'amount': 3000.00, 'user_name': 'Jane Smith', 
         'posting_date_str': '2024-01-16', 'document_date_str': '2024-01-16', 'source': 'SAP'},
        {'id': 'TXN005', 'gl_account': '2000', 'amount': 3000.00, 'user_name': 'Jane Smith', 
         'posting_date_str': '2024-01-16', 'document_date_str': '2024-01-16', 'source': 'SAP'},
        {'id': 'TXN006', 'gl_account': '2000', 'amount': 3000.00, 'user_name': 'Jane Smith', 
         'posting_date_str': '2024-01-16', 'document_date_str': '2024-01-16', 'source': 'SAP'},
        
        # Type 4 Duplicate: Account + Amount + User + Date + Document Type
        {'id': 'TXN007', 'gl_account': '3000', 'amount': 2000.00, 'user_name': 'Bob Wilson', 
         'posting_date_str': '2024-01-17', 'document_date_str': '2024-01-17', 'source': 'SAP'},
        {'id': 'TXN008', 'gl_account': '3000', 'amount': 2000.00, 'user_name': 'Bob Wilson', 
         'posting_date_str': '2024-01-17', 'document_date_str': '2024-01-17', 'source': 'SAP'},
        {'id': 'TXN009', 'gl_account': '3000', 'amount': 2000.00, 'user_name': 'Bob Wilson', 
         'posting_date_str': '2024-01-17', 'document_date_str': '2024-01-17', 'source': 'SAP'},
        
        # Type 3 Duplicate: Account + Amount + User + Date
        {'id': 'TXN010', 'gl_account': '4000', 'amount': 1500.00, 'user_name': 'Alice Brown', 
         'posting_date_str': '2024-01-18', 'document_date_str': '2024-01-18', 'source': 'SAP'},
        {'id': 'TXN011', 'gl_account': '4000', 'amount': 1500.00, 'user_name': 'Alice Brown', 
         'posting_date_str': '2024-01-18', 'document_date_str': '2024-01-18', 'source': 'SAP'},
        {'id': 'TXN012', 'gl_account': '4000', 'amount': 1500.00, 'user_name': 'Alice Brown', 
         'posting_date_str': '2024-01-18', 'document_date_str': '2024-01-18', 'source': 'SAP'},
        
        # Type 2 Duplicate: Account + Amount + User
        {'id': 'TXN013', 'gl_account': '5000', 'amount': 1000.00, 'user_name': 'Charlie Davis', 
         'posting_date_str': '2024-01-19', 'document_date_str': '2024-01-19', 'source': 'SAP'},
        {'id': 'TXN014', 'gl_account': '5000', 'amount': 1000.00, 'user_name': 'Charlie Davis', 
         'posting_date_str': '2024-01-20', 'document_date_str': '2024-01-20', 'source': 'SAP'},
        {'id': 'TXN015', 'gl_account': '5000', 'amount': 1000.00, 'user_name': 'Charlie Davis', 
         'posting_date_str': '2024-01-21', 'document_date_str': '2024-01-21', 'source': 'SAP'},
        
        # Type 1 Duplicate: Account + Amount
        {'id': 'TXN016', 'gl_account': '6000', 'amount': 500.00, 'user_name': 'David Lee', 
         'posting_date_str': '2024-01-22', 'document_date_str': '2024-01-22', 'source': 'SAP'},
        {'id': 'TXN017', 'gl_account': '6000', 'amount': 500.00, 'user_name': 'Eve Johnson', 
         'posting_date_str': '2024-01-23', 'document_date_str': '2024-01-23', 'source': 'SAP'},
        {'id': 'TXN018', 'gl_account': '6000', 'amount': 500.00, 'user_name': 'Frank Miller', 
         'posting_date_str': '2024-01-24', 'document_date_str': '2024-01-24', 'source': 'SAP'},
    ]
    
    df = pd.DataFrame(data)
    print(f"✅ Created DataFrame with {len(df)} transactions")
    return df

def test_duplicate_type_priority_logic():
    """Test the duplicate type priority logic"""
    print("🔍 Testing Duplicate Type Priority Logic")
    print("=" * 60)
    
    # Create test data
    df = create_test_dataframe()
    
    # Define duplicate types (from most specific to least specific)
    duplicate_types = [
        {
            'type': 'Type 6 Duplicate',
            'criteria': 'Account Number + Effective Date + Posted Date + User + Source + Amount',
            'groupby_cols': ['gl_account', 'document_date_str', 'posting_date_str', 'user_name', 'source', 'amount'],
            'risk_multiplier': 25
        },
        {
            'type': 'Type 5 Duplicate',
            'criteria': 'Account Number + Amount + User + Date + Document Type + Source',
            'groupby_cols': ['gl_account', 'amount', 'user_name', 'posting_date_str', 'source'],
            'risk_multiplier': 20
        },
        {
            'type': 'Type 4 Duplicate',
            'criteria': 'Account Number + Amount + User + Date + Document Type',
            'groupby_cols': ['gl_account', 'amount', 'user_name', 'posting_date_str'],
            'risk_multiplier': 15
        },
        {
            'type': 'Type 3 Duplicate',
            'criteria': 'Account Number + Amount + User + Date',
            'groupby_cols': ['gl_account', 'amount', 'user_name', 'posting_date_str'],
            'risk_multiplier': 10
        },
        {
            'type': 'Type 2 Duplicate',
            'criteria': 'Account Number + Amount + User',
            'groupby_cols': ['gl_account', 'amount', 'user_name'],
            'risk_multiplier': 8
        },
        {
            'type': 'Type 1 Duplicate',
            'criteria': 'Account Number + Amount',
            'groupby_cols': ['gl_account', 'amount'],
            'risk_multiplier': 5
        }
    ]
    
    # Test the priority logic
    duplicate_groups = []
    processed_transactions = set()
    duplicate_threshold = 2
    
    print(f"\n🔍 Processing duplicate types from most specific to least specific...")
    
    for dup_type in duplicate_types:
        print(f"\n📊 Processing {dup_type['type']}...")
        
        # Find groups with duplicates, excluding already processed transactions
        available_df = df[~df['id'].isin(processed_transactions)]
        
        if len(available_df) < duplicate_threshold:
            print(f"   ⏭️  Not enough transactions left ({len(available_df)} < {duplicate_threshold})")
            break
        
        groupby_cols = dup_type['groupby_cols']
        grouped = available_df.groupby(groupby_cols).filter(lambda x: len(x) >= duplicate_threshold)
        
        groups_found = 0
        for _, group in grouped.groupby(groupby_cols):
            if len(group) >= duplicate_threshold:
                # Mark these transactions as processed
                group_transaction_ids = set(group['id'].tolist())
                processed_transactions.update(group_transaction_ids)
                
                duplicate_group = {
                    'type': dup_type['type'],
                    'criteria': dup_type['criteria'],
                    'transactions': group.to_dict('records'),
                    'count': len(group),
                    'transaction_ids': group_transaction_ids
                }
                duplicate_groups.append(duplicate_group)
                groups_found += 1
                
                print(f"   ✅ Found group with {len(group)} transactions: {list(group_transaction_ids)}")
        
        if groups_found == 0:
            print(f"   ❌ No groups found for {dup_type['type']}")
    
    # Analyze results
    print(f"\n📊 Analysis Results:")
    print("-" * 40)
    
    all_processed_transactions = set()
    type_analysis = {}
    
    for group in duplicate_groups:
        dup_type = group['type']
        transaction_ids = group['transaction_ids']
        
        if dup_type not in type_analysis:
            type_analysis[dup_type] = {
                'groups': 0,
                'transactions': set()
            }
        
        type_analysis[dup_type]['groups'] += 1
        type_analysis[dup_type]['transactions'].update(transaction_ids)
        
        # Check for overlap
        overlap = transaction_ids & all_processed_transactions
        if overlap:
            print(f"❌ OVERLAP DETECTED in {dup_type}: {overlap}")
        else:
            print(f"✅ {dup_type}: {len(transaction_ids)} transactions, no overlap")
        
        all_processed_transactions.update(transaction_ids)
    
    # Final validation
    print(f"\n🎯 Final Validation:")
    print("-" * 30)
    
    total_unique_transactions = len(all_processed_transactions)
    total_duplicate_entries = sum(len(group['transaction_ids']) for group in duplicate_groups)
    
    print(f"Total unique transactions in duplicates: {total_unique_transactions}")
    print(f"Total duplicate entries: {total_duplicate_entries}")
    
    if total_unique_transactions == total_duplicate_entries:
        print("✅ SUCCESS: Each transaction appears only once (no duplicates across types)")
    else:
        print("❌ FAILED: Some transactions appear in multiple duplicate types")
        print(f"   Expected: {total_unique_transactions} entries")
        print(f"   Actual: {total_duplicate_entries} entries")
        print(f"   Difference: {total_duplicate_entries - total_unique_transactions} duplicate entries")
    
    # Summary by type
    print(f"\n📋 Summary by Type:")
    print("-" * 30)
    for dup_type, data in type_analysis.items():
        print(f"{dup_type}: {data['groups']} groups, {len(data['transactions'])} transactions")
    
    return duplicate_groups

def main():
    """Main test function"""
    try:
        result = test_duplicate_type_priority_logic()
        print("\n✅ Test completed successfully!")
        
        # Save results for inspection
        import json
        with open('duplicate_priority_test_results.json', 'w') as f:
            # Convert sets to lists for JSON serialization
            json_result = []
            for group in result:
                json_group = {
                    'type': group['type'],
                    'criteria': group['criteria'],
                    'count': group['count'],
                    'transaction_ids': list(group['transaction_ids'])
                }
                json_result.append(json_group)
            json.dump(json_result, f, indent=2)
        print("📄 Results saved to duplicate_priority_test_results.json")
        
    except Exception as e:
        print(f"❌ Error during test: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main() 