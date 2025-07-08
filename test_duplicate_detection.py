#!/usr/bin/env python3
"""
Test script to demonstrate improved duplicate detection logic
"""

import pandas as pd
import numpy as np
from datetime import datetime, date

def test_duplicate_detection():
    """Test the improved duplicate detection logic"""
    
    # Create test data with different scenarios
    test_data = [
        # Scenario 1: Same amount, different vendors (should NOT be flagged)
        {'date': date(2024, 1, 1), 'amount': 100.0, 'vendor': 'Vendor A', 'employee': 'John', 'description': 'Lunch'},
        {'date': date(2024, 1, 2), 'amount': 100.0, 'vendor': 'Vendor B', 'employee': 'Jane', 'description': 'Dinner'},
        
        # Scenario 2: Same amount, same vendor (SHOULD be flagged)
        {'date': date(2024, 1, 3), 'amount': 150.0, 'vendor': 'Vendor C', 'employee': 'John', 'description': 'Coffee'},
        {'date': date(2024, 1, 4), 'amount': 150.0, 'vendor': 'Vendor C', 'employee': 'John', 'description': 'Coffee'},
        
        # Scenario 3: Same amount, same employee (SHOULD be flagged)
        {'date': date(2024, 1, 5), 'amount': 200.0, 'vendor': 'Vendor D', 'employee': 'Alice', 'description': 'Transport'},
        {'date': date(2024, 1, 6), 'amount': 200.0, 'vendor': 'Vendor E', 'employee': 'Alice', 'description': 'Transport'},
        
        # Scenario 4: Same amount, same date (SHOULD be flagged)
        {'date': date(2024, 1, 7), 'amount': 75.0, 'vendor': 'Vendor F', 'employee': 'Bob', 'description': 'Snack'},
        {'date': date(2024, 1, 7), 'amount': 75.0, 'vendor': 'Vendor G', 'employee': 'Charlie', 'description': 'Snack'},
        
        # Scenario 5: Normal expenses (should NOT be flagged)
        {'date': date(2024, 1, 8), 'amount': 50.0, 'vendor': 'Vendor H', 'employee': 'David', 'description': 'Office supplies'},
        {'date': date(2024, 1, 9), 'amount': 300.0, 'vendor': 'Vendor I', 'employee': 'Eve', 'description': 'Equipment'},
    ]
    
    df = pd.DataFrame(test_data)
    
    # Apply the improved duplicate detection logic
    df = add_duplicate_features(df)
    
    print("=== Duplicate Detection Test Results ===\n")
    
    for i, row in df.iterrows():
        print(f"Expense {i+1}:")
        print(f"  Date: {row['date']}")
        print(f"  Amount: ${row['amount']:.2f}")
        print(f"  Vendor: {row['vendor']}")
        print(f"  Employee: {row['employee']}")
        print(f"  Description: {row['description']}")
        
        # Check for duplicates
        duplicate_flags = []
        if row['duplicate_amount']:
            duplicate_flags.append("DUPLICATE AMOUNT")
        if row['duplicate_vendor']:
            duplicate_flags.append("DUPLICATE VENDOR")
        if row['duplicate_description']:
            duplicate_flags.append("DUPLICATE DESCRIPTION")
            
        if duplicate_flags:
            print(f"  ⚠️  FLAGGED: {', '.join(duplicate_flags)}")
        else:
            print(f"  ✅ No duplicates detected")
        print()

def add_duplicate_features(df):
    """Apply the improved duplicate detection logic"""
    
    # Simple duplicate detection (old logic)
    df['duplicate_description'] = df['description'].duplicated().astype(int)
    
    # Intelligent duplicate detection (new logic)
    # Amount duplicates - only flag if same amount with same vendor or same employee
    df['duplicate_amount_same_vendor'] = df.groupby(['amount', 'vendor']).cumcount().astype(int)
    df['duplicate_amount_same_employee'] = df.groupby(['amount', 'employee']).cumcount().astype(int)
    df['duplicate_amount_same_date'] = df.groupby(['amount', 'date']).cumcount().astype(int)
    
    # Only flag as duplicate amount if it's suspicious (same vendor/employee/date)
    df['duplicate_amount'] = (
        ((df['duplicate_amount_same_vendor'] > 0) & (df['duplicate_amount_same_vendor'] <= 1)) |
        ((df['duplicate_amount_same_employee'] > 0) & (df['duplicate_amount_same_employee'] <= 1)) |
        ((df['duplicate_amount_same_date'] > 0) & (df['duplicate_amount_same_date'] <= 1))
    ).astype(int)
    
    # Vendor duplicates - only flag if same vendor with same amount or same employee
    df['duplicate_vendor_same_amount'] = df.groupby(['vendor', 'amount']).cumcount().astype(int)
    df['duplicate_vendor_same_employee'] = df.groupby(['vendor', 'employee']).cumcount().astype(int)
    
    df['duplicate_vendor'] = (
        ((df['duplicate_vendor_same_amount'] > 0) & (df['duplicate_vendor_same_amount'] <= 1)) |
        ((df['duplicate_vendor_same_employee'] > 0) & (df['duplicate_vendor_same_employee'] <= 1))
    ).astype(int)
    
    return df

if __name__ == "__main__":
    test_duplicate_detection() 