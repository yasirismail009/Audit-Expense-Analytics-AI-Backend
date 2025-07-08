#!/usr/bin/env python3
"""
Test script to verify the date handling fix
"""

import pandas as pd
import numpy as np
from datetime import datetime, date
import warnings
warnings.filterwarnings('ignore')

def test_date_handling():
    """Test that date handling works correctly"""
    
    # Create test data with string dates
    test_data = [
        {'date': '2024-01-01', 'amount': 100.0, 'vendor_supplier': 'Vendor A', 'employee': 'John', 'department': 'IT', 'category': 'Equipment', 'description': 'Test expense 1', 'payment_method': 'Corporate Card', 'approved_by': 'Mike Chen', 'notes': 'Receipt attached'},
        {'date': '2024-01-02', 'amount': 150.0, 'vendor_supplier': 'Vendor B', 'employee': 'Jane', 'department': 'Marketing', 'category': 'Advertising', 'description': 'Test expense 2', 'payment_method': 'Corporate Card', 'approved_by': 'Mike Chen', 'notes': 'Receipt attached'},
        {'date': '2024-01-03', 'amount': 200.0, 'vendor_supplier': 'Vendor C', 'employee': 'Alice', 'department': 'Sales', 'category': 'Travel', 'description': 'Test expense 3', 'payment_method': 'Personal Card', 'approved_by': 'Mike Chen', 'notes': 'Reimbursement needed'},
        {'date': '2024-01-04', 'amount': 75.0, 'vendor_supplier': 'Vendor D', 'employee': 'Bob', 'department': 'Admin', 'category': 'Office Supplies', 'description': 'Test expense 4', 'payment_method': 'Corporate Card', 'approved_by': 'Mike Chen', 'notes': ''},
        {'date': '2024-01-05', 'amount': 300.0, 'vendor_supplier': 'Vendor E', 'employee': 'Eve', 'department': 'IT', 'category': 'Software', 'description': 'Test expense 5', 'payment_method': 'Corporate Card', 'approved_by': 'Mike Chen', 'notes': 'Invoice received'},
    ]
    
    df = pd.DataFrame(test_data)
    
    print("=== Date Handling Test ===\n")
    print(f"Original date column type: {df['date'].dtype}")
    print(f"Sample dates: {df['date'].head().tolist()}")
    
    # Test the fix
    try:
        # Convert date column to datetime
        df['date'] = pd.to_datetime(df['date'], errors='coerce')
        print(f"\n✅ Date conversion successful!")
        print(f"Converted date column type: {df['date'].dtype}")
        print(f"Sample converted dates: {df['date'].head().tolist()}")
        
        # Test .dt accessor operations
        print(f"\n✅ Testing .dt accessor operations:")
        print(f"   Day of month: {df['date'].dt.day.tolist()}")
        print(f"   Month: {df['date'].dt.month.tolist()}")
        print(f"   Year: {df['date'].dt.year.tolist()}")
        
        # Test groupby with date
        print(f"\n✅ Testing date-based groupby:")
        monthly_totals = df.groupby(df['date'].dt.to_period('M'))['amount'].sum()
        print(f"   Monthly totals: {monthly_totals.to_dict()}")
        
        # Test day of month patterns
        day_counts = df.groupby(df['date'].dt.day).size()
        print(f"   Day of month counts: {day_counts.to_dict()}")
        
        print(f"\n✅ All date operations working correctly!")
        
    except Exception as e:
        print(f"\n❌ Date handling failed: {e}")
        return False
    
    return True

def test_advanced_metrics_with_dates():
    """Test the advanced metrics calculation with date handling"""
    
    try:
        from test_analytics_simple import SimpleExpenseAnalyzer
        
        # Create test data
        test_data = [
            {'date': '2024-01-01', 'amount': 100.0, 'vendor_supplier': 'Vendor A', 'employee': 'John', 'department': 'IT', 'category': 'Equipment', 'description': 'Test expense 1', 'payment_method': 'Corporate Card', 'approved_by': 'Mike Chen', 'notes': 'Receipt attached'},
            {'date': '2024-01-02', 'amount': 150.0, 'vendor_supplier': 'Vendor B', 'employee': 'Jane', 'department': 'Marketing', 'category': 'Advertising', 'description': 'Test expense 2', 'payment_method': 'Corporate Card', 'approved_by': 'Mike Chen', 'notes': 'Receipt attached'},
            {'date': '2024-01-03', 'amount': 200.0, 'vendor_supplier': 'Vendor C', 'employee': 'Alice', 'department': 'Sales', 'category': 'Travel', 'description': 'Test expense 3', 'payment_method': 'Personal Card', 'approved_by': 'Mike Chen', 'notes': 'Reimbursement needed'},
        ]
        
        print("\n=== Advanced Metrics Test ===\n")
        
        # Create analyzer and run analysis
        analyzer = SimpleExpenseAnalyzer()
        df = analyzer.prepare_sheet_data(test_data)
        
        if df is not None:
            print(f"✅ Data preparation successful!")
            print(f"   DataFrame shape: {df.shape}")
            print(f"   Date column type: {df['date'].dtype}")
            
            # Test advanced metrics calculation
            metrics = analyzer.calculate_advanced_metrics(df)
            
            if metrics:
                print(f"✅ Advanced metrics calculation successful!")
                print(f"   EVR: ${metrics.get('expense_velocity_ratio', 0):.2f}/day")
                print(f"   ACI: {metrics.get('approval_concentration_index', 0):.1f}%")
                print(f"   PMRS: {metrics.get('payment_method_risk_score', 0):.1f}%")
                print(f"   Date range: {metrics.get('basic_metrics', {}).get('date_range_days', 0)} days")
                
                # Check for date-based metrics
                etas_results = metrics.get('expense_timing_anomaly_score', [])
                if etas_results:
                    print(f"   ETAS results: {len(etas_results)} timing patterns found")
                
                rev_results = metrics.get('recurring_expense_variance', [])
                if rev_results:
                    print(f"   REV results: {len(rev_results)} variance patterns found")
                
                return True
            else:
                print(f"❌ Advanced metrics calculation failed!")
                return False
        else:
            print(f"❌ Data preparation failed!")
            return False
            
    except Exception as e:
        print(f"❌ Advanced metrics test failed: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    print("Testing Date Handling Fix")
    print("="*40)
    
    # Test basic date handling
    date_test_passed = test_date_handling()
    
    # Test advanced metrics with dates
    metrics_test_passed = test_advanced_metrics_with_dates()
    
    print("\n" + "="*40)
    if date_test_passed and metrics_test_passed:
        print("✅ All tests passed! Date handling is working correctly.")
    else:
        print("❌ Some tests failed. Please check the errors above.") 