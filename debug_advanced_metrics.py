#!/usr/bin/env python3
"""
Debug script to test advanced metrics calculation
"""

import pandas as pd
import numpy as np
from datetime import datetime, date, timedelta

def create_test_data():
    """Create test data for debugging"""
    data = [
        {
            'date': date.today() - timedelta(days=1),
            'category': 'IT',
            'subcategory': 'Software',
            'description': 'Adobe Creative Suite subscription',
            'employee': 'John Smith',
            'department': 'Marketing',
            'amount': 599.99,
            'currency': 'USD',
            'payment_method': 'Corporate Card',
            'vendor_supplier': 'Adobe Inc',
            'receipt_number': 'ADB001',
            'status': 'Approved',
            'approved_by': 'Manager A',
            'notes': 'Annual subscription'
        },
        {
            'date': date.today() - timedelta(days=2),
            'category': 'Travel',
            'subcategory': 'Airfare',
            'description': 'Flight to conference',
            'employee': 'John Smith',
            'department': 'Marketing',
            'amount': 1200.00,
            'currency': 'USD',
            'payment_method': 'Personal Card',
            'vendor_supplier': 'Delta Airlines',
            'receipt_number': 'DLT001',
            'status': 'Approved',
            'approved_by': 'Manager A',
            'notes': 'Business travel'
        },
        {
            'date': date.today() - timedelta(days=3),
            'category': 'Office Supplies',
            'subcategory': 'Stationery',
            'description': 'Printer paper and pens',
            'employee': 'Jane Doe',
            'department': 'Engineering',
            'amount': 45.50,
            'currency': 'USD',
            'payment_method': 'Corporate Card',
            'vendor_supplier': 'Office Depot',
            'receipt_number': 'OFF001',
            'status': 'Approved',
            'approved_by': 'Manager B',
            'notes': 'Office supplies'
        }
    ]
    
    df = pd.DataFrame(data)
    df['date'] = pd.to_datetime(df['date'])
    return df

def calculate_advanced_metrics_debug(df):
    """Calculate advanced metrics with debug output"""
    print("=== DEBUGGING ADVANCED METRICS ===")
    print(f"DataFrame shape: {df.shape}")
    print(f"DataFrame columns: {list(df.columns)}")
    print(f"Date column type: {df['date'].dtype}")
    print(f"Amount column type: {df['amount'].dtype}")
    
    if df is None or len(df) == 0:
        print("❌ No data to analyze")
        return {}
    
    # Ensure date column is datetime
    if 'date' in df.columns:
        df['date'] = pd.to_datetime(df['date'], errors='coerce')
        print(f"✅ Date column converted to datetime: {df['date'].dtype}")
    
    # Basic calculations
    total_expenses = len(df)
    total_amount = df['amount'].sum()
    date_range = (df['date'].max() - df['date'].min()).days + 1 if len(df) > 0 else 1
    
    print(f"📊 Basic calculations:")
    print(f"   Total expenses: {total_expenses}")
    print(f"   Total amount: ${total_amount:.2f}")
    print(f"   Date range: {date_range} days")
    
    # 1. Expense Velocity Ratio (EVR)
    evr = total_amount / date_range if date_range > 0 else 0
    print(f"✅ EVR calculated: ${evr:.2f}/day")
    
    # 2. Approval Concentration Index (ACI)
    approver_totals = df.groupby('approved_by')['amount'].sum()
    print(f"📋 Approver totals: {approver_totals.to_dict()}")
    largest_approver_total = approver_totals.max() if len(approver_totals) > 0 else 0
    aci = (largest_approver_total / total_amount * 100) if total_amount > 0 else 0
    print(f"✅ ACI calculated: {aci:.1f}%")
    
    # 3. Payment Method Risk Score (PMRS)
    personal_card_expenses = df[df['payment_method'].str.contains('personal', case=False, na=False)]['amount'].sum()
    pmrs = (personal_card_expenses / total_amount * 100) if total_amount > 0 else 0
    print(f"✅ PMRS calculated: {pmrs:.1f}% (${personal_card_expenses:.2f} personal card expenses)")
    
    # 4. Vendor Concentration Ratio (VCR)
    vendor_totals = df.groupby('vendor_supplier')['amount'].sum().sort_values(ascending=False)
    print(f"📋 Vendor totals: {vendor_totals.to_dict()}")
    top_5_vendors_total = vendor_totals.head(5).sum()
    vcr = (top_5_vendors_total / total_amount * 100) if total_amount > 0 else 0
    print(f"✅ VCR calculated: {vcr:.1f}%")
    
    # 5. High-Value Expense Frequency (HVEF)
    high_value_threshold = df['amount'].quantile(0.75) if len(df) > 0 else 0
    high_value_expenses = df[df['amount'] > high_value_threshold]
    hvef = (len(high_value_expenses) / total_expenses * 100) if total_expenses > 0 else 0
    print(f"✅ HVEF calculated: {hvef:.1f}% (threshold: ${high_value_threshold:.2f})")
    
    # Create result dictionary
    result = {
        'basic_metrics': {
            'total_expenses': total_expenses,
            'total_amount': float(total_amount),
            'average_expense': float(df['amount'].mean()) if len(df) > 0 else 0,
            'median_expense': float(df['amount'].median()) if len(df) > 0 else 0,
            'largest_expense': float(df['amount'].max()) if len(df) > 0 else 0,
            'smallest_expense': float(df['amount'].min()) if len(df) > 0 else 0,
            'date_range_days': date_range
        },
        'expense_velocity_ratio': float(evr),
        'approval_concentration_index': float(aci),
        'payment_method_risk_score': float(pmrs),
        'vendor_concentration_ratio': float(vcr),
        'high_value_expense_frequency': {
            'percentage': float(hvef),
            'threshold': float(high_value_threshold),
            'count': len(high_value_expenses),
            'total_count': total_expenses
        },
        'risk_indicators': {
            'high_aci_warning': aci > 50,
            'high_pmrs_warning': pmrs > 20,
            'high_vcr_warning': vcr > 80,
            'high_hvef_warning': hvef > 25,
        }
    }
    
    print(f"\n✅ Final result keys: {list(result.keys())}")
    print(f"✅ EVR in result: {result.get('expense_velocity_ratio', 'NOT FOUND')}")
    print(f"✅ ACI in result: {result.get('approval_concentration_index', 'NOT FOUND')}")
    
    return result

def main():
    """Main debug function"""
    print("🧪 DEBUGGING ADVANCED METRICS")
    print("=" * 50)
    
    # Create test data
    df = create_test_data()
    print(f"✅ Test data created with {len(df)} expenses")
    
    # Calculate advanced metrics
    metrics = calculate_advanced_metrics_debug(df)
    
    if metrics:
        print(f"\n🎉 SUCCESS! Advanced metrics calculated:")
        print(f"   EVR: ${metrics.get('expense_velocity_ratio', 0):.2f}/day")
        print(f"   ACI: {metrics.get('approval_concentration_index', 0):.1f}%")
        print(f"   PMRS: {metrics.get('payment_method_risk_score', 0):.1f}%")
        print(f"   VCR: {metrics.get('vendor_concentration_ratio', 0):.1f}%")
        
        # Test risk indicators
        risk_indicators = metrics.get('risk_indicators', {})
        print(f"\n⚠️  RISK INDICATORS:")
        print(f"   High ACI: {risk_indicators.get('high_aci_warning', False)}")
        print(f"   High PMRS: {risk_indicators.get('high_pmrs_warning', False)}")
        print(f"   High VCR: {risk_indicators.get('high_vcr_warning', False)}")
        print(f"   High HVEF: {risk_indicators.get('high_hvef_warning', False)}")
    else:
        print("❌ FAILED! No metrics calculated")

if __name__ == "__main__":
    main() 