#!/usr/bin/env python3
"""
Test script to demonstrate advanced expense analytics metrics
"""

import pandas as pd
import numpy as np
from datetime import datetime, date, timedelta
import json

def create_sample_expense_data():
    """Create realistic sample expense data for testing"""
    
    # Sample data based on the user's provided metrics
    sample_data = [
        # Marketing expenses
        {'date': date(2024, 1, 15), 'amount': 2800.0, 'vendor': 'EventPro Inc', 'employee': 'Sarah Johnson', 'department': 'Marketing', 'category': 'Events', 'description': 'Annual conference sponsorship', 'payment_method': 'Corporate Card', 'approved_by': 'Mike Chen', 'notes': 'Receipt attached'},
        {'date': date(2024, 1, 20), 'amount': 800.0, 'vendor': 'AdTech Solutions', 'employee': 'Sarah Johnson', 'department': 'Marketing', 'category': 'Advertising', 'description': 'Digital advertising campaign', 'payment_method': 'Corporate Card', 'approved_by': 'Mike Chen', 'notes': 'Invoice received'},
        {'date': date(2024, 1, 25), 'amount': 1500.0, 'vendor': 'PrintWorks', 'employee': 'Sarah Johnson', 'department': 'Marketing', 'category': 'Printing', 'description': 'Marketing materials printing', 'payment_method': 'Corporate Card', 'approved_by': 'Mike Chen', 'notes': ''},
        
        # IT expenses
        {'date': date(2024, 1, 10), 'amount': 1200.0, 'vendor': 'TechSupply Co', 'employee': 'David Rodriguez', 'department': 'IT', 'category': 'Equipment', 'description': 'Computer maintenance and repair', 'payment_method': 'Corporate Card', 'approved_by': 'Mike Chen', 'notes': 'Service receipt'},
        {'date': date(2024, 1, 18), 'amount': 450.0, 'vendor': 'SoftwareCorp', 'employee': 'David Rodriguez', 'department': 'IT', 'category': 'Software', 'description': 'Software license renewal', 'payment_method': 'Personal Card', 'approved_by': 'Mike Chen', 'notes': 'Reimbursement needed'},
        {'date': date(2024, 1, 22), 'amount': 300.0, 'vendor': 'NetworkPro', 'employee': 'David Rodriguez', 'department': 'IT', 'category': 'Services', 'description': 'Network security consultation', 'payment_method': 'Corporate Card', 'approved_by': 'Mike Chen', 'notes': 'Consultation report attached'},
        
        # Office expenses
        {'date': date(2024, 1, 5), 'amount': 200.0, 'vendor': 'OfficeMax', 'employee': 'Lisa Chen', 'department': 'Administration', 'category': 'Office Supplies', 'description': 'Office supplies and stationery', 'payment_method': 'Corporate Card', 'approved_by': 'Mike Chen', 'notes': 'Receipt attached'},
        {'date': date(2024, 1, 12), 'amount': 150.0, 'vendor': 'OfficeMax', 'employee': 'Lisa Chen', 'department': 'Administration', 'category': 'Office Supplies', 'description': 'Additional office supplies', 'payment_method': 'Corporate Card', 'approved_by': 'Mike Chen', 'notes': ''},
        {'date': date(2024, 1, 28), 'amount': 180.0, 'vendor': 'OfficeMax', 'employee': 'Lisa Chen', 'department': 'Administration', 'category': 'Office Supplies', 'description': 'Printer maintenance supplies', 'payment_method': 'Corporate Card', 'approved_by': 'Mike Chen', 'notes': 'Maintenance log attached'},
        
        # Travel expenses
        {'date': date(2024, 1, 8), 'amount': 850.0, 'vendor': 'TravelAgency', 'employee': 'John Smith', 'department': 'Sales', 'category': 'Travel', 'description': 'Business trip to client site', 'payment_method': 'Personal Card', 'approved_by': 'Mike Chen', 'notes': 'Reimbursement needed'},
        {'date': date(2024, 1, 16), 'amount': 45.0, 'vendor': 'Parking Corp', 'employee': 'John Smith', 'department': 'Sales', 'category': 'Travel', 'description': 'Client meeting parking', 'payment_method': 'Personal Card', 'approved_by': 'Mike Chen', 'notes': 'Parking receipt'},
        {'date': date(2024, 1, 24), 'amount': 120.0, 'vendor': 'Hotel Chain', 'employee': 'John Smith', 'department': 'Sales', 'category': 'Travel', 'description': 'Overnight stay for client meeting', 'payment_method': 'Personal Card', 'approved_by': 'Mike Chen', 'notes': 'Hotel receipt'},
        
        # Additional expenses to reach the target total
        {'date': date(2024, 1, 3), 'amount': 350.0, 'vendor': 'Catering Plus', 'employee': 'Lisa Chen', 'department': 'Administration', 'category': 'Meals', 'description': 'Team lunch meeting', 'payment_method': 'Corporate Card', 'approved_by': 'Mike Chen', 'notes': 'Receipt attached'},
        {'date': date(2024, 1, 7), 'amount': 280.0, 'vendor': 'TechSupply Co', 'employee': 'David Rodriguez', 'department': 'IT', 'category': 'Equipment', 'description': 'Computer accessories', 'payment_method': 'Corporate Card', 'approved_by': 'Mike Chen', 'notes': 'Invoice received'},
        {'date': date(2024, 1, 14), 'amount': 420.0, 'vendor': 'MarketingPro', 'employee': 'Sarah Johnson', 'department': 'Marketing', 'category': 'Advertising', 'description': 'Social media advertising', 'payment_method': 'Corporate Card', 'approved_by': 'Mike Chen', 'notes': 'Campaign report attached'},
        {'date': date(2024, 1, 19), 'amount': 180.0, 'vendor': 'OfficeMax', 'employee': 'Lisa Chen', 'department': 'Administration', 'category': 'Office Supplies', 'description': 'Paper and ink supplies', 'payment_method': 'Corporate Card', 'approved_by': 'Mike Chen', 'notes': ''},
        {'date': date(2024, 1, 26), 'amount': 320.0, 'vendor': 'TravelAgency', 'employee': 'John Smith', 'department': 'Sales', 'category': 'Travel', 'description': 'Regional sales meeting travel', 'payment_method': 'Personal Card', 'approved_by': 'Mike Chen', 'notes': 'Reimbursement needed'},
        {'date': date(2024, 1, 30), 'amount': 95.0, 'vendor': 'Parking Corp', 'employee': 'John Smith', 'department': 'Sales', 'category': 'Travel', 'description': 'Client visit parking', 'payment_method': 'Personal Card', 'approved_by': 'Mike Chen', 'notes': 'Parking receipt'},
        {'date': date(2024, 1, 31), 'amount': 250.0, 'vendor': 'TechSupply Co', 'employee': 'David Rodriguez', 'department': 'IT', 'category': 'Services', 'description': 'IT consulting services', 'payment_method': 'Corporate Card', 'approved_by': 'Mike Chen', 'notes': 'Consultation report'},
    ]
    
    return pd.DataFrame(sample_data)

def calculate_advanced_metrics(df):
    """Calculate all advanced expense analytics metrics"""
    
    # Basic calculations
    total_expenses = len(df)
    total_amount = df['amount'].sum()
    date_range = (df['date'].max() - df['date'].min()).days + 1
    
    print("=== ADVANCED EXPENSE ANALYTICS REPORT ===\n")
    
    # 1. Expense Velocity Ratio (EVR)
    evr = total_amount / date_range if date_range > 0 else 0
    print(f"1. Expense Velocity Ratio (EVR): ${evr:.2f}/day")
    print(f"   Total: ${total_amount:,.2f} ÷ {date_range} days = ${evr:.2f}/day\n")
    
    # 2. Approval Concentration Index (ACI)
    approver_totals = df.groupby('approved_by')['amount'].sum()
    largest_approver_total = approver_totals.max() if len(approver_totals) > 0 else 0
    aci = (largest_approver_total / total_amount * 100) if total_amount > 0 else 0
    print(f"2. Approval Concentration Index (ACI): {aci:.1f}%")
    print(f"   Largest approver: {approver_totals.idxmax()} (${largest_approver_total:,.2f})")
    print(f"   Concentration: {largest_approver_total:,.2f} ÷ {total_amount:,.2f} × 100 = {aci:.1f}%\n")
    
    # 3. Payment Method Risk Score (PMRS)
    personal_card_expenses = df[df['payment_method'].str.contains('personal', case=False, na=False)]['amount'].sum()
    pmrs = (personal_card_expenses / total_amount * 100) if total_amount > 0 else 0
    print(f"3. Payment Method Risk Score (PMRS): {pmrs:.1f}%")
    print(f"   Personal card expenses: ${personal_card_expenses:,.2f}")
    print(f"   Risk score: {personal_card_expenses:,.2f} ÷ {total_amount:,.2f} × 100 = {pmrs:.1f}%\n")
    
    # 4. Category Deviation Index (CDI)
    print("4. Category Deviation Index (CDI):")
    dept_category_spend = df.groupby(['department', 'category'])['amount'].sum().reset_index()
    dept_avg_spend = df.groupby('department')['amount'].mean()
    
    for _, row in dept_category_spend.iterrows():
        dept_avg = dept_avg_spend.get(row['department'], 0)
        if dept_avg > 0:
            cdi = abs(row['amount'] - dept_avg) / dept_avg
            print(f"   {row['department']} - {row['category']}: {cdi:.2f}x deviation")
            print(f"     Spend: ${row['amount']:,.2f}, Dept Avg: ${dept_avg:,.2f}")
    print()
    
    # 5. Vendor Concentration Ratio (VCR)
    vendor_totals = df.groupby('vendor_supplier')['amount'].sum().sort_values(ascending=False)
    top_5_vendors_total = vendor_totals.head(5).sum()
    vcr = (top_5_vendors_total / total_amount * 100) if total_amount > 0 else 0
    print(f"5. Vendor Concentration Ratio (VCR): {vcr:.1f}%")
    print(f"   Top 5 vendors total: ${top_5_vendors_total:,.2f}")
    print(f"   Concentration: {top_5_vendors_total:,.2f} ÷ {total_amount:,.2f} × 100 = {vcr:.1f}%")
    print("   Top vendors:")
    for vendor, amount in vendor_totals.head(5).items():
        print(f"     {vendor}: ${amount:,.2f}")
    print()
    
    # 6. High-Value Expense Frequency (HVEF)
    high_value_threshold = df['amount'].quantile(0.75)
    high_value_expenses = df[df['amount'] > high_value_threshold]
    hvef = (len(high_value_expenses) / total_expenses * 100) if total_expenses > 0 else 0
    print(f"6. High-Value Expense Frequency (HVEF): {hvef:.1f}%")
    print(f"   Threshold (75th percentile): ${high_value_threshold:.2f}")
    print(f"   High-value expenses: {len(high_value_expenses)} out of {total_expenses}")
    print(f"   Frequency: {len(high_value_expenses)} ÷ {total_expenses} × 100 = {hvef:.1f}%\n")
    
    # 7. Department Expense Intensity (DEI)
    print("7. Department Expense Intensity (DEI):")
    dept_expenses = df.groupby('department')['amount'].sum()
    for dept, amount in dept_expenses.items():
        print(f"   {dept}: ${amount:,.2f}")
    print()
    
    # 8. Expense Complexity Score (ECS)
    print("8. Expense Complexity Score (ECS):")
    complex_expenses = 0
    for _, row in df.iterrows():
        score = 0
        issues = []
        
        # Missing receipts
        if pd.isna(row['notes']) or row['notes'] == '':
            score += 3
            issues.append('Missing receipt')
        
        # Multiple approvers
        if pd.notna(row['approved_by']) and ',' in str(row['approved_by']):
            score += 2
            issues.append('Multiple approvers')
        
        # Vague descriptions
        if pd.isna(row['description']) or len(str(row['description'])) < 10:
            score += 1
            issues.append('Vague description')
        
        if score > 0:
            complex_expenses += 1
            print(f"   Score {score}: {row['description']} - {', '.join(issues)}")
    
    print(f"   Total complex expenses: {complex_expenses} out of {total_expenses}\n")
    
    # 9. Vendor Loyalty Index (VLI)
    print("9. Vendor Loyalty Index (VLI):")
    employee_vendor_counts = df.groupby('employee')['vendor_supplier'].nunique()
    employee_total_expenses = df.groupby('employee').size()
    
    for employee in df['employee'].unique():
        vendor_count = employee_vendor_counts.get(employee, 0)
        expense_count = employee_total_expenses.get(employee, 0)
        vli = vendor_count / expense_count if expense_count > 0 else 0
        print(f"   {employee}: {vendor_count} vendors for {expense_count} expenses (VLI: {vli:.2f})")
    print()
    
    # 10. Risk Indicators
    print("10. Risk Indicators:")
    print(f"   High ACI Warning (>50%): {'⚠️ YES' if aci > 50 else '✅ OK'}")
    print(f"   High PMRS Warning (>20%): {'⚠️ YES' if pmrs > 20 else '✅ OK'}")
    print(f"   High VCR Warning (>80%): {'⚠️ YES' if vcr > 80 else '✅ OK'}")
    print(f"   High HVEF Warning (>25%): {'⚠️ YES' if hvef > 25 else '✅ OK'}")
    print(f"   Complex Expenses (>5): {'⚠️ YES' if complex_expenses > 5 else '✅ OK'}")
    print()
    
    # Summary
    print("=== SUMMARY ===")
    print(f"Total Expenses: {total_expenses}")
    print(f"Total Amount: ${total_amount:,.2f}")
    print(f"Average Expense: ${df['amount'].mean():.2f}")
    print(f"Median Expense: ${df['amount'].median():.2f}")
    print(f"Largest Expense: ${df['amount'].max():.2f}")
    print(f"Smallest Expense: ${df['amount'].min():.2f}")
    print(f"Date Range: {date_range} days")

def main():
    """Main test function"""
    print("Creating sample expense data...")
    df = create_sample_expense_data()
    
    print(f"Sample data created with {len(df)} expenses")
    print(f"Total amount: ${df['amount'].sum():,.2f}")
    print(f"Date range: {df['date'].min()} to {df['date'].max()}")
    print()
    
    calculate_advanced_metrics(df)

if __name__ == "__main__":
    main() 