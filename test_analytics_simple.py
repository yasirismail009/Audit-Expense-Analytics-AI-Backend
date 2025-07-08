#!/usr/bin/env python3
"""
Simple test script for analytics functionality without Django dependencies
"""

import pandas as pd
import numpy as np
from datetime import datetime, date, timedelta
import warnings
warnings.filterwarnings('ignore')

class SimpleExpenseAnalyzer:
    """Simplified version of ExpenseSheetAnalyzer for testing"""
    
    def __init__(self):
        self.scaler = None
        self.label_encoders = {}
        self.models = {}
    
    def prepare_sheet_data(self, data):
        """Convert expense data to pandas DataFrame with features"""
        if isinstance(data, list):
            df = pd.DataFrame(data)
        else:
            df = data
        
        if len(df) == 0:
            return None
        
        # Ensure date column is datetime
        df['date'] = pd.to_datetime(df['date'])
        
        # Add engineered features
        df = self._add_features(df)
        
        return df
    
    def _add_features(self, df):
        """Add engineered features for fraud detection"""
        # Amount-based features
        df['amount_log'] = np.log1p(df['amount'])
        df['amount_zscore'] = (df['amount'] - df['amount'].mean()) / df['amount'].std()
        
        # Time-based features
        df['day_of_week'] = pd.to_datetime(df['date']).dt.dayofweek
        df['month'] = pd.to_datetime(df['date']).dt.month
        df['day_of_month'] = pd.to_datetime(df['date']).dt.day
        
        # Employee frequency
        employee_counts = df['employee'].value_counts()
        df['employee_frequency'] = df['employee'].map(employee_counts)
        
        # Vendor frequency
        vendor_counts = df['vendor_supplier'].value_counts()
        df['vendor_frequency'] = df['vendor_supplier'].map(vendor_counts)
        
        # Category frequency
        category_counts = df['category'].value_counts()
        df['category_frequency'] = df['category'].map(category_counts)
        
        # Amount percentiles
        df['amount_percentile'] = df['amount'].rank(pct=True)
        
        # Intelligent duplicate detection features
        df['duplicate_description'] = df['description'].duplicated().astype(int)
        
        # Amount duplicates - only flag if same amount with same vendor or same employee
        df['duplicate_amount_same_vendor'] = df.groupby(['amount', 'vendor_supplier']).cumcount().astype(int)
        df['duplicate_amount_same_employee'] = df.groupby(['amount', 'employee']).cumcount().astype(int)
        df['duplicate_amount_same_date'] = df.groupby(['amount', 'date']).cumcount().astype(int)
        
        # Only flag as duplicate amount if it's suspicious (same vendor/employee/date)
        df['duplicate_amount'] = (
            ((df['duplicate_amount_same_vendor'] > 0) & (df['duplicate_amount_same_vendor'] <= 1)) |
            ((df['duplicate_amount_same_employee'] > 0) & (df['duplicate_amount_same_employee'] <= 1)) |
            ((df['duplicate_amount_same_date'] > 0) & (df['duplicate_amount_same_date'] <= 1))
        ).astype(int)
        
        # Vendor duplicates - only flag if same vendor with same amount or same employee
        df['duplicate_vendor_same_amount'] = df.groupby(['vendor_supplier', 'amount']).cumcount().astype(int)
        df['duplicate_vendor_same_employee'] = df.groupby(['vendor_supplier', 'employee']).cumcount().astype(int)
        
        df['duplicate_vendor'] = (
            ((df['duplicate_vendor_same_amount'] > 0) & (df['duplicate_vendor_same_amount'] <= 1)) |
            ((df['duplicate_vendor_same_employee'] > 0) & (df['duplicate_vendor_same_employee'] <= 1))
        ).astype(int)
        
        return df
    
    def calculate_advanced_metrics(self, df):
        """Calculate advanced expense analytics metrics"""
        if df is None or len(df) == 0:
            return {}
        
        # Basic calculations
        total_expenses = len(df)
        total_amount = df['amount'].sum()
        date_range = (df['date'].max() - df['date'].min()).days + 1
        
        # 1. Expense Velocity Ratio (EVR)
        evr = total_amount / date_range if date_range > 0 else 0
        
        # 2. Approval Concentration Index (ACI)
        approver_totals = df.groupby('approved_by')['amount'].sum()
        largest_approver_total = approver_totals.max() if len(approver_totals) > 0 else 0
        aci = (largest_approver_total / total_amount * 100) if total_amount > 0 else 0
        
        # 3. Payment Method Risk Score (PMRS)
        personal_card_expenses = df[df['payment_method'].str.contains('personal', case=False, na=False)]['amount'].sum()
        pmrs = (personal_card_expenses / total_amount * 100) if total_amount > 0 else 0
        
        # 4. Category Deviation Index (CDI) - by department
        dept_category_spend = df.groupby(['department', 'category'])['amount'].sum().reset_index()
        dept_avg_spend = df.groupby('department')['amount'].mean()
        
        cdi_results = []
        for _, row in dept_category_spend.iterrows():
            dept_avg = dept_avg_spend.get(row['department'], 0)
            if dept_avg > 0:
                cdi = abs(row['amount'] - dept_avg) / dept_avg
                cdi_results.append({
                    'department': row['department'],
                    'category': row['category'],
                    'spend': float(row['amount']),
                    'department_avg': float(dept_avg),
                    'cdi': float(cdi)
                })
        
        # 5. Vendor Concentration Ratio (VCR)
        vendor_totals = df.groupby('vendor_supplier')['amount'].sum().sort_values(ascending=False)
        top_5_vendors_total = vendor_totals.head(5).sum()
        vcr = (top_5_vendors_total / total_amount * 100) if total_amount > 0 else 0
        
        # 6. High-Value Expense Frequency (HVEF)
        high_value_threshold = df['amount'].quantile(0.75)
        high_value_expenses = df[df['amount'] > high_value_threshold]
        hvef = (len(high_value_expenses) / total_expenses * 100) if total_expenses > 0 else 0
        
        # 7. Expense Complexity Score (ECS)
        ecs_scores = []
        for _, row in df.iterrows():
            score = 0
            issues = []
            
            # Missing receipts (assuming notes field indicates receipt status)
            if pd.isna(row['notes']) or row['notes'] == '':
                score += 3
                issues.append('Missing receipt documentation')
            
            # Multiple approvers (check if approved_by contains multiple names)
            if pd.notna(row['approved_by']) and ',' in str(row['approved_by']):
                score += 2
                issues.append('Multiple approvers')
            
            # Vague descriptions
            if pd.isna(row['description']) or len(str(row['description'])) < 10:
                score += 1
                issues.append('Vague description')
            
            ecs_scores.append({
                'expense_id': row.get('id', 'unknown'),
                'score': score,
                'issues': issues,
                'description': row['description']
            })
        
        # 8. Vendor Loyalty Index (VLI)
        employee_vendor_counts = df.groupby('employee')['vendor_supplier'].nunique()
        employee_total_expenses = df.groupby('employee').size()
        
        vli_results = []
        for employee in df['employee'].unique():
            vendor_count = employee_vendor_counts.get(employee, 0)
            expense_count = employee_total_expenses.get(employee, 0)
            vli = vendor_count / expense_count if expense_count > 0 else 0
            vli_results.append({
                'employee': employee,
                'unique_vendors': int(vendor_count),
                'total_expenses': int(expense_count),
                'vli_score': float(vli)
            })
        
        return {
            'basic_metrics': {
                'total_expenses': total_expenses,
                'total_amount': float(total_amount),
                'average_expense': float(df['amount'].mean()),
                'median_expense': float(df['amount'].median()),
                'largest_expense': float(df['amount'].max()),
                'smallest_expense': float(df['amount'].min()),
                'date_range_days': date_range
            },
            'expense_velocity_ratio': float(evr),
            'approval_concentration_index': float(aci),
            'payment_method_risk_score': float(pmrs),
            'category_deviation_index': cdi_results,
            'vendor_concentration_ratio': float(vcr),
            'high_value_expense_frequency': {
                'percentage': float(hvef),
                'threshold': float(high_value_threshold),
                'count': len(high_value_expenses),
                'total_count': total_expenses
            },
            'expense_complexity_scores': ecs_scores,
            'vendor_loyalty_index': vli_results,
            'risk_indicators': {
                'high_aci_warning': aci > 50,  # More than 50% concentration
                'high_pmrs_warning': pmrs > 20,  # More than 20% personal cards
                'high_vcr_warning': vcr > 80,  # More than 80% vendor concentration
                'high_hvef_warning': hvef > 25,  # More than 25% high-value expenses
                'complex_expenses': len([s for s in ecs_scores if s['score'] > 5])
            }
        }
    
    def analyze_expenses(self, data):
        """Analyze expense data and return results"""
        print("=== EXPENSE ANALYTICS ANALYSIS ===\n")
        
        # Prepare data
        df = self.prepare_sheet_data(data)
        if df is None:
            print("No data to analyze")
            return None
        
        # Calculate advanced metrics
        metrics = self.calculate_advanced_metrics(df)
        
        # Print results
        self._print_results(metrics, df)
        
        return metrics
    
    def _print_results(self, metrics, df):
        """Print analysis results"""
        basic = metrics['basic_metrics']
        
        print("📊 BASIC METRICS:")
        print(f"   Total Expenses: {basic['total_expenses']}")
        print(f"   Total Amount: ${basic['total_amount']:,.2f}")
        print(f"   Average Expense: ${basic['average_expense']:.2f}")
        print(f"   Median Expense: ${basic['median_expense']:.2f}")
        print(f"   Largest Expense: ${basic['largest_expense']:.2f}")
        print(f"   Smallest Expense: ${basic['smallest_expense']:.2f}")
        print(f"   Date Range: {basic['date_range_days']} days")
        
        print(f"\n📈 KEY RATIOS:")
        print(f"   Expense Velocity Ratio: ${metrics['expense_velocity_ratio']:.2f}/day")
        print(f"   Approval Concentration: {metrics['approval_concentration_index']:.1f}%")
        print(f"   Payment Method Risk: {metrics['payment_method_risk_score']:.1f}%")
        print(f"   Vendor Concentration: {metrics['vendor_concentration_ratio']:.1f}%")
        
        hvef = metrics['high_value_expense_frequency']
        print(f"\n💰 HIGH-VALUE EXPENSES:")
        print(f"   Frequency: {hvef['percentage']:.1f}%")
        print(f"   Threshold: ${hvef['threshold']:.2f}")
        print(f"   Count: {hvef['count']} out of {hvef['total_count']}")
        
        print(f"\n⚠️  RISK INDICATORS:")
        risks = metrics['risk_indicators']
        print(f"   High ACI: {'YES' if risks['high_aci_warning'] else 'NO'}")
        print(f"   High PMRS: {'YES' if risks['high_pmrs_warning'] else 'NO'}")
        print(f"   High VCR: {'YES' if risks['high_vcr_warning'] else 'NO'}")
        print(f"   High HVEF: {'YES' if risks['high_hvef_warning'] else 'NO'}")
        print(f"   Complex Expenses: {risks['complex_expenses']}")
        
        # Show duplicate detection results
        print(f"\n🔍 DUPLICATE DETECTION:")
        duplicate_amounts = df[df['duplicate_amount'] == 1]
        duplicate_vendors = df[df['duplicate_vendor'] == 1]
        duplicate_descriptions = df[df['duplicate_description'] == 1]
        
        print(f"   Amount Duplicates: {len(duplicate_amounts)}")
        print(f"   Vendor Duplicates: {len(duplicate_vendors)}")
        print(f"   Description Duplicates: {len(duplicate_descriptions)}")
        
        if len(duplicate_amounts) > 0:
            print("   Amount Duplicates Details:")
            for _, row in duplicate_amounts.iterrows():
                print(f"     ${row['amount']:.2f} - {row['description']} ({row['vendor_supplier']})")
        
        print("\n" + "="*50)

def create_sample_data():
    """Create sample expense data"""
    return [
        # Same amount, different vendors (should NOT be flagged)
        {'date': date(2024, 1, 1), 'amount': 100.0, 'vendor_supplier': 'Vendor A', 'employee': 'John', 'department': 'IT', 'category': 'Equipment', 'description': 'Lunch', 'payment_method': 'Corporate Card', 'approved_by': 'Mike Chen', 'notes': 'Receipt attached'},
        {'date': date(2024, 1, 2), 'amount': 100.0, 'vendor_supplier': 'Vendor B', 'employee': 'Jane', 'department': 'Marketing', 'category': 'Meals', 'description': 'Dinner', 'payment_method': 'Corporate Card', 'approved_by': 'Mike Chen', 'notes': 'Receipt attached'},
        
        # Same amount, same vendor (SHOULD be flagged)
        {'date': date(2024, 1, 3), 'amount': 150.0, 'vendor_supplier': 'Vendor C', 'employee': 'John', 'department': 'IT', 'category': 'Software', 'description': 'Coffee', 'payment_method': 'Corporate Card', 'approved_by': 'Mike Chen', 'notes': 'Receipt attached'},
        {'date': date(2024, 1, 4), 'amount': 150.0, 'vendor_supplier': 'Vendor C', 'employee': 'John', 'department': 'IT', 'category': 'Software', 'description': 'Coffee', 'payment_method': 'Corporate Card', 'approved_by': 'Mike Chen', 'notes': ''},
        
        # Same amount, same employee (SHOULD be flagged)
        {'date': date(2024, 1, 5), 'amount': 200.0, 'vendor_supplier': 'Vendor D', 'employee': 'Alice', 'department': 'Sales', 'category': 'Travel', 'description': 'Transport', 'payment_method': 'Personal Card', 'approved_by': 'Mike Chen', 'notes': 'Reimbursement needed'},
        {'date': date(2024, 1, 6), 'amount': 200.0, 'vendor_supplier': 'Vendor E', 'employee': 'Alice', 'department': 'Sales', 'category': 'Travel', 'description': 'Transport', 'payment_method': 'Personal Card', 'approved_by': 'Mike Chen', 'notes': 'Receipt attached'},
        
        # Normal expenses (should NOT be flagged)
        {'date': date(2024, 1, 7), 'amount': 50.0, 'vendor_supplier': 'Vendor F', 'employee': 'David', 'department': 'Admin', 'category': 'Office Supplies', 'description': 'Office supplies', 'payment_method': 'Corporate Card', 'approved_by': 'Mike Chen', 'notes': 'Receipt attached'},
        {'date': date(2024, 1, 8), 'amount': 300.0, 'vendor_supplier': 'Vendor G', 'employee': 'Eve', 'department': 'Marketing', 'category': 'Advertising', 'description': 'Equipment', 'payment_method': 'Corporate Card', 'approved_by': 'Mike Chen', 'notes': 'Invoice received'},
    ]

def main():
    """Main test function"""
    print("Testing Expense Analytics System")
    print("="*50)
    
    # Create sample data
    data = create_sample_data()
    print(f"Created {len(data)} sample expenses")
    
    # Create analyzer and run analysis
    analyzer = SimpleExpenseAnalyzer()
    results = analyzer.analyze_expenses(data)
    
    if results:
        print("\n✅ Analysis completed successfully!")
        print(f"Total amount analyzed: ${results['basic_metrics']['total_amount']:,.2f}")
    else:
        print("\n❌ Analysis failed!")

if __name__ == "__main__":
    main() 