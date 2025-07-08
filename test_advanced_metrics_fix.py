#!/usr/bin/env python3
"""
Test script to verify advanced metrics and chart data functionality
"""

import os
import sys
import django
from datetime import datetime, date, timedelta
from decimal import Decimal

# Setup Django
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'analytics.settings')
django.setup()

from core.models import ExpenseSheet, Expense, SheetAnalysis, ExpenseAnalysis
from core.analytics import ExpenseSheetAnalyzer

def create_test_data():
    """Create comprehensive test data for advanced metrics"""
    print("Creating test data...")
    
    # Create expense sheet
    sheet, created = ExpenseSheet.objects.get_or_create(
        sheet_name="Advanced_Metrics_Test_Sheet",
        sheet_date=date.today(),
        defaults={
            'total_expenses': 0,
            'total_amount': 0
        }
    )
    
    if not created:
        # Clear existing expenses
        sheet.expenses.all().delete()
    
    # Create diverse test data
    test_expenses = [
        # Employee 1 - Multiple expenses, some anomalies
        {
            'date': date.today() - timedelta(days=1),
            'category': 'IT',
            'subcategory': 'Software',
            'description': 'Adobe Creative Suite subscription',
            'employee': 'John Smith',
            'department': 'Marketing',
            'amount': Decimal('599.99'),
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
            'amount': Decimal('1200.00'),
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
            'employee': 'John Smith',
            'department': 'Marketing',
            'amount': Decimal('45.50'),
            'currency': 'USD',
            'payment_method': 'Corporate Card',
            'vendor_supplier': 'Office Depot',
            'receipt_number': 'OFF001',
            'status': 'Approved',
            'approved_by': 'Manager A',
            'notes': 'Office supplies'
        },
        
        # Employee 2 - Single expense (potential anomaly)
        {
            'date': date.today() - timedelta(days=1),
            'category': 'IT',
            'subcategory': 'Hardware',
            'description': 'Laptop replacement',
            'employee': 'Jane Doe',
            'department': 'Engineering',
            'amount': Decimal('2500.00'),
            'currency': 'USD',
            'payment_method': 'Corporate Card',
            'vendor_supplier': 'Dell Technologies',
            'receipt_number': 'DEL001',
            'status': 'Approved',
            'approved_by': 'Manager B',
            'notes': 'Equipment replacement'
        },
        
        # Employee 3 - Multiple expenses, different categories
        {
            'date': date.today() - timedelta(days=2),
            'category': 'Marketing',
            'subcategory': 'Advertising',
            'description': 'Google Ads campaign',
            'employee': 'Bob Wilson',
            'department': 'Sales',
            'amount': Decimal('800.00'),
            'currency': 'USD',
            'payment_method': 'Corporate Card',
            'vendor_supplier': 'Google',
            'receipt_number': 'GOO001',
            'status': 'Approved',
            'approved_by': 'Manager C',
            'notes': 'Digital advertising'
        },
        {
            'date': date.today() - timedelta(days=3),
            'category': 'Travel',
            'subcategory': 'Meals',
            'description': 'Client dinner',
            'employee': 'Bob Wilson',
            'department': 'Sales',
            'amount': Decimal('150.00'),
            'currency': 'USD',
            'payment_method': 'Personal Card',
            'vendor_supplier': 'Restaurant XYZ',
            'receipt_number': 'RES001',
            'status': 'Approved',
            'approved_by': 'Manager C',
            'notes': 'Client entertainment'
        },
        {
            'date': date.today() - timedelta(days=4),
            'category': 'IT',
            'subcategory': 'Software',
            'description': 'CRM software license',
            'employee': 'Bob Wilson',
            'department': 'Sales',
            'amount': Decimal('300.00'),
            'currency': 'USD',
            'payment_method': 'Corporate Card',
            'vendor_supplier': 'Salesforce',
            'receipt_number': 'SAL001',
            'status': 'Approved',
            'approved_by': 'Manager C',
            'notes': 'Software license'
        },
        
        # Employee 4 - High value expense (potential anomaly)
        {
            'date': date.today() - timedelta(days=1),
            'category': 'IT',
            'subcategory': 'Hardware',
            'description': 'Server equipment',
            'employee': 'Alice Johnson',
            'department': 'IT',
            'amount': Decimal('5000.00'),
            'currency': 'USD',
            'payment_method': 'Corporate Card',
            'vendor_supplier': 'HP Enterprise',
            'receipt_number': 'HPE001',
            'status': 'Approved',
            'approved_by': 'Manager D',
            'notes': 'Infrastructure upgrade'
        },
        
        # Duplicate suspicion - same amount, same vendor
        {
            'date': date.today() - timedelta(days=5),
            'category': 'Office Supplies',
            'subcategory': 'Furniture',
            'description': 'Office chair',
            'employee': 'Charlie Brown',
            'department': 'HR',
            'amount': Decimal('299.99'),
            'currency': 'USD',
            'payment_method': 'Corporate Card',
            'vendor_supplier': 'Office Depot',
            'receipt_number': 'OFF002',
            'status': 'Approved',
            'approved_by': 'Manager E',
            'notes': 'Ergonomic chair'
        },
        {
            'date': date.today() - timedelta(days=6),
            'category': 'Office Supplies',
            'subcategory': 'Furniture',
            'description': 'Office chair',
            'employee': 'David Miller',
            'department': 'Finance',
            'amount': Decimal('299.99'),
            'currency': 'USD',
            'payment_method': 'Corporate Card',
            'vendor_supplier': 'Office Depot',
            'receipt_number': 'OFF003',
            'status': 'Approved',
            'approved_by': 'Manager F',
            'notes': 'Ergonomic chair'
        }
    ]
    
    # Create expenses
    total_amount = Decimal('0')
    for expense_data in test_expenses:
        expense_data['expense_sheet'] = sheet
        expense = Expense.objects.create(**expense_data)
        total_amount += expense.amount
    
    # Update sheet totals
    sheet.total_expenses = len(test_expenses)
    sheet.total_amount = total_amount
    sheet.save()
    
    print(f"Created {len(test_expenses)} test expenses")
    print(f"Total amount: ${total_amount}")
    return sheet

def test_advanced_metrics():
    """Test advanced metrics calculation"""
    print("\n" + "="*50)
    print("TESTING ADVANCED METRICS")
    print("="*50)
    
    # Create test data
    sheet = create_test_data()
    
    # Run analysis
    analyzer = ExpenseSheetAnalyzer()
    sheet_analysis = analyzer.analyze_sheet(sheet)
    
    if not sheet_analysis:
        print("❌ Analysis failed!")
        return False
    
    print("✅ Analysis completed successfully")
    
    # Check advanced metrics
    analysis_details = getattr(sheet_analysis, 'analysis_details', {})
    
    print("\n📊 ADVANCED METRICS RESULTS:")
    print("-" * 30)
    
    # Basic metrics
    basic_metrics = analysis_details.get('basic_metrics', {})
    print(f"Total expenses: {basic_metrics.get('total_expenses', 0)}")
    print(f"Total amount: ${basic_metrics.get('total_amount', 0):.2f}")
    print(f"Average expense: ${basic_metrics.get('average_expense', 0):.2f}")
    print(f"Date range: {basic_metrics.get('date_range_days', 0)} days")
    
    # Advanced metrics
    print(f"\nExpense Velocity Ratio: ${analysis_details.get('expense_velocity_ratio', 0):.2f}/day")
    print(f"Approval Concentration Index: {analysis_details.get('approval_concentration_index', 0):.1f}%")
    print(f"Payment Method Risk Score: {analysis_details.get('payment_method_risk_score', 0):.1f}%")
    print(f"Vendor Concentration Ratio: {analysis_details.get('vendor_concentration_ratio', 0):.1f}%")
    
    # High value expense frequency
    hvef = analysis_details.get('high_value_expense_frequency', {})
    print(f"High Value Expense Frequency: {hvef.get('percentage', 0):.1f}%")
    print(f"High value threshold: ${hvef.get('threshold', 0):.2f}")
    
    # Risk indicators
    risk_indicators = analysis_details.get('risk_indicators', {})
    print(f"\n🚨 RISK INDICATORS:")
    print(f"High ACI warning: {risk_indicators.get('high_aci_warning', False)}")
    print(f"High PMRS warning: {risk_indicators.get('high_pmrs_warning', False)}")
    print(f"High VCR warning: {risk_indicators.get('high_vcr_warning', False)}")
    print(f"High HVEF warning: {risk_indicators.get('high_hvef_warning', False)}")
    print(f"Complex expenses: {risk_indicators.get('complex_expenses', 0)}")
    
    # Check chart data
    chart_data = analysis_details.get('chart_data', {})
    print(f"\n📈 CHART DATA AVAILABLE:")
    for chart_name, chart_info in chart_data.items():
        if isinstance(chart_info, dict) and 'labels' in chart_info and 'data' in chart_info:
            print(f"✅ {chart_name}: {len(chart_info['labels'])} data points")
        else:
            print(f"❌ {chart_name}: Invalid format")
    
    return True

def test_employee_anomaly_detection():
    """Test employee anomaly detection specifically"""
    print("\n" + "="*50)
    print("TESTING EMPLOYEE ANOMALY DETECTION")
    print("="*50)
    
    # Get the test sheet
    try:
        sheet = ExpenseSheet.objects.get(sheet_name="Advanced_Metrics_Test_Sheet")
    except ExpenseSheet.DoesNotExist:
        print("❌ Test sheet not found. Run test_advanced_metrics() first.")
        return False
    
    # Check employee anomaly detection
    analyzer = ExpenseSheetAnalyzer()
    df = analyzer.prepare_sheet_data(sheet)
    
    if df is None:
        print("❌ Could not prepare sheet data")
        return False
    
    # Count expenses per employee
    employee_counts = df['employee'].value_counts()
    print(f"\n👥 EMPLOYEE EXPENSE COUNTS:")
    for employee, count in employee_counts.items():
        print(f"  {employee}: {count} expenses")
    
    # Check which employees should be flagged as anomalies
    print(f"\n🚨 EMPLOYEE ANOMALY DETECTION:")
    for employee, count in employee_counts.items():
        is_anomaly = count <= 1
        status = "ANOMALY" if is_anomaly else "Normal"
        print(f"  {employee}: {count} expenses -> {status}")
    
    # Check if employee anomalies are detected in analysis
    sheet_analysis = analyzer.analyze_sheet(sheet)
    if sheet_analysis:
        analysis_details = getattr(sheet_analysis, 'analysis_details', {})
        employee_anomalies_detected = getattr(sheet_analysis, 'employee_anomalies_detected', 0)
        print(f"\n📊 ANALYSIS RESULTS:")
        print(f"Employee anomalies detected: {employee_anomalies_detected}")
        
        # Check individual expense analyses
        print(f"\n🔍 INDIVIDUAL EXPENSE ANALYSES:")
        for expense in sheet.expenses.all():
            try:
                analysis = expense.analysis
                if analysis.employee_anomaly:
                    print(f"  ✅ {expense.employee} - {expense.description} (${expense.amount}) - EMPLOYEE ANOMALY DETECTED")
                else:
                    print(f"  ⚪ {expense.employee} - {expense.description} (${expense.amount}) - Normal")
            except ExpenseAnalysis.DoesNotExist:
                print(f"  ❌ {expense.employee} - {expense.description} (${expense.amount}) - No analysis found")
    
    return True

def test_chart_data_quality():
    """Test the quality and completeness of chart data"""
    print("\n" + "="*50)
    print("TESTING CHART DATA QUALITY")
    print("="*50)
    
    try:
        sheet = ExpenseSheet.objects.get(sheet_name="Advanced_Metrics_Test_Sheet")
    except ExpenseSheet.DoesNotExist:
        print("❌ Test sheet not found. Run test_advanced_metrics() first.")
        return False
    
    analyzer = ExpenseSheetAnalyzer()
    sheet_analysis = analyzer.analyze_sheet(sheet)
    
    if not sheet_analysis:
        print("❌ Analysis failed!")
        return False
    
    analysis_details = getattr(sheet_analysis, 'analysis_details', {})
    chart_data = analysis_details.get('chart_data', {})
    
    print(f"\n📊 CHART DATA DETAILS:")
    
    expected_charts = [
        'department_expenses',
        'category_expenses', 
        'employee_expenses',
        'vendor_expenses',
        'payment_methods',
        'amount_distribution',
        'approval_concentration'
    ]
    
    for chart_name in expected_charts:
        if chart_name in chart_data:
            chart_info = chart_data[chart_name]
            if isinstance(chart_info, dict) and 'labels' in chart_info and 'data' in chart_info:
                labels = chart_info['labels']
                data = chart_info['data']
                print(f"✅ {chart_name}: {len(labels)} labels, {len(data)} data points")
                if len(labels) > 0:
                    print(f"   Sample: {labels[0]} = {data[0]}")
            else:
                print(f"❌ {chart_name}: Invalid format")
        else:
            print(f"❌ {chart_name}: Missing")
    
    # Test specific chart data
    if 'department_expenses' in chart_data:
        dept_chart = chart_data['department_expenses']
        print(f"\n🏢 DEPARTMENT EXPENSES CHART:")
        for i, (label, value) in enumerate(zip(dept_chart['labels'], dept_chart['data'])):
            print(f"  {label}: ${value:.2f}")
    
    if 'employee_expenses' in chart_data:
        emp_chart = chart_data['employee_expenses']
        print(f"\n👥 EMPLOYEE EXPENSES CHART:")
        for i, (label, value) in enumerate(zip(emp_chart['labels'], emp_chart['data'])):
            print(f"  {label}: ${value:.2f}")
    
    return True

def main():
    """Run all tests"""
    print("🧪 ADVANCED METRICS AND CHART DATA TEST")
    print("=" * 60)
    
    # Test 1: Advanced metrics
    success1 = test_advanced_metrics()
    
    # Test 2: Employee anomaly detection
    success2 = test_employee_anomaly_detection()
    
    # Test 3: Chart data quality
    success3 = test_chart_data_quality()
    
    print("\n" + "="*60)
    print("TEST SUMMARY")
    print("="*60)
    print(f"Advanced Metrics: {'✅ PASS' if success1 else '❌ FAIL'}")
    print(f"Employee Anomaly Detection: {'✅ PASS' if success2 else '❌ FAIL'}")
    print(f"Chart Data Quality: {'✅ PASS' if success3 else '❌ FAIL'}")
    
    if all([success1, success2, success3]):
        print("\n🎉 ALL TESTS PASSED!")
        print("Advanced metrics and chart data are working correctly.")
    else:
        print("\n⚠️  SOME TESTS FAILED!")
        print("Please check the output above for details.")

if __name__ == "__main__":
    main() 