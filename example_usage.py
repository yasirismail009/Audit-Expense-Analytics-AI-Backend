#!/usr/bin/env python3
"""
Example usage of the Expense Fraud Analytics System

This script demonstrates how to:
1. Create sample expense data
2. Run the analysis
3. Retrieve results via API
"""

import os
import sys
import django
import pandas as pd
from datetime import datetime, timedelta
import random

# Setup Django
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'analytics.settings')
django.setup()

from core.models import Expense, ExpenseAnalysis, AnalysisSession

def create_sample_data():
    """Create sample expense data for testing"""
    
    # Sample data
    employees = ['John Doe', 'Jane Smith', 'Bob Johnson', 'Alice Brown']
    departments = ['Sales', 'Marketing', 'Engineering', 'Finance']
    categories = ['Travel', 'Meals', 'Office Supplies', 'Software']
    vendors = ['Airline Co', 'Restaurant Inc', 'Office Depot', 'Microsoft']
    payment_methods = ['Credit Card', 'Cash', 'Bank Transfer']
    
    data = []
    
    # Generate 50 sample expenses
    for i in range(50):
        # Normal expenses (80%)
        if i < 40:
            amount = random.uniform(50, 500)
            date = datetime.now() - timedelta(days=random.randint(1, 30))
        # Suspicious expenses (20%)
        else:
            amount = random.uniform(1000, 5000)  # Higher amounts
            date = datetime.now() - timedelta(days=random.randint(1, 30))
            if random.random() > 0.5:
                date = date.replace(hour=23, minute=59)  # Late night submission
        
        expense = {
            'date': date.strftime('%Y-%m-%d'),
            'expense_id': f'EXP{i+1:03d}',
            'category': random.choice(categories),
            'subcategory': 'General',
            'description': f'Sample expense {i+1}',
            'employee': random.choice(employees),
            'department': random.choice(departments),
            'amount': round(amount, 2),
            'currency': 'USD',
            'payment_method': random.choice(payment_methods),
            'vendor_supplier': random.choice(vendors),
            'receipt_number': f'RCP{i+1:03d}',
            'status': 'Pending',
            'approved_by': 'Manager',
            'notes': ''
        }
        data.append(expense)
    
    return data

def save_sample_csv():
    """Save sample data to CSV file"""
    data = create_sample_data()
    df = pd.DataFrame(data)
    
    filename = 'sample_expenses.csv'
    df.to_csv(filename, index=False)
    print(f"Sample data saved to {filename}")
    return filename

def run_analysis(csv_file):
    """Run the expense analysis"""
    from django.core.management import call_command
    
    print(f"Running analysis on {csv_file}...")
    call_command('analyze_expenses', csv_file)
    print("Analysis complete!")

def show_results():
    """Display analysis results"""
    print("\n=== ANALYSIS RESULTS ===")
    
    # Get latest session
    try:
        session = AnalysisSession.objects.latest('created_at')
        print(f"Session ID: {session.session_id}")
        print(f"File: {session.file_name}")
        print(f"Total expenses: {session.total_expenses}")
        print(f"Flagged expenses: {session.flagged_expenses}")
        print(f"Flag rate: {(session.flagged_expenses/session.total_expenses*100):.1f}%")
    except AnalysisSession.DoesNotExist:
        print("No analysis sessions found")
        return
    
    # Show top 5 highest risk expenses
    print("\n=== TOP 5 HIGHEST RISK EXPENSES ===")
    high_risk = ExpenseAnalysis.objects.filter(
        risk_level__in=['HIGH', 'CRITICAL']
    ).order_by('-fraud_score')[:5]
    
    for analysis in high_risk:
        expense = analysis.expense
        print(f"Expense ID: {expense.expense_id}")
        print(f"  Amount: ${expense.amount}")
        print(f"  Employee: {expense.employee}")
        print(f"  Fraud Score: {analysis.fraud_score:.1f}")
        print(f"  Risk Level: {analysis.risk_level}")
        print(f"  Anomalies: {[k for k, v in analysis.__dict__.items() if k.endswith('_anomaly') and v]}")
        print()

def main():
    """Main function to demonstrate the system"""
    print("=== Expense Fraud Analytics System Demo ===\n")
    
    # Step 1: Create sample data
    print("1. Creating sample expense data...")
    csv_file = save_sample_csv()
    
    # Step 2: Run analysis
    print("\n2. Running fraud analysis...")
    run_analysis(csv_file)
    
    # Step 3: Show results
    print("\n3. Displaying results...")
    show_results()
    
    print("\n=== Demo Complete ===")
    print("\nTo get detailed analysis for a specific expense, use:")
    print("curl http://localhost:8000/expenses/EXP001/analysis/")
    
    # Clean up
    if os.path.exists(csv_file):
        os.remove(csv_file)
        print(f"\nCleaned up {csv_file}")

if __name__ == "__main__":
    main() 