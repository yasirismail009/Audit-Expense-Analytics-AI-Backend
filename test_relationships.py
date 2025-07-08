#!/usr/bin/env python3
"""
Test script to verify expense-session relationships
"""

import os
import django
import pandas as pd

# Setup Django
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'analytics.settings')
django.setup()

from core.models import Expense, ExpenseAnalysis, AnalysisSession

def test_relationships():
    """Test the relationships between expenses and analysis sessions"""
    
    print("=== Testing Expense-Session Relationships ===\n")
    
    # Check if we have any analysis sessions
    sessions = AnalysisSession.objects.all()
    print(f"Total analysis sessions: {sessions.count()}")
    
    for session in sessions:
        print(f"\nSession: {session.session_id}")
        print(f"File: {session.file_name}")
        print(f"Total expenses: {session.total_expenses}")
        print(f"Flagged expenses: {session.flagged_expenses}")
        
        # Check expenses linked to this session
        expenses = session.expenses.all()
        print(f"Expenses linked to session: {expenses.count()}")
        
        if expenses.count() > 0:
            print("Sample expenses:")
            for expense in expenses[:3]:  # Show first 3
                print(f"  - {expense.expense_id}: ${expense.amount} ({expense.employee})")
                print(f"    Analysis session: {expense.analysis_session.session_id if expense.analysis_session else 'None'}")
    
    # Check all expenses
    all_expenses = Expense.objects.all()
    print(f"\nTotal expenses in database: {all_expenses.count()}")
    
    # Check expenses with analysis sessions
    expenses_with_session = Expense.objects.filter(analysis_session__isnull=False)
    print(f"Expenses with analysis session: {expenses_with_session.count()}")
    
    # Check expenses without analysis sessions
    expenses_without_session = Expense.objects.filter(analysis_session__isnull=True)
    print(f"Expenses without analysis session: {expenses_without_session.count()}")
    
    if expenses_without_session.count() > 0:
        print("\nExpenses without analysis session:")
        for expense in expenses_without_session[:5]:  # Show first 5
            print(f"  - {expense.expense_id}: {expense.description}")
    
    # Check analysis records
    analyses = ExpenseAnalysis.objects.all()
    print(f"\nTotal analysis records: {analyses.count()}")
    
    if analyses.count() > 0:
        print("Sample analysis records:")
        for analysis in analyses[:3]:  # Show first 3
            expense = analysis.expense
            print(f"  - {expense.expense_id}: Score {analysis.fraud_score}, Risk {analysis.risk_level}")
            print(f"    Session: {expense.analysis_session.session_id if expense.analysis_session else 'None'}")

def create_test_data():
    """Create some test data to verify relationships"""
    
    print("\n=== Creating Test Data ===\n")
    
    # Create a test session
    session = AnalysisSession.objects.create(
        session_id="test_session_123",
        file_name="test_expenses.csv",
        total_expenses=3,
        flagged_expenses=1,
        analysis_status="COMPLETED",
        model_config={"test": True}
    )
    
    # Create test expenses linked to the session
    expenses_data = [
        {
            'expense_id': 'TEST001',
            'description': 'Test expense 1',
            'amount': 100.00,
            'employee': 'Test User',
            'department': 'Test Dept',
            'date': '2024-01-01',
            'category': 'Test',
            'subcategory': 'Test',
            'currency': 'USD',
            'payment_method': 'Test',
            'vendor_supplier': 'Test Vendor',
            'receipt_number': 'TEST001',
            'status': 'Pending',
            'approved_by': 'Test Manager',
            'notes': 'Test note'
        },
        {
            'expense_id': 'TEST002',
            'description': 'Test expense 2',
            'amount': 200.00,
            'employee': 'Test User 2',
            'department': 'Test Dept 2',
            'date': '2024-01-02',
            'category': 'Test 2',
            'subcategory': 'Test 2',
            'currency': 'USD',
            'payment_method': 'Test 2',
            'vendor_supplier': 'Test Vendor 2',
            'receipt_number': 'TEST002',
            'status': 'Pending',
            'approved_by': 'Test Manager 2',
            'notes': 'Test note 2'
        }
    ]
    
    for data in expenses_data:
        expense = Expense.objects.create(
            analysis_session=session,
            **data
        )
        
        # Create analysis for the expense
        ExpenseAnalysis.objects.create(
            expense=expense,
            fraud_score=25.0,
            isolation_forest_score=20.0,
            xgboost_score=30.0,
            lof_score=25.0,
            random_forest_score=25.0,
            risk_level='LOW',
            analysis_details={'test': True}
        )
    
    print("Test data created successfully!")
    return session

if __name__ == "__main__":
    # First test existing data
    test_relationships()
    
    # Ask if user wants to create test data
    response = input("\nDo you want to create test data? (y/n): ")
    if response.lower() == 'y':
        session = create_test_data()
        print(f"\nTest session created with ID: {session.session_id}")
        print("You can now test the API endpoints:")
        print(f"  - Debug: http://localhost:8000/expenses/TEST001/debug/")
        print(f"  - Analysis: http://localhost:8000/expenses/TEST001/analysis/")
        print(f"  - Session: http://localhost:8000/analysis/session/{session.session_id}/")
        print(f"  - Session expenses: http://localhost:8000/analysis/session/{session.session_id}/expenses/")
    
    print("\n=== Test Complete ===") 