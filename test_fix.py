#!/usr/bin/env python3
"""
Simple test script to verify the analytics system works
"""

import os
import sys
import django

# Add the project directory to Python path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# Setup Django
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'analytics.settings')
django.setup()

from core.analytics import SAPGLAnalyzer
from core.models import SAPGLPosting, AnalysisSession
from datetime import date, datetime
from decimal import Decimal

def test_analytics():
    """Test the analytics system"""
    print("Testing analytics system...")
    
    # Create a test analyzer
    analyzer = SAPGLAnalyzer()
    
    # Check if we have any transactions
    transaction_count = SAPGLPosting.objects.count()
    print(f"Found {transaction_count} transactions in database")
    
    if transaction_count == 0:
        print("No transactions found. Creating test data...")
        # Create some test transactions
        test_transactions = [
            SAPGLPosting(
                document_number="TEST001",
                posting_date=date(2025, 1, 15),
                document_date=date(2025, 1, 15),
                gl_account="1000000",
                amount_local_currency=Decimal("1000000.00"),
                local_currency="SAR",
                text="Test transaction 1",
                user_name="TEST_USER",
                document_type="FB",
                profit_center="1000",
                fiscal_year=2025,
                posting_period=1
            ),
            SAPGLPosting(
                document_number="TEST002",
                posting_date=date(2025, 1, 15),
                document_date=date(2025, 1, 15),
                gl_account="1000000",
                amount_local_currency=Decimal("1000000.00"),
                local_currency="SAR",
                text="Test transaction 2",
                user_name="TEST_USER",
                document_type="FB",
                profit_center="1000",
                fiscal_year=2025,
                posting_period=1
            ),
            SAPGLPosting(
                document_number="TEST003",
                posting_date=date(2025, 1, 16),
                document_date=date(2025, 1, 15),
                gl_account="2000000",
                amount_local_currency=Decimal("500000.00"),
                local_currency="SAR",
                text="Test transaction 3",
                user_name="ANOTHER_USER",
                document_type="FB",
                profit_center="2000",
                fiscal_year=2025,
                posting_period=1
            )
        ]
        
        for transaction in test_transactions:
            transaction.save()
        
        print("Created test transactions")
        transaction_count = SAPGLPosting.objects.count()
        print(f"Now have {transaction_count} transactions")
    
    # Create a test analysis session
    session = AnalysisSession.objects.create(
        session_name="Test Analysis",
        description="Test analysis session",
        status='PENDING'
    )
    
    print(f"Created analysis session: {session.id}")
    
    # Test the analytics
    try:
        print("Running analysis...")
        result = analyzer.analyze_transactions(session)
        print(f"Analysis result: {result}")
        
        if 'error' not in result:
            print("Analysis completed successfully!")
            
            # Test getting summary
            print("Getting analysis summary...")
            summary = analyzer.get_analysis_summary(session)
            print(f"Summary keys: {list(summary.keys())}")
            
            # Test specific anomaly detection
            transactions = list(SAPGLPosting.objects.all())
            print(f"Testing anomaly detection with {len(transactions)} transactions...")
            
            duplicates = analyzer.detect_duplicate_entries(transactions)
            print(f"Found {len(duplicates)} duplicate anomalies")
            
            user_anomalies = analyzer.detect_user_anomalies(transactions)
            print(f"Found {len(user_anomalies)} user anomalies")
            
            backdated = analyzer.detect_backdated_entries(transactions)
            print(f"Found {len(backdated)} backdated entries")
            
            closing = analyzer.detect_closing_entries(transactions)
            print(f"Found {len(closing)} closing entries")
            
            unusual = analyzer.detect_unusual_days(transactions)
            print(f"Found {len(unusual)} unusual day entries")
            
            holidays = analyzer.detect_holiday_entries(transactions)
            print(f"Found {len(holidays)} holiday entries")
            
            print("All anomaly tests completed successfully!")
            
        else:
            print(f"Analysis failed: {result['error']}")
            
    except Exception as e:
        print(f"Error during analysis: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_analytics() 