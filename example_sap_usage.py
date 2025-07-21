#!/usr/bin/env python3
"""
Example usage of the SAP GL Posting Analysis System

This script demonstrates how to:
1. Create sample SAP GL posting data
2. Process the data
3. Run analysis
4. Retrieve results via API
"""

import os
import sys
import django
import pandas as pd
from datetime import datetime, timedelta
import random
from decimal import Decimal
from django.db.models import Sum, Count

# Setup Django
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'analytics.settings')
django.setup()

from core.models import SAPGLPosting, AnalysisSession, TransactionAnalysis
from core.analytics import SAPGLAnalyzer

def create_sample_sap_data():
    """Create sample SAP GL posting data for testing"""
    
    # Sample data
    document_types = ['DZ', 'SA', 'TR', 'AB', 'DR', 'CR']
    gl_accounts = ['124130', '232000', '221100', '236000', '720001', '124010']
    profit_centers = ['1003', '9999', '2001', '3001', '4001']
    users = ['M.ALJOHA', 'A.MOHAMAD', 'A.ALQASIR', 'K.ZAKY', 'A.ALAKHR.', 'Y.ADEL']
    segments = ['14100000', '210000090', '330000010', '440000020']
    
    data = []
    
    # Generate 100 sample SAP GL postings
    for i in range(100):
        # Normal transactions (80%)
        if i < 80:
            amount = random.uniform(1000, 500000)
            posting_date = datetime.now() - timedelta(days=random.randint(1, 30))
        # High-value transactions (20%)
        else:
            amount = random.uniform(1000000, 50000000)  # 1M to 50M SAR
            posting_date = datetime.now() - timedelta(days=random.randint(1, 30))
        
        # Create document number
        doc_number = f"{random.randint(100000000, 999999999)}"
        
        posting = {
            'Document Number': doc_number,
            'Posting Date': posting_date.strftime('%m/%d/%Y'),
            'G/L Account': random.choice(gl_accounts),
            'Amount in Local Currency': round(amount, 2),
            'Local Currency': 'SAR',
            'Text': f'Sample transaction {i+1}',
            'Document Date': posting_date.strftime('%m/%d/%Y'),
            'Offsetting Account': random.choice(gl_accounts),
            'User Name': random.choice(users),
            'Entry Date': posting_date.strftime('%m/%d/%Y'),
            # Optional fields
            'Document type': random.choice(document_types),
            'Profit Center': random.choice(profit_centers),
            'Fiscal Year': 2025,
            'Posting period': 1,
            'Segment': random.choice(segments),
            'Year/Month': '2025/01'
        }
        
        # Add some Arabic text for some transactions
        if random.random() < 0.3:
            arabic_texts = ['تحصيل مستث', 'اقفال ضريبة', 'مدفوعات رواتب', 'مصروفات تشغيلية']
            posting['text'] = f"{posting['text']} - {random.choice(arabic_texts)}"
        
        data.append(posting)
    
    return data

def save_sample_csv():
    """Save sample data to CSV file"""
    data = create_sample_sap_data()
    df = pd.DataFrame(data)
    
    filename = 'sample_sap_data.csv'
    df.to_csv(filename, index=False)
    print(f"Sample SAP data saved to {filename}")
    return filename

def process_data_via_command(csv_file):
    """Process data using management command"""
    from django.core.management import call_command
    
    print(f"Processing SAP data from {csv_file}...")
    call_command('process_sap_data', csv_file)
    print("Data processing complete!")

def create_analysis_session():
    """Create an analysis session"""
    session = AnalysisSession.objects.create(
        session_name="Sample Analysis Session",
        description="Analysis of sample SAP GL posting data",
        date_from=datetime.now().date() - timedelta(days=30),
        date_to=datetime.now().date(),
        min_amount=Decimal('1000'),
        max_amount=Decimal('50000000')
    )
    print(f"Created analysis session: {session.id}")
    return session

def run_analysis(session):
    """Run analysis on the session"""
    analyzer = SAPGLAnalyzer()
    result = analyzer.analyze_transactions(session)
    
    if 'error' in result:
        print(f"Analysis failed: {result['error']}")
    else:
        print(f"Analysis completed successfully!")
        print(f"  Total transactions: {result['total_transactions']}")
        print(f"  Flagged transactions: {result['flagged_transactions']}")
        print(f"  High-value transactions: {result['high_value_transactions']}")
        print(f"  Anomalies detected: {result['anomalies_detected']}")

def show_analysis_results():
    """Display analysis results"""
    print("\n=== ANALYSIS RESULTS ===")
    
    # Get latest session
    try:
        session = AnalysisSession.objects.latest('created_at')
        print(f"Session ID: {session.id}")
        print(f"Session Name: {session.session_name}")
        print(f"Total transactions: {session.total_transactions}")
        print(f"Flagged transactions: {session.flagged_transactions}")
        print(f"Flag rate: {(session.flagged_transactions/session.total_transactions*100):.1f}%" if session.total_transactions > 0 else "N/A")
    except AnalysisSession.DoesNotExist:
        print("No analysis sessions found")
        return
    
    # Show top 5 highest risk transactions
    print("\n=== TOP 5 HIGHEST RISK TRANSACTIONS ===")
    high_risk = TransactionAnalysis.objects.filter(
        risk_level__in=['HIGH', 'CRITICAL']
    ).order_by('-risk_score')[:5]
    
    for analysis in high_risk:
        transaction = analysis.transaction
        print(f"Document: {transaction.document_number}")
        print(f"  Amount: {transaction.amount_local_currency} {transaction.local_currency}")
        print(f"  User: {transaction.user_name}")
        print(f"  Risk Score: {analysis.risk_score:.1f}")
        print(f"  Risk Level: {analysis.risk_level}")
        print(f"  Anomalies: {[k for k, v in analysis.__dict__.items() if k.endswith('_anomaly') and v]}")
        print()

def show_statistics():
    """Show system statistics"""
    print("\n=== SYSTEM STATISTICS ===")
    
    # Basic stats
    total_transactions = SAPGLPosting.objects.count()
    total_amount = SAPGLPosting.objects.aggregate(
        total=Sum('amount_local_currency')
    )['total'] or Decimal('0')
    
    print(f"Total transactions: {total_transactions}")
    print(f"Total amount: {total_amount:,.2f} SAR")
    print(f"Average amount: {(total_amount/total_transactions):,.2f} SAR" if total_transactions > 0 else "N/A")
    
    # High-value transactions
    high_value_count = SAPGLPosting.objects.filter(
        amount_local_currency__gt=1000000
    ).count()
    print(f"High-value transactions (>1M SAR): {high_value_count}")
    
    # User statistics
    unique_users = SAPGLPosting.objects.values('user_name').distinct().count()
    print(f"Unique users: {unique_users}")
    
    # Account statistics
    unique_accounts = SAPGLPosting.objects.values('gl_account').distinct().count()
    print(f"Unique G/L accounts: {unique_accounts}")
    
    # Top users by amount
    print("\n=== TOP 5 USERS BY TOTAL AMOUNT ===")
    top_users = SAPGLPosting.objects.values('user_name').annotate(
        total_amount=Sum('amount_local_currency'),
        transaction_count=Count('id')
    ).order_by('-total_amount')[:5]
    
    for user in top_users:
        print(f"{user['user_name']}: {user['total_amount']:,.2f} SAR ({user['transaction_count']} transactions)")

def main():
    """Main function to demonstrate the system"""
    print("=== SAP GL Posting Analysis System Demo ===\n")
    
    # Step 1: Create sample data
    print("1. Creating sample SAP GL posting data...")
    csv_file = save_sample_csv()
    
    # Step 2: Process data
    print("\n2. Processing SAP GL data...")
    process_data_via_command(csv_file)
    
    # Step 3: Show statistics
    print("\n3. Displaying system statistics...")
    show_statistics()
    
    # Step 4: Create and run analysis
    print("\n4. Creating analysis session...")
    session = create_analysis_session()
    
    print("\n5. Running analysis...")
    run_analysis(session)
    
    # Step 5: Show results
    print("\n6. Displaying analysis results...")
    show_analysis_results()
    
    print("\n=== Demo Complete ===")
    print("\nTo get detailed analysis for a specific transaction, use:")
    print("curl http://localhost:8000/api/postings/{transaction_id}/")
    print("\nTo get analysis results, use:")
    print("curl http://localhost:8000/api/analyses/?session_id={session_id}")
    
    # Clean up
    if os.path.exists(csv_file):
        os.remove(csv_file)
        print(f"\nCleaned up {csv_file}")

if __name__ == '__main__':
    main() 