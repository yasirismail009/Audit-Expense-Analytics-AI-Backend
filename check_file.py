#!/usr/bin/env python
import os
import sys
import django

# Setup Django
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'analytics.settings')
django.setup()

from core.models import DataFile, OverallAnalysisResult, RiskScoringDocument

def check_file(file_id):
    print(f"=== Checking file {file_id} ===")
    
    try:
        # Check if file exists
        df = DataFile.objects.get(id=file_id)
        print(f"✅ File found: {df.file_name}")
        print(f"   Status: {df.status}")
        print(f"   Total records: {df.total_records}")
        print(f"   Processed records: {df.processed_records}")
        print(f"   Uploaded at: {df.uploaded_at}")
        print(f"   Processed at: {df.processed_at}")
        print(f"   Min date: {df.min_date}")
        print(f"   Max date: {df.max_date}")
        print(f"   Min amount: {df.min_amount}")
        print(f"   Max amount: {df.max_amount}")
        
        # Check overall analysis results
        print(f"\n=== Overall Analysis Results ===")
        overall_results = OverallAnalysisResult.objects.filter(data_file=df)
        print(f"Overall analysis count: {overall_results.count()}")
        
        if overall_results.exists():
            for overall in overall_results:
                print(f"  - ID: {overall.id}")
                print(f"    Status: {overall.status}")
                print(f"    Analysis Type: {overall.analysis_type}")
                print(f"    Analysis Date: {overall.analysis_date}")
                print(f"    Processing Duration: {overall.processing_duration}")
                print(f"    Transaction Summary: {bool(overall.transaction_summary)}")
                print(f"    Flag Summary: {bool(overall.flag_summary)}")
                print(f"    Flagged Transactions: {len(overall.flagged_transactions) if overall.flagged_transactions else 0}")
        else:
            print("  No overall analysis results found")
        
        # Check risk scoring documents
        print(f"\n=== Risk Scoring Documents ===")
        risk_docs = RiskScoringDocument.objects.filter(data_file=df)
        print(f"Risk scoring count: {risk_docs.count()}")
        
        if risk_docs.exists():
            for risk in risk_docs:
                print(f"  - ID: {risk.id}")
                print(f"    Status: {risk.status}")
                print(f"    Document Type: {risk.document_type}")
                print(f"    Document Date: {risk.document_date}")
                print(f"    Total Transactions: {risk.total_transactions}")
                print(f"    Overall Risk Score: {risk.overall_risk_score}")
        else:
            print("  No risk scoring documents found")
        
        return True
        
    except DataFile.DoesNotExist:
        print(f"❌ File with ID {file_id} not found in database")
        return False
    except Exception as e:
        print(f"❌ Error: {e}")
        return False

if __name__ == "__main__":
    file_id = "65777825-16a3-444a-970a-30e815b59b61"
    check_file(file_id)
