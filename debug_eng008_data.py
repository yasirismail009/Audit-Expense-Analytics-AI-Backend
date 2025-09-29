#!/usr/bin/env python
"""
Debug script to check ENG-008 data for document verifications API
"""

import os
import sys
import django

# Set up Django
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'analytics.settings')
django.setup()

from core.models import (
    Engagement,
    DataFile, 
    SAPGLPosting,
    CompletenessTestResult
)

def debug_eng008_data():
    """Debug ENG-008 data for document verifications"""
    print("🔍 Debugging ENG-008 Data for Document Verifications")
    print("=" * 60)
    
    try:
        # Check if ENG-008 engagement exists
        try:
            engagement = Engagement.objects.get(engagement_id='ENG-008')
            print(f"✅ Engagement found: {engagement.engagement_id} - {engagement.engagement_name}")
        except Engagement.DoesNotExist:
            print("❌ ENG-008 engagement not found")
            return
        
        # Check data files
        data_files = DataFile.objects.filter(engagement=engagement)
        print(f"\n📁 Data Files ({data_files.count()}):")
        for df in data_files:
            print(f"   📄 {df.file_name} (Type: {df.file_type}, Status: {df.status})")
        
        # Check GL postings with document numbers
        gl_postings = SAPGLPosting.objects.filter(
            data_file__engagement=engagement
        ).exclude(document_number='').exclude(document_number__isnull=True)
        
        print(f"\n📊 GL Postings with Document Numbers: {gl_postings.count()}")
        
        if gl_postings.count() > 0:
            # Show sample document numbers
            sample_docs = gl_postings.values_list('document_number', flat=True).distinct()[:10]
            print(f"   Sample document numbers: {list(sample_docs)}")
            
            # Check document analysis
            document_analysis = {}
            for posting in gl_postings[:100]:  # Limit to first 100 for analysis
                doc_number = posting.document_number
                if doc_number not in document_analysis:
                    document_analysis[doc_number] = {
                        'debit_total': 0,
                        'credit_total': 0,
                        'transaction_count': 0
                    }
                
                amount = float(posting.amount_local_currency or 0)
                if amount > 0:
                    document_analysis[doc_number]['debit_total'] += amount
                else:
                    document_analysis[doc_number]['credit_total'] += abs(amount)
                
                document_analysis[doc_number]['transaction_count'] += 1
            
            print(f"\n📋 Document Analysis (first 100 postings):")
            print(f"   Unique documents: {len(document_analysis)}")
            
            # Show sample document analysis
            for i, (doc_num, analysis) in enumerate(list(document_analysis.items())[:5]):
                net_balance = analysis['debit_total'] - analysis['credit_total']
                is_balanced = abs(net_balance) < 0.01
                print(f"   📄 {doc_num}: Debit={analysis['debit_total']:.2f}, Credit={analysis['credit_total']:.2f}, Net={net_balance:.2f}, Balanced={is_balanced}")
        else:
            print("   ❌ No GL postings with document numbers found")
        
        # Check completeness test results
        completeness_results = CompletenessTestResult.objects.filter(engagement=engagement)
        print(f"\n🧪 Completeness Test Results: {completeness_results.count()}")
        
        if completeness_results.count() > 0:
            latest_result = completeness_results.order_by('-test_timestamp').first()
            print(f"   Latest test: {latest_result.id} at {latest_result.test_timestamp}")
            print(f"   Status: {latest_result.status}")
        else:
            print("   ❌ No completeness test results found")
        
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    debug_eng008_data()
