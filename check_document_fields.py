#!/usr/bin/env python
"""
Check what document-related fields contain data in GL postings
"""

import os
import sys
import django

# Set up Django
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'analytics.settings')
django.setup()

from core.models import SAPGLPosting, Engagement

def check_document_fields():
    """Check document-related fields in GL postings"""
    print("🔍 Checking Document Fields in GL Postings for ENG-008")
    print("=" * 60)
    
    try:
        # Get ENG-008 engagement
        engagement = Engagement.objects.get(engagement_id='ENG-008')
        print(f"✅ Engagement: {engagement.engagement_id}")
        
        # Get sample GL postings
        gl_postings = SAPGLPosting.objects.filter(data_file__engagement=engagement)[:10]
        print(f"📊 Sample GL postings: {gl_postings.count()}")
        
        if gl_postings.count() > 0:
            # Check document-related fields
            document_fields = [
                'document_number',
                'document_type', 
                'clearing_document',
                'reference_document',
                'invoice_reference',
                'sales_document',
                'assignment'
            ]
            
            print(f"\n📋 Document Field Analysis:")
            for field in document_fields:
                # Count non-empty values
                non_empty_count = gl_postings.exclude(**{f"{field}__isnull": True}).exclude(**{f"{field}": ''}).count()
                total_count = gl_postings.count()
                percentage = (non_empty_count / total_count * 100) if total_count > 0 else 0
                
                print(f"   {field}: {non_empty_count}/{total_count} ({percentage:.1f}%)")
                
                # Show sample values
                if non_empty_count > 0:
                    sample_values = gl_postings.exclude(**{f"{field}__isnull": True}).exclude(**{f"{field}": ''}).values_list(field, flat=True).distinct()[:5]
                    print(f"      Sample values: {list(sample_values)}")
                else:
                    print(f"      All values are empty")
            
            # Show a sample GL posting
            print(f"\n📄 Sample GL Posting:")
            sample = gl_postings.first()
            print(f"   ID: {sample.id}")
            print(f"   Document Number: '{sample.document_number}'")
            print(f"   Document Type: '{sample.document_type}'")
            print(f"   Clearing Document: '{sample.clearing_document}'")
            print(f"   Reference Document: '{sample.reference_document}'")
            print(f"   Invoice Reference: '{sample.invoice_reference}'")
            print(f"   Sales Document: '{sample.sales_document}'")
            print(f"   Assignment: '{sample.assignment}'")
            print(f"   Amount: {sample.amount_local_currency}")
            print(f"   GL Account: {sample.gl_account}")
            print(f"   User: {sample.user_name}")
            print(f"   Posting Date: {sample.posting_date}")
        
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    check_document_fields()
