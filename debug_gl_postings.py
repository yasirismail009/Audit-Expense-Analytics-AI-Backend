#!/usr/bin/env python
"""
Debug GL postings for ENG-008
"""

import os
import sys
import django

# Set up Django
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'analytics.settings')
django.setup()

from core.models import Engagement, DataFile, SAPGLPosting

def debug_gl_postings():
    """Debug GL postings for ENG-008"""
    print("🔍 Debugging GL Postings for ENG-008")
    print("=" * 50)
    
    try:
        # Check engagement
        engagement = Engagement.objects.get(engagement_id='ENG-008')
        print(f"✅ Engagement: {engagement.engagement_id}")
        
        # Check data files
        data_files = DataFile.objects.filter(engagement=engagement)
        print(f"📁 Data files: {data_files.count()}")
        for df in data_files:
            print(f"   - {df.file_name} (Type: {df.file_type})")
        
        # Check all GL postings for this engagement
        all_gl_postings = SAPGLPosting.objects.filter(data_file__engagement=engagement)
        print(f"\n📊 Total GL postings: {all_gl_postings.count()}")
        
        if all_gl_postings.count() > 0:
            # Check document numbers
            with_doc_numbers = all_gl_postings.exclude(document_number='').exclude(document_number__isnull=True)
            print(f"📄 GL postings with document numbers: {with_doc_numbers.count()}")
            
            if with_doc_numbers.count() > 0:
                # Show sample document numbers
                sample_docs = with_doc_numbers.values_list('document_number', flat=True).distinct()[:5]
                print(f"   Sample document numbers: {list(sample_docs)}")
            else:
                # Check what document numbers exist
                all_doc_numbers = all_gl_postings.values_list('document_number', flat=True).distinct()[:10]
                print(f"   All document numbers (including empty): {list(all_doc_numbers)}")
                
                # Check for empty strings vs null
                empty_strings = all_gl_postings.filter(document_number='').count()
                null_values = all_gl_postings.filter(document_number__isnull=True).count()
                print(f"   Empty strings: {empty_strings}")
                print(f"   Null values: {null_values}")
        else:
            print("❌ No GL postings found for ENG-008")
            
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    debug_gl_postings()
