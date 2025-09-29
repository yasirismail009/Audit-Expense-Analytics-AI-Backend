#!/usr/bin/env python3
"""
Check GL postings document numbers for ENG-008
"""

import sys
import os
sys.path.append('/app')
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'analytics.settings')

import django
django.setup()

from core.models import SAPGLPosting

# Check GL postings for ENG-008
gl_postings = SAPGLPosting.objects.filter(data_file__engagement__engagement_id='ENG-008')[:5]
print(f'Found {gl_postings.count()} GL postings for ENG-008')
print('\nFirst 5 GL postings:')
for posting in gl_postings:
    print(f'ID: {posting.id}')
    print(f'Document Number: "{posting.document_number}"')
    print(f'Document Type: "{posting.document_type}"')
    print(f'Clearing Document: "{posting.clearing_document}"')
    print(f'Reference Document: "{posting.reference_document}"')
    print(f'Invoice Reference: "{posting.invoice_reference}"')
    print('---')
