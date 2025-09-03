#!/usr/bin/env python3
"""
Django management command to validate and clean up duplicate analysis data.
This command ensures that all duplicate entries have both transactions and proper credit/debit consistency.
"""

from django.core.management.base import BaseCommand
from django.db import transaction
from core.models import DuplicateAnalysisResult, SAPGLPosting
import logging

logger = logging.getLogger(__name__)

class Command(BaseCommand):
    help = 'Validate and clean up duplicate analysis data'

    def add_arguments(self, parser):
        parser.add_argument(
            '--fix',
            action='store_true',
            help='Fix invalid duplicate entries by removing them',
        )
        parser.add_argument(
            '--file-id',
            type=str,
            help='Process only a specific file ID',
        )

    def handle(self, *args, **options):
        self.stdout.write("Starting duplicate data validation...")
        
        # Get duplicate analysis results
        if options['file_id']:
            duplicate_results = DuplicateAnalysisResult.objects.filter(
                data_file_id=options['file_id'],
                status='COMPLETED'
            )
        else:
            duplicate_results = DuplicateAnalysisResult.objects.filter(status='COMPLETED')
        
        total_duplicates = 0
        invalid_duplicates = 0
        fixed_duplicates = 0
        
        for duplicate_result in duplicate_results:
            self.stdout.write(f"Processing duplicate analysis: {duplicate_result.id}")
            
            if not duplicate_result.duplicate_list:
                self.stdout.write("  No duplicate list found, skipping...")
                continue
            
            # Validate each duplicate entry
            valid_duplicates = []
            for dup in duplicate_result.duplicate_list:
                total_duplicates += 1
                
                # Check if both transactions exist
                if not dup.get('transaction1') or not dup.get('transaction2'):
                    self.stdout.write(f"  Invalid duplicate: missing transactions - {dup}")
                    invalid_duplicates += 1
                    continue
                
                # Check if transactions are not None
                if dup['transaction1'] is None or dup['transaction2'] is None:
                    self.stdout.write(f"  Invalid duplicate: None transactions - {dup}")
                    invalid_duplicates += 1
                    continue
                
                # Check credit/debit consistency
                transaction1_type = dup['transaction1'].get('transaction_type', 'DEBIT')
                transaction2_type = dup['transaction2'].get('transaction_type', 'DEBIT')
                
                if transaction1_type != transaction2_type:
                    self.stdout.write(f"  Inconsistent credit/debit: {transaction1_type} vs {transaction2_type}")
                    invalid_duplicates += 1
                    continue
                
                # Valid duplicate
                valid_duplicates.append(dup)
            
            # Update the duplicate result if fixing is enabled
            if options['fix'] and len(valid_duplicates) != len(duplicate_result.duplicate_list):
                with transaction.atomic():
                    duplicate_result.duplicate_list = valid_duplicates
                    duplicate_result.save()
                    fixed_duplicates += 1
                    self.stdout.write(f"  Fixed duplicate analysis: {len(valid_duplicates)} valid duplicates out of {len(duplicate_result.duplicate_list)}")
        
        # Summary
        self.stdout.write("\n" + "=" * 50)
        self.stdout.write("DUPLICATE DATA VALIDATION SUMMARY")
        self.stdout.write("=" * 50)
        self.stdout.write(f"Total duplicate entries processed: {total_duplicates}")
        self.stdout.write(f"Invalid duplicate entries found: {invalid_duplicates}")
        self.stdout.write(f"Duplicate analyses fixed: {fixed_duplicates}")
        
        if invalid_duplicates > 0:
            self.stdout.write(f"\n⚠️  {invalid_duplicates} invalid duplicate entries found!")
            if not options['fix']:
                self.stdout.write("Run with --fix to automatically clean up invalid entries")
        else:
            self.stdout.write("\n✅ All duplicate entries are valid!")
        
        self.stdout.write("\nValidation completed successfully!")
