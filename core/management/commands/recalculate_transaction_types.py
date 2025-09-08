#!/usr/bin/env python3
"""
Django management command to recalculate transaction types for existing SAPGLPosting records.
This command uses the new logic that considers both account type and amount sign.
"""

from django.core.management.base import BaseCommand
from django.db import transaction
from core.models import SAPGLPosting
from django.utils import timezone
import logging

logger = logging.getLogger(__name__)

class Command(BaseCommand):
    help = 'Recalculate transaction types for existing SAPGLPosting records using new logic'

    def add_arguments(self, parser):
        parser.add_argument(
            '--batch-size',
            type=int,
            default=1000,
            help='Number of records to process in each batch (default: 1000)'
        )
        parser.add_argument(
            '--dry-run',
            action='store_true',
            help='Show what would be changed without making changes'
        )
        parser.add_argument(
            '--file-id',
            type=int,
            help='Only process records from a specific data file ID'
        )

    def handle(self, *args, **options):
        batch_size = options['batch_size']
        dry_run = options['dry_run']
        file_id = options.get('file_id')

        self.stdout.write(
            self.style.SUCCESS(
                f"Starting transaction type recalculation (batch size: {batch_size}, dry run: {dry_run})"
            )
        )

        # Build query
        queryset = SAPGLPosting.objects.all()
        if file_id:
            queryset = queryset.filter(data_file_id=file_id)
            self.stdout.write(f"Filtering to data file ID: {file_id}")

        total_records = queryset.count()
        self.stdout.write(f"Total records to process: {total_records}")

        if total_records == 0:
            self.stdout.write(self.style.WARNING("No records found to process"))
            return

        # Process in batches
        updated_count = 0
        unchanged_count = 0
        error_count = 0

        for offset in range(0, total_records, batch_size):
            batch = queryset[offset:offset + batch_size]
            
            self.stdout.write(f"Processing batch {offset//batch_size + 1} ({offset + 1}-{min(offset + batch_size, total_records)})")
            
            for posting in batch:
                try:
                    old_type = posting.transaction_type
                    new_type = posting.get_proper_transaction_type()
                    
                    if old_type != new_type:
                        if not dry_run:
                            posting.transaction_type = new_type
                            posting.save(update_fields=['transaction_type'])
                        
                        updated_count += 1
                        self.stdout.write(
                            f"  Updated {posting.id}: {old_type} -> {new_type} "
                            f"(Account: {posting.gl_account}, Amount: {posting.amount_local_currency})"
                        )
                    else:
                        unchanged_count += 1
                        
                except Exception as e:
                    error_count += 1
                    self.stdout.write(
                        self.style.ERROR(f"  Error processing {posting.id}: {e}")
                    )

        # Summary
        self.stdout.write("\n" + "="*50)
        self.stdout.write("RECALCULATION SUMMARY")
        self.stdout.write("="*50)
        self.stdout.write(f"Total records processed: {total_records}")
        self.stdout.write(f"Records updated: {updated_count}")
        self.stdout.write(f"Records unchanged: {unchanged_count}")
        self.stdout.write(f"Errors: {error_count}")
        
        if dry_run:
            self.stdout.write(
                self.style.WARNING(
                    "\nDRY RUN MODE - No changes were made to the database"
                )
            )
        else:
            self.stdout.write(
                self.style.SUCCESS(
                    f"\nSuccessfully updated {updated_count} transaction types"
                )
            )

        if error_count > 0:
            self.stdout.write(
                self.style.ERROR(
                    f"\nEncountered {error_count} errors during processing"
                )
            )
