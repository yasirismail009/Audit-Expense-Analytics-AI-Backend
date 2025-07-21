from django.core.management.base import BaseCommand, CommandError
from django.db import transaction
from django.utils import timezone
import pandas as pd
import csv
import os
from datetime import datetime
from decimal import Decimal

from core.models import SAPGLPosting, DataFile

class Command(BaseCommand):
    help = 'Process SAP GL posting data from CSV file'

    def add_arguments(self, parser):
        parser.add_argument('csv_file', type=str, help='Path to CSV file')
        parser.add_argument(
            '--dry-run',
            action='store_true',
            help='Show what would be processed without saving to database',
        )
        parser.add_argument(
            '--batch-size',
            type=int,
            default=1000,
            help='Number of records to process in each batch',
        )
        parser.add_argument(
            '--skip-header',
            action='store_true',
            help='Skip the first row (header)',
        )

    def handle(self, *args, **options):
        csv_file = options['csv_file']
        dry_run = options['dry_run']
        batch_size = options['batch_size']
        skip_header = options['skip_header']

        if not os.path.exists(csv_file):
            raise CommandError(f'File {csv_file} does not exist')

        self.stdout.write(f'Processing file: {csv_file}')
        
        if dry_run:
            self.stdout.write(self.style.WARNING('DRY RUN MODE - No data will be saved'))

        try:
            # Create DataFile record
            file_size = os.path.getsize(csv_file)
            file_name = os.path.basename(csv_file)
            
            if not dry_run:
                data_file = DataFile.objects.create(
                    file_name=file_name,
                    file_size=file_size,
                    status='PROCESSING'
                )
            else:
                data_file = None

            # Process the file
            result = self._process_file(csv_file, data_file, dry_run, batch_size, skip_header)

            if result['success']:
                self.stdout.write(
                    self.style.SUCCESS(
                        f'Successfully processed {result["processed"]} records'
                    )
                )
                if result['failed'] > 0:
                    self.stdout.write(
                        self.style.WARNING(
                            f'Failed to process {result["failed"]} records'
                        )
                    )
                
                if not dry_run and data_file:
                    data_file.status = 'COMPLETED'
                    data_file.processed_records = result['processed']
                    data_file.failed_records = result['failed']
                    data_file.total_records = result['processed'] + result['failed']
                    data_file.processed_at = timezone.now()
                    data_file.min_date = result.get('min_date')
                    data_file.max_date = result.get('max_date')
                    data_file.min_amount = result.get('min_amount')
                    data_file.max_amount = result.get('max_amount')
                    data_file.save()
            else:
                self.stdout.write(
                    self.style.ERROR(f'Error processing file: {result["error"]}')
                )
                if not dry_run and data_file:
                    data_file.status = 'FAILED'
                    data_file.error_message = result['error']
                    data_file.processed_at = timezone.now()
                    data_file.save()

        except Exception as e:
            self.stdout.write(
                self.style.ERROR(f'Unexpected error: {str(e)}')
            )

    def _process_file(self, csv_file, data_file, dry_run, batch_size, skip_header):
        """Process CSV file and return results"""
        try:
            processed_count = 0
            failed_count = 0
            min_date = None
            max_date = None
            min_amount = None
            max_amount = None
            
            # Read CSV file
            with open(csv_file, 'r', encoding='utf-8') as file:
                csv_reader = csv.DictReader(file)
                
                # Skip header if requested
                if skip_header:
                    next(csv_reader, None)
                
                batch = []
                
                for row_num, row in enumerate(csv_reader, start=1):
                    try:
                        posting = self._create_posting_from_row(row)
                        if posting:
                            batch.append(posting)
                            processed_count += 1
                            
                            # Update date and amount ranges
                            if posting.posting_date:
                                if min_date is None or posting.posting_date < min_date:
                                    min_date = posting.posting_date
                                if max_date is None or posting.posting_date > max_date:
                                    max_date = posting.posting_date
                            
                            if min_amount is None or posting.amount_local_currency < min_amount:
                                min_amount = posting.amount_local_currency
                            if max_amount is None or posting.amount_local_currency > max_amount:
                                max_amount = posting.amount_local_currency
                        else:
                            failed_count += 1
                            
                    except Exception as e:
                        self.stdout.write(
                            self.style.WARNING(f'Error processing row {row_num}: {e}')
                        )
                        failed_count += 1
                    
                    # Process batch
                    if len(batch) >= batch_size:
                        if not dry_run:
                            self._save_batch(batch)
                        batch = []
                        
                        # Progress update
                        self.stdout.write(f'Processed {processed_count} records...')
                
                # Process remaining batch
                if batch and not dry_run:
                    self._save_batch(batch)
            
            return {
                'success': True,
                'processed': processed_count,
                'failed': failed_count,
                'min_date': min_date,
                'max_date': max_date,
                'min_amount': min_amount,
                'max_amount': max_amount
            }
            
        except Exception as e:
            return {
                'success': False,
                'error': str(e)
            }

    def _create_posting_from_row(self, row):
        """Create SAPGLPosting from CSV row"""
        try:
            # Check required fields
            required_fields = {
                'Document Number': row.get('Document Number', ''),
                'Posting Date': row.get('Posting Date', ''),
                'G/L Account': row.get('G/L Account', ''),
                'Amount in Local Currency': row.get('Amount in Local Currency', ''),
                'Local Currency': row.get('Local Currency', ''),
                'Text': row.get('Text', ''),
                'Document Date': row.get('Document Date', ''),
                'Offsetting Account': row.get('Offsetting Account', ''),
                'User Name': row.get('User Name', ''),
                'Entry Date': row.get('Entry Date', '')
            }
            
            # Validate required fields
            missing_fields = [field for field, value in required_fields.items() if not value.strip()]
            if missing_fields:
                raise ValueError(f"Missing required fields: {', '.join(missing_fields)}")
            
            # Create posting with required fields
            posting = SAPGLPosting(
                document_number=required_fields['Document Number'].strip(),
                posting_date=self._parse_date(required_fields['Posting Date']),
                gl_account=required_fields['G/L Account'].strip(),
                amount_local_currency=self._parse_amount(required_fields['Amount in Local Currency']),
                local_currency=required_fields['Local Currency'].strip() or 'SAR',
                text=required_fields['Text'].strip(),
                document_date=self._parse_date(required_fields['Document Date']),
                offsetting_account=required_fields['Offsetting Account'].strip(),
                user_name=required_fields['User Name'].strip(),
                entry_date=self._parse_date(required_fields['Entry Date'])
            )
            
            # Add optional fields if present
            optional_fields = {
                'document_type': row.get('Document type', ''),
                'profit_center': row.get('Profit Center', ''),
                'fiscal_year': row.get('Fiscal Year', ''),
                'posting_period': row.get('Posting period', ''),
                'segment': row.get('Segment', ''),
                'clearing_document': row.get('Clearing Document', ''),
                'invoice_reference': row.get('Invoice Reference', ''),
                'sales_document': row.get('Sales Document', ''),
                'assignment': row.get('Assignment', ''),
                'year_month': row.get('Year/Month', ''),
                'cost_center': row.get('Cost Center', ''),
                'wbs_element': row.get('WBS Element', ''),
                'plant': row.get('Plant', ''),
                'material': row.get('Material', ''),
                'billing_document': row.get('Billing Document', ''),
                'purchasing_document': row.get('Purchasing Document', ''),
                'order_number': row.get('Order', ''),
                'asset_number': row.get('Asset', ''),
                'network': row.get('Network', ''),
                'tax_code': row.get('Tax Code', ''),
                'account_assignment': row.get('Account Assignment', '')
            }
            
            for field, value in optional_fields.items():
                if value and value.strip():
                    if field in ['fiscal_year', 'posting_period']:
                        setattr(posting, field, self._parse_int(value))
                    else:
                        setattr(posting, field, value.strip())
            
            return posting
            
        except Exception as e:
            raise Exception(f"Error creating posting: {e}")

    def _parse_amount(self, value):
        """Parse amount value"""
        if not value:
            return Decimal('0')
        
        # Remove commas and convert to Decimal
        try:
            cleaned_value = str(value).replace(',', '').replace(' ', '')
            return Decimal(cleaned_value)
        except:
            return Decimal('0')

    def _parse_int(self, value):
        """Parse integer value"""
        if not value:
            return 0
        
        try:
            return int(str(value).strip())
        except:
            return 0

    def _parse_date(self, value):
        """Parse date value"""
        if not value:
            return None
        
        try:
            # Try different date formats
            date_formats = [
                '%m/%d/%Y',
                '%Y-%m-%d',
                '%d/%m/%Y',
                '%m-%d-%Y'
            ]
            
            for fmt in date_formats:
                try:
                    return datetime.strptime(str(value).strip(), fmt).date()
                except:
                    continue
            
            return None
        except:
            return None

    def _save_batch(self, batch):
        """Save a batch of postings to database"""
        with transaction.atomic():
            SAPGLPosting.objects.bulk_create(batch, ignore_conflicts=True) 