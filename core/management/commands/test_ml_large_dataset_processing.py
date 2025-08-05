#!/usr/bin/env python3
"""
Django management command to test ML-integrated large dataset processing
"""

import os
import django
import time
import logging
from datetime import datetime, timedelta
from decimal import Decimal
from django.core.management.base import BaseCommand, CommandError
from django.utils import timezone
from django.db import transaction
from django.core.cache import cache

# Setup Django
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'analytics.settings')
django.setup()

from core.models import DataFile, SAPGLPosting
from core.large_dataset_optimizer import LargeDatasetOptimizer, LargeDatasetMonitor

logger = logging.getLogger(__name__)

class Command(BaseCommand):
    help = 'Test ML-integrated large dataset processing with synthetic data'
    
    def add_arguments(self, parser):
        parser.add_argument(
            '--records',
            type=int,
            default=38000,
            help='Number of synthetic records to generate (default: 38000)'
        )
        parser.add_argument(
            '--workers',
            type=int,
            default=4,
            help='Number of parallel workers (default: 4)'
        )
        parser.add_argument(
            '--batch-size',
            type=int,
            default=2000,
            help='Batch size for processing (default: 2000)'
        )
        parser.add_argument(
            '--memory-limit',
            type=int,
            default=1000,
            help='Memory limit in MB (default: 1000)'
        )
        parser.add_argument(
            '--generate-data',
            action='store_true',
            help='Generate synthetic test data'
        )
        parser.add_argument(
            '--run-optimization',
            action='store_true',
            help='Run the ML-integrated optimization'
        )
        parser.add_argument(
            '--monitor',
            action='store_true',
            help='Monitor system resources during processing'
        )
        parser.add_argument(
            '--data-file-id',
            type=str,
            help='Specific data file ID to process'
        )
    
    def handle(self, *args, **options):
        self.stdout.write(
            self.style.SUCCESS('=== ML-Integrated Large Dataset Processing Test ===')
        )
        
        records_count = options['records']
        max_workers = options['workers']
        batch_size = options['batch_size']
        memory_limit = options['memory_limit']
        
        # Generate synthetic data if requested
        data_file_id = options['data_file_id']
        if options['generate_data'] or not data_file_id:
            data_file_id = self._generate_synthetic_data(records_count)
        
        if not data_file_id:
            raise CommandError("No data file available for processing")
        
        self.stdout.write(f"Using data file ID: {data_file_id}")
        
        # Run optimization if requested
        if options['run_optimization']:
            self._run_ml_optimization(data_file_id, max_workers, batch_size, memory_limit)
        
        # Monitor if requested
        if options['monitor']:
            self._monitor_processing(data_file_id)
        
        self.stdout.write(
            self.style.SUCCESS('=== Test completed ===')
        )
    
    def _generate_synthetic_data(self, records_count: int) -> str:
        """Generate synthetic test data"""
        self.stdout.write(f"Generating {records_count} synthetic records...")
        
        # Create a test data file
        data_file = DataFile.objects.create(
            file_name=f"test_ml_large_dataset_{timezone.now().strftime('%Y%m%d_%H%M%S')}.csv",
            upload_date=timezone.now(),
            fiscal_year=2024,
            audit_start_date=datetime(2024, 1, 1).date(),
            audit_end_date=datetime(2024, 12, 31).date(),
            total_transactions=records_count,
            file_size_mb=records_count * 0.001  # Approximate size
        )
        
        # Generate synthetic transactions
        transactions_to_create = []
        
        # Sample data for realistic testing
        accounts = [f"1000{i:03d}" for i in range(1, 101)]  # 100 accounts
        users = [f"USER_{i:03d}" for i in range(1, 51)]     # 50 users
        document_numbers = [f"DOC_{i:06d}" for i in range(1, records_count + 1)]
        
        start_date = datetime(2024, 1, 1)
        
        for i in range(records_count):
            # Create some anomalies for testing
            is_duplicate = i % 100 == 0  # Every 100th record is a duplicate
            is_backdated = i % 200 == 0  # Every 200th record is backdated
            is_weekend = i % 300 == 0    # Every 300th record is on weekend
            is_holiday = i % 500 == 0    # Every 500th record is on holiday
            is_closing = i % 400 == 0    # Every 400th record is closing entry
            
            # Calculate posting date
            days_offset = i % 365
            posting_date = start_date + timedelta(days=days_offset)
            
            # Create backdated entries
            if is_backdated:
                document_date = posting_date - timedelta(days=30)
            else:
                document_date = posting_date
            
            # Create weekend entries
            if is_weekend:
                # Force to weekend (Friday or Saturday)
                while posting_date.weekday() not in [4, 5]:  # Friday=4, Saturday=5
                    posting_date += timedelta(days=1)
            
            # Create holiday entries (simplified - just use specific dates)
            if is_holiday:
                posting_date = datetime(2024, 1, 1).date()  # New Year's Day
            
            # Create closing entries (month-end)
            if is_closing:
                posting_date = datetime(2024, posting_date.month, 28).date()  # Near month-end
            
            transaction = SAPGLPosting(
                data_file=data_file,
                document_number=document_numbers[i],
                document_date=document_date,
                posting_date=posting_date,
                gl_account=accounts[i % len(accounts)],
                amount_local_currency=Decimal(str(1000 + (i % 10000))),
                transaction_type='DEBIT' if i % 2 == 0 else 'CREDIT',
                user_name=users[i % len(users)],
                text=f"Test transaction {i+1}",
                fiscal_year=2024,
                posting_period=posting_date.month,
                source="SYNTHETIC_TEST",
                is_duplicate=is_duplicate,
                is_backdated=is_backdated,
                overall_risk_score=0.0,
                anomaly_types=[]
            )
            
            transactions_to_create.append(transaction)
            
            # Progress update
            if (i + 1) % 5000 == 0:
                self.stdout.write(f"Generated {i + 1} records...")
        
        # Bulk create transactions
        self.stdout.write("Saving transactions to database...")
        with transaction.atomic():
            SAPGLPosting.objects.bulk_create(transactions_to_create, batch_size=1000)
        
        self.stdout.write(
            self.style.SUCCESS(f"Successfully generated {records_count} synthetic records")
        )
        
        return str(data_file.id)
    
    def _run_ml_optimization(self, data_file_id: str, max_workers: int, batch_size: int, memory_limit: int):
        """Run ML-integrated optimization"""
        self.stdout.write("Starting ML-integrated large dataset optimization...")
        
        # Initialize optimizer
        optimizer = LargeDatasetOptimizer(
            data_file_id=data_file_id,
            max_workers=max_workers,
            batch_size=batch_size,
            memory_limit_mb=memory_limit
        )
        
        # Run optimization
        start_time = time.time()
        result = optimizer.optimize_for_large_dataset()
        end_time = time.time()
        
        # Display results
        self.stdout.write("\n=== Optimization Results ===")
        self.stdout.write(f"Success: {result.get('success', False)}")
        
        if result.get('success'):
            stats = result.get('stats', {})
            self.stdout.write(f"Total Records: {stats.get('total_records', 0)}")
            self.stdout.write(f"Processed Records: {stats.get('processed_records', 0)}")
            self.stdout.write(f"Failed Records: {stats.get('failed_records', 0)}")
            self.stdout.write(f"Success Rate: {stats.get('processed_records', 0) / stats.get('total_records', 1) * 100:.2f}%")
            self.stdout.write(f"Processing Time: {stats.get('processing_time_seconds', 0):.2f} seconds")
            self.stdout.write(f"Records per Second: {stats.get('processed_records', 0) / stats.get('processing_time_seconds', 1):.2f}")
            self.stdout.write(f"Peak Memory: {stats.get('memory_peak_mb', 0):.2f} MB")
            self.stdout.write(f"Batches Processed: {stats.get('batches_processed', 0)}")
            self.stdout.write(f"ML Optimized Analyses: {stats.get('ml_optimized_analyses', 0)}")
            
            # Performance metrics
            total_time = end_time - start_time
            self.stdout.write(f"\nTotal Execution Time: {total_time:.2f} seconds")
            
            if stats.get('total_records', 0) > 0:
                throughput = stats.get('processed_records', 0) / total_time
                self.stdout.write(f"Overall Throughput: {throughput:.2f} records/second")
            
            # ML integration status
            if result.get('ml_integration'):
                self.stdout.write(
                    self.style.SUCCESS("✓ ML integration active")
                )
            else:
                self.stdout.write(
                    self.style.WARNING("⚠ ML integration not active")
                )
        
        else:
            self.stdout.write(
                self.style.ERROR(f"Optimization failed: {result.get('error', 'Unknown error')}")
            )
    
    def _monitor_processing(self, data_file_id: str):
        """Monitor processing status and system resources"""
        self.stdout.write("Monitoring processing status...")
        
        monitor = LargeDatasetMonitor(data_file_id)
        
        # Get processing status
        status = monitor.get_processing_status()
        self.stdout.write(f"Status: {status.get('status', 'UNKNOWN')}")
        
        if status.get('start_time'):
            self.stdout.write(f"Start Time: {status['start_time']}")
        
        if status.get('performance'):
            perf = status['performance']
            self.stdout.write(f"Performance: {perf.get('records_per_second', 0):.2f} records/sec")
            self.stdout.write(f"Memory Peak: {perf.get('memory_peak_mb', 0):.2f} MB")
            self.stdout.write(f"Success Rate: {perf.get('success_rate', 0):.2f}%")
        
        # Get current system resources
        resources = monitor.get_system_resources()
        self.stdout.write(f"CPU Usage: {resources.get('cpu_percent', 0):.1f}%")
        self.stdout.write(f"Memory Usage: {resources.get('memory', {}).get('percent', 0):.1f}%")
        self.stdout.write(f"Disk Usage: {resources.get('disk_usage', 0):.1f}%")
    
    def _display_system_info(self):
        """Display system information"""
        import psutil
        
        self.stdout.write("\n=== System Information ===")
        self.stdout.write(f"CPU Cores: {psutil.cpu_count()}")
        self.stdout.write(f"Total Memory: {psutil.virtual_memory().total / (1024**3):.2f} GB")
        self.stdout.write(f"Available Memory: {psutil.virtual_memory().available / (1024**3):.2f} GB")
        self.stdout.write(f"Disk Space: {psutil.disk_usage('/').free / (1024**3):.2f} GB free")
        
        # Django database info
        from django.db import connection
        with connection.cursor() as cursor:
            cursor.execute("SELECT version();")
            db_version = cursor.fetchone()[0]
            self.stdout.write(f"Database: {db_version}")
        
        # Cache info
        cache_info = cache.get('test_key', 'Not available')
        if cache_info == 'Not available':
            cache.set('test_key', 'Available', timeout=10)
            cache_info = 'Available'
        self.stdout.write(f"Cache: {cache_info}") 