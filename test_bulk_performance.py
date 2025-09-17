#!/usr/bin/env python
"""
Performance Test Script for Optimized Bulk Data Insertion

This script tests the performance improvements for large dataset processing.
It simulates processing 300K+ records and measures the time taken.

Usage:
    python test_bulk_performance.py

Expected Results:
    - Original: ~1.5 hours (5400 seconds) for 300K records
    - Optimized: ~5-10 minutes (300-600 seconds) for 300K records
    - Performance improvement: 10x-18x faster
"""

import os
import sys
import django
import time
import logging
from decimal import Decimal
from datetime import datetime, date
import tempfile
import pandas as pd

# Setup Django
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'analytics.settings')
django.setup()

from core.models import DataFile, SAPGLPosting, Engagement, Client
from core.file_processing_utils import DataProcessor, FileReader
from users.models import User

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def create_test_data_file(num_records=300000):
    """Create a test CSV file with specified number of records"""
    logger.info(f"Creating test CSV file with {num_records:,} records...")
    
    # Create test CSV data
    data = []
    for i in range(num_records):
        data.append({
            'Document': f'{1000000 + i}',
            'Document type': 'SA' if i % 2 == 0 else 'DR',
            'Posting Date': '1/15/2025',
            'Document Date': '1/15/2025',
            'Entry Date': '1/15/2025',
            'G/L Account': f'{110000 + (i % 1000)}',
            'Amount in Local Currency': round(Decimal(str(100 + (i % 10000))), 2),
            'Local Currency': 'SAR',
            'Profit Center': f'PC{1000 + (i % 100)}',
            'User Name': f'USER{(i % 50) + 1:03d}',
            'Fiscal Year': 2025,
            'Posting period': (i % 12) + 1,
            'Text': f'Test transaction {i}',
            'Segment': f'SEG{(i % 10) + 1}',
            'Clearing Document': '',
            'Offsetting': f'{2000000 + i}' if i % 5 == 0 else '',
            'Invoice Reference': f'{3000000 + i}' if i % 10 == 0 else '',
            'Sales Document': '',
            'Assignment': f'ASSIGN{i % 100}',
            'Year/Month': '2025/01'
        })
    
    # Create DataFrame and save to temporary CSV
    df = pd.DataFrame(data)
    temp_file = tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False)
    df.to_csv(temp_file.name, index=False)
    temp_file.close()
    
    logger.info(f"✅ Test CSV created: {temp_file.name} ({num_records:,} records)")
    return temp_file.name


def setup_test_environment():
    """Setup test client, engagement, and data file"""
    logger.info("Setting up test environment...")
    
    # Create test user if needed
    user, created = User.objects.get_or_create(
        username='test_performance_user',
        defaults={
            'email': 'test@performance.com',
            'first_name': 'Performance',
            'last_name': 'Test'
        }
    )
    
    # Create test client
    client, created = Client.objects.get_or_create(
        client_name='Performance Test Client',
        defaults={
            'contact_person': 'Test Contact',
            'email': 'test@client.com',
            'phone': '+1234567890',
            'address': 'Test Address'
        }
    )
    
    # Create test engagement
    engagement, created = Engagement.objects.get_or_create(
        engagement_id='PERF_TEST_2025',
        defaults={
            'client': client,
            'engagement_name': 'Performance Test Engagement',
            'fiscal_year': 2025,
            'start_date': date(2025, 1, 1),
            'end_date': date(2025, 12, 31),
            'status': 'ACTIVE'
        }
    )
    
    # Create test data file
    data_file = DataFile.objects.create(
        engagement=engagement,
        file_name='performance_test_gl.csv',
        file_type='GL_LIST',
        file_size=50 * 1024 * 1024,  # 50MB estimated
        status='PENDING'
    )
    
    logger.info(f"✅ Test environment ready - DataFile ID: {data_file.id}")
    return data_file


def run_performance_test(num_records=300000, use_optimized=True):
    """Run the performance test"""
    logger.info("🚀" + "="*80)
    logger.info("🚀 BULK DATA INSERTION PERFORMANCE TEST")
    logger.info(f"📊 Records to process: {num_records:,}")
    logger.info(f"⚡ Using optimized method: {use_optimized}")
    logger.info("🚀" + "="*80)
    
    # Setup test environment
    data_file = setup_test_environment()
    
    # Create test data file
    csv_file_path = create_test_data_file(num_records)
    
    try:
        # Read the CSV file
        logger.info("📖 Reading CSV file...")
        read_start = time.time()
        file_reader = FileReader()
        df = file_reader.read_file(csv_file_path)
        read_duration = time.time() - read_start
        logger.info(f"✅ CSV read completed in {read_duration:.2f} seconds")
        
        # Create processor and test data processing
        processor = DataProcessor(data_file)
        
        if use_optimized:
            # Use optimized batch size
            processor.batch_size = 10000
            logger.info("⚡ Using OPTIMIZED bulk processing method")
        else:
            # Use old batch size
            processor.batch_size = 200
            logger.info("🐌 Using LEGACY individual save method")
        
        # Start performance test
        logger.info("🏁 Starting data processing performance test...")
        process_start = time.time()
        
        # Process the data
        result = processor.process_gl_data(df)
        
        process_duration = time.time() - process_start
        total_duration = read_duration + process_duration
        
        # Calculate performance metrics
        records_per_second = result['processed_count'] / process_duration if process_duration > 0 else 0
        total_records_per_second = result['processed_count'] / total_duration if total_duration > 0 else 0
        
        # Log results
        logger.info("🏆" + "="*80)
        logger.info("🏆 PERFORMANCE TEST RESULTS")
        logger.info(f"✅ Records processed: {result['processed_count']:,}")
        logger.info(f"❌ Records failed: {result['failed_count']:,}")
        logger.info(f"⏱️  CSV read time: {read_duration:.2f} seconds")
        logger.info(f"⏱️  Processing time: {process_duration:.2f} seconds")
        logger.info(f"⏱️  Total time: {total_duration:.2f} seconds")
        logger.info(f"⚡ Processing speed: {records_per_second:.0f} records/second")
        logger.info(f"⚡ Overall speed: {total_records_per_second:.0f} records/second")
        logger.info(f"📈 Success rate: {(result['processed_count'] / num_records * 100):.1f}%")
        
        # Performance comparison
        if use_optimized:
            old_estimated_time = num_records * 0.018  # 18ms per record (old method)
            improvement_factor = old_estimated_time / process_duration if process_duration > 0 else 0
            logger.info(f"🎯 Estimated old method time: {old_estimated_time:.0f} seconds ({old_estimated_time/60:.1f} minutes)")
            logger.info(f"🚀 Performance improvement: {improvement_factor:.1f}x faster!")
            
            if process_duration < 600:  # Less than 10 minutes
                logger.info("✅ TARGET ACHIEVED: Processing completed in under 10 minutes!")
            else:
                logger.info("⚠️  Still slower than 10-minute target, but much better than before")
        
        logger.info("🏆" + "="*80)
        
        return {
            'success': True,
            'processed_count': result['processed_count'],
            'failed_count': result['failed_count'],
            'read_duration': read_duration,
            'process_duration': process_duration,
            'total_duration': total_duration,
            'records_per_second': records_per_second,
            'total_records_per_second': total_records_per_second
        }
        
    except Exception as e:
        logger.error(f"❌ Performance test failed: {e}")
        return {'success': False, 'error': str(e)}
        
    finally:
        # Cleanup
        try:
            os.unlink(csv_file_path)
            logger.info(f"🧹 Cleaned up test file: {csv_file_path}")
        except:
            pass


def cleanup_test_data():
    """Clean up test data"""
    logger.info("🧹 Cleaning up test data...")
    try:
        # Delete test SAPGLPosting records
        test_postings = SAPGLPosting.objects.filter(
            data_file__engagement__engagement_id='PERF_TEST_2025'
        )
        deleted_count = test_postings.count()
        test_postings.delete()
        logger.info(f"🗑️  Deleted {deleted_count:,} test SAPGLPosting records")
        
        # Delete test data file
        DataFile.objects.filter(
            engagement__engagement_id='PERF_TEST_2025'
        ).delete()
        
        # Delete test engagement
        Engagement.objects.filter(
            engagement_id='PERF_TEST_2025'
        ).delete()
        
        # Delete test client
        Client.objects.filter(
            client_name='Performance Test Client'
        ).delete()
        
        logger.info("✅ Test data cleanup completed")
        
    except Exception as e:
        logger.error(f"⚠️  Error during cleanup: {e}")


def main():
    """Main function to run performance tests"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Performance Test for Bulk Data Insertion')
    parser.add_argument('--records', type=int, default=50000, 
                      help='Number of records to test (default: 50000)')
    parser.add_argument('--full-test', action='store_true', 
                      help='Run full 300K record test (takes longer)')
    parser.add_argument('--cleanup', action='store_true', 
                      help='Clean up test data and exit')
    
    args = parser.parse_args()
    
    if args.cleanup:
        cleanup_test_data()
        return
    
    num_records = 300000 if args.full_test else args.records
    
    logger.info("🎯 Starting performance test suite...")
    
    # Run optimized test
    result = run_performance_test(num_records=num_records, use_optimized=True)
    
    if result['success']:
        # Cleanup test data
        cleanup_test_data()
        
        # Final summary
        logger.info("🎯" + "="*80)
        logger.info("🎯 PERFORMANCE TEST SUMMARY")
        logger.info(f"📊 Records tested: {num_records:,}")
        logger.info(f"⏱️  Total time: {result['total_duration']:.2f} seconds ({result['total_duration']/60:.1f} minutes)")
        logger.info(f"⚡ Speed: {result['total_records_per_second']:.0f} records/second")
        
        if result['total_duration'] < 600:  # Less than 10 minutes
            logger.info("🏆 SUCCESS: Target of <10 minutes achieved!")
        elif result['total_duration'] < 1800:  # Less than 30 minutes
            logger.info("✅ GOOD: Much better than original 1.5 hours!")
        else:
            logger.info("⚠️  NEEDS MORE OPTIMIZATION: Still too slow")
            
        logger.info("🎯" + "="*80)
    else:
        logger.error("❌ Performance test failed")
        cleanup_test_data()


if __name__ == '__main__':
    main()
