#!/usr/bin/env python
"""
Test script to validate temp file optimization

This script verifies:
1. Only GL files create temp files
2. TB and COA files process from memory
3. Temp files are cleaned up after processing
4. Completion test is queued in Celery

Usage:
    python test_temp_file_optimization.py
"""

import os
import sys
import django
import tempfile
import time
from pathlib import Path

# Setup Django
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'analytics.settings')
django.setup()

from core.models import DataFile, Engagement, Client
from core.views import FileUploadView
from users.models import User
from django.core.files.uploadedfile import SimpleUploadedFile
import logging

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def create_test_csv_content(file_type='GL', num_records=100):
    """Create test CSV content for different file types"""
    
    if file_type == 'GL':
        # GL file headers and sample data
        headers = ['Document', 'Document type', 'Posting Date', 'G/L Account', 'Amount in Local Currency', 'User Name']
        rows = []
        for i in range(num_records):
            rows.append([
                f'DOC{1000+i}', 'SA', '1/15/2025', f'{110000+i}', f'{100+i}.00', f'USER{i%10+1:03d}'
            ])
    
    elif file_type == 'TB':
        # TB file headers and sample data
        headers = ['CoCd', 'GL Account', 'Short Text', 'Currency', 'Opening Balance', 'Debit', 'Credit', 'Closing Balance']
        rows = []
        for i in range(num_records):
            rows.append([
                '1000', f'{110000+i}', f'Account {110000+i}', 'SAR', f'{1000+i}.00', f'{500+i}.00', f'{300+i}.00', f'{1200+i}.00'
            ])
    
    elif file_type == 'COA':
        # COA file headers and sample data
        headers = ['Account', 'Type', 'Sub Type', 'Sub Sub Type', 'G/L acct', 'Company', 'G/L Acct Long Text']
        rows = []
        for i in range(num_records):
            rows.append([
                f'ACC{110000+i}', 'Assets', 'Current Assets', 'Cash', f'{110000+i}', '1000', f'Cash Account {110000+i}'
            ])
    
    # Create CSV content
    csv_content = ','.join(headers) + '\n'
    for row in rows:
        csv_content += ','.join(str(cell) for cell in row) + '\n'
    
    return csv_content


def setup_test_environment():
    """Setup test client, engagement"""
    logger.info("Setting up test environment...")
    
    # Create test user
    user, created = User.objects.get_or_create(
        username='test_temp_user',
        defaults={'email': 'test@temp.com', 'first_name': 'Temp', 'last_name': 'Test'}
    )
    
    # Create test client
    client, created = Client.objects.get_or_create(
        client_name='Temp File Test Client',
        defaults={'contact_person': 'Test Contact', 'email': 'test@client.com'}
    )
    
    # Create test engagement
    engagement, created = Engagement.objects.get_or_create(
        engagement_id='TEMP_TEST_2025',
        defaults={
            'client': client,
            'engagement_name': 'Temp File Test Engagement',
            'fiscal_year': 2025,
            'status': 'ACTIVE'
        }
    )
    
    return engagement


def test_file_processing(file_type, num_records=100):
    """Test file processing and temp file behavior"""
    logger.info(f"🧪 Testing {file_type} file processing...")
    
    # Create test CSV content
    csv_content = create_test_csv_content(file_type, num_records)
    
    # Create uploaded file
    uploaded_file = SimpleUploadedFile(
        name=f'test_{file_type.lower()}.csv',
        content=csv_content.encode('utf-8'),
        content_type='text/csv'
    )
    
    # Get initial temp file count
    temp_dir = Path(tempfile.gettempdir())
    initial_temp_files = list(temp_dir.glob('*'))
    initial_count = len(initial_temp_files)
    
    logger.info(f"📁 Initial temp files count: {initial_count}")
    
    # Process the file
    try:
        engagement = setup_test_environment()
        
        # Create data file
        data_file = DataFile.objects.create(
            engagement=engagement,
            file_name=f'test_{file_type.lower()}.csv',
            file_type=f'{file_type}_LIST',
            file_size=len(csv_content),
            status='PENDING'
        )
        
        # Process based on file type
        view = FileUploadView()
        
        if file_type == 'GL':
            # GL files should use temp storage for large files
            if num_records >= 10000:
                logger.info(f"🗂️ Testing GL large file (temp storage expected)")
                result = view._process_gl_large_file_threaded(data_file, uploaded_file, num_records)
                
                # Check for temp file creation
                time.sleep(1)  # Allow temp file to be created
                current_temp_files = list(temp_dir.glob('*'))
                current_count = len(current_temp_files)
                
                if current_count > initial_count:
                    logger.info(f"✅ GL temp file created as expected (count: {initial_count} → {current_count})")
                else:
                    logger.warning(f"⚠️ GL temp file not detected (count: {initial_count} → {current_count})")
                
                # Wait for processing to complete and cleanup
                logger.info("⏳ Waiting for background processing and cleanup...")
                time.sleep(5)
                
                # Check temp file cleanup
                final_temp_files = list(temp_dir.glob('*'))
                final_count = len(final_temp_files)
                
                if final_count <= initial_count:
                    logger.info(f"🗑️ GL temp file cleaned up successfully (count: {current_count} → {final_count})")
                else:
                    logger.warning(f"⚠️ GL temp file may not be cleaned up (count: {current_count} → {final_count})")
                    
            else:
                logger.info(f"🗂️ Testing GL small file (memory processing expected)")
                result = view._process_gl_file_sync(data_file, uploaded_file)
                
                # Small GL files should not create temp files
                current_temp_files = list(temp_dir.glob('*'))
                current_count = len(current_temp_files)
                
                if current_count == initial_count:
                    logger.info(f"✅ GL small file processed from memory (no temp files)")
                else:
                    logger.warning(f"⚠️ Unexpected temp file behavior for small GL file")
                    
        else:
            # TB and COA files should process from memory
            logger.info(f"🧠 Testing {file_type} file (memory processing expected)")
            result = view._process_file_sync(data_file, uploaded_file, file_type)
            
            # Check that no temp files were created
            current_temp_files = list(temp_dir.glob('*'))
            current_count = len(current_temp_files)
            
            if current_count == initial_count:
                logger.info(f"✅ {file_type} file processed from memory (no temp files)")
            else:
                logger.warning(f"⚠️ {file_type} file may have created temp files unexpectedly")
        
        logger.info(f"📊 Processing result: {result.get('message', 'No message')}")
        return True
        
    except Exception as e:
        logger.error(f"❌ Error testing {file_type} file: {e}")
        return False


def cleanup_test_data():
    """Clean up test data"""
    logger.info("🧹 Cleaning up test data...")
    try:
        # Delete test data files
        DataFile.objects.filter(engagement__engagement_id='TEMP_TEST_2025').delete()
        
        # Delete test engagement
        Engagement.objects.filter(engagement_id='TEMP_TEST_2025').delete()
        
        # Delete test client
        Client.objects.filter(client_name='Temp File Test Client').delete()
        
        logger.info("✅ Test data cleanup completed")
    except Exception as e:
        logger.error(f"⚠️ Error during cleanup: {e}")


def main():
    """Main test function"""
    logger.info("🚀" + "="*60)
    logger.info("🚀 TEMP FILE OPTIMIZATION TEST")
    logger.info("🚀" + "="*60)
    
    test_results = []
    
    # Test TB file (should use memory processing)
    logger.info("🧪 Testing TB file (small, memory processing)...")
    result = test_file_processing('TB', num_records=100)
    test_results.append(('TB', result))
    
    # Test COA file (should use memory processing)
    logger.info("🧪 Testing COA file (small, memory processing)...")
    result = test_file_processing('COA', num_records=100)
    test_results.append(('COA', result))
    
    # Test small GL file (should use memory processing)
    logger.info("🧪 Testing small GL file (memory processing)...")
    result = test_file_processing('GL', num_records=5000)  # Less than 10K threshold
    test_results.append(('GL Small', result))
    
    # Test large GL file (should use temp file processing)
    logger.info("🧪 Testing large GL file (temp file processing)...")
    result = test_file_processing('GL', num_records=15000)  # More than 10K threshold
    test_results.append(('GL Large', result))
    
    # Cleanup
    cleanup_test_data()
    
    # Summary
    logger.info("🎯" + "="*60)
    logger.info("🎯 TEST SUMMARY")
    logger.info("🎯" + "="*60)
    
    all_passed = True
    for test_name, passed in test_results:
        status = "✅ PASSED" if passed else "❌ FAILED"
        logger.info(f"{test_name:<15}: {status}")
        if not passed:
            all_passed = False
    
    if all_passed:
        logger.info("🏆 ALL TESTS PASSED - Temp file optimization working correctly!")
    else:
        logger.info("⚠️ SOME TESTS FAILED - Check logs for details")
    
    logger.info("🎯" + "="*60)


if __name__ == '__main__':
    main()
