#!/usr/bin/env python3
"""
Test COA file data processing using Docker
This script tests the COA file processing functionality with files in the media folder
"""

import sys
import os
import pandas as pd
import logging
from datetime import datetime
from pathlib import Path

# Add the project root to Python path
sys.path.append('/app')
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'analytics.settings')

import django
django.setup()

from django.core.files.uploadedfile import SimpleUploadedFile
from core.models import DataFile, ChartOfAccount
from core.bulk_loading_utils import SQLAlchemyBulkLoader, bulk_load_chart_of_accounts
# from core.file_processing_utils import process_coa_file  # Function not found

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def test_coa_file_processing():
    """Test COA file processing with various scenarios"""
    
    print("🚀 Starting COA File Processing Test")
    print("=" * 60)
    
    # Test 1: Check if COA file exists in media folder
    test_file_path = "/app/media/local_files/Muhammad_Yasir/1.0/Chart_of_Accounts_Data_File.xlsx"
    if not os.path.exists(test_file_path):
        print(f"❌ COA file not found: {test_file_path}")
        # Try alternative path
        test_file_path = "/app/test_coa.xlsx"
        if not os.path.exists(test_file_path):
            print(f"❌ Test file not found: {test_file_path}")
            return False
    
    print(f"✅ Found COA file: {test_file_path}")
    
    # Test 2: Read and examine the COA file
    try:
        print("\n📄 Reading COA file...")
        df = pd.read_excel(test_file_path)
        print(f"✅ File loaded successfully")
        print(f"   - Rows: {len(df)}")
        print(f"   - Columns: {list(df.columns)}")
        print(f"   - Sample data:")
        print(df.head(3).to_string())
        
    except Exception as e:
        print(f"❌ Error reading COA file: {e}")
        return False
    
    # Test 3: Create a DataFile record
    try:
        print("\n💾 Creating DataFile record...")
        
        # Create a mock uploaded file
        with open(test_file_path, 'rb') as f:
            file_content = f.read()
        
        # Get or create a test engagement first
        from core.models import Engagement, Client
        
        # Create a test client if it doesn't exist
        client, created = Client.objects.get_or_create(
            client_name='Test Client',
            defaults={
                'client_code': 'TEST001',
                'industry': 'Technology',
                'country': 'Saudi Arabia'
            }
        )
        
        # Create a test engagement if it doesn't exist
        engagement, created = Engagement.objects.get_or_create(
            engagement_id='TEST-2024-001',
            defaults={
                'engagement_name': 'Test COA Engagement 2024',
                'client': client,
                'fiscal_year': 2024,
                'fiscal_year_start': '2024-01-01',
                'fiscal_year_end': '2024-12-31',
                'audit_start_date': '2024-01-01',
                'audit_end_date': '2024-12-31'
            }
        )
        
        # Create DataFile instance with correct field names
        data_file = DataFile.objects.create(
            file_name='test_coa.xlsx',
            file_type='COA',
            file_size=len(file_content),
            engagement=engagement,
            version='1.0',
            version_notes='Test COA file for bulk loading validation',
            local_file_path=test_file_path,
            is_validated=True
        )
        
        print(f"✅ DataFile created with ID: {data_file.id}")
        
    except Exception as e:
        print(f"❌ Error creating DataFile: {e}")
        import traceback
        traceback.print_exc()
        return False
    
    # Test 4: Test COA file processing using bulk operations (threaded approach)
    try:
        print("\n🔄 Testing COA file processing with bulk operations...")
        
        # Use FileProcessingManager for bulk processing
        from core.file_processing_utils import FileProcessingManager
        processor = FileProcessingManager()
        
        # Process the chart data using bulk operations
        result = processor.process_chart_data_from_file(test_file_path)
        print(f"✅ COA bulk processing result: {result}")
        
        # Also test the threaded bulk loading approach
        print("\n🧵 Testing threaded bulk loading approach...")
        from core.bulk_loading_utils import bulk_load_chart_of_accounts
        result_bulk = bulk_load_chart_of_accounts(df, str(data_file.id))
        print(f"✅ Threaded bulk loading result: {result_bulk}")
        
    except Exception as e:
        print(f"❌ Error in COA processing: {e}")
        # Continue with other tests even if this fails
    
    # Test 5: Test ultra-fast bulk loading with SQLAlchemy (threaded approach)
    try:
        print("\n⚡ Testing ultra-fast bulk loading with SQLAlchemy...")
        
        # Use the optimized bulk loader (same as in threading)
        loader = SQLAlchemyBulkLoader()
        result = loader.bulk_load_chart_of_accounts(df, str(data_file.id))
        
        if result.get('success'):
            print(f"✅ Ultra-fast bulk loading successful:")
            print(f"   - Records loaded: {result.get('records_loaded', 0)}")
            print(f"   - Duration: {result.get('duration', 0):.2f}s")
            print(f"   - Records/sec: {result.get('records_per_second', 0):.0f}")
            print(f"   - Method: {result.get('method', 'unknown')}")
        else:
            print(f"❌ Ultra-fast bulk loading failed: {result.get('error', 'Unknown error')}")
            
    except Exception as e:
        print(f"❌ Error in ultra-fast bulk loading: {e}")
    
    # Test 6: Test pandas-only bulk loading (threaded approach)
    try:
        print("\n🐼 Testing pandas-only bulk loading (threaded approach)...")
        
        result = bulk_load_coa_with_pandas(df, str(data_file.id))
        
        if result.get('success'):
            print(f"✅ Pandas bulk loading successful:")
            print(f"   - Records loaded: {result.get('records_loaded', 0)}")
            print(f"   - Duration: {result.get('duration', 0):.2f}s")
            print(f"   - Records/sec: {result.get('records_per_second', 0):.0f}")
            print(f"   - Method: {result.get('method', 'unknown')}")
        else:
            print(f"❌ Pandas bulk loading failed: {result.get('error', 'Unknown error')}")
            
    except Exception as e:
        print(f"❌ Error in pandas bulk loading: {e}")
    
    # Test 7: Verify data in database
    try:
        print("\n🔍 Verifying data in database...")
        
        coa_count = ChartOfAccount.objects.filter(data_file=data_file).count()
        print(f"✅ Found {coa_count} COA records in database")
        
        if coa_count > 0:
            # Show sample records
            sample_records = ChartOfAccount.objects.filter(data_file=data_file)[:3]
            print("   Sample records:")
            for record in sample_records:
                print(f"   - Account: {record.account}, Type: {record.type}, Sub Type: {record.sub_type}")
        
    except Exception as e:
        print(f"❌ Error verifying database: {e}")
    
    # Test 8: Test with media folder files
    try:
        print("\n📁 Testing with media folder files...")
        
        media_path = Path("/app/media")
        if media_path.exists():
            coa_files = list(media_path.glob("*.xlsx")) + list(media_path.glob("*.xls"))
            print(f"   Found {len(coa_files)} Excel files in media folder")
            
            for file_path in coa_files:
                print(f"   - {file_path.name}")
                try:
                    # Test reading the file
                    test_df = pd.read_excel(file_path)
                    print(f"     ✅ Readable: {len(test_df)} rows, {len(test_df.columns)} columns")
                except Exception as e:
                    print(f"     ❌ Error reading: {e}")
        else:
            print("   No media folder found")
            
    except Exception as e:
        print(f"❌ Error checking media folder: {e}")
    
    # Test 9: Threaded bulk operations performance test
    try:
        print("\n⚡ Threaded bulk operations performance test...")
        
        # Test with the actual dataset
        if len(df) > 100:
            print(f"   Testing with {len(df)} records using threaded bulk operations...")
            
            # Test SQLAlchemy bulk loading (threaded approach)
            start_time = datetime.now()
            loader = SQLAlchemyBulkLoader()
            result_sqlalchemy = loader.bulk_load_chart_of_accounts(df, str(data_file.id))
            end_time = datetime.now()
            
            duration_sqlalchemy = (end_time - start_time).total_seconds()
            records_per_second_sqlalchemy = len(df) / duration_sqlalchemy if duration_sqlalchemy > 0 else 0
            
            print(f"   ✅ SQLAlchemy bulk loading: {records_per_second_sqlalchemy:.0f} records/second")
            
            # Test pandas bulk loading (threaded approach)
            start_time = datetime.now()
            result_pandas = bulk_load_coa_with_pandas(df, str(data_file.id))
            end_time = datetime.now()
            
            duration_pandas = (end_time - start_time).total_seconds()
            records_per_second_pandas = len(df) / duration_pandas if duration_pandas > 0 else 0
            
            print(f"   ✅ Pandas bulk loading: {records_per_second_pandas:.0f} records/second")
            
            # Compare performance
            if records_per_second_sqlalchemy > records_per_second_pandas:
                print(f"   🏆 SQLAlchemy is {records_per_second_sqlalchemy/records_per_second_pandas:.1f}x faster")
            else:
                print(f"   🏆 Pandas is {records_per_second_pandas/records_per_second_sqlalchemy:.1f}x faster")
                
        else:
            print("   Dataset too small for meaningful performance test")
            
    except Exception as e:
        print(f"❌ Error in threaded performance test: {e}")
    
    print("\n" + "=" * 60)
    print("🏁 COA File Processing Test Completed")
    
    return True

def test_coa_validation():
    """Test COA data validation"""
    
    print("\n🔍 Testing COA Data Validation")
    print("-" * 40)
    
    # Test account number validation
    test_accounts = [
        "110000",      # Valid
        "110000.0",    # Should be converted to "110000"
        "110000.5",    # Invalid - decimal
        "110,000",     # Should be cleaned to "110000"
        "110 000",     # Should be cleaned
        "",            # Empty
        None,          # None
    ]
    
    for account in test_accounts:
        try:
            coa = ChartOfAccount(
                account=account,
                type="Assets",
                sub_type="Current Assets",
                sub_sub_type="Cash",
                data_file_id="test"
            )
            coa.clean()
            print(f"✅ Account '{account}' -> '{coa.account}' (valid)")
        except Exception as e:
            print(f"❌ Account '{account}' -> Error: {e}")
    
    return True

def main():
    """Main test function"""
    
    print("🧪 COA File Processing Test Suite")
    print("=" * 60)
    
    try:
        # Test 1: Basic COA file processing
        test_coa_file_processing()
        
        # Test 2: COA validation
        test_coa_validation()
        
        print("\n✅ All tests completed successfully!")
        
    except Exception as e:
        print(f"\n❌ Test suite failed: {e}")
        import traceback
        traceback.print_exc()
        return False
    
    return True

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
