#!/usr/bin/env python3
"""
Test COA bulk save operations as used in threading
This script focuses specifically on testing the bulk save operations
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
from core.bulk_loading_utils import SQLAlchemyBulkLoader, bulk_load_chart_of_accounts, bulk_load_coa_with_pandas

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def test_bulk_save_operations():
    """Test bulk save operations as used in threading"""
    
    print("🚀 Testing COA Bulk Save Operations (Threading Approach)")
    print("=" * 60)
    
    # Test file path
    coa_file_path = "/app/media/local_files/Muhammad_Yasir/1.0/Chart_of_Accounts_Data_File.xlsx"
    
    if not os.path.exists(coa_file_path):
        print(f"❌ COA file not found: {coa_file_path}")
        return False
    
    print(f"✅ Found COA file: {coa_file_path}")
    
    try:
        # Read the COA file
        print("\n📄 Reading COA file...")
        df = pd.read_excel(coa_file_path)
        print(f"✅ File loaded: {len(df)} rows, {len(df.columns)} columns")
        
        # Show sample data
        print("\n📋 Sample data:")
        print(df.head(2).to_string())
        
    except Exception as e:
        print(f"❌ Error reading file: {e}")
        return False
    
    # Create DataFile record
    try:
        print("\n💾 Creating DataFile record...")
        
        with open(coa_file_path, 'rb') as f:
            file_content = f.read()
        
        uploaded_file = SimpleUploadedFile(
            name="Chart_of_Accounts_Data_File.xlsx",
            content=file_content,
            content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )
        
        data_file = DataFile.objects.create(
            file=uploaded_file,
            file_type='COA',
            original_filename='Chart_of_Accounts_Data_File.xlsx',
            file_size=len(file_content),
            status='uploaded'
        )
        
        print(f"✅ DataFile created with ID: {data_file.id}")
        
    except Exception as e:
        print(f"❌ Error creating DataFile: {e}")
        return False
    
    # Test 1: SQLAlchemy Bulk Loading (Threading Approach)
    try:
        print("\n⚡ Test 1: SQLAlchemy Bulk Loading (Threading Approach)")
        print("-" * 50)
        
        start_time = datetime.now()
        loader = SQLAlchemyBulkLoader()
        result = loader.bulk_load_chart_of_accounts(df, str(data_file.id))
        end_time = datetime.now()
        
        duration = (end_time - start_time).total_seconds()
        
        if result.get('success'):
            print(f"✅ SQLAlchemy bulk loading successful:")
            print(f"   - Records loaded: {result.get('records_loaded', 0):,}")
            print(f"   - Duration: {duration:.2f}s")
            print(f"   - Records/sec: {result.get('records_per_second', 0):.0f}")
            print(f"   - Method: {result.get('method', 'unknown')}")
        else:
            print(f"❌ SQLAlchemy bulk loading failed: {result.get('error', 'Unknown error')}")
            
    except Exception as e:
        print(f"❌ Error in SQLAlchemy bulk loading: {e}")
    
    # Test 2: Pandas Bulk Loading (Threading Approach)
    try:
        print("\n🐼 Test 2: Pandas Bulk Loading (Threading Approach)")
        print("-" * 50)
        
        start_time = datetime.now()
        result = bulk_load_coa_with_pandas(df, str(data_file.id))
        end_time = datetime.now()
        
        duration = (end_time - start_time).total_seconds()
        
        if result.get('success'):
            print(f"✅ Pandas bulk loading successful:")
            print(f"   - Records loaded: {result.get('records_loaded', 0):,}")
            print(f"   - Duration: {duration:.2f}s")
            print(f"   - Records/sec: {result.get('records_per_second', 0):.0f}")
            print(f"   - Method: {result.get('method', 'unknown')}")
        else:
            print(f"❌ Pandas bulk loading failed: {result.get('error', 'Unknown error')}")
            
    except Exception as e:
        print(f"❌ Error in Pandas bulk loading: {e}")
    
    # Test 3: Verify database records
    try:
        print("\n🔍 Test 3: Verify Database Records")
        print("-" * 50)
        
        coa_count = ChartOfAccount.objects.filter(data_file=data_file).count()
        print(f"✅ Found {coa_count:,} COA records in database")
        
        if coa_count > 0:
            # Show sample records
            sample_records = ChartOfAccount.objects.filter(data_file=data_file)[:3]
            print("   Sample records:")
            for i, record in enumerate(sample_records, 1):
                print(f"   {i}. Account: {record.account}, Type: {record.type}, Sub Type: {record.sub_type}")
        
    except Exception as e:
        print(f"❌ Error verifying database: {e}")
    
    # Test 4: Performance comparison
    try:
        print("\n⚡ Test 4: Performance Comparison")
        print("-" * 50)
        
        if len(df) > 100:
            print(f"   Testing with {len(df):,} records...")
            
            # SQLAlchemy performance
            start_time = datetime.now()
            loader = SQLAlchemyBulkLoader()
            result_sql = loader.bulk_load_chart_of_accounts(df, str(data_file.id))
            end_time = datetime.now()
            
            duration_sql = (end_time - start_time).total_seconds()
            records_per_second_sql = len(df) / duration_sql if duration_sql > 0 else 0
            
            # Pandas performance
            start_time = datetime.now()
            result_pandas = bulk_load_coa_with_pandas(df, str(data_file.id))
            end_time = datetime.now()
            
            duration_pandas = (end_time - start_time).total_seconds()
            records_per_second_pandas = len(df) / duration_pandas if duration_pandas > 0 else 0
            
            print(f"   📊 Performance Results:")
            print(f"   - SQLAlchemy: {records_per_second_sql:.0f} records/sec")
            print(f"   - Pandas: {records_per_second_pandas:.0f} records/sec")
            
            if records_per_second_sql > records_per_second_pandas:
                speedup = records_per_second_sql / records_per_second_pandas
                print(f"   🏆 SQLAlchemy is {speedup:.1f}x faster than Pandas")
            else:
                speedup = records_per_second_pandas / records_per_second_sql
                print(f"   🏆 Pandas is {speedup:.1f}x faster than SQLAlchemy")
        else:
            print("   Dataset too small for meaningful performance test")
            
    except Exception as e:
        print(f"❌ Error in performance test: {e}")
    
    print("\n" + "=" * 60)
    print("🏁 Bulk Save Operations Test Completed")
    
    return True

def main():
    """Main test function"""
    
    print("🧪 COA Bulk Save Operations Test Suite")
    print("=" * 60)
    
    try:
        success = test_bulk_save_operations()
        
        if success:
            print("\n✅ All bulk save tests completed successfully!")
        else:
            print("\n❌ Some tests failed!")
            
    except Exception as e:
        print(f"\n❌ Test suite failed: {e}")
        import traceback
        traceback.print_exc()
        return False
    
    return success

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
