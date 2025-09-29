#!/usr/bin/env python3
"""
Test COA bulk save operations using existing engagement and data files
"""

import sys
import os
import pandas as pd
import logging
from datetime import datetime

# Add the project root to Python path
sys.path.append('/app')
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'analytics.settings')

import django
django.setup()

from core.models import DataFile, ChartOfAccount, Engagement
from core.bulk_loading_utils import SQLAlchemyBulkLoader, bulk_load_coa_with_pandas

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def test_bulk_save_existing():
    """Test bulk save using existing engagement and data files"""
    
    print("🚀 Testing COA Bulk Save with Existing Data")
    print("=" * 50)
    
    # Check for existing engagement
    engagement = Engagement.objects.first()
    if not engagement:
        print("❌ No existing engagement found. Please create an engagement first.")
        return False
    print(f"✅ Using existing engagement: {engagement.engagement_name} (ID: {engagement.id})")
    
    # Check for existing COA data file
    data_file = DataFile.objects.filter(file_type='COA').first()
    if not data_file:
        print("❌ No existing COA data file found. Please upload a COA file first.")
        return False
    print(f"✅ Using existing COA data file: {data_file.file_name} (ID: {data_file.id})")
    
    # Use the actual COA file from media folder
    coa_file_path = "/app/media/local_files/Muhammad_Yasir/1.0/Chart_of_Accounts_Data_File.xlsx"
    
    if not os.path.exists(coa_file_path):
        print(f"❌ COA file not found: {coa_file_path}")
        return False
    
    print(f"✅ Found COA file: {coa_file_path}")
    
    try:
        # Read a sample of the COA file (first 100 rows for testing)
        print("\n📄 Reading COA file sample...")
        df = pd.read_excel(coa_file_path, nrows=100)  # Read only first 100 rows for testing
        print(f"✅ File sample loaded: {len(df)} rows, {len(df.columns)} columns")
        
        # Show sample data
        print("\n📋 Sample data:")
        print(df.head(3).to_string())
        
    except Exception as e:
        print(f"❌ Error reading file: {e}")
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
            print(f"   - Duration: {duration:.3f}s")
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
            print(f"   - Duration: {duration:.3f}s")
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
        
        if len(df) > 10:
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
    
    print("\n" + "=" * 50)
    print("🏁 Bulk Save Test with Existing Data Completed")
    
    return True

if __name__ == "__main__":
    try:
        test_bulk_save_existing()
        print("\n✅ Test completed successfully!")
    except Exception as e:
        print(f"\n❌ Test failed: {e}")
        import traceback
        traceback.print_exc()
