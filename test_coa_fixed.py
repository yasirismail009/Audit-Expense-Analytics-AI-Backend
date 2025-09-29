#!/usr/bin/env python3
"""
Fixed COA bulk loading test with correct column mapping
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

from core.models import ChartOfAccount, DataFile, Engagement
from core.bulk_loading_utils import SQLAlchemyBulkLoader

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def test_fixed_bulk_load():
    """Test bulk load with correct column mapping"""
    
    print("🔧 Testing Fixed COA Bulk Load")
    print("=" * 50)
    
    # Create test data with correct column names matching database schema
    test_data = {
        'account': ['110000', '120000', '130000', '140000', '150000'],
        'type': ['Assets', 'Assets', 'Assets', 'Assets', 'Assets'],
        'sub_type': ['Current Assets', 'Current Assets', 'Current Assets', 'Current Assets', 'Current Assets'],
        'sub_sub_type': ['Cash', 'Cash', 'Cash', 'Cash', 'Cash'],
        'company': ['SMEH', 'SMEH', 'SMEH', 'SMEH', 'SMEH'],
        'branch': ['Contracting', 'Contracting', 'Contracting', 'Contracting', 'Contracting'],
        'cost_center': ['1001', '1002', '1003', '1004', '1005'],
        'gl_account': ['110000/1001', '120000/1002', '130000/1003', '140000/1004', '150000/1005'],
        'code': ['1001', '1002', '1003', '1004', '1005'],
        'gl_account_long_text': ['Cash Account 1', 'Cash Account 2', 'Cash Account 3', 'Cash Account 4', 'Cash Account 5']
    }
    
    df = pd.DataFrame(test_data)
    print(f"✅ Created test data: {len(df)} records")
    print("Sample data:")
    print(df.head())
    
    # Get existing engagement and data file
    engagement = Engagement.objects.first()
    if not engagement:
        print("❌ No engagement found")
        return False
    
    data_file = DataFile.objects.filter(file_type='COA').first()
    if not data_file:
        print("❌ No COA data file found")
        return False
    
    print(f"✅ Using engagement: {engagement.engagement_name}")
    print(f"✅ Using data file: {data_file.file_name}")
    
    # Test 1: Direct Django ORM bulk create
    try:
        print("\n🔧 Test 1: Direct Django ORM Bulk Create")
        print("-" * 40)
        
        # Clear existing records for this data file
        ChartOfAccount.objects.filter(data_file=data_file).delete()
        print("🧹 Cleared existing records")
        
        # Create records manually
        records = []
        for _, row in df.iterrows():
            record = ChartOfAccount(
                data_file=data_file,
                account=row['account'],
                type=row['type'],
                sub_type=row['sub_type'],
                sub_sub_type=row['sub_sub_type'],
                company=row['company'],
                branch=row['branch'],
                cost_center=row['cost_center'],
                gl_account=row['gl_account'],
                code=row['code'],
                gl_account_long_text=row['gl_account_long_text']
            )
            records.append(record)
        
        # Bulk create
        start_time = datetime.now()
        ChartOfAccount.objects.bulk_create(records)
        end_time = datetime.now()
        
        duration = (end_time - start_time).total_seconds()
        records_per_second = len(records) / duration if duration > 0 else 0
        
        print(f"✅ Direct bulk create successful:")
        print(f"   - Records created: {len(records)}")
        print(f"   - Duration: {duration:.3f}s")
        print(f"   - Records/sec: {records_per_second:.0f}")
        
        # Verify
        count = ChartOfAccount.objects.filter(data_file=data_file).count()
        print(f"✅ Records in database: {count}")
        
        if count > 0:
            # Show sample records
            sample_records = ChartOfAccount.objects.filter(data_file=data_file)[:3]
            print("Sample records:")
            for i, record in enumerate(sample_records, 1):
                print(f"   {i}. Account: {record.account}, Type: {record.type}, Company: {record.company}")
        
        return True
        
    except Exception as e:
        print(f"❌ Direct bulk create failed: {e}")
        import traceback
        traceback.print_exc()
        return False

def test_sqlalchemy_fixed():
    """Test SQLAlchemy with fixed column mapping"""
    
    print("\n⚡ Test 2: SQLAlchemy with Fixed Column Mapping")
    print("-" * 50)
    
    # Create test data with correct column names
    test_data = {
        'account': ['210000', '220000', '230000', '240000', '250000'],
        'type': ['Liabilities', 'Liabilities', 'Liabilities', 'Liabilities', 'Liabilities'],
        'sub_type': ['Current Liabilities', 'Current Liabilities', 'Current Liabilities', 'Current Liabilities', 'Current Liabilities'],
        'sub_sub_type': ['Accounts Payable', 'Accounts Payable', 'Accounts Payable', 'Accounts Payable', 'Accounts Payable'],
        'company': ['SMEH', 'SMEH', 'SMEH', 'SMEH', 'SMEH'],
        'branch': ['Contracting', 'Contracting', 'Contracting', 'Contracting', 'Contracting'],
        'cost_center': ['2001', '2002', '2003', '2004', '2005'],
        'gl_account': ['210000/2001', '220000/2002', '230000/2003', '240000/2004', '250000/2005'],
        'code': ['2001', '2002', '2003', '2004', '2005'],
        'gl_account_long_text': ['AP Account 1', 'AP Account 2', 'AP Account 3', 'AP Account 4', 'AP Account 5']
    }
    
    df = pd.DataFrame(test_data)
    print(f"✅ Created test data: {len(df)} records")
    
    # Get existing data file
    data_file = DataFile.objects.filter(file_type='COA').first()
    if not data_file:
        print("❌ No COA data file found")
        return False
    
    try:
        print("\n🔧 Testing SQLAlchemy bulk load...")
        
        loader = SQLAlchemyBulkLoader()
        result = loader.bulk_load_chart_of_accounts(df, str(data_file.id))
        
        if result.get('success'):
            print(f"✅ SQLAlchemy bulk load successful:")
            print(f"   - Records loaded: {result.get('records_loaded', 0)}")
            print(f"   - Duration: {result.get('duration', 0):.3f}s")
            print(f"   - Records/sec: {result.get('records_per_second', 0):.0f}")
            
            # Verify
            count = ChartOfAccount.objects.filter(data_file=data_file).count()
            print(f"✅ Total records in database: {count}")
            
            return True
        else:
            print(f"❌ SQLAlchemy bulk load failed: {result.get('error', 'Unknown error')}")
            return False
            
    except Exception as e:
        print(f"❌ SQLAlchemy bulk load error: {e}")
        import traceback
        traceback.print_exc()
        return False

def main():
    """Main test function"""
    
    print("🔧 Fixed COA Bulk Loading Test")
    print("=" * 60)
    
    try:
        # Test 1: Direct Django ORM
        direct_success = test_fixed_bulk_load()
        
        # Test 2: SQLAlchemy
        sqlalchemy_success = test_sqlalchemy_fixed()
        
        # Summary
        print("\n" + "=" * 60)
        print("📊 Test Summary:")
        print(f"   - Direct Django ORM: {'✅ Success' if direct_success else '❌ Failed'}")
        print(f"   - SQLAlchemy bulk load: {'✅ Success' if sqlalchemy_success else '❌ Failed'}")
        
        # Final count
        total_count = ChartOfAccount.objects.count()
        print(f"   - Total COA records in database: {total_count}")
        
        if total_count > 0:
            print("🎉 SUCCESS: Data is now being saved to the database!")
        else:
            print("❌ FAILED: No data was saved to the database")
        
    except Exception as e:
        print(f"\n❌ Test failed: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
