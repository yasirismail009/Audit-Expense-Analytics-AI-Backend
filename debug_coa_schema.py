#!/usr/bin/env python3
"""
Debug COA database schema and bulk loading issues
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

from django.db import connection
from core.models import ChartOfAccount, DataFile, Engagement
from core.bulk_loading_utils import SQLAlchemyBulkLoader

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def debug_database_schema():
    """Debug the database schema for chart_of_accounts table"""
    
    print("🔍 Debugging Database Schema")
    print("=" * 50)
    
    # Check table columns
    with connection.cursor() as cursor:
        cursor.execute("""
            SELECT column_name, data_type, is_nullable 
            FROM information_schema.columns 
            WHERE table_name = 'chart_of_accounts' 
            ORDER BY ordinal_position;
        """)
        columns = cursor.fetchall()
        
        print("📋 chart_of_accounts table columns:")
        for col_name, data_type, is_nullable in columns:
            print(f"  - {col_name}: {data_type} ({'NULL' if is_nullable == 'YES' else 'NOT NULL'})")
    
    # Check if table exists and has data
    with connection.cursor() as cursor:
        cursor.execute("SELECT COUNT(*) FROM chart_of_accounts;")
        count = cursor.fetchone()[0]
        print(f"\n📊 Current records in chart_of_accounts: {count}")
    
    return columns

def test_simple_bulk_load():
    """Test simple bulk load with proper column mapping"""
    
    print("\n🧪 Testing Simple Bulk Load")
    print("=" * 50)
    
    # Create simple test data with correct column names
    test_data = {
        'account': ['110000', '120000', '130000'],
        'type': ['Assets', 'Assets', 'Assets'],
        'sub_type': ['Current Assets', 'Current Assets', 'Current Assets'],
        'sub_sub_type': ['Cash', 'Cash', 'Cash'],
        'company': ['SMEH', 'SMEH', 'SMEH'],
        'branch': ['Contracting', 'Contracting', 'Contracting'],
        'cost_center': ['1001', '1002', '1003'],
        'gl_account': ['110000/1001', '120000/1002', '130000/1003']
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
    
    # Test direct database insert
    try:
        print("\n🔧 Testing direct database insert...")
        
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
                gl_account=row['gl_account']
            )
            records.append(record)
        
        # Bulk create
        ChartOfAccount.objects.bulk_create(records)
        print(f"✅ Direct bulk create successful: {len(records)} records")
        
        # Verify
        count = ChartOfAccount.objects.filter(data_file=data_file).count()
        print(f"✅ Records in database: {count}")
        
        return True
        
    except Exception as e:
        print(f"❌ Direct bulk create failed: {e}")
        return False

def test_sqlalchemy_bulk_load():
    """Test SQLAlchemy bulk load with proper column mapping"""
    
    print("\n⚡ Testing SQLAlchemy Bulk Load")
    print("=" * 50)
    
    # Create test data with correct column names
    test_data = {
        'account': ['210000', '220000', '230000'],
        'type': ['Liabilities', 'Liabilities', 'Liabilities'],
        'sub_type': ['Current Liabilities', 'Current Liabilities', 'Current Liabilities'],
        'sub_sub_type': ['Accounts Payable', 'Accounts Payable', 'Accounts Payable'],
        'company': ['SMEH', 'SMEH', 'SMEH'],
        'branch': ['Contracting', 'Contracting', 'Contracting'],
        'cost_center': ['2001', '2002', '2003'],
        'gl_account': ['210000/2001', '220000/2002', '230000/2003']
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
    """Main debug function"""
    
    print("🐛 COA Database Debug Suite")
    print("=" * 60)
    
    try:
        # Step 1: Debug database schema
        columns = debug_database_schema()
        
        # Step 2: Test direct bulk create
        direct_success = test_simple_bulk_load()
        
        # Step 3: Test SQLAlchemy bulk load
        sqlalchemy_success = test_sqlalchemy_bulk_load()
        
        # Summary
        print("\n" + "=" * 60)
        print("📊 Debug Summary:")
        print(f"   - Direct bulk create: {'✅ Success' if direct_success else '❌ Failed'}")
        print(f"   - SQLAlchemy bulk load: {'✅ Success' if sqlalchemy_success else '❌ Failed'}")
        
        # Final count
        total_count = ChartOfAccount.objects.count()
        print(f"   - Total COA records in database: {total_count}")
        
    except Exception as e:
        print(f"\n❌ Debug failed: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
