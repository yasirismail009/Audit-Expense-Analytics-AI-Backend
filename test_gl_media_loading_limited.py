#!/usr/bin/env python3
"""
Test GL data loading from media files using bulk operations - LIMITED TO FIRST 10 RECORDS
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

from core.models import SAPGLPosting, DataFile, Engagement, Client
from core.bulk_loading_utils import SQLAlchemyBulkLoader

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def test_gl_media_loading_limited():
    """Test loading GL data from media files and saving to database - FIRST 10 RECORDS ONLY"""
    
    print("🧪 Testing GL Data Loading from Media Files (FIRST 10 RECORDS ONLY)")
    print("=" * 70)
    
    # Check current database state
    print("\n📊 Current Database State:")
    gl_count_before = SAPGLPosting.objects.count()
    print(f"   - GL Postings: {gl_count_before}")
    
    # Look for GL files in media folder
    print("\n📁 Searching for GL files in media folder...")
    
    media_path = Path("/app/media")
    gl_files = []
    
    if media_path.exists():
        # Look for Excel files that might contain GL data
        for pattern in ["*.xlsx", "*.xls"]:
            gl_files.extend(media_path.rglob(pattern))
        
        print(f"   Found {len(gl_files)} Excel files in media folder")
        
        for file_path in gl_files:
            print(f"   - {file_path.relative_to(media_path)}")
    else:
        print("   No media folder found")
        return False
    
    if not gl_files:
        print("❌ No GL files found in media folder")
        return False
    
    # Try to find a GL file (look for files with GL-related names)
    gl_file = None
    for file_path in gl_files:
        file_name = file_path.name.lower()
        if any(keyword in file_name for keyword in ['gl', 'general', 'ledger', 'posting', 'transaction']):
            gl_file = file_path
            break
    
    if not gl_file:
        # Use the first Excel file if no GL-specific file found
        gl_file = gl_files[0]
    
    print(f"\n📄 Using GL file: {gl_file.name}")
    
    # Read the GL file - LIMITED TO FIRST 10 RECORDS
    try:
        print("\n📖 Reading GL file (FIRST 10 RECORDS ONLY)...")
        df = pd.read_excel(gl_file)
        
        # LIMIT TO FIRST 10 RECORDS FOR TESTING
        df_limited = df.head(10)
        
        print(f"✅ File loaded successfully")
        print(f"   - Total rows in file: {len(df)}")
        print(f"   - Testing with: {len(df_limited)} records (FIRST 10 ONLY)")
        print(f"   - Columns: {list(df_limited.columns)}")
        print(f"   - Sample data:")
        print(df_limited.to_string())
        
    except Exception as e:
        print(f"❌ Error reading GL file: {e}")
        return False
    
    # Get or create test engagement and data file
    try:
        print("\n💾 Setting up test data...")
        
        # Get existing client and engagement
        client = Client.objects.first()
        if not client:
            print("❌ No clients found")
            return False
            
        engagement = Engagement.objects.first()
        if not engagement:
            print("❌ No engagements found")
            return False
        
        # Create a unique DataFile for this test
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        data_file = DataFile.objects.create(
            file_name=f'gl_test_limited_{timestamp}.xlsx',
            file_type='GL',
            file_size=gl_file.stat().st_size,
            engagement=engagement,
            version='1.0',
            version_notes=f'Test GL file loading from {gl_file.name} - FIRST 10 RECORDS ONLY',
            local_file_path=str(gl_file),
            is_validated=True
        )
        
        print(f"✅ Created DataFile with ID: {data_file.id}")
        
    except Exception as e:
        print(f"❌ Error setting up test data: {e}")
        return False
    
    # Test GL bulk loading with LIMITED DATA
    print("\n🚀 Testing GL Bulk Loading (FIRST 10 RECORDS ONLY)...")
    
    try:
        loader = SQLAlchemyBulkLoader()
        
        # Test GL bulk loading with limited data
        gl_result = loader.bulk_load_gl_with_pandas(df_limited, str(data_file.id))
        
        if gl_result.get('success'):
            print(f"✅ GL bulk loading successful:")
            print(f"   - Records loaded: {gl_result.get('records_loaded', 0)}")
            print(f"   - Duration: {gl_result.get('duration', 0):.2f}s")
            print(f"   - Records/sec: {gl_result.get('records_per_second', 0):.0f}")
            print(f"   - Method: {gl_result.get('method', 'unknown')}")
        else:
            print(f"❌ GL bulk loading failed: {gl_result.get('error', 'Unknown error')}")
            return False
            
    except Exception as e:
        print(f"❌ Error in GL bulk loading: {e}")
        import traceback
        traceback.print_exc()
        return False
    
    finally:
        loader.close()
    
    # Check database state after loading
    print("\n📊 Database State After Loading:")
    gl_count_after = SAPGLPosting.objects.count()
    print(f"   - GL Postings: {gl_count_after} (was {gl_count_before})")
    
    # Verify specific records
    print("\n🔍 Verifying Saved Records:")
    
    # Check GL records for this DataFile
    gl_records = SAPGLPosting.objects.filter(data_file=data_file)
    print(f"   - GL records for this DataFile: {gl_records.count()}")
    
    if gl_records.exists():
        print(f"   - Sample GL records:")
        for i, record in enumerate(gl_records[:3], 1):
            print(f"     {i}. ID: {record.id}")
            print(f"        Document: {record.document_number}")
            print(f"        GL Account: {record.gl_account}")
            print(f"        Amount: {record.amount_local_currency}")
            print(f"        Currency: {record.local_currency}")
            print(f"        User: {record.user_name}")
            print(f"        Posting Date: {record.posting_date}")
            print()
    
    # Summary
    print("\n" + "=" * 70)
    if gl_count_after > gl_count_before:
        print("✅ SUCCESS: GL data loaded and saved to database!")
        print(f"   - Added {gl_count_after - gl_count_before} GL records")
        print(f"   - Source file: {gl_file.name}")
        print(f"   - DataFile ID: {data_file.id}")
        print(f"   - Test limited to: 10 records (out of {len(df)} total)")
    else:
        print("❌ ISSUE: No new GL records were saved")
        print(f"   - GL count change: {gl_count_after - gl_count_before}")
    
    return True

if __name__ == "__main__":
    success = test_gl_media_loading_limited()
    sys.exit(0 if success else 1)
