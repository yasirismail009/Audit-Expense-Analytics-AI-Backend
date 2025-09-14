#!/usr/bin/env python3
"""
Script to flush all data from the analytics database
"""

import os
import sys
import django

# Add the project root to Python path
sys.path.append('/app')

# Setup Django
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'analytics.settings')
django.setup()

from core.models import (
    DataFile, SAPGLPosting, SAPGLPostingError, 
    TrialBalance, ChartOfAccount, GLAccount,
    FileProcessingJob, FileProcessingTask
)
from users.models import User

def flush_all_data():
    """Flush all data from the database"""
    
    print("🧹 FLUSHING ALL DATA FROM DATABASE")
    print("=" * 50)
    
    try:
        # Get counts before deletion (with error handling for missing tables)
        gl_count = SAPGLPosting.objects.count()
        
        try:
            error_count = SAPGLPostingError.objects.count()
        except Exception:
            error_count = 0
            print("⚠️  SAPGLPostingError table doesn't exist yet")
        
        tb_count = TrialBalance.objects.count()
        chart_count = ChartOfAccount.objects.count()
        gl_account_count = GLAccount.objects.count()
        data_file_count = DataFile.objects.count()
        job_count = FileProcessingJob.objects.count()
        task_count = FileProcessingTask.objects.count()
        user_count = User.objects.count()
        
        print(f"📊 Current Data Counts:")
        print(f"   - GL Postings: {gl_count}")
        print(f"   - GL Errors: {error_count}")
        print(f"   - Trial Balance: {tb_count}")
        print(f"   - Chart of Accounts: {chart_count}")
        print(f"   - GL Accounts: {gl_account_count}")
        print(f"   - Data Files: {data_file_count}")
        print(f"   - Processing Jobs: {job_count}")
        print(f"   - Processing Tasks: {task_count}")
        print(f"   - Users: {user_count}")
        
        # Confirm deletion
        print(f"\n⚠️  WARNING: This will delete ALL data from the database!")
        print("🔄 Starting data deletion...")
        
        # Delete in order to respect foreign key constraints
        print("\n🗑️  Deleting GL Posting Errors...")
        try:
            SAPGLPostingError.objects.all().delete()
            print(f"✅ Deleted {error_count} GL Posting Errors")
        except Exception:
            print("⚠️  SAPGLPostingError table doesn't exist, skipping...")
        
        print("\n🗑️  Deleting GL Postings...")
        SAPGLPosting.objects.all().delete()
        print(f"✅ Deleted {gl_count} GL Postings")
        
        print("\n🗑️  Deleting Trial Balance records...")
        TrialBalance.objects.all().delete()
        print(f"✅ Deleted {tb_count} Trial Balance records")
        
        print("\n🗑️  Deleting Chart of Accounts records...")
        ChartOfAccount.objects.all().delete()
        print(f"✅ Deleted {chart_count} Chart of Accounts records")
        
        print("\n🗑️  Deleting GL Accounts...")
        GLAccount.objects.all().delete()
        print(f"✅ Deleted {gl_account_count} GL Accounts")
        
        print("\n🗑️  Deleting Processing Tasks...")
        FileProcessingTask.objects.all().delete()
        print(f"✅ Deleted {task_count} Processing Tasks")
        
        print("\n🗑️  Deleting Processing Jobs...")
        FileProcessingJob.objects.all().delete()
        print(f"✅ Deleted {job_count} Processing Jobs")
        
        print("\n🗑️  Deleting Data Files...")
        DataFile.objects.all().delete()
        print(f"✅ Deleted {data_file_count} Data Files")
        
        # Note: Not deleting users as they might be needed for authentication
        print(f"\n👤 Users preserved: {user_count} (not deleted for security)")
        
        # Verify deletion
        print("\n🔍 Verifying deletion...")
        remaining_gl = SAPGLPosting.objects.count()
        
        try:
            remaining_errors = SAPGLPostingError.objects.count()
        except Exception:
            remaining_errors = 0
        
        remaining_tb = TrialBalance.objects.count()
        remaining_chart = ChartOfAccount.objects.count()
        remaining_gl_accounts = GLAccount.objects.count()
        remaining_files = DataFile.objects.count()
        remaining_jobs = FileProcessingJob.objects.count()
        remaining_tasks = FileProcessingTask.objects.count()
        
        print(f"📊 Remaining Data Counts:")
        print(f"   - GL Postings: {remaining_gl}")
        print(f"   - GL Errors: {remaining_errors}")
        print(f"   - Trial Balance: {remaining_tb}")
        print(f"   - Chart of Accounts: {remaining_chart}")
        print(f"   - GL Accounts: {remaining_gl_accounts}")
        print(f"   - Data Files: {remaining_files}")
        print(f"   - Processing Jobs: {remaining_jobs}")
        print(f"   - Processing Tasks: {remaining_tasks}")
        
        if (remaining_gl == 0 and remaining_errors == 0 and remaining_tb == 0 and 
            remaining_chart == 0 and remaining_gl_accounts == 0 and 
            remaining_files == 0 and remaining_jobs == 0 and remaining_tasks == 0):
            print("\n✅ SUCCESS: All data has been flushed from the database!")
        else:
            print("\n⚠️  WARNING: Some data may still remain in the database")
        
        print("\n🎯 Data flush completed!")
        
    except Exception as e:
        print(f"❌ Error during data flush: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    flush_all_data()
