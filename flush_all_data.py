#!/usr/bin/env python3
"""
Script to flush all data from the database without restarting Docker
This will clear all analysis results, jobs, and transaction data
"""

import os
import django

# Setup Django
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'analytics.settings')
django.setup()

from core.models import *
from django.db import connection

def flush_all_data():
    """Flush all data from the database"""
    print("🗑️  FLUSHING ALL DATA FROM DATABASE")
    print("=" * 50)
    
    # Get counts before deletion
    print("\n📊 CURRENT DATA COUNTS:")
    print(f"  DataFiles: {DataFile.objects.count()}")
    print(f"  FileProcessingJobs: {FileProcessingJob.objects.count()}")
    print(f"  SAPGLPostings: {SAPGLPosting.objects.count()}")
    print(f"  GeneralAnalysisResult: {GeneralAnalysisResult.objects.count()}")
    print(f"  DuplicateAnalysisResult: {DuplicateAnalysisResult.objects.count()}")
    print(f"  BackdatedAnalysisResult: {BackdatedAnalysisResult.objects.count()}")
    print(f"  UserAnalysisResult: {UserAnalysisResult.objects.count()}")
    print(f"  ClosingEntriesAnalysisResult: {ClosingEntriesAnalysisResult.objects.count()}")
    print(f"  UnusualDaysAnalysisResult: {UnusualDaysAnalysisResult.objects.count()}")
    print(f"  HolidayAnalysisResult: {HolidayAnalysisResult.objects.count()}")
    print(f"  OverallAnalysisResult: {OverallAnalysisResult.objects.count()}")
    print(f"  RiskScoringDocument: {RiskScoringDocument.objects.count()}")
    
    # Confirmation
    print("\n⚠️  WARNING: This will delete ALL data!")
    print("   - All uploaded files")
    print("   - All processing jobs")
    print("   - All transaction data")
    print("   - All analysis results")
    print("   - All ML training data")
    
    confirm = input("\n❓ Are you sure you want to continue? (yes/no): ")
    
    if confirm.lower() != 'yes':
        print("❌ Operation cancelled.")
        return
    
    print("\n🗑️  Starting data deletion...")
    
    try:
        # Delete in reverse order to avoid foreign key constraints
        
        # 1. Delete all analysis results
        print("   🗑️  Deleting analysis results...")
        GeneralAnalysisResult.objects.all().delete()
        DuplicateAnalysisResult.objects.all().delete()
        BackdatedAnalysisResult.objects.all().delete()
        UserAnalysisResult.objects.all().delete()
        ClosingEntriesAnalysisResult.objects.all().delete()
        UnusualDaysAnalysisResult.objects.all().delete()
        HolidayAnalysisResult.objects.all().delete()
        OverallAnalysisResult.objects.all().delete()
        RiskScoringDocument.objects.all().delete()
        print("   ✅ Analysis results deleted")
        
        # 2. Delete all processing jobs
        print("   🗑️  Deleting processing jobs...")
        FileProcessingJob.objects.all().delete()
        print("   ✅ Processing jobs deleted")
        
        # 3. Delete all transaction data
        print("   🗑️  Deleting transaction data...")
        SAPGLPosting.objects.all().delete()
        print("   ✅ Transaction data deleted")
        
        # 4. Delete all data files
        print("   🗑️  Deleting data files...")
        DataFile.objects.all().delete()
        print("   ✅ Data files deleted")
        
        # 5. Reset auto-increment counters (PostgreSQL)
        print("   🔄 Resetting auto-increment counters...")
        with connection.cursor() as cursor:
            cursor.execute("""
                SELECT setval(pg_get_serial_sequence('core_datafile', 'id'), 1, false);
                SELECT setval(pg_get_serial_sequence('core_fileprocessingjob', 'id'), 1, false);
                SELECT setval(pg_get_serial_sequence('core_sapglposting', 'id'), 1, false);
            """)
        print("   ✅ Auto-increment counters reset")
        
        # Verify deletion
        print("\n📊 VERIFICATION - DATA COUNTS AFTER DELETION:")
        print(f"  DataFiles: {DataFile.objects.count()}")
        print(f"  FileProcessingJobs: {FileProcessingJob.objects.count()}")
        print(f"  SAPGLPostings: {SAPGLPosting.objects.count()}")
        print(f"  GeneralAnalysisResult: {GeneralAnalysisResult.objects.count()}")
        print(f"  DuplicateAnalysisResult: {DuplicateAnalysisResult.objects.count()}")
        print(f"  BackdatedAnalysisResult: {BackdatedAnalysisResult.objects.count()}")
        print(f"  UserAnalysisResult: {UserAnalysisResult.objects.count()}")
        print(f"  ClosingEntriesAnalysisResult: {ClosingEntriesAnalysisResult.objects.count()}")
        print(f"  UnusualDaysAnalysisResult: {UnusualDaysAnalysisResult.objects.count()}")
        print(f"  HolidayAnalysisResult: {HolidayAnalysisResult.objects.count()}")
        print(f"  OverallAnalysisResult: {OverallAnalysisResult.objects.count()}")
        print(f"  RiskScoringDocument: {RiskScoringDocument.objects.count()}")
        
        print("\n✅ SUCCESS: All data has been flushed from the database!")
        print("   The system is now clean and ready for new data.")
        
    except Exception as e:
        print(f"\n❌ ERROR: Failed to flush data: {e}")
        print("   Please check the error and try again.")

def flush_analysis_only():
    """Flush only analysis results, keep files and transactions"""
    print("🗑️  FLUSHING ANALYSIS RESULTS ONLY")
    print("=" * 50)
    
    # Get counts before deletion
    print("\n📊 CURRENT ANALYSIS COUNTS:")
    print(f"  GeneralAnalysisResult: {GeneralAnalysisResult.objects.count()}")
    print(f"  DuplicateAnalysisResult: {DuplicateAnalysisResult.objects.count()}")
    print(f"  BackdatedAnalysisResult: {BackdatedAnalysisResult.objects.count()}")
    print(f"  UserAnalysisResult: {UserAnalysisResult.objects.count()}")
    print(f"  ClosingEntriesAnalysisResult: {ClosingEntriesAnalysisResult.objects.count()}")
    print(f"  UnusualDaysAnalysisResult: {UnusualDaysAnalysisResult.objects.count()}")
    print(f"  HolidayAnalysisResult: {HolidayAnalysisResult.objects.count()}")
    print(f"  OverallAnalysisResult: {OverallAnalysisResult.objects.count()}")
    print(f"  RiskScoringDocument: {RiskScoringDocument.objects.count()}")
    
    # Confirmation
    print("\n⚠️  WARNING: This will delete ALL analysis results!")
    print("   - All analysis results will be deleted")
    print("   - Files and transactions will be kept")
    print("   - Jobs will be reset to PENDING status")
    
    confirm = input("\n❓ Are you sure you want to continue? (yes/no): ")
    
    if confirm.lower() != 'yes':
        print("❌ Operation cancelled.")
        return
    
    print("\n🗑️  Starting analysis deletion...")
    
    try:
        # Delete all analysis results
        print("   🗑️  Deleting analysis results...")
        GeneralAnalysisResult.objects.all().delete()
        DuplicateAnalysisResult.objects.all().delete()
        BackdatedAnalysisResult.objects.all().delete()
        UserAnalysisResult.objects.all().delete()
        ClosingEntriesAnalysisResult.objects.all().delete()
        UnusualDaysAnalysisResult.objects.all().delete()
        HolidayAnalysisResult.objects.all().delete()
        OverallAnalysisResult.objects.all().delete()
        RiskScoringDocument.objects.all().delete()
        print("   ✅ Analysis results deleted")
        
        # Reset job statuses to PENDING
        print("   🔄 Resetting job statuses...")
        FileProcessingJob.objects.all().update(
            status='PENDING',
            started_at=None,
            completed_at=None,
            processing_duration=None,
            analytics_results={},
            error_message=''
        )
        print("   ✅ Job statuses reset")
        
        # Verify deletion
        print("\n📊 VERIFICATION - ANALYSIS COUNTS AFTER DELETION:")
        print(f"  GeneralAnalysisResult: {GeneralAnalysisResult.objects.count()}")
        print(f"  DuplicateAnalysisResult: {DuplicateAnalysisResult.objects.count()}")
        print(f"  BackdatedAnalysisResult: {BackdatedAnalysisResult.objects.count()}")
        print(f"  UserAnalysisResult: {UserAnalysisResult.objects.count()}")
        print(f"  ClosingEntriesAnalysisResult: {ClosingEntriesAnalysisResult.objects.count()}")
        print(f"  UnusualDaysAnalysisResult: {UnusualDaysAnalysisResult.objects.count()}")
        print(f"  HolidayAnalysisResult: {HolidayAnalysisResult.objects.count()}")
        print(f"  OverallAnalysisResult: {OverallAnalysisResult.objects.count()}")
        print(f"  RiskScoringDocument: {RiskScoringDocument.objects.count()}")
        
        print("\n✅ SUCCESS: All analysis results have been flushed!")
        print("   Files and transactions are preserved.")
        print("   Jobs are reset to PENDING status and ready for reprocessing.")
        
    except Exception as e:
        print(f"\n❌ ERROR: Failed to flush analysis results: {e}")
        print("   Please check the error and try again.")

def main():
    """Main function"""
    print("🗑️  DATA FLUSH UTILITY")
    print("=" * 50)
    print("Choose an option:")
    print("1. Flush ALL data (files, jobs, transactions, analysis)")
    print("2. Flush analysis results only (keep files and transactions)")
    print("3. Exit")
    
    choice = input("\nEnter your choice (1-3): ")
    
    if choice == '1':
        flush_all_data()
    elif choice == '2':
        flush_analysis_only()
    elif choice == '3':
        print("👋 Exiting...")
    else:
        print("❌ Invalid choice. Please run the script again.")

if __name__ == "__main__":
    main() 