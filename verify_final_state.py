#!/usr/bin/env python
"""
Verify the final state of the system after fixing the Celery data flow issue
"""
import os
import sys
import django

# Setup Django
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'analytics.settings')
django.setup()

from core.models import SAPGLPosting, DataFile, FileProcessingJob

def verify_final_state():
    """Verify the final state of the system"""
    print("🎯 FINAL STATE VERIFICATION")
    print("=" * 50)
    
    # Check transactions
    transaction_count = SAPGLPosting.objects.count()
    print(f"📊 Transactions: {transaction_count}")
    
    # Check data file
    data_file = DataFile.objects.first()
    if data_file:
        print(f"📁 Data File: {data_file.file_name}")
        print(f"   Total Records: {data_file.total_records}")
        print(f"   Processed Records: {data_file.processed_records}")
        print(f"   Status: {data_file.status}")
    
    # Check processing jobs
    jobs = FileProcessingJob.objects.all()
    print(f"🔧 Processing Jobs: {jobs.count()}")
    
    for job in jobs:
        print(f"   Job {job.id}:")
        print(f"     Status: {job.status}")
        print(f"     Analytics Results: {len(job.analytics_results) if job.analytics_results else 0} keys")
        print(f"     Anomaly Results: {len(job.anomaly_results) if job.anomaly_results else 0} keys")
    
    # Summary
    print("\n" + "=" * 50)
    print("✅ SUCCESS SUMMARY:")
    if transaction_count == 63:
        print("  ✅ All 63 transactions are in the database")
    else:
        print(f"  ❌ Expected 63 transactions, found {transaction_count}")
    
    if data_file and data_file.total_records == 63:
        print("  ✅ Data file shows correct record count (63)")
    else:
        print(f"  ❌ Data file shows incorrect record count ({data_file.total_records if data_file else 'N/A'})")
    
    completed_jobs = [j for j in jobs if j.status == 'COMPLETED']
    if completed_jobs:
        print(f"  ✅ {len(completed_jobs)} job(s) completed successfully")
        for job in completed_jobs:
            if job.analytics_results:
                print(f"     - Job {job.id}: Analytics completed with {len(job.analytics_results)} keys")
            if job.anomaly_results:
                print(f"     - Job {job.id}: Anomaly detection completed with {len(job.anomaly_results)} keys")
    else:
        print("  ❌ No jobs completed successfully")

if __name__ == "__main__":
    verify_final_state() 