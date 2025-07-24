#!/usr/bin/env python
"""
Script to process queued backdated analysis jobs without requiring Celery broker
"""

import os
import sys
import django
from datetime import datetime, timedelta

# Setup Django environment
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'analytics.settings')
django.setup()

from core.models import FileProcessingJob, BackdatedAnalysisResult, DataFile
from core.tasks import run_backdated_analysis

def process_pending_backdated_jobs():
    """Process pending backdated analysis jobs"""
    print("🔧 Processing pending backdated analysis jobs...")
    print("=" * 50)
    
    # Get pending jobs with backdated analysis
    pending_jobs = FileProcessingJob.objects.filter(
        status='PENDING',
        requested_anomalies__contains=['backdated']
    ).order_by('created_at')
    
    print(f"📋 Found {pending_jobs.count()} pending backdated analysis jobs")
    
    if pending_jobs.count() == 0:
        print("ℹ️  No pending backdated analysis jobs found")
        return
    
    processed_count = 0
    failed_count = 0
    
    for job in pending_jobs:
        print(f"\n🔄 Processing job: {job.id}")
        print(f"   📁 File: {job.data_file.file_name if job.data_file else 'N/A'}")
        print(f"   📅 Created: {job.created_at.strftime('%Y-%m-%d %H:%M:%S')}")
        
        try:
            # Update job status to processing
            job.status = 'PROCESSING'
            job.started_at = datetime.now()
            job.save()
            
            # Run backdated analysis directly
            print("   🔍 Running backdated analysis...")
            result = run_backdated_analysis(str(job.id))
            
            if result and result.get('status') == 'completed':
                print("   ✅ Backdated analysis completed successfully!")
                processed_count += 1
                
                # Check if results were saved
                backdated_analysis = BackdatedAnalysisResult.objects.filter(
                    data_file=job.data_file,
                    analysis_type='enhanced_backdated'
                ).first()
                
                if backdated_analysis:
                    print(f"   📊 Results saved: {backdated_analysis.get_backdated_count()} entries found")
                    print(f"   💰 Total amount: {backdated_analysis.get_total_amount():,.2f}")
                else:
                    print("   ⚠️  Results not found in database")
            else:
                print(f"   ❌ Backdated analysis failed: {result.get('error', 'Unknown error')}")
                failed_count += 1
                
        except Exception as e:
            print(f"   ❌ Error processing job: {e}")
            failed_count += 1
            
            # Update job status to failed
            job.status = 'FAILED'
            job.error_message = str(e)
            job.completed_at = datetime.now()
            job.save()
    
    print("\n" + "=" * 50)
    print("📊 Processing Summary:")
    print(f"   ✅ Successfully processed: {processed_count}")
    print(f"   ❌ Failed: {failed_count}")
    print(f"   📋 Total jobs: {pending_jobs.count()}")

def check_and_fix_failed_jobs():
    """Check and fix failed backdated analysis jobs"""
    print("\n🔧 Checking failed backdated analysis jobs...")
    print("=" * 50)
    
    # Get failed jobs with backdated analysis
    failed_jobs = FileProcessingJob.objects.filter(
        status='FAILED',
        requested_anomalies__contains=['backdated']
    ).order_by('-created_at')
    
    print(f"📋 Found {failed_jobs.count()} failed backdated analysis jobs")
    
    if failed_jobs.count() == 0:
        print("ℹ️  No failed backdated analysis jobs found")
        return
    
    for job in failed_jobs[:5]:  # Show last 5 failed jobs
        print(f"\n❌ Failed Job: {job.id}")
        print(f"   📁 File: {job.data_file.file_name if job.data_file else 'N/A'}")
        print(f"   📅 Created: {job.created_at.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"   ⚠️  Error: {job.error_message}")
        
        # Check if backdated analysis exists despite failure
        backdated_analysis = BackdatedAnalysisResult.objects.filter(
            data_file=job.data_file,
            analysis_type='enhanced_backdated'
        ).first()
        
        if backdated_analysis:
            print(f"   ✅ Backdated analysis exists: {backdated_analysis.get_backdated_count()} entries")
        else:
            print(f"   ❌ No backdated analysis found")

def main():
    """Main function"""
    print("🚀 Backdated Analysis Queue Processor")
    print("=" * 50)
    
    # Process pending jobs
    process_pending_backdated_jobs()
    
    # Check failed jobs
    check_and_fix_failed_jobs()
    
    print("\n" + "=" * 50)
    print("✅ Queue processing completed!")
    print("💡 Run 'python monitor_backdated_queue.py' to check results")

if __name__ == "__main__":
    main() 