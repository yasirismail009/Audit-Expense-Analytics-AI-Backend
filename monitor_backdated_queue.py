#!/usr/bin/env python
"""
Script to monitor backdated analysis queue and job status
"""

import os
import sys
import django
from datetime import datetime, timedelta

# Setup Django environment
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'analytics.settings')
django.setup()

from core.models import FileProcessingJob, BackdatedAnalysisResult, DataFile

def check_queue_status():
    """Check the status of queued jobs"""
    print("🔍 Checking backdated analysis queue status...")
    print("=" * 50)
    
    # Get all processing jobs
    jobs = FileProcessingJob.objects.all().order_by('-created_at')
    
    print(f"📊 Total processing jobs: {jobs.count()}")
    
    # Filter jobs with backdated analysis
    backdated_jobs = jobs.filter(requested_anomalies__contains=['backdated'])
    print(f"📋 Jobs with backdated analysis: {backdated_jobs.count()}")
    
    if backdated_jobs.count() == 0:
        print("ℹ️  No jobs with backdated analysis found")
        return
    
    print("\n📋 Backdated Analysis Jobs:")
    print("-" * 50)
    
    for job in backdated_jobs[:10]:  # Show last 10 jobs
        status_icon = {
            'PENDING': '⏳',
            'QUEUED': '📋',
            'PROCESSING': '🔄',
            'COMPLETED': '✅',
            'FAILED': '❌',
            'CELERY_ERROR': '⚠️'
        }.get(job.status, '❓')
        
        print(f"{status_icon} Job ID: {job.id}")
        print(f"   📁 File: {job.data_file.file_name if job.data_file else 'N/A'}")
        print(f"   📊 Status: {job.status}")
        print(f"   📅 Created: {job.created_at.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"   ⏱️  Duration: {job.processing_duration:.2f}s" if job.processing_duration else "   ⏱️  Duration: N/A")
        
        # Check if backdated analysis exists
        backdated_analysis = BackdatedAnalysisResult.objects.filter(
            data_file=job.data_file,
            analysis_type='enhanced_backdated'
        ).first()
        
        if backdated_analysis:
            print(f"   📈 Backdated Analysis: ✅ (ID: {backdated_analysis.id})")
            print(f"   📊 Entries Found: {backdated_analysis.get_backdated_count()}")
            print(f"   💰 Total Amount: {backdated_analysis.get_total_amount():,.2f}")
        else:
            print(f"   📈 Backdated Analysis: ❌ (Not found)")
        
        print()

def check_recent_backdated_results():
    """Check recent backdated analysis results"""
    print("📈 Recent Backdated Analysis Results:")
    print("=" * 50)
    
    # Get recent backdated analysis results
    recent_results = BackdatedAnalysisResult.objects.filter(
        analysis_type='enhanced_backdated'
    ).order_by('-analysis_date')[:5]
    
    if recent_results.count() == 0:
        print("ℹ️  No backdated analysis results found")
        return
    
    for result in recent_results:
        print(f"📊 Analysis ID: {result.id}")
        print(f"   📁 File: {result.data_file.file_name}")
        print(f"   📅 Date: {result.analysis_date.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"   📈 Entries: {result.get_backdated_count()}")
        print(f"   💰 Amount: {result.get_total_amount():,.2f}")
        print(f"   ⏱️  Duration: {result.processing_duration:.2f}s" if result.processing_duration else "   ⏱️  Duration: N/A")
        print(f"   📊 Status: {result.status}")
        
        # Show risk distribution
        risk_dist = result.get_risk_distribution()
        if risk_dist:
            print(f"   🎯 Risk Distribution:")
            print(f"      - High: {risk_dist.get('high_risk', 0)}")
            print(f"      - Medium: {risk_dist.get('medium_risk', 0)}")
            print(f"      - Low: {risk_dist.get('low_risk', 0)}")
        
        print()

def check_celery_connection():
    """Check if Celery is running"""
    print("🔧 Checking Celery connection...")
    print("=" * 50)
    
    try:
        from celery import current_app
        from celery.result import AsyncResult
        
        # Try to get Celery app info
        app = current_app
        print(f"✅ Celery app: {app.main}")
        
        # Try to connect to broker
        try:
            app.control.inspect().active()
            print("✅ Celery broker connection: OK")
            print("✅ Celery workers: Available")
        except Exception as e:
            print(f"⚠️  Celery broker connection: Failed - {e}")
            print("💡 Tip: Start Redis server and Celery worker")
        
    except Exception as e:
        print(f"❌ Celery connection error: {e}")
        print("💡 Tip: Make sure Celery is properly configured")

def main():
    """Main function"""
    print("🚀 Backdated Analysis Queue Monitor")
    print("=" * 50)
    
    # Check Celery connection
    check_celery_connection()
    print()
    
    # Check queue status
    check_queue_status()
    print()
    
    # Check recent results
    check_recent_backdated_results()
    
    print("=" * 50)
    print("💡 To start Celery services, run: python start_celery_for_backdated.py")
    print("💡 To process queued jobs, run: python manage.py process_queued_jobs")

if __name__ == "__main__":
    main() 