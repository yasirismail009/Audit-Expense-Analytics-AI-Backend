#!/usr/bin/env python3
"""
Check for uploaded files that haven't been processed in Docker environment.
This script provides comprehensive diagnostics for file processing status.
"""

import os
import sys
import django
import json
import redis
from datetime import datetime, timedelta
from collections import defaultdict

# Add the project directory to Python path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# Setup Django
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'analytics.settings')
django.setup()

from django.conf import settings
from django.utils import timezone
from core.models import DataFile, FileProcessingJob, SAPGLPosting
from core.tasks import run_restructured_analysis


class FileProcessingChecker:
    """Comprehensive file processing status checker"""
    
    def __init__(self):
        self.redis_client = None
        self.connect_redis()
    
    def connect_redis(self):
        """Connect to Redis for Celery task monitoring"""
        try:
            redis_url = getattr(settings, 'REDIS_URL', 'redis://localhost:6379/0')
            self.redis_client = redis.from_url(redis_url)
            self.redis_client.ping()
            print("✅ Redis connection successful")
        except Exception as e:
            print(f"⚠️  Redis connection failed: {e}")
            self.redis_client = None
    
    def check_docker_services(self):
        """Check if Docker services are running"""
        print("\n" + "="*60)
        print("🐳 DOCKER SERVICES STATUS")
        print("="*60)
        
        try:
            import subprocess
            result = subprocess.run(['docker', 'ps'], capture_output=True, text=True)
            if result.returncode == 0:
                print("✅ Docker is running")
                lines = result.stdout.strip().split('\n')
                if len(lines) > 1:
                    print("📋 Running containers:")
                    for line in lines[1:]:
                        if line.strip():
                            print(f"   {line}")
                else:
                    print("⚠️  No containers running")
            else:
                print("❌ Docker is not running or not accessible")
        except Exception as e:
            print(f"❌ Error checking Docker: {e}")
    
    def check_celery_status(self):
        """Check Celery worker and beat status"""
        print("\n" + "="*60)
        print("🌿 CELERY STATUS")
        print("="*60)
        
        if not self.redis_client:
            print("❌ Cannot check Celery status - Redis not available")
            return
        
        try:
            # Check Celery worker status
            worker_stats = self.redis_client.hgetall('celery:stats')
            if worker_stats:
                print("✅ Celery workers are active")
                for worker, stats in worker_stats.items():
                    if isinstance(stats, bytes):
                        stats = stats.decode('utf-8')
                    try:
                        stats_data = json.loads(stats)
                        print(f"   Worker: {worker.decode('utf-8') if isinstance(worker, bytes) else worker}")
                        print(f"     - Pool: {stats_data.get('pool', {}).get('max-concurrency', 'N/A')}")
                        print(f"     - Processed: {stats_data.get('total', {}).get('total', 'N/A')}")
                    except:
                        print(f"   Worker: {worker} (stats parsing failed)")
            else:
                print("❌ No Celery workers detected")
            
            # Check active tasks
            active_tasks = self.redis_client.smembers('celery:active')
            if active_tasks:
                print(f"📋 Active tasks: {len(active_tasks)}")
                for task in active_tasks:
                    print(f"   - {task.decode('utf-8') if isinstance(task, bytes) else task}")
            else:
                print("📋 No active tasks")
                
        except Exception as e:
            print(f"❌ Error checking Celery status: {e}")
    
    def check_file_status(self):
        """Check uploaded files and their processing status"""
        print("\n" + "="*60)
        print("📁 FILE UPLOAD STATUS")
        print("="*60)
        
        # Get all data files
        data_files = DataFile.objects.all().order_by('-uploaded_at')
        
        if not data_files:
            print("📋 No files uploaded yet")
            return
        
        print(f"📊 Total files uploaded: {len(data_files)}")
        
        # Group by status
        status_counts = defaultdict(int)
        unprocessed_files = []
        processing_files = []
        failed_files = []
        
        for data_file in data_files:
            status_counts[data_file.status] += 1
            
            # Check processing jobs for this file
            processing_jobs = FileProcessingJob.objects.filter(data_file=data_file)
            
            if data_file.status == 'PENDING':
                unprocessed_files.append((data_file, processing_jobs))
            elif data_file.status == 'PROCESSING':
                processing_files.append((data_file, processing_jobs))
            elif data_file.status == 'FAILED':
                failed_files.append((data_file, processing_jobs))
        
        # Display status summary
        print("\n📈 File Status Summary:")
        for status, count in status_counts.items():
            print(f"   {status}: {count}")
        
        # Show unprocessed files
        if unprocessed_files:
            print(f"\n❌ UNPROCESSED FILES ({len(unprocessed_files)}):")
            for data_file, jobs in unprocessed_files:
                print(f"\n   📄 File: {data_file.file_name}")
                print(f"      ID: {data_file.id}")
                print(f"      Size: {data_file.file_size} bytes")
                print(f"      Uploaded: {data_file.uploaded_at}")
                print(f"      Status: {data_file.status}")
                print(f"      Processing Jobs: {jobs.count()}")
                
                for job in jobs:
                    print(f"        - Job {job.id}: {job.status}")
                    if job.error_message:
                        print(f"          Error: {job.error_message}")
        
        # Show processing files
        if processing_files:
            print(f"\n🔄 PROCESSING FILES ({len(processing_files)}):")
            for data_file, jobs in processing_files:
                print(f"\n   📄 File: {data_file.file_name}")
                print(f"      ID: {data_file.id}")
                print(f"      Uploaded: {data_file.uploaded_at}")
                print(f"      Processing Jobs: {jobs.count()}")
                
                for job in jobs:
                    print(f"        - Job {job.id}: {job.status}")
                    if job.started_at:
                        duration = timezone.now() - job.started_at
                        print(f"          Started: {job.started_at} ({duration})")
        
        # Show failed files
        if failed_files:
            print(f"\n💥 FAILED FILES ({len(failed_files)}):")
            for data_file, jobs in failed_files:
                print(f"\n   📄 File: {data_file.file_name}")
                print(f"      ID: {data_file.id}")
                print(f"      Uploaded: {data_file.uploaded_at}")
                
                for job in jobs:
                    print(f"        - Job {job.id}: {job.status}")
                    if job.error_message:
                        print(f"          Error: {job.error_message}")
    
    def check_processing_jobs(self):
        """Check processing jobs status"""
        print("\n" + "="*60)
        print("⚙️  PROCESSING JOBS STATUS")
        print("="*60)
        
        jobs = FileProcessingJob.objects.all().order_by('-created_at')
        
        if not jobs:
            print("📋 No processing jobs found")
            return
        
        print(f"📊 Total processing jobs: {len(jobs)}")
        
        # Group by status
        status_counts = defaultdict(int)
        for job in jobs:
            status_counts[job.status] += 1
        
        print("\n📈 Job Status Summary:")
        for status, count in status_counts.items():
            print(f"   {status}: {count}")
        
        # Show stuck jobs (processing for too long)
        stuck_threshold = timedelta(minutes=30)
        stuck_jobs = []
        
        for job in jobs:
            if job.status == 'PROCESSING' and job.started_at:
                duration = timezone.now() - job.started_at
                if duration > stuck_threshold:
                    stuck_jobs.append((job, duration))
        
        if stuck_jobs:
            print(f"\n⚠️  STUCK JOBS ({len(stuck_jobs)}):")
            for job, duration in stuck_jobs:
                print(f"\n   🔄 Job: {job.id}")
                print(f"      File: {job.data_file.file_name}")
                print(f"      Started: {job.started_at}")
                print(f"      Duration: {duration}")
                print(f"      Run Anomalies: {job.run_anomalies}")
                print(f"      Requested Anomalies: {job.requested_anomalies}")
    
    def check_database_health(self):
        """Check database connectivity and basic health"""
        print("\n" + "="*60)
        print("🗄️  DATABASE HEALTH")
        print("="*60)
        
        try:
            # Test database connection
            from django.db import connection
            with connection.cursor() as cursor:
                cursor.execute("SELECT 1")
                print("✅ Database connection successful")
            
            # Check table counts
            data_file_count = DataFile.objects.count()
            processing_job_count = FileProcessingJob.objects.count()
            posting_count = SAPGLPosting.objects.count()
            
            print(f"📊 Database Statistics:")
            print(f"   DataFile records: {data_file_count}")
            print(f"   FileProcessingJob records: {processing_job_count}")
            print(f"   SAPGLPosting records: {posting_count}")
            
        except Exception as e:
            print(f"❌ Database error: {e}")
    
    def check_temp_files(self):
        """Check for temporary upload files"""
        print("\n" + "="*60)
        print("📂 TEMPORARY FILES")
        print("="*60)
        
        temp_dir = getattr(settings, 'TEMP_DIR', 'temp_uploads')
        
        if os.path.exists(temp_dir):
            files = os.listdir(temp_dir)
            if files:
                print(f"📋 Temporary files found: {len(files)}")
                for file in files[:10]:  # Show first 10
                    file_path = os.path.join(temp_dir, file)
                    size = os.path.getsize(file_path)
                    mtime = datetime.fromtimestamp(os.path.getmtime(file_path))
                    print(f"   {file} ({size} bytes, {mtime})")
                if len(files) > 10:
                    print(f"   ... and {len(files) - 10} more files")
            else:
                print("📋 No temporary files found")
        else:
            print(f"📋 Temporary directory not found: {temp_dir}")
    
    def suggest_fixes(self):
        """Suggest fixes for common issues"""
        print("\n" + "="*60)
        print("🔧 SUGGESTED FIXES")
        print("="*60)
        
        # Check for common issues and suggest fixes
        pending_files = DataFile.objects.filter(status='PENDING').count()
        failed_jobs = FileProcessingJob.objects.filter(status='FAILED').count()
        stuck_jobs = FileProcessingJob.objects.filter(
            status='PROCESSING',
            started_at__lt=timezone.now() - timedelta(minutes=30)
        ).count()
        
        if pending_files > 0:
            print(f"📋 {pending_files} files with PENDING status:")
            print("   → Run: python manage.py process_queued_jobs")
            print("   → Or restart Celery workers: docker-compose restart celery_worker")
        
        if failed_jobs > 0:
            print(f"💥 {failed_jobs} failed processing jobs:")
            print("   → Check logs: docker-compose logs celery_worker")
            print("   → Retry failed jobs: python manage.py process_queued_jobs --retry-failed")
        
        if stuck_jobs > 0:
            print(f"⚠️  {stuck_jobs} jobs stuck in processing:")
            print("   → Restart Celery workers: docker-compose restart celery_worker")
            print("   → Check for memory issues or long-running tasks")
        
        if not self.redis_client:
            print("🌿 Redis connection issues:")
            print("   → Start Redis: docker-compose up redis -d")
            print("   → Check Redis logs: docker-compose logs redis")
        
        print("\n🔄 General troubleshooting:")
        print("   → Restart all services: docker-compose down && docker-compose up -d")
        print("   → Check logs: docker-compose logs")
        print("   → Monitor Celery: http://localhost:5555 (Flower)")
    
    def run_comprehensive_check(self):
        """Run all checks"""
        print("🔍 COMPREHENSIVE FILE PROCESSING CHECK")
        print("="*60)
        print(f"Check time: {timezone.now()}")
        
        self.check_docker_services()
        self.check_celery_status()
        self.check_database_health()
        self.check_file_status()
        self.check_processing_jobs()
        self.check_temp_files()
        self.suggest_fixes()
        
        print("\n" + "="*60)
        print("✅ Check completed")
        print("="*60)


def main():
    """Main function"""
    checker = FileProcessingChecker()
    checker.run_comprehensive_check()


if __name__ == "__main__":
    main() 