from django.core.management.base import BaseCommand
from django.utils import timezone
from django.db.models import Q
from datetime import timedelta
from collections import defaultdict

from core.models import DataFile, FileProcessingJob, SAPGLPosting


class Command(BaseCommand):
    help = 'Check for uploaded files that have not been processed'

    def add_arguments(self, parser):
        parser.add_argument(
            '--detailed',
            action='store_true',
            help='Show detailed information about each file',
        )
        parser.add_argument(
            '--fix',
            action='store_true',
            help='Attempt to fix common issues automatically',
        )
        parser.add_argument(
            '--retry-failed',
            action='store_true',
            help='Retry failed processing jobs',
        )

    def handle(self, *args, **options):
        self.stdout.write(
            self.style.SUCCESS('🔍 Checking for unprocessed files...')
        )
        
        # Get all data files
        data_files = DataFile.objects.all().order_by('-uploaded_at')
        
        if not data_files:
            self.stdout.write('📋 No files uploaded yet')
            return
        
        self.stdout.write(f'📊 Total files uploaded: {len(data_files)}')
        
        # Analyze file status
        status_summary = defaultdict(int)
        unprocessed_files = []
        processing_files = []
        failed_files = []
        stuck_files = []
        
        for data_file in data_files:
            status_summary[data_file.status] += 1
            
            # Get processing jobs for this file
            processing_jobs = FileProcessingJob.objects.filter(data_file=data_file)
            
            # Check if file is stuck (processing for too long)
            is_stuck = False
            if data_file.status == 'PROCESSING':
                for job in processing_jobs.filter(status='PROCESSING'):
                    if job.started_at and (timezone.now() - job.started_at) > timedelta(minutes=30):
                        is_stuck = True
                        break
            
            # Categorize files
            if data_file.status == 'PENDING':
                unprocessed_files.append((data_file, processing_jobs))
            elif data_file.status == 'PROCESSING':
                if is_stuck:
                    stuck_files.append((data_file, processing_jobs))
                else:
                    processing_files.append((data_file, processing_jobs))
            elif data_file.status == 'FAILED':
                failed_files.append((data_file, processing_jobs))
        
        # Display summary
        self.stdout.write('\n📈 File Status Summary:')
        for status, count in status_summary.items():
            self.stdout.write(f'   {status}: {count}')
        
        # Show unprocessed files
        if unprocessed_files:
            self.stdout.write(
                self.style.WARNING(f'\n❌ UNPROCESSED FILES ({len(unprocessed_files)}):')
            )
            for data_file, jobs in unprocessed_files:
                self._display_file_info(data_file, jobs, options['detailed'])
        
        # Show stuck files
        if stuck_files:
            self.stdout.write(
                self.style.ERROR(f'\n⚠️  STUCK FILES ({len(stuck_files)}):')
            )
            for data_file, jobs in stuck_files:
                self._display_file_info(data_file, jobs, options['detailed'])
        
        # Show processing files
        if processing_files:
            self.stdout.write(
                self.style.SUCCESS(f'\n🔄 PROCESSING FILES ({len(processing_files)}):')
            )
            for data_file, jobs in processing_files:
                self._display_file_info(data_file, jobs, options['detailed'])
        
        # Show failed files
        if failed_files:
            self.stdout.write(
                self.style.ERROR(f'\n💥 FAILED FILES ({len(failed_files)}):')
            )
            for data_file, jobs in failed_files:
                self._display_file_info(data_file, jobs, options['detailed'])
        
        # Check processing jobs
        self._check_processing_jobs(options)
        
        # Suggest fixes
        self._suggest_fixes(unprocessed_files, failed_files, stuck_files, options)
        
        self.stdout.write(
            self.style.SUCCESS('\n✅ Check completed')
        )
    
    def _display_file_info(self, data_file, jobs, detailed=False):
        """Display information about a file"""
        self.stdout.write(f'\n   📄 File: {data_file.file_name}')
        self.stdout.write(f'      ID: {data_file.id}')
        self.stdout.write(f'      Size: {data_file.file_size} bytes')
        self.stdout.write(f'      Uploaded: {data_file.uploaded_at}')
        self.stdout.write(f'      Status: {data_file.status}')
        
        if detailed:
            self.stdout.write(f'      Client: {data_file.client_name}')
            self.stdout.write(f'      Company: {data_file.company_name}')
            self.stdout.write(f'      Engagement ID: {data_file.engagement_id}')
            self.stdout.write(f'      Fiscal Year: {data_file.fiscal_year}')
        
        # Show processing jobs
        if jobs.exists():
            self.stdout.write(f'      Processing Jobs: {jobs.count()}')
            for job in jobs:
                self.stdout.write(f'        - Job {job.id}: {job.status}')
                if job.started_at:
                    duration = timezone.now() - job.started_at
                    self.stdout.write(f'          Started: {job.started_at} ({duration})')
                if job.error_message:
                    self.stdout.write(f'          Error: {job.error_message}')
        else:
            self.stdout.write('      Processing Jobs: None')
    
    def _check_processing_jobs(self, options):
        """Check processing jobs status"""
        self.stdout.write('\n⚙️  PROCESSING JOBS STATUS:')
        
        jobs = FileProcessingJob.objects.all().order_by('-created_at')
        
        if not jobs:
            self.stdout.write('   📋 No processing jobs found')
            return
        
        # Group by status
        status_counts = defaultdict(int)
        for job in jobs:
            status_counts[job.status] += 1
        
        self.stdout.write('   📈 Job Status Summary:')
        for status, count in status_counts.items():
            self.stdout.write(f'      {status}: {count}')
        
        # Check for stuck jobs
        stuck_threshold = timedelta(minutes=30)
        stuck_jobs = jobs.filter(
            status='PROCESSING',
            started_at__lt=timezone.now() - stuck_threshold
        )
        
        if stuck_jobs.exists():
            self.stdout.write(
                self.style.ERROR(f'   ⚠️  Stuck jobs: {stuck_jobs.count()}')
            )
            for job in stuck_jobs[:5]:  # Show first 5
                duration = timezone.now() - job.started_at
                self.stdout.write(f'      - Job {job.id}: {duration} (File: {job.data_file.file_name})')
    
    def _suggest_fixes(self, unprocessed_files, failed_files, stuck_files, options):
        """Suggest fixes for common issues"""
        self.stdout.write('\n🔧 SUGGESTED FIXES:')
        
        if unprocessed_files:
            self.stdout.write(f'   📋 {len(unprocessed_files)} unprocessed files:')
            self.stdout.write('      → Run: python manage.py process_queued_jobs')
            self.stdout.write('      → Or restart Celery: docker-compose restart celery_worker')
        
        if failed_files:
            self.stdout.write(f'   💥 {len(failed_files)} failed files:')
            self.stdout.write('      → Check logs: docker-compose logs celery_worker')
            if options['retry_failed']:
                self._retry_failed_jobs(failed_files)
        
        if stuck_files:
            self.stdout.write(f'   ⚠️  {len(stuck_files)} stuck files:')
            self.stdout.write('      → Restart Celery: docker-compose restart celery_worker')
            self.stdout.write('      → Check for memory issues or long-running tasks')
        
        self.stdout.write('\n   🔄 General troubleshooting:')
        self.stdout.write('      → Restart all: docker-compose down && docker-compose up -d')
        self.stdout.write('      → Check logs: docker-compose logs')
        self.stdout.write('      → Monitor Celery: http://localhost:5555 (Flower)')
        
        if options['fix']:
            self._auto_fix_issues(unprocessed_files, failed_files, stuck_files)
    
    def _retry_failed_jobs(self, failed_files):
        """Retry failed processing jobs"""
        self.stdout.write('   🔄 Retrying failed jobs...')
        
        for data_file, jobs in failed_files:
            failed_jobs = jobs.filter(status='FAILED')
            for job in failed_jobs:
                try:
                    # Reset job status
                    job.status = 'PENDING'
                    job.error_message = ''
                    job.started_at = None
                    job.completed_at = None
                    job.save()
                    
                    self.stdout.write(f'      ✅ Reset job {job.id} for file {data_file.file_name}')
                except Exception as e:
                    self.stdout.write(
                        self.style.ERROR(f'      ❌ Failed to reset job {job.id}: {e}')
                    )
    
    def _auto_fix_issues(self, unprocessed_files, failed_files, stuck_files):
        """Automatically fix common issues"""
        self.stdout.write('   🔧 Attempting auto-fixes...')
        
        # Reset stuck jobs
        if stuck_files:
            for data_file, jobs in stuck_files:
                stuck_jobs = jobs.filter(status='PROCESSING')
                for job in stuck_jobs:
                    try:
                        job.status = 'PENDING'
                        job.started_at = None
                        job.save()
                        self.stdout.write(f'      ✅ Reset stuck job {job.id}')
                    except Exception as e:
                        self.stdout.write(
                            self.style.ERROR(f'      ❌ Failed to reset job {job.id}: {e}')
                        )
        
        # Retry failed jobs
        if failed_files:
            self._retry_failed_jobs(failed_files) 