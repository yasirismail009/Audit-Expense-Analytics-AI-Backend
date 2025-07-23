from django.core.management.base import BaseCommand
from core.tasks import process_queued_jobs
from core.models import FileProcessingJob
from django.utils import timezone

class Command(BaseCommand):
    help = 'Process queued jobs manually'

    def add_arguments(self, parser):
        parser.add_argument(
            '--job-id',
            type=str,
            help='Process specific job by ID',
        )
        parser.add_argument(
            '--all',
            action='store_true',
            help='Process all queued jobs',
        )
        parser.add_argument(
            '--dry-run',
            action='store_true',
            help='Show what would be processed without actually processing',
        )

    def handle(self, *args, **options):
        job_id = options.get('job_id')
        process_all = options.get('all')
        dry_run = options.get('dry_run')

        if dry_run:
            self.stdout.write(self.style.WARNING('DRY RUN MODE - No jobs will be processed'))

        if job_id:
            # Process specific job
            try:
                job = FileProcessingJob.objects.get(id=job_id)
                self.stdout.write(f"Found job {job_id}: {job.data_file.file_name if job.data_file else 'No file'}")
                
                if job.status == 'QUEUED':
                    if not dry_run:
                        self.stdout.write(f"Processing job {job_id}...")
                        process_queued_jobs()
                        self.stdout.write(self.style.SUCCESS(f"Job {job_id} processed"))
                    else:
                        self.stdout.write(f"Would process job {job_id}")
                else:
                    self.stdout.write(self.style.WARNING(f"Job {job_id} is not queued (status: {job.status})"))
                    
            except FileProcessingJob.DoesNotExist:
                self.stdout.write(self.style.ERROR(f"Job {job_id} not found"))
                
        elif process_all:
            # Process all queued jobs
            queued_jobs = FileProcessingJob.objects.filter(status='QUEUED')
            count = queued_jobs.count()
            
            self.stdout.write(f"Found {count} queued jobs")
            
            if count > 0:
                if not dry_run:
                    self.stdout.write("Processing all queued jobs...")
                    process_queued_jobs()
                    self.stdout.write(self.style.SUCCESS(f"Processed {count} jobs"))
                else:
                    self.stdout.write(f"Would process {count} jobs")
            else:
                self.stdout.write("No queued jobs found")
                
        else:
            # Show status
            total_jobs = FileProcessingJob.objects.count()
            queued_jobs = FileProcessingJob.objects.filter(status='QUEUED').count()
            processing_jobs = FileProcessingJob.objects.filter(status='PROCESSING').count()
            completed_jobs = FileProcessingJob.objects.filter(status='COMPLETED').count()
            failed_jobs = FileProcessingJob.objects.filter(status='FAILED').count()
            
            self.stdout.write("Job Queue Status:")
            self.stdout.write(f"  Total jobs: {total_jobs}")
            self.stdout.write(f"  Queued: {queued_jobs}")
            self.stdout.write(f"  Processing: {processing_jobs}")
            self.stdout.write(f"  Completed: {completed_jobs}")
            self.stdout.write(f"  Failed: {failed_jobs}")
            
            if queued_jobs > 0:
                self.stdout.write("\nUse --all to process all queued jobs")
                self.stdout.write("Use --job-id <id> to process a specific job")
                self.stdout.write("Use --dry-run to see what would be processed") 