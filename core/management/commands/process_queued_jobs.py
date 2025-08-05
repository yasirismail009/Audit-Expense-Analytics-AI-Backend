from django.core.management.base import BaseCommand, CommandError
from django.utils import timezone
from core.models import FileProcessingJob, SAPGLPosting
from core.tasks import run_restructured_analysis
import time
import logging

logger = logging.getLogger(__name__)

class Command(BaseCommand):
    help = 'Process queued jobs for file analysis'

    def add_arguments(self, parser):
        parser.add_argument(
            '--all',
            action='store_true',
            help='Process all pending and failed jobs',
        )
        parser.add_argument(
            '--pending',
            action='store_true',
            help='Process only pending jobs',
        )
        parser.add_argument(
            '--failed',
            action='store_true',
            help='Process only failed jobs',
        )
        parser.add_argument(
            '--job-id',
            type=str,
            help='Process specific job by ID',
        )
        parser.add_argument(
            '--sync',
            action='store_true',
            help='Process jobs synchronously (without Celery)',
        )
        parser.add_argument(
            '--dry-run',
            action='store_true',
            help='Show what would be processed without actually processing',
        )

    def handle(self, *args, **options):
        self.stdout.write(
            self.style.SUCCESS('🚀 Starting Job Processing...')
        )
        
        # Determine which jobs to process
        if options['job_id']:
            jobs = FileProcessingJob.objects.filter(id=options['job_id'])
            if not jobs.exists():
                raise CommandError(f'Job with ID {options["job_id"]} not found')
        elif options['all']:
            jobs = FileProcessingJob.objects.filter(
                status__in=['PENDING', 'QUEUED', 'FAILED']
            ).order_by('created_at')
        elif options['pending']:
            jobs = FileProcessingJob.objects.filter(status__in=['PENDING', 'QUEUED']).order_by('created_at')
        elif options['failed']:
            jobs = FileProcessingJob.objects.filter(status='FAILED').order_by('created_at')
        else:
            # Default: process pending and queued jobs
            jobs = FileProcessingJob.objects.filter(status__in=['PENDING', 'QUEUED']).order_by('created_at')
        
        job_count = jobs.count()
        
        if job_count == 0:
            self.stdout.write(
                self.style.WARNING('⚠️  No jobs found to process')
            )
            return
        
        self.stdout.write(
            self.style.SUCCESS(f'📋 Found {job_count} jobs to process')
        )
        
        if options['dry_run']:
            self.stdout.write(
                self.style.WARNING('🔍 DRY RUN MODE - No actual processing will occur')
            )
            for job in jobs:
                self.stdout.write(
                    f'  - Job {job.id}: {job.data_file.file_name} ({job.status})'
                )
            return
        
        # Process jobs
        processed_count = 0
        failed_count = 0
        
        for job in jobs:
            try:
                self.stdout.write(f'\n📋 Processing Job: {job.id}')
                self.stdout.write(f'   File: {job.data_file.file_name}')
                self.stdout.write(f'   Status: {job.status}')
                self.stdout.write(f'   Created: {job.created_at}')
                self.stdout.write(f'   Run Anomalies: {job.run_anomalies}')
                self.stdout.write(f'   Requested Anomalies: {job.requested_anomalies}')
                
                if options['sync']:
                    # Process synchronously
                    success = self._process_job_sync(job)
                else:
                    # Process with Celery
                    success = self._process_job_celery(job)
                
                if success:
                    processed_count += 1
                    self.stdout.write(
                        self.style.SUCCESS(f'   ✅ Job processed successfully')
                    )
                else:
                    failed_count += 1
                    self.stdout.write(
                        self.style.ERROR(f'   ❌ Job processing failed')
                    )
                    
            except Exception as e:
                failed_count += 1
                self.stdout.write(
                    self.style.ERROR(f'   ❌ Error processing job: {e}')
                )
                job.status = 'FAILED'
                job.error_message = str(e)
                job.completed_at = timezone.now()
                job.save()
        
        # Summary
        self.stdout.write(f'\n📊 Processing Summary:')
        self.stdout.write(f'   ✅ Successfully processed: {processed_count}')
        self.stdout.write(f'   ❌ Failed: {failed_count}')
        self.stdout.write(f'   📋 Total: {job_count}')
        
        if processed_count > 0:
            self.stdout.write(
                self.style.SUCCESS(f'\n🎉 Successfully processed {processed_count} jobs!')
            )
        else:
            self.stdout.write(
                self.style.ERROR(f'\n❌ No jobs were processed successfully')
            )

    def _process_job_celery(self, job):
        """Process job using Celery"""
        try:
            # Update job status to PROCESSING
            job.status = 'PROCESSING'
            job.started_at = timezone.now()
            job.save()
            
            self.stdout.write('   🔄 Starting Celery analysis...')
            
            # Trigger the analysis task
            result = run_restructured_analysis.delay(str(job.id))
            
            self.stdout.write(f'   ✅ Celery task submitted: {result.id}')
            
            # Update job status to QUEUED
            job.status = 'QUEUED'
            job.save()
            
            return True
            
        except Exception as e:
            self.stdout.write(f'   ❌ Celery error: {e}')
            job.status = 'FAILED'
            job.error_message = str(e)
            job.completed_at = timezone.now()
            job.save()
            return False

    def _process_job_sync(self, job):
        """Process job synchronously with full analysis pipeline"""
        try:
            # Update job status to PROCESSING
            job.status = 'PROCESSING'
            job.started_at = timezone.now()
            job.save()
            
            self.stdout.write('   🔄 Starting full analysis pipeline...')
            start_time = time.time()
            
            # Get transactions for this file
            transactions = SAPGLPosting.objects.filter(data_file=job.data_file)
            self.stdout.write(f'   📊 Found {len(transactions)} transactions')
            
            # Create a mock task context for the analysis functions
            class MockTask:
                def __init__(self, job_id):
                    self.request = type('Request', (), {'id': f'sync-{job_id}'})()
            
            mock_task = MockTask(job.id)
            
            # Run all analysis tasks synchronously
            analysis_results = {}
            
            # 1. General Analysis
            self.stdout.write('   📈 Running General Analysis...')
            try:
                from core.sync_analysis import run_general_analysis_sync
                general_result = run_general_analysis_sync(str(job.id))
                analysis_results['general_analysis'] = general_result
                self.stdout.write(f'   ✅ General Analysis completed')
            except Exception as e:
                self.stdout.write(f'   ❌ General Analysis failed: {e}')
                analysis_results['general_analysis'] = {'error': str(e)}
            
            # 2. Duplicate Analysis
            self.stdout.write('   🔍 Running Duplicate Analysis...')
            try:
                from core.sync_analysis import run_duplicate_analysis_sync
                duplicate_result = run_duplicate_analysis_sync(str(job.id))
                analysis_results['duplicate_analysis'] = duplicate_result
                self.stdout.write(f'   ✅ Duplicate Analysis completed')
            except Exception as e:
                self.stdout.write(f'   ❌ Duplicate Analysis failed: {e}')
                analysis_results['duplicate_analysis'] = {'error': str(e)}
            
            # 3. Backdated Analysis
            self.stdout.write('   📅 Running Backdated Analysis...')
            try:
                from core.sync_analysis import run_backdated_analysis_sync
                backdated_result = run_backdated_analysis_sync(str(job.id))
                analysis_results['backdated_analysis'] = backdated_result
                self.stdout.write(f'   ✅ Backdated Analysis completed')
            except Exception as e:
                self.stdout.write(f'   ❌ Backdated Analysis failed: {e}')
                analysis_results['backdated_analysis'] = {'error': str(e)}
            
            # 4. Overall Analysis
            self.stdout.write('   📊 Running Overall Analysis...')
            try:
                from core.sync_analysis import run_overall_analysis_sync
                overall_result = run_overall_analysis_sync(str(job.id))
                analysis_results['overall_analysis'] = overall_result
                self.stdout.write(f'   ✅ Overall Analysis completed')
            except Exception as e:
                self.stdout.write(f'   ❌ Overall Analysis failed: {e}')
                analysis_results['overall_analysis'] = {'error': str(e)}
            
            # 5. Risk Analysis
            self.stdout.write('   ⚠️  Running Risk Analysis...')
            try:
                from core.sync_analysis import run_risk_analysis_sync
                risk_result = run_risk_analysis_sync(str(job.id))
                analysis_results['risk_analysis'] = risk_result
                self.stdout.write(f'   ✅ Risk Analysis completed')
            except Exception as e:
                self.stdout.write(f'   ❌ Risk Analysis failed: {e}')
                analysis_results['risk_analysis'] = {'error': str(e)}
            
            # Calculate total processing duration
            total_duration = time.time() - start_time
            
            # Update job with comprehensive results
            job.analytics_results = {
                'summary': {
                    'total_transactions': len(transactions),
                    'total_amount': float(sum(t.amount_local_currency for t in transactions)),
                    'unique_users': len(set(t.user_name for t in transactions)),
                    'unique_accounts': len(set(t.gl_account for t in transactions)),
                    'processed_synchronously': True,
                    'processing_time': total_duration,
                    'analysis_results': analysis_results
                },
                'task_ids': {
                    'general_analysis_task_id': f'sync-{job.id}-general',
                    'duplicate_analysis_task_id': f'sync-{job.id}-duplicate',
                    'backdated_analysis_task_id': f'sync-{job.id}-backdated',
                    'overall_analysis_task_id': f'sync-{job.id}-overall',
                    'risk_analysis_task_id': f'sync-{job.id}-risk',
                },
                'analysis_tables': {
                    'general_analysis': 'GeneralAnalysisResult',
                    'duplicate_analysis': 'DuplicateAnalysisResult',
                    'backdated_analysis': 'BackdatedAnalysisResult',
                    'overall_analysis': 'OverallAnalysisResult',
                    'risk_analysis': 'RiskScoringDocument',
                }
            }
            
            # Update job status
            job.status = 'COMPLETED'
            job.completed_at = timezone.now()
            job.processing_duration = total_duration
            job.save()
            
            self.stdout.write(f'   ✅ Full analysis pipeline completed in {total_duration:.2f} seconds')
            return True
            
        except Exception as e:
            self.stdout.write(f'   ❌ Sync error: {e}')
            job.status = 'FAILED'
            job.error_message = str(e)
            job.completed_at = timezone.now()
            job.save()
            return False 