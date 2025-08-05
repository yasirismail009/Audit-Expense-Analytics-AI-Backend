from django.core.management.base import BaseCommand
from core.sync_analysis import (
    run_general_analysis_sync,
    run_duplicate_analysis_sync,
    run_backdated_analysis_sync,
    run_user_analysis_sync,
    run_overall_analysis_sync,
    run_risk_analysis_sync,
    run_unusual_days_analysis_sync,
    run_closing_entries_analysis_sync,
    run_holiday_analysis_sync
)
from core.models import FileProcessingJob, DataFile

class Command(BaseCommand):
    help = 'Run all analyses for data files'

    def add_arguments(self, parser):
        parser.add_argument(
            '--file-id',
            type=str,
            help='Run analyses for specific file by ID',
        )
        parser.add_argument(
            '--analysis-type',
            type=str,
            choices=['general', 'duplicate', 'backdated', 'user', 'overall', 'risk', 'unusual_days', 'closing_entries', 'holiday', 'all'],
            default='all',
            help='Type of analysis to run',
        )

    def handle(self, *args, **options):
        file_id = options.get('file_id')
        analysis_type = options.get('analysis_type')
        
        if file_id:
            try:
                data_file = DataFile.objects.get(id=file_id)
                jobs = FileProcessingJob.objects.filter(data_file=data_file, status='COMPLETED')
            except DataFile.DoesNotExist:
                self.stdout.write(self.style.ERROR(f"File with ID {file_id} not found"))
                return
        else:
            jobs = FileProcessingJob.objects.filter(status='COMPLETED')
        
        if not jobs.exists():
            self.stdout.write(self.style.WARNING("No completed processing jobs found"))
            return
        
        self.stdout.write(f"Running {analysis_type} analysis for {jobs.count()} job(s)")
        
        for job in jobs:
            self.stdout.write(f"\n📊 Processing job: {job.id}")
            self.stdout.write(f"   File: {job.data_file.file_name}")
            
            try:
                if analysis_type in ['general', 'all']:
                    self.stdout.write("   🔍 Running General Analysis...")
                    result = run_general_analysis_sync(str(job.id))
                    self.stdout.write(f"   ✅ General Analysis completed: {result}")
                
                if analysis_type in ['duplicate', 'all']:
                    self.stdout.write("   🔍 Running Duplicate Analysis...")
                    result = run_duplicate_analysis_sync(str(job.id))
                    self.stdout.write(f"   ✅ Duplicate Analysis completed: {result}")
                
                if analysis_type in ['backdated', 'all']:
                    self.stdout.write("   🔍 Running Backdated Analysis...")
                    result = run_backdated_analysis_sync(str(job.id))
                    self.stdout.write(f"   ✅ Backdated Analysis completed: {result}")
                
                if analysis_type in ['user', 'all']:
                    self.stdout.write("   🔍 Running User Analysis...")
                    result = run_user_analysis_sync(str(job.id))
                    self.stdout.write(f"   ✅ User Analysis completed: {result}")
                
                if analysis_type in ['overall', 'all']:
                    self.stdout.write("   🔍 Running Overall Analysis...")
                    result = run_overall_analysis_sync(str(job.id))
                    self.stdout.write(f"   ✅ Overall Analysis completed: {result}")
                
                if analysis_type in ['risk', 'all']:
                    self.stdout.write("   🔍 Running Risk Analysis...")
                    result = run_risk_analysis_sync(str(job.id))
                    self.stdout.write(f"   ✅ Risk Analysis completed: {result}")
                
                if analysis_type in ['unusual_days', 'all']:
                    self.stdout.write("   🔍 Running Unusual Days Analysis...")
                    result = run_unusual_days_analysis_sync(str(job.id))
                    self.stdout.write(f"   ✅ Unusual Days Analysis completed: {result}")
                
                if analysis_type in ['closing_entries', 'all']:
                    self.stdout.write("   🔍 Running Closing Entries Analysis...")
                    result = run_closing_entries_analysis_sync(str(job.id))
                    self.stdout.write(f"   ✅ Closing Entries Analysis completed: {result}")
                
                if analysis_type in ['holiday', 'all']:
                    self.stdout.write("   🔍 Running Holiday Analysis...")
                    result = run_holiday_analysis_sync(str(job.id))
                    self.stdout.write(f"   ✅ Holiday Analysis completed: {result}")
                
                self.stdout.write(self.style.SUCCESS(f"   🎉 All analyses completed for job {job.id}"))
                
            except Exception as e:
                self.stdout.write(self.style.ERROR(f"   ❌ Error processing job {job.id}: {str(e)}"))
                continue
        
        self.stdout.write(self.style.SUCCESS("\n🎉 All analyses completed successfully!")) 