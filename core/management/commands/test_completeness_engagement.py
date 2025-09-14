from django.core.management.base import BaseCommand
from core.models import DataFile, SAPGLPosting, TrialBalance
from core.tasks import run_gl_completeness_analysis

class Command(BaseCommand):
    help = 'Run completeness test for a specific engagement'

    def add_arguments(self, parser):
        parser.add_argument(
            '--engagement-id',
            type=str,
            help='Engagement ID to test (if not provided, will list available engagements)',
        )

    def handle(self, *args, **options):
        self.stdout.write('🧪 Engagement-Based Completeness Test')
        self.stdout.write('=' * 50)
        
        try:
            # Get engagement ID
            engagement_id = options.get('engagement_id')
            
            if not engagement_id:
                # List available engagements
                self.stdout.write('📋 Available Engagements:')
                engagements = DataFile.objects.values_list('engagement_id', flat=True).distinct()
                for eng in engagements:
                    if eng:  # Skip None/empty engagement IDs
                        gl_count = SAPGLPosting.objects.filter(data_file__engagement_id=eng).count()
                        tb_count = TrialBalance.objects.filter(data_file__engagement_id=eng).count()
                        self.stdout.write(f'  - {eng}: {gl_count} GL records, {tb_count} TB records')
                return
            
            # Check if engagement exists
            if not DataFile.objects.filter(engagement_id=engagement_id).exists():
                self.stdout.write(self.style.ERROR(f'❌ Engagement {engagement_id} not found'))
                return
            
            # Get GL and TB data for this engagement
            gl_count = SAPGLPosting.objects.filter(data_file__engagement_id=engagement_id).count()
            tb_count = TrialBalance.objects.filter(data_file__engagement_id=engagement_id).count()
            
            self.stdout.write(f'📊 Engagement: {engagement_id}')
            self.stdout.write(f'   GL Records: {gl_count:,}')
            self.stdout.write(f'   TB Records: {tb_count:,}')
            
            if gl_count == 0:
                self.stdout.write(self.style.ERROR('❌ No GL data found for this engagement'))
                return
                
            if tb_count == 0:
                self.stdout.write(self.style.WARNING('⚠️  No TB data found for this engagement'))
                self.stdout.write('   Completeness test will run with GL-only validation')
            
            # Find a GL data file for this engagement
            gl_file = DataFile.objects.filter(engagement_id=engagement_id).first()
            if not gl_file:
                self.stdout.write(self.style.ERROR('❌ No data file found for this engagement'))
                return
            
            self.stdout.write(f'📁 Using GL file: {gl_file.file_name}')
            
            # Run the completeness analysis
            self.stdout.write('🔄 Running completeness analysis...')
            result = run_gl_completeness_analysis(gl_file.id)
            
            self.stdout.write(self.style.SUCCESS('✅ Analysis completed!'))
            self.stdout.write(f'📊 Result: {result}')
            
        except Exception as e:
            self.stdout.write(self.style.ERROR(f'❌ Error: {e}'))
            import traceback
            self.stdout.write(traceback.format_exc())
