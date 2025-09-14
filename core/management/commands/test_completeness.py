from django.core.management.base import BaseCommand
from core.models import DataFile
from core.tasks import run_gl_completeness_analysis

class Command(BaseCommand):
    help = 'Run completeness test directly'

    def handle(self, *args, **options):
        self.stdout.write('🧪 Direct Completeness Test')
        self.stdout.write('=' * 40)
        
        try:
            # Get first available file
            file = DataFile.objects.first()
            if not file:
                self.stdout.write(self.style.ERROR('❌ No data files found in database'))
                return
                
            self.stdout.write(f'📁 Using file: {file.id} - {file.file_name}')
            
            # Run the completeness analysis directly
            self.stdout.write('🔄 Running completeness analysis...')
            result = run_gl_completeness_analysis(file.id)
            
            self.stdout.write(self.style.SUCCESS('✅ Analysis completed!'))
            self.stdout.write(f'📊 Result: {result}')
            
        except Exception as e:
            self.stdout.write(self.style.ERROR(f'❌ Error: {e}'))
            import traceback
            self.stdout.write(traceback.format_exc())
