from django.core.management.base import BaseCommand
from django.utils import timezone
from datetime import timedelta
from core.analytics import ExpenseSheetAnalyzer
from core.models import ExpenseSheet

class Command(BaseCommand):
    help = 'Scheduled model training based on new data'

    def add_arguments(self, parser):
        parser.add_argument(
            '--force',
            action='store_true',
            help='Force retraining regardless of new data',
        )
        parser.add_argument(
            '--days',
            type=int,
            default=1,
            help='Number of days to look back for new data (default: 1)',
        )

    def handle(self, *args, **options):
        analyzer = ExpenseSheetAnalyzer()
        
        if options['force']:
            self.stdout.write('Force retraining models...')
            success = analyzer.train_models()
            if success:
                self.stdout.write(self.style.SUCCESS('Models retrained successfully!'))
            else:
                self.stdout.write(self.style.ERROR('Model retraining failed'))
            return
        
        # Check for new data in specified time period
        days_back = options['days']
        cutoff_date = timezone.now() - timedelta(days=days_back)
        new_sheets = ExpenseSheet.objects.filter(uploaded_at__gte=cutoff_date)
        
        self.stdout.write(f'Checking for new sheets in last {days_back} day(s)...')
        self.stdout.write(f'Found {new_sheets.count()} new sheets')
        
        if new_sheets.count() >= analyzer.training_config['auto_train_threshold']:
            self.stdout.write('New data detected, retraining models...')
            success = analyzer.train_models()
            if success:
                self.stdout.write(self.style.SUCCESS('Models retrained successfully!'))
                analyzer._last_training_time = timezone.now()
            else:
                self.stdout.write(self.style.ERROR('Model retraining failed'))
        else:
            self.stdout.write('No new data sufficient for retraining')
            self.stdout.write(f'Threshold: {analyzer.training_config["auto_train_threshold"]} sheets')
        
        # Check model performance
        if analyzer.evaluate_model_performance():
            self.stdout.write('Performance check suggests retraining...')
            success = analyzer.train_models()
            if success:
                self.stdout.write(self.style.SUCCESS('Models retrained due to performance issues!'))
            else:
                self.stdout.write(self.style.ERROR('Performance-based retraining failed')) 