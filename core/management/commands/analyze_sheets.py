from django.core.management.base import BaseCommand
from core.models import ExpenseSheet, SheetAnalysis
from core.analytics import ExpenseSheetAnalyzer

class Command(BaseCommand):
    help = 'Analyze expense sheets for fraud detection and train models'

    def add_arguments(self, parser):
        parser.add_argument(
            '--train',
            action='store_true',
            help='Train models on existing data',
        )
        parser.add_argument(
            '--analyze-all',
            action='store_true',
            help='Analyze all expense sheets',
        )
        parser.add_argument(
            '--sheet-id',
            type=int,
            help='Analyze specific sheet by ID',
        )

    def handle(self, *args, **options):
        analyzer = ExpenseSheetAnalyzer()
        
        if options['train']:
            self.stdout.write('Training models...')
            success = analyzer.train_models()
            if success:
                self.stdout.write(
                    self.style.SUCCESS('Models trained successfully!')
                )
            else:
                self.stdout.write(
                    self.style.ERROR('Model training failed - insufficient data')
                )
        
        if options['analyze_all']:
            self.stdout.write('Analyzing all expense sheets...')
            sheets = ExpenseSheet.objects.all()
            
            for sheet in sheets:
                self.stdout.write(f'Analyzing sheet: {sheet.display_name}')
                try:
                    sheet_analysis = analyzer.analyze_sheet(sheet)
                    if sheet_analysis:
                        self.stdout.write(
                            self.style.SUCCESS(
                                f'✓ Sheet analyzed - Score: {sheet_analysis.overall_fraud_score:.1f}, '
                                f'Risk: {sheet_analysis.risk_level}, '
                                f'Flagged: {sheet_analysis.total_flagged_expenses}'
                            )
                        )
                    else:
                        self.stdout.write(
                            self.style.WARNING(f'⚠ No analysis - insufficient data')
                        )
                except Exception as e:
                    self.stdout.write(
                        self.style.ERROR(f'✗ Error analyzing sheet: {e}')
                    )
        
        if options['sheet_id']:
            try:
                sheet = ExpenseSheet.objects.get(id=options['sheet_id'])
                self.stdout.write(f'Analyzing sheet: {sheet.display_name}')
                
                sheet_analysis = analyzer.analyze_sheet(sheet)
                if sheet_analysis:
                    self.stdout.write(
                        self.style.SUCCESS(
                            f'Analysis completed!\n'
                            f'Overall Fraud Score: {sheet_analysis.overall_fraud_score:.1f}\n'
                            f'Risk Level: {sheet_analysis.risk_level}\n'
                            f'Total Flagged Expenses: {sheet_analysis.total_flagged_expenses}\n'
                            f'High Risk Expenses: {sheet_analysis.high_risk_expenses}\n'
                            f'Critical Risk Expenses: {sheet_analysis.critical_risk_expenses}\n'
                            f'Flag Rate: {sheet_analysis.flag_rate:.1f}%'
                        )
                    )
                else:
                    self.stdout.write(
                        self.style.WARNING('Analysis failed - insufficient data')
                    )
            except ExpenseSheet.DoesNotExist:
                self.stdout.write(
                    self.style.ERROR(f'Sheet with ID {options["sheet_id"]} not found')
                )
            except Exception as e:
                self.stdout.write(
                    self.style.ERROR(f'Error: {e}')
                )
        
        if not any([options['train'], options['analyze_all'], options['sheet_id']]):
            self.stdout.write('No action specified. Use --help for options.')
            self.stdout.write('Available options:')
            self.stdout.write('  --train        Train models on existing data')
            self.stdout.write('  --analyze-all   Analyze all expense sheets')
            self.stdout.write('  --sheet-id ID   Analyze specific sheet by ID') 