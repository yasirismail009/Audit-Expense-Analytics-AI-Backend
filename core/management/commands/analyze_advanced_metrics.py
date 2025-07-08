from django.core.management.base import BaseCommand
from django.db import transaction
from core.models import ExpenseSheet, SheetAnalysis
from core.analytics import ExpenseSheetAnalyzer
import json

class Command(BaseCommand):
    help = 'Run advanced expense analytics on all expense sheets'

    def add_arguments(self, parser):
        parser.add_argument(
            '--sheet-id',
            type=int,
            help='Analyze specific expense sheet by ID',
        )
        parser.add_argument(
            '--output',
            type=str,
            help='Output file path for detailed results (JSON)',
        )
        parser.add_argument(
            '--verbose',
            action='store_true',
            help='Show detailed analysis results',
        )

    def handle(self, *args, **options):
        analyzer = ExpenseSheetAnalyzer()
        
        # Get expense sheets to analyze
        if options['sheet_id']:
            try:
                sheets = [ExpenseSheet.objects.get(id=options['sheet_id'])]
                self.stdout.write(f"Analyzing specific sheet: {sheets[0].display_name}")
            except ExpenseSheet.DoesNotExist:
                self.stdout.write(self.style.ERROR(f"Expense sheet with ID {options['sheet_id']} not found"))
                return
        else:
            sheets = ExpenseSheet.objects.all()
            self.stdout.write(f"Analyzing {sheets.count()} expense sheets")

        results = []
        
        for sheet in sheets:
            self.stdout.write(f"\nAnalyzing: {sheet.display_name}")
            
            try:
                # Run analysis
                sheet_analysis = analyzer.analyze_sheet(sheet)
                
                if sheet_analysis:
                    # Get the advanced metrics from the analysis
                    advanced_metrics = getattr(sheet_analysis, 'expense_velocity_ratio', None)
                    
                    if advanced_metrics is not None:
                        # Extract advanced metrics from the analysis
                        basic_metrics = getattr(sheet_analysis, 'basic_metrics', {})
                        risk_indicators = getattr(sheet_analysis, 'risk_indicators', {})
                        
                        result = {
                            'sheet_id': sheet.id,
                            'sheet_name': sheet.display_name,
                            'basic_metrics': basic_metrics,
                            'expense_velocity_ratio': getattr(sheet_analysis, 'expense_velocity_ratio', 0),
                            'approval_concentration_index': getattr(sheet_analysis, 'approval_concentration_index', 0),
                            'payment_method_risk_score': getattr(sheet_analysis, 'payment_method_risk_score', 0),
                            'vendor_concentration_ratio': getattr(sheet_analysis, 'vendor_concentration_ratio', 0),
                            'high_value_expense_frequency': getattr(sheet_analysis, 'high_value_expense_frequency', {}),
                            'risk_indicators': risk_indicators,
                            'overall_fraud_score': getattr(sheet_analysis, 'overall_fraud_score', 0),
                            'risk_level': getattr(sheet_analysis, 'risk_level', 'UNKNOWN')
                        }
                        
                        results.append(result)
                        
                        if options['verbose']:
                            self._print_detailed_results(result)
                        else:
                            self._print_summary_results(result)
                    else:
                        self.stdout.write(self.style.WARNING("No advanced metrics found in analysis"))
                else:
                    self.stdout.write(self.style.WARNING("Analysis returned no results"))
                    
            except Exception as e:
                self.stdout.write(self.style.ERROR(f"Error analyzing sheet {sheet.id}: {str(e)}"))
                continue
        
        # Save results to file if requested
        if options['output']:
            try:
                with open(options['output'], 'w') as f:
                    json.dump(results, f, indent=2, default=str)
                self.stdout.write(self.style.SUCCESS(f"Results saved to {options['output']}"))
            except Exception as e:
                self.stdout.write(self.style.ERROR(f"Error saving results: {str(e)}"))
        
        # Print overall summary
        if results:
            self._print_overall_summary(results)
        else:
            self.stdout.write(self.style.WARNING("No analysis results to display"))

    def _print_summary_results(self, result):
        """Print summary of analysis results"""
        self.stdout.write(f"  Risk Level: {result['risk_level']}")
        self.stdout.write(f"  Fraud Score: {result['overall_fraud_score']:.1f}")
        self.stdout.write(f"  EVR: ${result['expense_velocity_ratio']:.2f}/day")
        self.stdout.write(f"  ACI: {result['approval_concentration_index']:.1f}%")
        self.stdout.write(f"  PMRS: {result['payment_method_risk_score']:.1f}%")
        self.stdout.write(f"  VCR: {result['vendor_concentration_ratio']:.1f}%")

    def _print_detailed_results(self, result):
        """Print detailed analysis results"""
        self.stdout.write("\n" + "="*50)
        self.stdout.write(f"Detailed Analysis: {result['sheet_name']}")
        self.stdout.write("="*50)
        
        # Basic metrics
        basic = result['basic_metrics']
        self.stdout.write(f"\n📊 BASIC METRICS:")
        self.stdout.write(f"   Total Expenses: {basic.get('total_expenses', 0)}")
        self.stdout.write(f"   Total Amount: ${basic.get('total_amount', 0):,.2f}")
        self.stdout.write(f"   Average Expense: ${basic.get('average_expense', 0):.2f}")
        self.stdout.write(f"   Date Range: {basic.get('date_range_days', 0)} days")
        
        # Key ratios
        self.stdout.write(f"\n📈 KEY RATIOS:")
        self.stdout.write(f"   Expense Velocity Ratio: ${result['expense_velocity_ratio']:.2f}/day")
        self.stdout.write(f"   Approval Concentration: {result['approval_concentration_index']:.1f}%")
        self.stdout.write(f"   Payment Method Risk: {result['payment_method_risk_score']:.1f}%")
        self.stdout.write(f"   Vendor Concentration: {result['vendor_concentration_ratio']:.1f}%")
        
        # High-value expenses
        hvef = result['high_value_expense_frequency']
        self.stdout.write(f"\n💰 HIGH-VALUE EXPENSES:")
        self.stdout.write(f"   Frequency: {hvef.get('percentage', 0):.1f}%")
        self.stdout.write(f"   Threshold: ${hvef.get('threshold', 0):.2f}")
        self.stdout.write(f"   Count: {hvef.get('count', 0)} out of {hvef.get('total_count', 0)}")
        
        # Risk indicators
        risks = result['risk_indicators']
        self.stdout.write(f"\n⚠️  RISK INDICATORS:")
        self.stdout.write(f"   High ACI: {'YES' if risks.get('high_aci_warning') else 'NO'}")
        self.stdout.write(f"   High PMRS: {'YES' if risks.get('high_pmrs_warning') else 'NO'}")
        self.stdout.write(f"   High VCR: {'YES' if risks.get('high_vcr_warning') else 'NO'}")
        self.stdout.write(f"   High HVEF: {'YES' if risks.get('high_hvef_warning') else 'NO'}")
        self.stdout.write(f"   Complex Expenses: {risks.get('complex_expenses', 0)}")
        
        # Overall assessment
        self.stdout.write(f"\n🎯 OVERALL ASSESSMENT:")
        self.stdout.write(f"   Risk Level: {result['risk_level']}")
        self.stdout.write(f"   Fraud Score: {result['overall_fraud_score']:.1f}/100")
        
        self.stdout.write("\n" + "="*50 + "\n")

    def _print_overall_summary(self, results):
        """Print overall summary of all analyses"""
        self.stdout.write("\n" + "="*60)
        self.stdout.write("OVERALL SUMMARY")
        self.stdout.write("="*60)
        
        total_sheets = len(results)
        high_risk_sheets = len([r for r in results if r['risk_level'] in ['HIGH', 'CRITICAL']])
        avg_fraud_score = sum(r['overall_fraud_score'] for r in results) / total_sheets
        avg_evr = sum(r['expense_velocity_ratio'] for r in results) / total_sheets
        avg_aci = sum(r['approval_concentration_index'] for r in results) / total_sheets
        
        self.stdout.write(f"\n📋 ANALYZED SHEETS: {total_sheets}")
        self.stdout.write(f"⚠️  HIGH RISK SHEETS: {high_risk_sheets} ({high_risk_sheets/total_sheets*100:.1f}%)")
        self.stdout.write(f"📊 AVERAGE FRAUD SCORE: {avg_fraud_score:.1f}/100")
        self.stdout.write(f"💰 AVERAGE EVR: ${avg_evr:.2f}/day")
        self.stdout.write(f"👤 AVERAGE ACI: {avg_aci:.1f}%")
        
        # Risk level distribution
        risk_levels = {}
        for result in results:
            level = result['risk_level']
            risk_levels[level] = risk_levels.get(level, 0) + 1
        
        self.stdout.write(f"\n🎯 RISK LEVEL DISTRIBUTION:")
        for level, count in sorted(risk_levels.items()):
            percentage = count / total_sheets * 100
            self.stdout.write(f"   {level}: {count} sheets ({percentage:.1f}%)")
        
        self.stdout.write("="*60 + "\n") 