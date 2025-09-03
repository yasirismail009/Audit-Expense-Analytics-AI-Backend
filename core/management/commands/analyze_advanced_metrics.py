from django.core.management.base import BaseCommand
from django.db import transaction
from core.models import DataFile, FileProcessingJob, OverallAnalysisResult, RiskScoringDocument, UserAnalysisResult, DuplicateAnalysisResult, BackdatedAnalysisResult, HolidayAnalysisResult, ClosingEntriesAnalysisResult, UnusualDaysAnalysisResult
from django.utils import timezone
import json

class Command(BaseCommand):
    help = 'Run overall analysis on all data files and generate comprehensive reports'

    def add_arguments(self, parser):
        parser.add_argument(
            '--file-id',
            type=str,
            help='Analyze specific file by ID',
        )
        parser.add_argument(
            '--verbose',
            action='store_true',
            help='Show detailed analysis results',
        )

    def handle(self, *args, **options):
        self.stdout.write("🔍 Starting Overall Analysis...")
        
        # Get files to analyze
        if options['file_id']:
            try:
                files = [DataFile.objects.get(id=options['file_id'])]
                self.stdout.write(f"Analyzing specific file: {files[0].file_name}")
            except DataFile.DoesNotExist:
                self.stdout.write(self.style.ERROR(f"File with ID {options['file_id']} not found"))
                return
        else:
            files = DataFile.objects.filter(status='COMPLETED')
            self.stdout.write(f"Analyzing {files.count()} completed files")

        results = []
        
        for data_file in files:
            self.stdout.write(f"\n📊 Analyzing: {data_file.file_name}")
            
            try:
                # Get all analysis results for this file
                analysis_summary = self._analyze_file(data_file)
                
                if analysis_summary:
                    results.append(analysis_summary)
                    
                    if options['verbose']:
                        self._print_detailed_results(analysis_summary)
                    else:
                        self._print_summary_results(analysis_summary)
                else:
                    self.stdout.write(self.style.WARNING("No analysis results found for this file"))
                    
            except Exception as e:
                self.stdout.write(self.style.ERROR(f"Error analyzing file {data_file.id}: {str(e)}"))
                continue
        
        # Print overall summary
        if results:
            self._print_overall_summary(results)
        else:
            self.stdout.write(self.style.WARNING("No analysis results to display"))

    def _analyze_file(self, data_file):
        """Analyze a single file and return summary"""
        try:
            # Get processing job
            job = FileProcessingJob.objects.filter(data_file=data_file, status='COMPLETED').first()
            if not job:
                return None
            
            # Get various analysis results
            overall_analysis = OverallAnalysisResult.objects.filter(data_file=data_file).first()
            risk_document = RiskScoringDocument.objects.filter(data_file=data_file).first()
            user_analysis = UserAnalysisResult.objects.filter(data_file=data_file).first()
            duplicate_analysis = DuplicateAnalysisResult.objects.filter(data_file=data_file).first()
            backdated_analysis = BackdatedAnalysisResult.objects.filter(data_file=data_file).first()
            holiday_analysis = HolidayAnalysisResult.objects.filter(data_file=data_file).first()
            closing_entries_analysis = ClosingEntriesAnalysisResult.objects.filter(data_file=data_file).first()
            unusual_days_analysis = UnusualDaysAnalysisResult.objects.filter(data_file=data_file).first()
            
            # Calculate summary statistics
            total_transactions = getattr(overall_analysis, 'transaction_summary', {}).get('total_transactions', 0) if overall_analysis else 0
            total_amount = getattr(overall_analysis, 'transaction_summary', {}).get('total_amount', 0) if overall_analysis else 0
            
            # Risk analysis
            risk_level = 'UNKNOWN'
            risk_score = 0.0
            if risk_document:
                risk_score = risk_document.overall_risk_score
                if risk_score >= 80:
                    risk_level = 'CRITICAL'
                elif risk_score >= 60:
                    risk_level = 'HIGH'
                elif risk_score >= 40:
                    risk_level = 'MEDIUM'
                else:
                    risk_level = 'LOW'
            
            # Anomaly counts
            duplicate_count = duplicate_analysis.get_duplicate_count() if duplicate_analysis else 0
            backdated_count = backdated_analysis.get_backdated_count() if backdated_analysis else 0
            holiday_count = len(holiday_analysis.holiday_postings) if holiday_analysis and holiday_analysis.holiday_postings else 0
            user_anomalies = len(user_analysis.user_anomalies) if user_analysis and user_analysis.user_anomalies else 0
            unusual_days_count = unusual_days_analysis.get_weekend_transactions_count() if unusual_days_analysis else 0
            closing_entries_count = closing_entries_analysis.get_closing_entries_count() if closing_entries_analysis else 0
            
            # Calculate total anomalies
            total_anomalies = duplicate_count + backdated_count + holiday_count + user_anomalies + unusual_days_count + closing_entries_count
            
            return {
                'file_id': str(data_file.id),
                'file_name': data_file.file_name,
                'total_transactions': total_transactions,
                'total_amount': total_amount,
                'risk_level': risk_level,
                'risk_score': risk_score,
                'duplicate_count': duplicate_count,
                'backdated_count': backdated_count,
                'holiday_count': holiday_count,
                'holiday_breakdown': holiday_analysis.analysis_summary.get('holiday_breakdown', []) if holiday_analysis else [],
                'user_anomalies': user_anomalies,
                'unusual_days_count': unusual_days_count,
                'closing_entries_count': closing_entries_count,
                'total_anomalies': total_anomalies,
                'anomaly_percentage': (total_anomalies / total_transactions * 100) if total_transactions > 0 else 0,
                'has_overall_analysis': overall_analysis is not None,
                'has_risk_analysis': risk_document is not None,
                'has_user_analysis': user_analysis is not None,
                'has_duplicate_analysis': duplicate_analysis is not None,
                'has_backdated_analysis': backdated_analysis is not None,
                'has_holiday_analysis': holiday_analysis is not None,
                'has_closing_entries_analysis': closing_entries_analysis is not None,
                'has_unusual_days_analysis': unusual_days_analysis is not None,
            }
            
        except Exception as e:
            self.stdout.write(self.style.ERROR(f"Error in _analyze_file: {str(e)}"))
            return None

    def _print_summary_results(self, result):
        """Print summary of analysis results"""
        self.stdout.write(f"  📊 Transactions: {result['total_transactions']:,}")
        self.stdout.write(f"  💰 Total Amount: ${result['total_amount']:,.2f}")
        self.stdout.write(f"  ⚠️  Risk Level: {result['risk_level']} ({result['risk_score']:.1f})")
        self.stdout.write(f"  🚨 Anomalies: {result['total_anomalies']} ({result['anomaly_percentage']:.1f}%)")

    def _print_detailed_results(self, result):
        """Print detailed analysis results"""
        self.stdout.write("\n" + "="*60)
        self.stdout.write(f"Detailed Analysis: {result['file_name']}")
        self.stdout.write("="*60)
        
        # Basic metrics
        self.stdout.write(f"\n📊 BASIC METRICS:")
        self.stdout.write(f"   Total Transactions: {result['total_transactions']:,}")
        self.stdout.write(f"   Total Amount: ${result['total_amount']:,.2f}")
        self.stdout.write(f"   Average Transaction: ${result['total_amount']/result['total_transactions']:,.2f}" if result['total_transactions'] > 0 else "   Average Transaction: N/A")
        
        # Risk analysis
        self.stdout.write(f"\n⚠️  RISK ANALYSIS:")
        self.stdout.write(f"   Risk Level: {result['risk_level']}")
        self.stdout.write(f"   Risk Score: {result['risk_score']:.1f}/100")
        
        # Anomaly breakdown
        self.stdout.write(f"\n🚨 ANOMALY BREAKDOWN:")
        self.stdout.write(f"   Duplicate Entries: {result['duplicate_count']}")
        self.stdout.write(f"   Backdated Entries: {result['backdated_count']}")
        self.stdout.write(f"   Holiday Postings: {result['holiday_count']}")
        
        # Show holiday breakdown if available
        if result.get('holiday_breakdown'):
            self.stdout.write(f"   📅 Holiday Details:")
            for holiday_name, count in result['holiday_breakdown']:
                self.stdout.write(f"      • {holiday_name}: {count} transactions")
        
        self.stdout.write(f"   User Anomalies: {result['user_anomalies']}")
        self.stdout.write(f"   Unusual Days (Weekend): {result.get('unusual_days_count', 0)}")
        self.stdout.write(f"   Closing Entries: {result.get('closing_entries_count', 0)}")
        self.stdout.write(f"   Total Anomalies: {result['total_anomalies']} ({result['anomaly_percentage']:.1f}%)")
        
        # Analysis coverage
        self.stdout.write(f"\n📋 ANALYSIS COVERAGE:")
        analyses = [
            ('Overall Analysis', result['has_overall_analysis']),
            ('Risk Analysis', result['has_risk_analysis']),
            ('User Analysis', result['has_user_analysis']),
            ('Duplicate Analysis', result['has_duplicate_analysis']),
            ('Backdated Analysis', result['has_backdated_analysis']),
            ('Holiday Analysis', result['has_holiday_analysis']),
            ('Closing Entries Analysis', result['has_closing_entries_analysis']),
            ('Unusual Days Analysis', result['has_unusual_days_analysis']),
        ]
        
        for analysis_name, has_analysis in analyses:
            status = "✅" if has_analysis else "❌"
            self.stdout.write(f"   {status} {analysis_name}")
        
        self.stdout.write("="*60 + "\n")

    def _print_overall_summary(self, results):
        """Print overall summary of all analyses"""
        self.stdout.write("\n" + "="*70)
        self.stdout.write("OVERALL SUMMARY")
        self.stdout.write("="*70)
        
        total_files = len(results)
        total_transactions = sum(r['total_transactions'] for r in results)
        total_amount = sum(r['total_amount'] for r in results)
        total_anomalies = sum(r['total_anomalies'] for r in results)
        
        # Risk level distribution
        risk_levels = {}
        for result in results:
            level = result['risk_level']
            risk_levels[level] = risk_levels.get(level, 0) + 1
        
        # High risk files
        high_risk_files = len([r for r in results if r['risk_level'] in ['HIGH', 'CRITICAL']])
        
        self.stdout.write(f"\n📋 ANALYZED FILES: {total_files}")
        self.stdout.write(f"📊 TOTAL TRANSACTIONS: {total_transactions:,}")
        self.stdout.write(f"💰 TOTAL AMOUNT: ${total_amount:,.2f}")
        self.stdout.write(f"🚨 TOTAL ANOMALIES: {total_anomalies:,}")
        self.stdout.write(f"⚠️  HIGH RISK FILES: {high_risk_files} ({high_risk_files/total_files*100:.1f}%)")
        
        # Risk level distribution
        self.stdout.write(f"\n🎯 RISK LEVEL DISTRIBUTION:")
        for level in ['LOW', 'MEDIUM', 'HIGH', 'CRITICAL']:
            count = risk_levels.get(level, 0)
            percentage = count / total_files * 100 if total_files > 0 else 0
            self.stdout.write(f"   {level}: {count} files ({percentage:.1f}%)")
        
        # Average metrics
        avg_risk_score = sum(r['risk_score'] for r in results) / total_files if total_files > 0 else 0
        avg_anomaly_percentage = sum(r['anomaly_percentage'] for r in results) / total_files if total_files > 0 else 0
        
        self.stdout.write(f"\n📈 AVERAGE METRICS:")
        self.stdout.write(f"   Average Risk Score: {avg_risk_score:.1f}/100")
        self.stdout.write(f"   Average Anomaly Rate: {avg_anomaly_percentage:.1f}%")
        
        self.stdout.write("="*70 + "\n") 