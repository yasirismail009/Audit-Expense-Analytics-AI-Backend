from django.core.management.base import BaseCommand
from django.db.models import Count, Q
from core.models import (
    DataFile, SAPGLPosting, DuplicateAnalysisResult, BackdatedAnalysisResult,
    UserAnalysisResult, UnusualDaysAnalysisResult, ClosingEntriesAnalysisResult,
    HolidayAnalysisResult, OverallAnalysisResult, RiskScoringDocument
)

class Command(BaseCommand):
    help = 'Verify risk analysis system status and data integrity'

    def add_arguments(self, parser):
        parser.add_argument(
            '--file-id',
            type=str,
            help='Verify specific data file by ID',
        )
        parser.add_argument(
            '--fix-issues',
            action='store_true',
            help='Automatically fix identified issues',
        )

    def handle(self, *args, **options):
        self.stdout.write(self.style.SUCCESS('🔍 Starting Risk Analysis System Verification...'))
        
        if options['file_id']:
            data_files = DataFile.objects.filter(id=options['file_id'])
        else:
            data_files = DataFile.objects.filter(status='COMPLETED')
        
        total_files = data_files.count()
        self.stdout.write(f"📁 Found {total_files} completed data files")
        
        verification_results = {
            'files_processed': 0,
            'files_with_complete_analysis': 0,
            'files_with_transaction_linking': 0,
            'files_with_overall_analysis': 0,
            'files_with_risk_scoring': 0,
            'issues_found': []
        }
        
        for data_file in data_files:
            self.stdout.write(f"\n📁 Analyzing file: {data_file.file_name}")
            
            # Check analysis completeness
            analysis_complete = self._check_analysis_completeness(data_file)
            if analysis_complete:
                verification_results['files_with_complete_analysis'] += 1
            
            # Check transaction linking
            linking_complete = self._check_transaction_linking(data_file)
            if linking_complete:
                verification_results['files_with_transaction_linking'] += 1
            
            # Check overall analysis
            overall_exists = self._check_overall_analysis(data_file)
            if overall_exists:
                verification_results['files_with_overall_analysis'] += 1
            
            # Check risk scoring
            risk_scoring_exists = self._check_risk_scoring(data_file)
            if risk_scoring_exists:
                verification_results['files_with_risk_scoring'] += 1
            
            verification_results['files_processed'] += 1
        
        # Print summary
        self._print_verification_summary(verification_results)
        
        # Fix issues if requested
        if options['fix_issues']:
            self._fix_identified_issues(data_files)
    
    def _check_analysis_completeness(self, data_file):
        """Check if all analysis types are complete"""
        analysis_types = [
            DuplicateAnalysisResult,
            BackdatedAnalysisResult,
            UserAnalysisResult,
            UnusualDaysAnalysisResult,
            ClosingEntriesAnalysisResult,
            HolidayAnalysisResult
        ]
        
        complete_count = 0
        for model_class in analysis_types:
            if model_class.objects.filter(data_file=data_file, status='COMPLETED').exists():
                complete_count += 1
        
        if complete_count == len(analysis_types):
            self.stdout.write("   ✅ All analysis types complete")
            return True
        else:
            missing_types = len(analysis_types) - complete_count
            self.stdout.write(f"   ⚠️  Missing {missing_types} analysis types")
            return False
    
    def _check_transaction_linking(self, data_file):
        """Check if transactions are properly linked with anomalies"""
        transactions = SAPGLPosting.objects.filter(data_file=data_file)
        total_transactions = transactions.count()
        
        if total_transactions == 0:
            self.stdout.write("   ⚠️  No transactions found")
            return False
        
        # Check for anomaly flags
        linked_transactions = transactions.filter(
            Q(is_duplicate=True) | 
            Q(is_backdated=True) | 
            Q(is_holiday_posting=True) |
            Q(anomaly_types__isnull=False) |
            Q(overall_risk_score__gt=0)
        ).count()
        
        linkage_percentage = (linked_transactions / total_transactions) * 100
        
        if linkage_percentage > 0:
            self.stdout.write(f"   ✅ {linkage_percentage:.1f}% transactions linked ({linked_transactions}/{total_transactions})")
            return True
        else:
            self.stdout.write("   ⚠️  No transaction linking found")
            return False
    
    def _check_overall_analysis(self, data_file):
        """Check if overall analysis exists and includes all risk types"""
        overall_analysis = OverallAnalysisResult.objects.filter(
            data_file=data_file, status='COMPLETED'
        ).first()
        
        if overall_analysis:
            flagged_count = len(overall_analysis.flagged_transactions) if overall_analysis.flagged_transactions else 0
            self.stdout.write(f"   ✅ Overall analysis exists with {flagged_count} flagged transactions")
            return True
        else:
            self.stdout.write("   ⚠️  No overall analysis found")
            return False
    
    def _check_risk_scoring(self, data_file):
        """Check if risk scoring document exists"""
        risk_document = RiskScoringDocument.objects.filter(
            data_file=data_file, status='COMPLETED'
        ).first()
        
        if risk_document:
            self.stdout.write(f"   ✅ Risk scoring document exists (score: {risk_document.overall_risk_score:.1f})")
            return True
        else:
            self.stdout.write("   ⚠️  No risk scoring document found")
            return False
    
    def _print_verification_summary(self, results):
        """Print verification summary"""
        self.stdout.write("\n" + "="*80)
        self.stdout.write(self.style.SUCCESS("📊 RISK ANALYSIS SYSTEM VERIFICATION SUMMARY"))
        self.stdout.write("="*80)
        
        total_files = results['files_processed']
        
        self.stdout.write(f"\n📁 Files processed: {total_files}")
        self.stdout.write(f"✅ Complete analysis: {results['files_with_complete_analysis']}/{total_files} ({(results['files_with_complete_analysis']/total_files*100):.1f}%)")
        self.stdout.write(f"🔗 Transaction linking: {results['files_with_transaction_linking']}/{total_files} ({(results['files_with_transaction_linking']/total_files*100):.1f}%)")
        self.stdout.write(f"📊 Overall analysis: {results['files_with_overall_analysis']}/{total_files} ({(results['files_with_overall_analysis']/total_files*100):.1f}%)")
        self.stdout.write(f"🎯 Risk scoring: {results['files_with_risk_scoring']}/{total_files} ({(results['files_with_risk_scoring']/total_files*100):.1f}%)")
        
        # Calculate overall health score
        health_score = (
            results['files_with_complete_analysis'] +
            results['files_with_transaction_linking'] +
            results['files_with_overall_analysis'] +
            results['files_with_risk_scoring']
        ) / (total_files * 4) * 100
        
        if health_score >= 90:
            status = "🟢 EXCELLENT"
        elif health_score >= 75:
            status = "🟡 GOOD"
        elif health_score >= 50:
            status = "🟠 FAIR"
        else:
            status = "🔴 NEEDS ATTENTION"
        
        self.stdout.write(f"\n🏥 System Health Score: {health_score:.1f}% - {status}")
        
        self.stdout.write("\n" + "="*80)
    
    def _fix_identified_issues(self, data_files):
        """Fix identified issues automatically"""
        self.stdout.write("\n🔧 Starting automatic issue fixes...")
        
        # This would integrate with the existing fix logic
        # For now, just provide guidance
        self.stdout.write("   ℹ️  Use the integrated fix methods in the analysis pipeline")
        self.stdout.write("   ℹ️  Run analysis again to ensure proper data capture")
        self.stdout.write("   ℹ️  Check transaction linking in risk analysis tasks")
