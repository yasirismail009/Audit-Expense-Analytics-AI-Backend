from django.core.management.base import BaseCommand
from core.models import HolidayAnalysisResult

class Command(BaseCommand):
    help = 'Display detailed holiday analysis information'

    def handle(self, *args, **options):
        self.stdout.write("🎄 HOLIDAY ANALYSIS DETAILS")
        self.stdout.write("=" * 50)
        
        holiday_analysis = HolidayAnalysisResult.objects.first()
        if not holiday_analysis:
            self.stdout.write(self.style.ERROR("No holiday analysis found"))
            return
        
        # Basic statistics
        self.stdout.write(f"\n📊 BASIC STATISTICS:")
        self.stdout.write(f"   Total Holiday Transactions: {holiday_analysis.analysis_info.get('holiday_transactions_count', 0)}")
        self.stdout.write(f"   Total Holiday Amount: ${holiday_analysis.analysis_info.get('total_holiday_amount', 0):,.2f}")
        
        # Holiday breakdown
        holiday_breakdown = holiday_analysis.analysis_info.get('holiday_breakdown', [])
        if holiday_breakdown:
            self.stdout.write(f"\n🎁 HOLIDAY BREAKDOWN:")
            for holiday_name, count in holiday_breakdown:
                self.stdout.write(f"   {holiday_name}: {count} transactions")
        
        # Sample transactions
        self.stdout.write(f"\n📋 SAMPLE HOLIDAY TRANSACTIONS:")
        sample_transactions = holiday_analysis.holiday_postings[:10] if holiday_analysis.holiday_postings else []
        for i, transaction in enumerate(sample_transactions, 1):
            holiday_name = transaction.get('holiday_name', 'Unknown')
            posting_date = transaction.get('posting_date', 'Unknown')
            amount = transaction.get('amount', 0)
            user = transaction.get('user', 'Unknown')
            account = transaction.get('account', 'Unknown')
            
            self.stdout.write(f"   {i}. {holiday_name} ({posting_date})")
            self.stdout.write(f"      Amount: ${amount:,.2f}")
            self.stdout.write(f"      User: {user}")
            self.stdout.write(f"      Account: {account}")
            self.stdout.write("")
        
        # Chart data summary
        chart_data = holiday_analysis.chart_data
        if chart_data:
            self.stdout.write(f"\n📈 CHART DATA AVAILABLE:")
            for chart_name in chart_data.keys():
                self.stdout.write(f"   ✅ {chart_name}")
        
        # Risk assessment
        self.stdout.write(f"\n⚠️  RISK ASSESSMENT:")
        self.stdout.write(f"   Risk Level: {holiday_analysis.analysis_info.get('compliance_assessment', {}).get('risk_level', 'Unknown')}")
        self.stdout.write(f"   Holiday Percentage: {holiday_analysis.analysis_info.get('compliance_assessment', {}).get('holiday_percentage', 0):.2f}%")
        
        self.stdout.write("\n" + "=" * 50) 